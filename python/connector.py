#!/usr/bin/env python3
"""
chrome-devtools connector -- runs on PC 32 (Windows machine with Chrome).

Dials the relay server on Server 33 via WebSocket (32 -> 33) and forwards
tools/list and tools/call requests to the locally-running chrome-devtools-mcp
process, then sends the result back through the same WebSocket channel.

Network topology
----------------
  relay_server.py (33)
       ^
  WebSocket (32 dials 33)
       |
  connector.py (32, this file)
       | subprocess stdio
  chrome-devtools-mcp  <->  Chrome DevTools (9222)

Usage (on PC 32)
----------------
  # Chrome already open with --remote-debugging-port=9222
  python connector.py --relay-url ws://33-host:7000 --browser-url http://127.0.0.1:9222

  # Auto-connect to a running Chrome instance
  python connector.py --relay-url ws://33-host:7000 --auto-connect

Options
-------
  --relay-url        WebSocket URL of the relay server on 33
  --mcp-cmd          chrome-devtools-mcp executable (default: chrome-devtools-mcp)
  --browser-url      Chrome DevTools HTTP URL, e.g. http://127.0.0.1:9222
  --ws-endpoint      Chrome DevTools WebSocket URL
  --auto-connect     Auto-connect to an existing Chrome instance
  --user-data-dir    Chrome user-data-dir path
  --reconnect-delay  Seconds between reconnect attempts (default: 5)
  --tool-timeout     Seconds to wait for a tool call response (default: 120)

Requirements
------------
  pip install "websockets>=13"

Protocol note
-------------
  chrome-devtools-mcp uses @modelcontextprotocol/sdk (Node.js) which serialises
  every JSON-RPC message as a single UTF-8 line terminated with \n.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from typing import Any, Optional

try:
    from websockets.asyncio.client import connect, ClientConnection
    import websockets.exceptions
except ImportError:
    print(
        "ERROR: websockets package not found (need >= 13). Install it with:\n"
        "  pip install 'websockets>=13'",
        file=sys.stderr,
    )
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s [connector] %(levelname)s %(message)s",
)
log = logging.getLogger("connector")


# -- MCP subprocess helpers --------------------------------------------------
#
# chrome-devtools-mcp uses @modelcontextprotocol/sdk's StdioServerTransport
# which serialises each JSON-RPC message as a single UTF-8 line terminated
# with '\n'.  The connector must use the same framing.


async def read_from_proc(
    proc: asyncio.subprocess.Process,
    deadline: float,
) -> dict[str, Any]:
    """
    Read one newline-delimited JSON-RPC message from the subprocess stdout.
    Raises EOFError on process exit, TimeoutError if *deadline* (monotonic
    seconds) is exceeded.
    """
    if proc.stdout is None:
        raise RuntimeError("Subprocess has no stdout stream")

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("deadline exceeded while reading subprocess")
        try:
            chunk = await asyncio.wait_for(
                proc.stdout.readline(), timeout=remaining
            )
        except asyncio.TimeoutError:
            raise TimeoutError("deadline exceeded while reading subprocess")
        if not chunk:
            raise EOFError("Subprocess stdout closed")
        if chunk.strip():
            parsed = json.loads(chunk.decode("utf-8"))
            log.debug("mcp->connector  stdout: %s", chunk.decode("utf-8").rstrip())
            return parsed
        if time.monotonic() >= deadline:
            raise TimeoutError("deadline exceeded while reading subprocess")


async def write_to_proc(
    proc: asyncio.subprocess.Process, msg: dict[str, Any]
) -> None:
    """Write one newline-delimited JSON-RPC message to the subprocess stdin."""
    if proc.stdin is None:
        raise RuntimeError("Subprocess has no stdin stream")

    line = (json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8")
    log.debug("connector->mcp  stdin: %s", line.decode("utf-8").rstrip())
    proc.stdin.write(line)
    await proc.stdin.drain()


# -- MCP subprocess wrapper --------------------------------------------------


class McpProcess:
    """
    Manages a single chrome-devtools-mcp subprocess.

    Requests are serialised via an asyncio.Lock so that only one request is
    in-flight at a time on the subprocess stdio channel.  Notifications sent
    by the subprocess between a request and its response are silently skipped.
    """

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self._proc = proc
        self._lock = asyncio.Lock()

    async def call(
        self,
        req_id: str,
        method: str,
        params: dict[str, Any],
        timeout: float = 120,
    ) -> dict[str, Any]:
        """
        Forward an MCP request to the subprocess and return its result.

        Notifications (messages without an 'id') that arrive before the
        matching response are silently dropped.  Raises RuntimeError on
        subprocess error responses and TimeoutError on timeout.
        """
        if self._lock.locked():
            log.warning(
                "connector->mcp  id=%s method=%s  lock contention: another "
                "request is already in-flight; queuing behind it",
                req_id,
                method,
            )
        async with self._lock:
            log.debug(
                "connector->mcp  id=%s method=%s  lock acquired, forwarding",
                req_id,
                method,
            )
            msg: dict[str, Any] = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method,
                "params": params,
            }
            await write_to_proc(self._proc, msg)

            deadline = time.monotonic() + timeout
            call_start = time.monotonic()
            while True:
                try:
                    resp = await read_from_proc(self._proc, deadline)
                except TimeoutError:
                    elapsed = time.monotonic() - call_start
                    log.error(
                        "connector->mcp  id=%s method=%s  no response in "
                        "%.0fs -- terminating subprocess",
                        req_id,
                        method,
                        elapsed,
                    )
                    await self.terminate()
                    raise RuntimeError(
                        f"MCP subprocess did not respond within {timeout:.0f}s. "
                        "If Chrome is showing a remote-debugging authorisation "
                        "prompt, please click 'Allow' to continue. "
                        "You can adjust the timeout with --tool-timeout."
                    )

                resp_id = resp.get("id")
                if resp_id is None:
                    log.debug(
                        "Subprocess notification: %s",
                        resp.get("method", "?"),
                    )
                    continue
                if str(resp_id) != str(req_id):
                    log.warning(
                        "Unexpected response id=%s (expected %s) -- skipping",
                        resp_id,
                        req_id,
                    )
                    continue
                break

        elapsed = time.monotonic() - call_start
        log.debug(
            "connector->mcp  id=%s method=%s  response received in %.2fs",
            req_id,
            method,
            elapsed,
        )
        if "error" in resp:
            raise RuntimeError(
                json.dumps(resp["error"], ensure_ascii=False)
            )
        return resp.get("result", {})

    def is_alive(self) -> bool:
        return self._proc.returncode is None

    async def terminate(self) -> None:
        try:
            self._proc.terminate()
            await self._proc.wait()
        except Exception:
            pass


# -- Subprocess lifecycle ----------------------------------------------------


async def start_mcp_process(mcp_args: list[str]) -> McpProcess:
    """
    Spawn the chrome-devtools-mcp subprocess, perform the MCP initialisation
    handshake, and return a ready-to-use McpProcess.
    """
    log.info("Spawning MCP subprocess: %s", " ".join(mcp_args))

    proc = await asyncio.create_subprocess_exec(
        *mcp_args,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=None,  # inherit parent stderr so logs are visible
    )

    # Initialisation handshake
    init_req: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": "__init__",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "chrome-devtools-connector",
                "version": "0.1.0",
            },
        },
    }
    await write_to_proc(proc, init_req)

    deadline = time.monotonic() + 60
    while True:
        try:
            resp = await read_from_proc(proc, deadline)
        except TimeoutError:
            raise RuntimeError(
                "MCP subprocess did not respond to initialize within 60 s. "
                "Check that Chrome is reachable."
            )

        resp_id = resp.get("id")
        if resp_id is None:
            log.debug("Pre-init notification: %s", resp.get("method", "?"))
            continue
        if str(resp_id) != "__init__":
            log.warning("Unexpected pre-init response id=%s", resp_id)
            continue
        break

    log.info(
        "MCP subprocess initialised (server: %s)",
        resp.get("result", {}).get("serverInfo", {}).get("name", "?"),
    )

    await write_to_proc(
        proc, {"jsonrpc": "2.0", "method": "notifications/initialized"}
    )

    return McpProcess(proc)


# -- Per-message handler -----------------------------------------------------


async def handle_relay_message(
    raw: str | bytes,
    mcp: McpProcess,
    ws: ClientConnection,
    tool_timeout: float,
) -> None:
    """
    Parse one request from the relay, forward it to the MCP subprocess, and
    send the response back over the WebSocket.
    """
    text = raw if isinstance(raw, str) else raw.decode("utf-8")
    log.debug("relay->connector  raw WS frame: %r", text[:500])
    try:
        req = json.loads(text)
    except json.JSONDecodeError:
        log.warning("Non-JSON message from relay: %r", text[:200])
        return

    req_id = req.get("id")
    method: str = req.get("method", "")
    params: dict[str, Any] = req.get("params") or {}

    # Notifications (no "id") are fire-and-forget -- nothing to reply to.
    if req_id is None:
        log.debug("Relay notification: %s", method)
        return

    req_id_str = str(req_id)

    try:
        result = await mcp.call(req_id_str, method, params, timeout=tool_timeout)
        log.debug(
            "relay->connector  id=%s  method=%s  result_keys=%s",
            req_id_str,
            method,
            list(result.keys()) if isinstance(result, dict) else f'<{type(result).__name__}>',
        )
        response: dict[str, Any] = {"id": req_id, "result": result}
    except RuntimeError as exc:
        msg = str(exc)
        parsed: dict[str, Any] | None = None
        try:
            parsed = json.loads(msg)
        except json.JSONDecodeError:
            pass
        if isinstance(parsed, dict) and parsed.get("code") == -32601:
            log.warning("Unhandled MCP method=%s: %s", method, msg)
            response = {"id": req_id, "error": parsed}
        else:
            log.exception("MCP subprocess error for method=%s", method)
            response = {"id": req_id, "error": {"code": -32603, "message": msg}}
    except Exception as exc:
        log.exception("MCP subprocess error for method=%s", method)
        response = {
            "id": req_id,
            "error": {"code": -32603, "message": str(exc)},
        }

    await ws.send(json.dumps(response, ensure_ascii=False))
    log.debug(
        "connector->relay  id=%s method=%s  response sent (%s)",
        req_id_str,
        method,
        "error" if "error" in response else "success",
    )


# -- WebSocket connect-and-run loop ------------------------------------------


async def connect_and_run(relay_url: str, mcp: McpProcess, tool_timeout: float) -> None:
    """
    Connect to the relay WebSocket and process requests until the connection
    drops or the MCP subprocess dies.
    """
    log.info("Connecting to relay at %s", relay_url)
    async with connect(relay_url) as ws:
        log.info("Connected to relay -- ready to serve Chrome DevTools")

        pending_tasks: set[asyncio.Task[None]] = set()

        async for raw in ws:
            if not mcp.is_alive():
                log.error("MCP subprocess has exited -- closing connection")
                break

            task: asyncio.Task[None] = asyncio.create_task(
                handle_relay_message(raw, mcp, ws, tool_timeout)
            )
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

        if pending_tasks:
            log.debug(
                "Waiting for %d in-flight task(s) to finish before disconnect",
                len(pending_tasks),
            )
            await asyncio.gather(*pending_tasks, return_exceptions=True)


# -- Main reconnect loop -----------------------------------------------------


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "chrome-devtools connector (PC 32 side). "
            "Dials the relay on Server 33 and forwards tools/list and "
            "tools/call requests to chrome-devtools-mcp."
        )
    )

    parser.add_argument(
        "--relay-url",
        required=True,
        help="WebSocket URL of the relay server on Server 33, e.g. ws://192.168.1.33:7000",
    )
    parser.add_argument(
        "--mcp-cmd",
        default="chrome-devtools-mcp",
        help="chrome-devtools-mcp executable name or path (default: chrome-devtools-mcp)",
    )
    parser.add_argument(
        "--browser-url",
        help="Chrome DevTools HTTP URL, e.g. http://127.0.0.1:9222",
    )
    parser.add_argument(
        "--ws-endpoint",
        help="Chrome DevTools WebSocket URL",
    )
    parser.add_argument(
        "--auto-connect",
        action="store_true",
        help="Auto-connect to an existing Chrome instance",
    )
    parser.add_argument(
        "--user-data-dir",
        help="Chrome user-data-dir path",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=5.0,
        help="Seconds to wait between reconnect attempts (default: 5)",
    )
    parser.add_argument(
        "--tool-timeout",
        type=float,
        default=120.0,
        help=(
            "Seconds to wait for the MCP subprocess to respond to a single "
            "tool call before giving up (default: 120)."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG-level logging (logs all WS and stdio message content)",
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    mcp_args = [args.mcp_cmd]
    if args.browser_url:
        mcp_args += ["--browser-url", args.browser_url]
    if args.ws_endpoint:
        mcp_args += ["--ws-endpoint", args.ws_endpoint]
    if args.auto_connect:
        mcp_args.append("--auto-connect")
    if args.user_data_dir:
        mcp_args += ["--user-data-dir", args.user_data_dir]

    mcp: Optional[McpProcess] = None
    attempt = 0

    while True:
        attempt += 1
        try:
            if mcp is None or not mcp.is_alive():
                if mcp is not None:
                    log.warning("MCP subprocess died -- restarting")
                    await mcp.terminate()
                mcp = await start_mcp_process(mcp_args)

            await connect_and_run(args.relay_url, mcp, args.tool_timeout)
            log.info(
                "Disconnected from relay. Reconnecting in %.1fs ...",
                args.reconnect_delay,
            )

        except OSError as exc:
            log.warning(
                "Relay not reachable at %s (attempt %d): %s. "
                "Retrying in %.1fs ...",
                args.relay_url,
                attempt,
                exc,
                args.reconnect_delay,
            )
        except EOFError:
            log.error("MCP subprocess closed its stdout -- restarting")
            if mcp is not None:
                await mcp.terminate()
            mcp = None
        except KeyboardInterrupt:
            log.info("Interrupted -- shutting down")
            if mcp is not None:
                await mcp.terminate()
            break
        except Exception as exc:
            log.exception("Unexpected error (attempt %d): %s", attempt, exc)

        await asyncio.sleep(args.reconnect_delay)


if __name__ == "__main__":
    asyncio.run(main())
