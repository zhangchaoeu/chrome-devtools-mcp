#!/usr/bin/env python3
"""
chrome-devtools connector — runs on PC 32 (Windows machine with Chrome).

Dials the relay server on Server 33 via WebSocket (32 → 33) and forwards
every MCP request to the locally-running chrome-devtools-mcp process, then
sends the result back through the same WebSocket channel.

Network topology
----------------
  relay_server.py (33)
       ▲
  WebSocket (32 dials 33)
       │
  connector.py (32, this file)
       │ subprocess stdio
  chrome-devtools-mcp  ←→  Chrome DevTools (9222)

Usage (on PC 32)
----------------
  # Chrome already open with --remote-debugging-port=9222
  python connector.py --relay-url ws://33-host:7000 --browser-url http://127.0.0.1:9222

  # Let chrome-devtools-mcp auto-connect to an existing Chrome profile
  python connector.py --relay-url ws://33-host:7000 --auto-connect

  # Specify a Chrome user-data-dir
  python connector.py --relay-url ws://33-host:7000 \
      --user-data-dir "C:\\Users\\Me\\AppData\\Local\\Google\\Chrome\\User Data"

Options
-------
  --relay-url        WebSocket URL of the relay server on 33 (required)
  --mcp-cmd          chrome-devtools-mcp executable (default: chrome-devtools-mcp)
  --browser-url      Chrome DevTools HTTP URL, e.g. http://127.0.0.1:9222
  --ws-endpoint      Chrome DevTools WebSocket URL
  --auto-connect     Auto-connect to a running Chrome instance
  --user-data-dir    Chrome user-data-dir path
  --reconnect-delay  Seconds between reconnect attempts (default: 5)

Requirements
------------
  pip install "websockets>=13"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
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


# ── MCP subprocess helpers ───────────────────────────────────────────────────


async def read_from_proc(
    proc: asyncio.subprocess.Process,
) -> dict[str, Any]:
    """
    Read one Content-Length-framed MCP JSON-RPC message from the subprocess
    stdout.  Raises EOFError if the process closes its stdout.
    """
    if proc.stdout is None:
        raise RuntimeError("Subprocess has no stdout stream")

    header = b""
    while True:
        ch = await proc.stdout.read(1)
        if not ch:
            raise EOFError("Subprocess stdout closed unexpectedly")
        header += ch
        if header.endswith(b"\r\n\r\n"):
            break

    content_length = 0
    for line in header.decode("ascii", errors="replace").split("\r\n"):
        if line.lower().startswith("content-length:"):
            content_length = int(line.split(":", 1)[1].strip())

    if content_length == 0:
        return {}

    body = await proc.stdout.readexactly(content_length)
    return json.loads(body)


async def write_to_proc(
    proc: asyncio.subprocess.Process, msg: dict[str, Any]
) -> None:
    """Write one MCP JSON-RPC message to the subprocess stdin."""
    if proc.stdin is None:
        raise RuntimeError("Subprocess has no stdin stream")

    body = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    proc.stdin.write(header + body)
    await proc.stdin.drain()


# ── MCP subprocess wrapper ───────────────────────────────────────────────────


class McpProcess:
    """
    Manages a single chrome-devtools-mcp subprocess.

    Requests are serialised via an asyncio.Lock so that only one request is
    in-flight at a time on the subprocess stdio channel.  This keeps the
    implementation simple while still supporting concurrent requests from the
    relay (they are queued and processed one after another).
    """

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self._proc = proc
        self._lock = asyncio.Lock()

    async def call(
        self, req_id: str, method: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Forward an MCP request to the subprocess and return its result.

        Raises RuntimeError if the subprocess returns an error response.
        """
        async with self._lock:
            msg: dict[str, Any] = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method,
                "params": params,
            }
            await write_to_proc(self._proc, msg)
            resp = await read_from_proc(self._proc)

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


# ── Subprocess lifecycle ─────────────────────────────────────────────────────


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

    # ── Initialisation handshake ──────────────────────────────────────────
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

    resp = await asyncio.wait_for(read_from_proc(proc), timeout=30)
    if resp.get("id") != "__init__":
        raise RuntimeError(
            f"Unexpected initialisation response: {resp}"
        )
    log.info(
        "MCP subprocess initialised (server: %s)",
        resp.get("result", {}).get("serverInfo", {}).get("name", "?"),
    )

    # Send the required 'initialized' notification.
    await write_to_proc(
        proc, {"jsonrpc": "2.0", "method": "notifications/initialized"}
    )

    return McpProcess(proc)


# ── Per-message handler ───────────────────────────────────────────────────────


async def handle_relay_message(
    raw: str | bytes,
    mcp: McpProcess,
    ws: ClientConnection,
) -> None:
    """
    Parse one request from the relay, forward it to the MCP subprocess, and
    send the response back over the WebSocket.
    """
    text = raw if isinstance(raw, str) else raw.decode("utf-8")
    try:
        req = json.loads(text)
    except json.JSONDecodeError:
        log.warning("Non-JSON message from relay: %r", text[:200])
        return

    req_id: str = req.get("id", "")
    method: str = req.get("method", "")
    params: dict[str, Any] = req.get("params") or {}

    try:
        result = await mcp.call(req_id, method, params)
        response: dict[str, Any] = {"id": req_id, "result": result}
    except Exception as exc:
        log.exception("MCP subprocess error for method=%s", method)
        response = {
            "id": req_id,
            "error": {"code": -32603, "message": str(exc)},
        }

    await ws.send(json.dumps(response, ensure_ascii=False))


# ── WebSocket connect-and-run loop ────────────────────────────────────────────


async def connect_and_run(relay_url: str, mcp: McpProcess) -> None:
    """
    Connect to the relay WebSocket and process requests until the connection
    drops or the MCP subprocess dies.
    """
    log.info("Connecting to relay at %s", relay_url)
    async with connect(relay_url) as ws:
        log.info("Connected to relay — ready to serve Chrome DevTools")

        # Process incoming relay messages concurrently using asyncio tasks so
        # that a slow tool call does not block later requests from arriving.
        pending_tasks: set[asyncio.Task[None]] = set()

        async for raw in ws:
            if not mcp.is_alive():
                log.error("MCP subprocess has exited — closing connection")
                break

            task: asyncio.Task[None] = asyncio.create_task(
                handle_relay_message(raw, mcp, ws)
            )
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

        # Wait for in-flight tasks before reconnecting.
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)


# ── Main reconnect loop ───────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "chrome-devtools connector (PC 32 side). "
            "Dials the relay on Server 33 and forwards MCP requests to the "
            "local chrome-devtools-mcp process."
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
    args = parser.parse_args()

    # Build the chrome-devtools-mcp command line.
    mcp_args = [args.mcp_cmd]
    if args.browser_url:
        mcp_args += ["--browser-url", args.browser_url]
    if args.ws_endpoint:
        mcp_args += ["--ws-endpoint", args.ws_endpoint]
    if args.auto_connect:
        mcp_args.append("--auto-connect")
    if args.user_data_dir:
        mcp_args += ["--user-data-dir", args.user_data_dir]

    # Start the MCP subprocess once; restart it only if it dies.
    mcp: Optional[McpProcess] = None

    attempt = 0
    while True:
        attempt += 1
        try:
            if mcp is None or not mcp.is_alive():
                if mcp is not None:
                    log.warning("MCP subprocess died — restarting")
                    await mcp.terminate()
                mcp = await start_mcp_process(mcp_args)

            await connect_and_run(args.relay_url, mcp)
            log.info(
                "Disconnected from relay. Reconnecting in %.1fs …",
                args.reconnect_delay,
            )

        except OSError as exc:
            log.warning(
                "Relay not reachable at %s (attempt %d): %s. "
                "Retrying in %.1fs …",
                args.relay_url,
                attempt,
                exc,
                args.reconnect_delay,
            )
        except EOFError:
            log.error("MCP subprocess closed its stdout — restarting")
            if mcp is not None:
                await mcp.terminate()
            mcp = None
        except KeyboardInterrupt:
            log.info("Interrupted — shutting down")
            if mcp is not None:
                await mcp.terminate()
            break
        except Exception as exc:
            log.exception("Unexpected error (attempt %d): %s", attempt, exc)

        await asyncio.sleep(args.reconnect_delay)


if __name__ == "__main__":
    asyncio.run(main())
