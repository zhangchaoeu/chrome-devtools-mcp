#!/usr/bin/env python3
"""
chrome-devtools connector — runs on PC 32 (Windows machine with Chrome).

Two mutually exclusive modes
-----------------------------

--relay-url  (WebSocket mode, existing)
  Dials the relay server on Server 33 via WebSocket (32 → 33) and forwards
  every MCP request to the locally-running chrome-devtools-mcp process, then
  sends the result back through the same WebSocket channel.

--local-stdin  (local stdio mode, for mcp inspect testing)
  Acts as a proxy MCP server on its own stdin/stdout (so mcp inspect can
  connect directly) while forwarding every tool call to the local
  chrome-devtools-mcp subprocess via the official MCP Python SDK
  (ClientSession + stdio_client).

Network topologies
------------------
  relay mode:
    relay_server.py (33)
         ▲
    WebSocket (32 dials 33)
         │
    connector.py (32, this file)
         │ subprocess stdio
    chrome-devtools-mcp  ←→  Chrome DevTools (9222)

  local-stdin mode:
    mcp inspect (or any MCP client)
         │ stdio
    connector.py --local-stdin (proxy MCP server on own stdio)
         │ subprocess stdio (via ClientSession + stdio_client)
    chrome-devtools-mcp  ←→  Chrome DevTools (9222)

Usage (on PC 32)
----------------
  # Relay mode — Chrome already open with --remote-debugging-port=9222
  python connector.py --relay-url ws://33-host:7000 --browser-url http://127.0.0.1:9222

  # Relay mode — auto-connect to a running Chrome instance
  python connector.py --relay-url ws://33-host:7000 --auto-connect

  # Local stdio mode for mcp inspect (no relay needed)
  python connector.py --local-stdin --browser-url http://127.0.0.1:9222

Options
-------
  --relay-url        WebSocket URL of the relay server on 33
  --local-stdin      Run as a local stdio MCP proxy (mutually exclusive with --relay-url)
  --mcp-cmd          chrome-devtools-mcp executable (default: chrome-devtools-mcp)
  --browser-url      Chrome DevTools HTTP URL, e.g. http://127.0.0.1:9222
  --ws-endpoint      Chrome DevTools WebSocket URL
  --auto-connect     Auto-connect to a running Chrome instance
  --user-data-dir    Chrome user-data-dir path
  --reconnect-delay  Seconds between reconnect attempts (default: 5, relay mode only)

Requirements
------------
  pip install "websockets>=13" "mcp>=1.23.0"

Protocol note
-------------
  chrome-devtools-mcp uses @modelcontextprotocol/sdk (Node.js) which serialises
  every JSON-RPC message as a single UTF-8 line terminated with \\n.  This is
  *different* from the HTTP-style Content-Length framing sometimes described in
  older MCP documentation.  The official MCP Python SDK's stdio_client uses the
  same newline-delimited JSON framing, so it is fully compatible.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import sys
import time
from typing import Any, Iterable, Optional

from pydantic import AnyUrl

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

try:
    import mcp.types as mcp_types
    from mcp.client.session import ClientSession
    from mcp.client.stdio import stdio_client, StdioServerParameters
    from mcp.server import Server
    from mcp.server.lowlevel.helper_types import ReadResourceContents
    from mcp.server.stdio import stdio_server
except ImportError:
    print(
        "ERROR: mcp package not found (need >= 1.23.0). Install it with:\n"
        "  pip install 'mcp>=1.23.0'",
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

    chunks: list[bytes] = []
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
            return json.loads(chunk.decode("utf-8"))
        # blank line — check deadline before looping
        if time.monotonic() >= deadline:
            raise TimeoutError("deadline exceeded while reading subprocess")


async def write_to_proc(
    proc: asyncio.subprocess.Process, msg: dict[str, Any]
) -> None:
    """Write one newline-delimited JSON-RPC message to the subprocess stdin."""
    if proc.stdin is None:
        raise RuntimeError("Subprocess has no stdin stream")

    line = (json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8")
    proc.stdin.write(line)
    await proc.stdin.drain()


# ── MCP subprocess wrapper ───────────────────────────────────────────────────


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
        async with self._lock:
            msg: dict[str, Any] = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method,
                "params": params,
            }
            await write_to_proc(self._proc, msg)

            deadline = time.monotonic() + timeout
            while True:
                try:
                    resp = await read_from_proc(self._proc, deadline)
                except TimeoutError:
                    await self.terminate()
                    raise RuntimeError(
                        f"MCP subprocess did not respond within {timeout:.0f}s. "
                        "If Chrome is showing a remote-debugging authorisation "
                        "prompt, please click 'Allow' to continue. "
                        "You can adjust the timeout with --tool-timeout."
                    )

                resp_id = resp.get("id")
                if resp_id is None:
                    # Notification — log and skip.
                    log.debug(
                        "Subprocess notification: %s",
                        resp.get("method", "?"),
                    )
                    continue
                if str(resp_id) != str(req_id):
                    log.warning(
                        "Unexpected response id=%s (expected %s) — skipping",
                        resp_id,
                        req_id,
                    )
                    continue
                break

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

    deadline = time.monotonic() + 60  # allow up to 60 s for Chrome to start
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

    # Send the required 'initialized' notification.
    await write_to_proc(
        proc, {"jsonrpc": "2.0", "method": "notifications/initialized"}
    )

    return McpProcess(proc)


# ── Optional MCP methods handled locally ─────────────────────────────────────
#
# chrome-devtools-mcp only implements tools/list and tools/call.  Some MCP
# clients unconditionally probe for prompts and resources regardless of the
# server capabilities advertised during initialisation.  Forwarding those
# requests to the subprocess produces noisy "Method not found" errors.
# Return stub responses locally so the subprocess never sees them.

_STUB_RESULTS: dict[str, dict[str, Any]] = {
    "prompts/list": {"prompts": []},
    "resources/list": {"resources": []},
    "resources/templates/list": {"resourceTemplates": []},
}


# ── Per-message handler ───────────────────────────────────────────────────────


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
    try:
        req = json.loads(text)
    except json.JSONDecodeError:
        log.warning("Non-JSON message from relay: %r", text[:200])
        return

    req_id = req.get("id")
    method: str = req.get("method", "")
    params: dict[str, Any] = req.get("params") or {}

    # Notifications (no "id") are fire-and-forget — nothing to reply to.
    if req_id is None:
        log.debug("Relay notification: %s", method)
        return

    req_id_str = str(req_id)

    # Handle optional capability methods locally; the subprocess only
    # implements tools, so these would otherwise return "Method not found".
    if method in _STUB_RESULTS:
        await ws.send(
            json.dumps(
                {"id": req_id, "result": _STUB_RESULTS[method]},
                ensure_ascii=False,
            )
        )
        return

    if method == "prompts/get":
        name = params.get("name", "")
        await ws.send(
            json.dumps(
                {
                    "id": req_id,
                    "error": {
                        "code": -32602,
                        "message": f"Prompt not found: {name}",
                    },
                },
                ensure_ascii=False,
            )
        )
        return

    try:
        result = await mcp.call(req_id_str, method, params, timeout=tool_timeout)
        response: dict[str, Any] = {"id": req_id, "result": result}
    except RuntimeError as exc:
        msg = str(exc)
        # McpProcess.call() serialises the subprocess error object as JSON.
        # If the code is -32601 (Method not found) log at WARNING instead of
        # ERROR so it doesn't pollute monitoring dashboards.
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


# ── WebSocket connect-and-run loop ────────────────────────────────────────────


async def connect_and_run(relay_url: str, mcp: McpProcess, tool_timeout: float) -> None:
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
                handle_relay_message(raw, mcp, ws, tool_timeout)
            )
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

        # Wait for in-flight tasks before reconnecting.
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)


# ── Local stdio mode (SDK-based proxy) ───────────────────────────────────────


def build_proxy_server(session: ClientSession) -> Server:
    """
    Build an MCP Server that proxies all tool, prompt, and resource calls
    through a ClientSession connected to the chrome-devtools-mcp subprocess.

    Using the official mcp SDK on both sides avoids any hand-written
    JSON-RPC parsing or framing.
    """
    proxy: Server = Server("chrome-devtools-proxy")

    @proxy.list_tools()
    async def list_tools() -> list[mcp_types.Tool]:
        result = await session.list_tools()
        return result.tools

    @proxy.call_tool()
    async def call_tool(
        name: str,
        arguments: dict[str, Any] | None,
    ) -> mcp_types.CallToolResult:
        return await session.call_tool(name, arguments)

    @proxy.list_prompts()
    async def list_prompts() -> list[mcp_types.Prompt]:
        result = await session.list_prompts()
        return result.prompts

    @proxy.get_prompt()
    async def get_prompt(
        name: str,
        arguments: dict[str, str] | None,
    ) -> mcp_types.GetPromptResult:
        return await session.get_prompt(name, arguments)

    @proxy.list_resources()
    async def list_resources() -> list[mcp_types.Resource]:
        result = await session.list_resources()
        return result.resources

    @proxy.read_resource()
    async def read_resource(uri: AnyUrl) -> Iterable[ReadResourceContents]:
        result = await session.read_resource(uri)
        items: list[ReadResourceContents] = []
        for c in result.contents:
            if isinstance(c, mcp_types.BlobResourceContents):
                items.append(
                    ReadResourceContents(
                        content=base64.b64decode(c.blob),
                        mime_type=c.mimeType,
                    )
                )
            else:
                items.append(
                    ReadResourceContents(
                        content=c.text,
                        mime_type=c.mimeType,
                    )
                )
        return items

    return proxy


async def run_local_stdin_mode(mcp_args: list[str]) -> None:
    """
    Run the connector in local stdio mode.

    mcp inspect (or any MCP client) communicates with this process via its
    own stdin/stdout.  The connector spawns chrome-devtools-mcp as a
    subprocess and bridges all tool calls through the official SDK's
    ClientSession (stdio_client).

    chrome-devtools-mcp uses newline-delimited JSON framing, which is the
    same framing used by the SDK's stdio_client, so no custom transport is
    needed.
    """
    cmd = mcp_args[0]
    sub_args = mcp_args[1:]

    server_params = StdioServerParameters(command=cmd, args=sub_args)
    log.info(
        "Starting local stdin mode — subprocess: %s", " ".join(mcp_args)
    )

    async with stdio_client(server_params, errlog=sys.stderr) as (
        read_stream,
        write_stream,
    ):
        async with ClientSession(
            read_stream,
            write_stream,
            client_info=mcp_types.Implementation(
                name="chrome-devtools-connector",
                version="0.1.0",
            ),
        ) as session:
            await session.initialize()
            log.info(
                "Connected to chrome-devtools-mcp via SDK — "
                "ready to serve mcp inspect"
            )

            proxy = build_proxy_server(session)
            async with stdio_server() as (srv_read, srv_write):
                await proxy.run(
                    srv_read,
                    srv_write,
                    proxy.create_initialization_options(),
                )


# ── Main reconnect loop ───────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "chrome-devtools connector (PC 32 side). "
            "Either dials the relay on Server 33 (--relay-url) or acts as a "
            "local stdio MCP proxy for mcp inspect (--local-stdin)."
        )
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--relay-url",
        help="WebSocket URL of the relay server on Server 33, e.g. ws://192.168.1.33:7000",
    )
    mode_group.add_argument(
        "--local-stdin",
        action="store_true",
        help=(
            "Run as a local stdio MCP proxy. "
            "mcp inspect (or any MCP client) can connect via stdio. "
            "Mutually exclusive with --relay-url."
        ),
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
        help="Seconds to wait between reconnect attempts (default: 5, relay mode only)",
    )
    parser.add_argument(
        "--tool-timeout",
        type=float,
        default=120.0,
        help=(
            "Seconds to wait for the MCP subprocess to respond to a single "
            "tool call before giving up (default: 120). Increase this value "
            "if you need more time to accept the Chrome remote-debugging "
            "authorisation prompt."
        ),
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

    # ── Local stdio mode ──────────────────────────────────────────────────────
    if args.local_stdin:
        try:
            await run_local_stdin_mode(mcp_args)
        except KeyboardInterrupt:
            log.info("Interrupted — shutting down")
        return

    # ── Relay (WebSocket) mode ────────────────────────────────────────────────
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

            await connect_and_run(args.relay_url, mcp, args.tool_timeout)
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
