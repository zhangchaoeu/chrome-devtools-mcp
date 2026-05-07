#!/usr/bin/env python3
"""
chrome-devtools relay server — runs on Server 33.

Presents a standard MCP stdio interface to Hermes (or any MCP client that
uses stdio) and forwards every tool request to the connector running on PC 32
via a *reverse* WebSocket tunnel: PC 32 dials into this server, not the other
way around.

Network topology
----------------
  Hermes (33) ──stdio──► relay_server.py (33, this file)
                                  │
                          WebSocket server
                          ws://0.0.0.0:7000
                                  ▲
                      WebSocket client (initiated by 32)
                                  │
                         connector.py (32)
                                  │
                         chrome-devtools-mcp
                                  │
                            Chrome browser

Usage (on Server 33)
--------------------
  python relay_server.py [--port 7000] [--host 0.0.0.0]

Hermes / MCP client config (server.json or claude_desktop_config.json):
  {
    "mcpServers": {
      "chrome_devtools": {
        "command": "python",
        "args": ["/path/to/relay_server.py", "--port", "7000"]
      }
    }
  }

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
import uuid
from typing import Any, Optional

try:
    from websockets.asyncio.server import ServerConnection, serve
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
    format="%(asctime)s [relay] %(levelname)s %(message)s",
)
log = logging.getLogger("relay")


# ── MCP stdio framing helpers ────────────────────────────────────────────────


def _blocking_read_message() -> Optional[bytes]:
    """Read one Content-Length-framed MCP message from stdin (blocking)."""
    header = b""
    while True:
        ch = sys.stdin.buffer.read(1)
        if not ch:
            return None  # EOF — Hermes closed the pipe
        header += ch
        if header.endswith(b"\r\n\r\n"):
            break

    content_length = 0
    for line in header.decode("ascii", errors="replace").split("\r\n"):
        if line.lower().startswith("content-length:"):
            content_length = int(line.split(":", 1)[1].strip())

    if content_length == 0:
        return b"{}"
    return sys.stdin.buffer.read(content_length)


async def read_stdin_message(
    loop: asyncio.AbstractEventLoop,
) -> Optional[dict[str, Any]]:
    """Async wrapper: read one MCP JSON-RPC message from stdin."""
    raw = await loop.run_in_executor(None, _blocking_read_message)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("Malformed JSON from stdin: %s", exc)
        return {}


def write_stdout_message(msg: dict[str, Any]) -> None:
    """Write one MCP JSON-RPC message to stdout (Content-Length framed)."""
    body = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    sys.stdout.buffer.write(header + body)
    sys.stdout.buffer.flush()


# ── Relay state ──────────────────────────────────────────────────────────────


class RelayState:
    """
    Holds the active connector WebSocket and a map of in-flight request futures.

    All access happens from the single asyncio event loop thread; the Lock
    protects shared mutable state during concurrent tool calls.
    """

    def __init__(self) -> None:
        self._connector: Optional[ServerConnection] = None
        # req_id → Future that will receive the connector's response
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    # ── Connector lifecycle ──────────────────────────────────────────────────

    async def set_connector(self, ws: ServerConnection) -> None:
        async with self._lock:
            if self._connector is not None:
                log.warning("New connector arrived — dropping previous one")
                old = self._connector
                self._connector = ws
                try:
                    await old.close(1001, "Replaced by new connector")
                except Exception:
                    pass
            else:
                self._connector = ws
        log.info("Connector registered")

    async def clear_connector(self) -> None:
        async with self._lock:
            self._connector = None
            # Immediately fail all in-flight requests.
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(
                        ConnectionError("Connector disconnected unexpectedly")
                    )
            self._pending.clear()
        log.info("Connector unregistered")

    # ── Request forwarding ───────────────────────────────────────────────────

    async def forward(
        self, method: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Send an MCP request to the connector and await its response.

        Raises ConnectionError if no connector is currently connected.
        """
        req_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()

        # Register future and capture connector reference while holding the
        # lock, then release before awaiting I/O.
        async with self._lock:
            if self._connector is None:
                raise ConnectionError(
                    "No connector is connected. "
                    "Start connector.py on PC 32 first."
                )
            self._pending[req_id] = fut
            connector = self._connector

        payload = json.dumps(
            {"id": req_id, "method": method, "params": params}
        )
        await connector.send(payload)

        return await fut

    async def resolve(self, data: dict[str, Any]) -> None:
        """Resolve a pending future from a message sent by the connector."""
        req_id = data.get("id")
        if not req_id:
            return

        async with self._lock:
            fut = self._pending.pop(req_id, None)

        if fut is None or fut.done():
            log.warning("No pending request for id=%s", req_id)
            return

        if "error" in data:
            fut.set_exception(RuntimeError(str(data["error"])))
        else:
            fut.set_result(data.get("result", {}))


# ── WebSocket handler (one per connector connection from 32) ─────────────────


async def handle_connector(
    websocket: ServerConnection,
    state: RelayState,
) -> None:
    """
    Called once per incoming WebSocket connection from the connector on 32.

    Stays alive until the connector disconnects, dispatching responses to
    in-flight relay futures.
    """
    await state.set_connector(websocket)
    try:
        async for raw in websocket:
            try:
                data = json.loads(raw)
                await state.resolve(data)
            except json.JSONDecodeError:
                log.warning(
                    "Non-JSON message from connector: %r", str(raw)[:200]
                )
    except websockets.exceptions.ConnectionClosed as exc:
        log.info("Connector closed the connection: %s", exc)
    except Exception as exc:
        log.exception("Unexpected error in connector handler: %s", exc)
    finally:
        await state.clear_connector()


# ── MCP protocol (Hermes over stdio) ────────────────────────────────────────

_SERVER_INFO = {"name": "chrome-devtools-relay", "version": "0.1.0"}


async def handle_mcp(
    msg: dict[str, Any], state: RelayState
) -> Optional[dict[str, Any]]:
    """
    Process one MCP JSON-RPC message from Hermes.

    Returns a response dict, or None for notifications (which have no id and
    require no reply).
    """
    msg_id = msg.get("id")
    method = msg.get("method", "")
    params: dict[str, Any] = msg.get("params") or {}

    # Notifications carry no id — no reply needed.
    if msg_id is None:
        return None

    try:
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "protocolVersion": params.get(
                        "protocolVersion", "2024-11-05"
                    ),
                    "capabilities": {"tools": {}},
                    "serverInfo": _SERVER_INFO,
                },
            }

        if method == "ping":
            return {"jsonrpc": "2.0", "id": msg_id, "result": {}}

        if method in ("tools/list", "tools/call"):
            result = await state.forward(method, params)
            return {"jsonrpc": "2.0", "id": msg_id, "result": result}

        # Unknown method
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {method}",
            },
        }

    except Exception as exc:
        log.exception("Error handling method=%s id=%s", method, msg_id)
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": -32603, "message": str(exc)},
        }


async def stdio_loop(state: RelayState) -> None:
    """
    Main loop: read MCP messages from Hermes over stdin, handle them, write
    responses back over stdout.
    """
    loop = asyncio.get_running_loop()
    while True:
        msg = await read_stdin_message(loop)
        if msg is None:
            log.info("stdin EOF — shutting down relay")
            break
        if not msg:
            continue

        response = await handle_mcp(msg, state)
        if response is not None:
            write_stdout_message(response)


# ── Entry point ──────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "chrome-devtools relay (Server 33 side). "
            "Presents an MCP stdio interface to Hermes and forwards requests "
            "to the connector on PC 32 via a reverse WebSocket tunnel."
        )
    )
    parser.add_argument(
        "--port",
        type=int,
        default=7000,
        help="WebSocket port to listen on for the PC 32 connector (default: 7000)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Interface to bind the WebSocket server to (default: 0.0.0.0)",
    )
    args = parser.parse_args()

    state = RelayState()

    print(
        f"chrome-devtools-relay: waiting for connector on "
        f"ws://{args.host}:{args.port}",
        file=sys.stderr,
    )

    async with serve(
        lambda ws: handle_connector(ws, state),
        args.host,
        args.port,
    ):
        await stdio_loop(state)


if __name__ == "__main__":
    asyncio.run(main())
