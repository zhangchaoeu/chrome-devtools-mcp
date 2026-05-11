#!/usr/bin/env python3
"""
chrome-devtools relay server -- runs on Server 33.

Accepts a reverse WebSocket connection from connector.py on PC 32 and
exposes tools/list and tools/call to MCP agents via stdio.

Network topology
----------------
  MCP Agent (Claude Desktop / any MCP host)
       | stdio (launches relay_server.py as a subprocess)
  relay_server.py (33, this file)
       |
       WebSocket server :7000
       ^ <- 32 dials 33
       WebSocket client
       |
  connector.py (32)
       | subprocess stdio
  chrome-devtools-mcp (32)
       |
  Chrome browser (32)

Usage (on Server 33)
--------------------
  Configure relay_server.py as a stdio MCP server in your agent host, e.g.
  Claude Desktop (~/.claude/claude_desktop_config.json):

    {
      "mcpServers": {
        "chrome-devtools": {
          "command": "python",
          "args": ["/path/to/relay_server.py", "--port", "7000"]
        }
      }
    }

  The agent host launches this script as a subprocess; the connector from
  PC 32 dials in on the WebSocket port independently.

Requirements
------------
  pip install -r requirements.txt
  # websockets>=13  mcp>=1.23.0
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

try:
    import mcp.types as mcp_types
    from mcp.server import Server
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
    format="%(asctime)s [relay] %(levelname)s %(message)s",
)
log = logging.getLogger("relay")


# -- Relay state -------------------------------------------------------------


class RelayState:
    """
    Holds the active connector WebSocket and a map of in-flight request futures.
    """

    def __init__(self) -> None:
        self._connector: Optional[ServerConnection] = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def set_connector(self, ws: ServerConnection) -> None:
        async with self._lock:
            if self._connector is not None:
                log.warning("New connector arrived -- dropping previous one")
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
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(
                        ConnectionError("Connector disconnected unexpectedly")
                    )
            self._pending.clear()
        log.info("Connector unregistered")

    async def forward(
        self, method: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Send an MCP request to the connector and await its response."""
        req_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()

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


# -- WebSocket handler (one per connector connection from 32) ----------------


async def handle_connector(
    websocket: ServerConnection,
    state: RelayState,
) -> None:
    """Called once per incoming WebSocket connection from the connector on 32."""
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


# -- MCP server (tools only) -------------------------------------------------


def _to_content_item(
    item: Any,
) -> mcp_types.TextContent | mcp_types.ImageContent | mcp_types.EmbeddedResource:
    """Convert a raw content dict (from the connector) to an mcp SDK type."""
    if isinstance(item, dict):
        kind = item.get("type", "text")
        if kind == "image":
            return mcp_types.ImageContent(
                type="image",
                data=item.get("data", ""),
                mimeType=item.get("mimeType", "image/png"),
            )
        if kind == "resource":
            raw_res = item.get("resource", {})
            uri: str = raw_res.get("uri", "")
            mime: Optional[str] = raw_res.get("mimeType")
            if raw_res.get("blob") is not None:
                res_contents: (
                    mcp_types.BlobResourceContents
                    | mcp_types.TextResourceContents
                ) = mcp_types.BlobResourceContents(
                    uri=uri,
                    blob=raw_res["blob"],
                    mimeType=mime,
                )
            else:
                res_contents = mcp_types.TextResourceContents(
                    uri=uri,
                    text=raw_res.get("text", ""),
                    mimeType=mime,
                )
            return mcp_types.EmbeddedResource(
                type="resource", resource=res_contents
            )
        return mcp_types.TextContent(
            type="text",
            text=item.get("text", json.dumps(item, ensure_ascii=False)),
        )
    return mcp_types.TextContent(type="text", text=str(item))


def build_mcp_server(state: RelayState) -> Server:
    """Build an MCP Server that proxies tools/list and tools/call to the connector."""
    server: Server = Server("chrome-devtools-relay")

    @server.list_tools()
    async def list_tools() -> list[mcp_types.Tool]:
        try:
            result = await state.forward("tools/list", {})
        except ConnectionError as exc:
            log.warning("tools/list: no connector connected -- %s", exc)
            return []
        tools = result.get("tools", [])
        return [
            mcp_types.Tool(
                name=t["name"],
                description=t.get("description", ""),
                inputSchema=t.get(
                    "inputSchema", {"type": "object", "properties": {}}
                ),
            )
            for t in tools
        ]

    @server.call_tool()
    async def call_tool(
        name: str,
        arguments: dict[str, Any] | None,
    ) -> list[
        mcp_types.TextContent
        | mcp_types.ImageContent
        | mcp_types.EmbeddedResource
    ]:
        result = await state.forward(
            "tools/call", {"name": name, "arguments": arguments or {}}
        )
        return [_to_content_item(item) for item in result.get("content", [])]

    return server


# -- WebSocket connector server ----------------------------------------------


async def run_ws_server(state: RelayState, host: str, port: int) -> None:
    """Run the reverse-tunnel WebSocket server that PC 32 dials into."""
    async with serve(lambda ws: handle_connector(ws, state), host, port):
        log.info(
            "Connector WebSocket server listening on ws://%s:%d", host, port
        )
        await asyncio.get_running_loop().create_future()


# -- Entry point -------------------------------------------------------------


async def run_relay(ws_host: str, ws_port: int) -> None:
    """Start the WebSocket connector server and serve MCP over stdio."""
    state = RelayState()
    server = build_mcp_server(state)
    init_opts = server.create_initialization_options()

    ws_task = asyncio.create_task(run_ws_server(state, ws_host, ws_port))
    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, init_opts)
    finally:
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "chrome-devtools relay (Server 33 side). "
            "Presents tools/list and tools/call via MCP over stdio and "
            "forwards requests to the connector on PC 32 via a reverse "
            "WebSocket tunnel."
        )
    )
    parser.add_argument(
        "--port",
        type=int,
        default=7000,
        help="WebSocket port for the PC 32 connector (default: 7000)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind interface for the connector WebSocket server (default: 0.0.0.0)",
    )
    args = parser.parse_args()

    log.info(
        "Starting relay: stdio MCP server, connector WS on ws://%s:%d",
        args.host,
        args.port,
    )
    asyncio.run(run_relay(args.host, args.port))


if __name__ == "__main__":
    main()
