#!/usr/bin/env python3
"""
chrome-devtools relay server — runs on Server 33.

Exposes a standard MCP server interface to any MCP client and forwards every
tool request to the connector running on PC 32 via a reverse WebSocket tunnel:
PC 32 dials into this server, not the other way around.

Two MCP transports are supported so that clients like MCP Inspector can
connect via URL:

  --transport stdio  (default) — for Claude Desktop / Hermes / any client that
                                  spawns this script as a subprocess.
  --transport sse               — HTTP/SSE server on --http-port (default 7001),
                                  for MCP Inspector and web-based MCP clients.

Network topology
----------------
  MCP Client (33) ──stdio or HTTP/SSE──► relay_server.py (33, this file)
                                                  │
                                          WebSocket server :7000
                                                  ▲  ← 32 dials 33
                                          WebSocket client
                                                  │
                                           connector.py (32)
                                                  │ subprocess stdio
                                         chrome-devtools-mcp (32)
                                                  │
                                            Chrome browser (32)

Usage (on Server 33)
--------------------
  # stdio mode (Claude Desktop / Hermes):
  python relay_server.py [--port 7000]

  # SSE/HTTP mode (MCP Inspector, connect via URL http://server33:7001/sse):
  python relay_server.py --transport sse [--http-port 7001] [--port 7000]

Requirements
------------
  pip install -r requirements.txt
  # websockets>=13  mcp>=1.23.0  starlette>=0.49.1  uvicorn>=0.34.2
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import sys
import uuid
from typing import Any, Iterable, Optional

from pydantic import AnyUrl

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
    format="%(asctime)s [relay] %(levelname)s %(message)s",
)
log = logging.getLogger("relay")


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


# ── MCP server (official SDK) ─────────────────────────────────────────────────


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
        # default → text
        return mcp_types.TextContent(
            type="text",
            text=item.get("text", json.dumps(item, ensure_ascii=False)),
        )
    return mcp_types.TextContent(type="text", text=str(item))


def build_mcp_server(state: RelayState) -> Server:
    """
    Construct an MCP Server that proxies tool calls to the connector on PC 32.

    Using the official mcp SDK ensures correct protocol handling (including
    the initialize/initialized handshake) for all compliant MCP clients.
    """
    server: Server = Server("chrome-devtools-relay")

    @server.list_tools()
    async def list_tools() -> list[mcp_types.Tool]:
        try:
            result = await state.forward("tools/list", {})
        except ConnectionError as exc:
            log.warning("tools/list: no connector connected — %s", exc)
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

    @server.list_prompts()
    async def list_prompts() -> list[mcp_types.Prompt]:
        try:
            result = await state.forward("prompts/list", {})
        except ConnectionError as exc:
            log.warning("prompts/list: no connector connected — %s", exc)
            return []
        return [
            mcp_types.Prompt(
                name=p["name"],
                description=p.get("description"),
                arguments=[
                    mcp_types.PromptArgument(
                        name=a["name"],
                        description=a.get("description"),
                        required=a.get("required"),
                    )
                    for a in p.get("arguments", [])
                ]
                if p.get("arguments")
                else None,
            )
            for p in result.get("prompts", [])
        ]

    @server.get_prompt()
    async def get_prompt(
        name: str,
        arguments: dict[str, str] | None,
    ) -> mcp_types.GetPromptResult:
        result = await state.forward(
            "prompts/get", {"name": name, "arguments": arguments or {}}
        )
        messages: list[mcp_types.PromptMessage] = []
        for msg in result.get("messages", []):
            raw_content = msg.get("content", {})
            content_type = raw_content.get("type", "text")
            if content_type == "image":
                content: (
                    mcp_types.TextContent | mcp_types.ImageContent
                ) = mcp_types.ImageContent(
                    type="image",
                    data=raw_content.get("data", ""),
                    mimeType=raw_content.get("mimeType", "image/png"),
                )
            else:
                content = mcp_types.TextContent(
                    type="text",
                    text=raw_content.get(
                        "text", json.dumps(raw_content, ensure_ascii=False)
                    ),
                )
            messages.append(
                mcp_types.PromptMessage(
                    role=msg.get("role", "user"),
                    content=content,
                )
            )
        return mcp_types.GetPromptResult(
            description=result.get("description"),
            messages=messages,
        )

    @server.list_resources()
    async def list_resources() -> list[mcp_types.Resource]:
        try:
            result = await state.forward("resources/list", {})
        except ConnectionError as exc:
            log.warning("resources/list: no connector connected — %s", exc)
            return []
        return [
            mcp_types.Resource(
                name=r["name"],
                uri=r["uri"],
                description=r.get("description"),
                mimeType=r.get("mimeType"),
            )
            for r in result.get("resources", [])
        ]

    @server.read_resource()
    async def read_resource(uri: AnyUrl) -> Iterable[ReadResourceContents]:
        result = await state.forward(
            "resources/read", {"uri": str(uri)}
        )
        items: list[ReadResourceContents] = []
        for c in result.get("contents", []):
            if c.get("blob") is not None:
                items.append(
                    ReadResourceContents(
                        content=base64.b64decode(c["blob"]),
                        mime_type=c.get("mimeType"),
                    )
                )
            else:
                items.append(
                    ReadResourceContents(
                        content=c.get("text", ""),
                        mime_type=c.get("mimeType"),
                    )
                )
        return items

    return server


# ── WebSocket connector server ────────────────────────────────────────────────


async def run_ws_server(state: RelayState, host: str, port: int) -> None:
    """Run the reverse-tunnel WebSocket server that PC 32 dials into."""
    async with serve(lambda ws: handle_connector(ws, state), host, port):
        log.info(
            "Connector WebSocket server listening on ws://%s:%d", host, port
        )
        await asyncio.get_running_loop().create_future()  # run until cancelled


# ── Entry point ───────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "chrome-devtools relay (Server 33 side). "
            "Presents an MCP interface (stdio or HTTP/SSE) to MCP clients and "
            "forwards tool requests to the connector on PC 32 via a reverse "
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
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help=(
            "MCP transport: 'stdio' (default, for Claude Desktop / Hermes) or "
            "'sse' (HTTP/SSE server, for MCP Inspector and web-based clients)"
        ),
    )
    parser.add_argument(
        "--http-host",
        default="0.0.0.0",
        help="Bind address for the HTTP/SSE server when --transport=sse (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=7001,
        help="Port for the HTTP/SSE MCP server when --transport=sse (default: 7001)",
    )
    args = parser.parse_args()

    state = RelayState()
    server = build_mcp_server(state)

    # Always start the WebSocket server so PC 32 can connect at any time.
    ws_task = asyncio.create_task(run_ws_server(state, args.host, args.port))

    try:
        if args.transport == "stdio":
            log.info(
                "Starting relay in stdio mode (connector WS on ws://%s:%d)",
                args.host,
                args.port,
            )
            async with stdio_server() as (read_stream, write_stream):
                await server.run(
                    read_stream,
                    write_stream,
                    server.create_initialization_options(),
                )

        else:  # sse
            try:
                from mcp.server.sse import SseServerTransport
                from starlette.responses import Response
                from starlette.types import Receive, Scope, Send
                import uvicorn
            except ImportError as exc:
                print(
                    f"ERROR: SSE transport requires extra packages: {exc}\n"
                    "  pip install 'starlette>=0.49.1' 'uvicorn>=0.34.2'",
                    file=sys.stderr,
                )
                sys.exit(1)

            sse_transport = SseServerTransport("/messages/")
            init_opts = server.create_initialization_options()

            async def sse_endpoint(
                scope: Scope, receive: Receive, send: Send
            ) -> None:
                async with sse_transport.connect_sse(
                    scope, receive, send
                ) as streams:
                    await server.run(streams[0], streams[1], init_opts)

            async def asgi_router(
                scope: Scope, receive: Receive, send: Send
            ) -> None:
                """
                Minimal ASGI router that does NOT strip path prefixes or
                modify scope['root_path'].  This ensures SseServerTransport
                computes the correct client POST URL (/messages/?session_id=…).
                """
                if scope["type"] == "lifespan":
                    event = await receive()
                    if event["type"] == "lifespan.startup":
                        await send({"type": "lifespan.startup.complete"})
                    event = await receive()
                    if event["type"] == "lifespan.shutdown":
                        await send({"type": "lifespan.shutdown.complete"})
                    return

                path: str = scope.get("path", "")

                if scope["type"] == "http":
                    if path == "/sse":
                        await sse_endpoint(scope, receive, send)
                    elif path.startswith("/messages"):
                        await sse_transport.handle_post_message(
                            scope, receive, send
                        )
                    else:
                        await Response("Not Found", status_code=404)(
                            scope, receive, send
                        )

            log.info(
                "Starting relay in SSE mode: http://%s:%d/sse "
                "(connector WS on ws://%s:%d)",
                args.http_host,
                args.http_port,
                args.host,
                args.port,
            )
            uvi_config = uvicorn.Config(
                asgi_router,
                host=args.http_host,
                port=args.http_port,
                log_level="warning",
            )
            uvi_server = uvicorn.Server(uvi_config)
            await uvi_server.serve()

    finally:
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
