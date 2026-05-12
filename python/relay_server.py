#!/usr/bin/env python3
"""
chrome-devtools relay server -- runs on Server 33.

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
  MCP Agent A        MCP Agent B   (multiple agents OK)
       | stdio             | stdio
  relay_server.py    relay_server.py  ← secondary: bridges to primary via UNIX socket
  (primary, 33)      (secondary, 33)
       |                   |
       +-------------------+  /tmp/relay-{port}.sock
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

Multi-agent support (stdio mode)
---------------------------------
  The first relay_server.py that starts becomes the *primary*: it binds the
  WebSocket port and creates a UNIX-domain socket at /tmp/relay-{port}.sock.

  Every subsequent relay_server.py detects the primary via that UNIX socket and
  becomes a *secondary*: it bridges its own stdin/stdout to the primary over the
  UNIX socket, so all agents share the same connector WebSocket connection.

  In SSE mode each HTTP connection is already independent, so no UNIX socket is
  needed — multiple agents simply open separate SSE connections to the same server.

Usage (on Server 33)
--------------------
  # stdio mode (Claude Desktop / Hermes / multi-Hermes):
  python relay_server.py [--port 7000]

  # SSE/HTTP mode (MCP Inspector, connect via URL http://server33:7001/sse):
  python relay_server.py --transport sse [--http-port 7001] [--port 7000]

Requirements
------------
  pip install -r requirements.txt
  # websockets>=13  mcp>=1.23.0
  # For SSE mode also: starlette>=0.49.1  uvicorn>=0.34.2
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import sys
import tempfile
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Iterable, Optional

from pydantic import AnyUrl

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

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
    from mcp.shared.message import SessionMessage
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

    def __init__(self, connector_timeout: float = 30.0) -> None:
        self._connector: Optional[ServerConnection] = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()
        # Signalled whenever a connector is present; cleared when it leaves.
        self._connected = asyncio.Event()
        self.connector_timeout = connector_timeout

    async def wait_for_connector(self) -> None:
        """
        Block until a connector is registered or connector_timeout seconds
        elapse.  Raises ConnectionError on timeout.
        """
        if self._connected.is_set():
            return
        log.info(
            "Waiting up to %.1fs for connector to connect ...",
            self.connector_timeout,
        )
        try:
            await asyncio.wait_for(
                self._connected.wait(), timeout=self.connector_timeout
            )
        except asyncio.TimeoutError:
            raise ConnectionError(
                f"No connector connected after {self.connector_timeout:.0f}s. "
                "Start connector.py on PC 32 first."
            )
        log.info("Connector is now available, proceeding")

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
        self._connected.set()
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
        self._connected.clear()
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
        log.debug(
            "relay->connector  id=%s  method=%s  payload=%s",
            req_id,
            method,
            payload,
        )
        await connector.send(payload)
        result = await fut
        log.debug(
            "connector->relay  id=%s  method=%s  result_keys=%s",
            req_id,
            method,
            list(result.keys()),
        )
        return result

    async def resolve(self, data: dict[str, Any]) -> None:
        """Resolve a pending future from a message sent by the connector."""
        req_id = data.get("id")
        if not req_id:
            log.debug("connector->relay  message without id (ignored): %r", str(data)[:200])
            return

        log.debug("connector->relay  raw response id=%s", req_id)
        async with self._lock:
            fut = self._pending.pop(req_id, None)

        if fut is None or fut.done():
            log.warning("No pending request for id=%s", req_id)
            return

        if "error" in data:
            log.warning("connector->relay  error for id=%s: %s", req_id, data["error"])
            fut.set_exception(RuntimeError(str(data["error"])))
        else:
            fut.set_result(data.get("result", {}))


# -- WebSocket handler (one per connector connection from 32) ----------------


async def handle_connector(
    websocket: ServerConnection,
    state: RelayState,
) -> None:
    """Called once per incoming WebSocket connection from the connector on 32."""
    remote = websocket.remote_address
    log.info("Connector connected from %s", remote)
    await state.set_connector(websocket)
    try:
        async for raw in websocket:
            log.debug("connector->relay  raw frame: %r", str(raw)[:500])
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
    """
    Build an MCP Server that proxies tools, prompts, and resources to the
    connector on PC 32.
    """
    server: Server = Server("chrome-devtools-relay")

    @server.list_tools()
    async def list_tools() -> list[mcp_types.Tool]:
        log.info("tools/list requested by MCP agent")
        try:
            await state.wait_for_connector()
            result = await state.forward("tools/list", {})
        except ConnectionError as exc:
            log.warning("tools/list: connector not available -- %s", exc)
            return []
        tools = result.get("tools", [])
        log.info("tools/list: returning %d tool(s)", len(tools))
        log.debug("tools/list: names=%s", [t.get("name") for t in tools])
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
        log.info("tools/call  name=%s  arguments=%r", name, arguments)
        result = await state.forward(
            "tools/call", {"name": name, "arguments": arguments or {}}
        )
        content = result.get("content", [])
        log.debug("tools/call  name=%s  content_items=%d", name, len(content))
        return [_to_content_item(item) for item in content]

    @server.list_prompts()
    async def list_prompts() -> list[mcp_types.Prompt]:
        try:
            result = await state.forward("prompts/list", {})
        except Exception as exc:
            log.debug("prompts/list: returning empty list (%s)", exc)
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
        except Exception as exc:
            log.debug("resources/list: returning empty list (%s)", exc)
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


# -- Multi-instance support: primary/secondary via UNIX socket ---------------


def _sock_path(port: int) -> str:
    """
    UNIX-domain socket path used to share a primary relay with secondaries.

    Note: UNIX-domain sockets are not supported on Windows.  The relay server
    is designed for Linux/macOS deployments.
    """
    return os.path.join(tempfile.gettempdir(), f"relay-{port}.sock")


async def _check_if_primary(sock_path: str) -> bool:
    """
    Return True if this process should run as primary (no live relay found).
    Return False if another relay is already listening on *sock_path*.
    """
    try:
        _, writer = await asyncio.open_unix_connection(sock_path)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return False  # connected → we are a secondary
    except (ConnectionRefusedError, FileNotFoundError, OSError):
        return True   # no relay present → we are the primary


@asynccontextmanager
async def _unix_socket_mcp_session(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> AsyncIterator[
    tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
    ]
]:
    """
    Wrap an asyncio UNIX-socket stream pair as anyio MCP memory-object streams,
    mirroring what ``mcp.server.stdio.stdio_server()`` does for stdin/stdout.
    """
    read_stream_writer: MemoryObjectSendStream[SessionMessage | Exception]
    read_stream: MemoryObjectReceiveStream[SessionMessage | Exception]
    write_stream: MemoryObjectSendStream[SessionMessage]
    write_stream_reader: MemoryObjectReceiveStream[SessionMessage]

    read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(0)

    async def socket_reader() -> None:
        async with read_stream_writer:
            while True:
                line_bytes = await reader.readline()
                if not line_bytes:
                    break
                line = line_bytes.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    message = mcp_types.JSONRPCMessage.model_validate_json(line)
                    await read_stream_writer.send(SessionMessage(message))
                except Exception as exc:
                    await read_stream_writer.send(exc)

    async def socket_writer() -> None:
        async with write_stream_reader:
            async for session_message in write_stream_reader:
                json_str = session_message.message.model_dump_json(
                    by_alias=True, exclude_none=True
                )
                writer.write((json_str + "\n").encode("utf-8"))
                await writer.drain()

    async with anyio.create_task_group() as tg:
        tg.start_soon(socket_reader)
        tg.start_soon(socket_writer)
        try:
            yield read_stream, write_stream
        finally:
            tg.cancel_scope.cancel()


async def _handle_unix_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    server: Server,
    init_opts: Any,
) -> None:
    """Serve one secondary relay as an independent MCP session over a UNIX socket."""
    log.info("Secondary relay connected via UNIX socket")
    try:
        async with _unix_socket_mcp_session(reader, writer) as (
            read_stream,
            write_stream,
        ):
            await server.run(read_stream, write_stream, init_opts)
    except Exception as exc:
        log.info("Secondary relay session ended: %s", exc)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    log.info("Secondary relay disconnected")


async def _run_unix_socket_server(
    sock_path: str,
    server: Server,
    init_opts: Any,
) -> None:
    """
    Listen on *sock_path* and serve each connecting secondary relay as an
    independent MCP session, all backed by the same :class:`RelayState`.
    """
    # Remove a stale socket file left by a crashed previous primary.
    try:
        os.unlink(sock_path)
    except FileNotFoundError:
        pass

    async def client_cb(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await _handle_unix_client(reader, writer, server, init_opts)

    unix_server = await asyncio.start_unix_server(client_cb, path=sock_path)
    log.info("Secondary-relay UNIX socket listening at %s", sock_path)
    async with unix_server:
        await unix_server.serve_forever()


async def _run_secondary(sock_path: str) -> None:
    """
    Secondary mode: bridge this process's stdin/stdout to the primary relay
    over its UNIX socket so that both Hermes instances share one connector.
    """
    log.info("Secondary mode: bridging stdio → primary relay at %s", sock_path)
    reader, writer = await asyncio.open_unix_connection(sock_path)

    stdin_buf = sys.stdin.buffer
    stdout_buf = sys.stdout.buffer

    async def stdin_to_socket() -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                # read1() returns whatever is buffered (up to N bytes) without
                # waiting for more; run in executor so we don't block the loop.
                chunk = await loop.run_in_executor(None, stdin_buf.read1, 65536)
                if not chunk:
                    break
                writer.write(chunk)
                await writer.drain()
        except Exception as exc:
            log.debug("stdin→socket bridge ended: %s", exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def socket_to_stdout() -> None:
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                stdout_buf.write(chunk)
                stdout_buf.flush()
        except Exception as exc:
            log.debug("socket→stdout bridge ended: %s", exc)

    # Run both directions; stop as soon as either side closes.
    loop = asyncio.get_running_loop()
    stdin_task = loop.create_task(stdin_to_socket())
    stdout_task = loop.create_task(socket_to_stdout())
    done, pending = await asyncio.wait(
        [stdin_task, stdout_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for t in pending:
        t.cancel()
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
    log.info("Secondary relay bridge closed")


# -- WebSocket connector server ----------------------------------------------
async def run_ws_server(state: RelayState, host: str, port: int) -> None:
    """Run the reverse-tunnel WebSocket server that PC 32 dials into."""
    async with serve(lambda ws: handle_connector(ws, state), host, port):
        log.info(
            "Connector WebSocket server listening on ws://%s:%d", host, port
        )
        await asyncio.get_running_loop().create_future()


# -- Entry point -------------------------------------------------------------


async def run_relay(
    ws_host: str,
    ws_port: int,
    connector_timeout: float,
    transport: str = "stdio",
    http_host: str = "0.0.0.0",
    http_port: int = 7001,
) -> None:
    """
    Start the relay.

    stdio mode (default):
      If no other relay is running on this port, become the *primary*: start the
      WebSocket server, open a UNIX-socket server for secondaries, and serve MCP
      over stdio.

      If a primary is already running, become a *secondary*: bridge this
      process's stdio to the primary over its UNIX socket so both agents share
      the same connector WebSocket connection.

    sse mode:
      Start an HTTP/SSE server on *http_host*:*http_port*.  Each connecting MCP
      client gets its own session backed by the shared RelayState, so multiple
      agents work naturally without UNIX-socket bridging.
    """
    state = RelayState(connector_timeout=connector_timeout)
    server = build_mcp_server(state)
    init_opts = server.create_initialization_options()

    if transport == "sse":
        # SSE mode: HTTP server, no UNIX socket needed (each HTTP connection
        # is an independent session that all share the same RelayState).
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

        async def sse_endpoint(
            scope: Scope, receive: Receive, send: Send
        ) -> None:
            async with sse_transport.connect_sse(scope, receive, send) as streams:
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
                    await sse_transport.handle_post_message(scope, receive, send)
                else:
                    await Response("Not Found", status_code=404)(
                        scope, receive, send
                    )

        ws_task = asyncio.create_task(run_ws_server(state, ws_host, ws_port))
        try:
            log.info(
                "Starting relay in SSE mode: http://%s:%d/sse "
                "(connector WS on ws://%s:%d)",
                http_host, http_port, ws_host, ws_port,
            )
            uvi_config = uvicorn.Config(
                asgi_router,
                host=http_host,
                port=http_port,
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
        return

    # stdio mode: primary/secondary via UNIX socket.
    sock_path = _sock_path(ws_port)

    if not await _check_if_primary(sock_path):
        # Secondary mode — just bridge stdio to the running primary.
        await _run_secondary(sock_path)
        return

    # Primary mode: WebSocket server + UNIX socket server + stdio MCP session.
    log.info(
        "Primary mode: stdio MCP server, connector WS on ws://%s:%d, "
        "connector-timeout=%.1fs",
        ws_host, ws_port, connector_timeout,
    )
    ws_task = asyncio.create_task(run_ws_server(state, ws_host, ws_port))
    unix_task = asyncio.create_task(
        _run_unix_socket_server(sock_path, server, init_opts)
    )
    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, init_opts)
    finally:
        ws_task.cancel()
        unix_task.cancel()
        for task in [ws_task, unix_task]:
            try:
                await task
            except asyncio.CancelledError:
                pass
        # Remove the UNIX socket so stale files don't confuse future starts.
        try:
            os.unlink(sock_path)
        except FileNotFoundError:
            pass


def main() -> None:
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
        help=(
            "Bind address for the HTTP/SSE server when --transport=sse "
            "(default: 0.0.0.0)"
        ),
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=7001,
        help="Port for the HTTP/SSE MCP server when --transport=sse (default: 7001)",
    )
    parser.add_argument(
        "--connector-timeout",
        type=float,
        default=30.0,
        help=(
            "Seconds to wait for the PC 32 connector to connect before "
            "giving up on a tools/list request (default: 30)"
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

    asyncio.run(
        run_relay(
            ws_host=args.host,
            ws_port=args.port,
            connector_timeout=args.connector_timeout,
            transport=args.transport,
            http_host=args.http_host,
            http_port=args.http_port,
        )
    )


if __name__ == "__main__":
    main()
