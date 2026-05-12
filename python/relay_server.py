#!/usr/bin/env python3
"""
chrome-devtools relay server -- runs on Server 33.

Accepts a reverse WebSocket connection from connector.py on PC 32 and
exposes tools/list and tools/call to MCP agents via stdio.

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

Multi-agent support
-------------------
  The first relay_server.py that starts becomes the *primary*: it binds the
  WebSocket port and creates a UNIX-domain socket at /tmp/relay-{port}.sock.

  Every subsequent relay_server.py detects the primary via that UNIX socket and
  becomes a *secondary*: it bridges its own stdin/stdout to the primary over the
  UNIX socket, so all agents share the same connector WebSocket connection.

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
import os
import sys
import tempfile
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

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
    """Build an MCP Server that proxies tools/list and tools/call to the connector."""
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


async def run_relay(ws_host: str, ws_port: int, connector_timeout: float) -> None:
    """
    Start the relay.

    If no other relay is running on this port, become the *primary*: start the
    WebSocket server, open a UNIX-socket server for secondaries, and serve MCP
    over stdio.

    If a primary is already running, become a *secondary*: bridge this
    process's stdio to the primary over its UNIX socket so both agents share
    the same connector WebSocket connection.
    """
    sock_path = _sock_path(ws_port)

    if not await _check_if_primary(sock_path):
        # Secondary mode — just bridge stdio to the running primary.
        await _run_secondary(sock_path)
        return

    # Primary mode: WebSocket server + UNIX socket server + stdio MCP session.
    log.info("Primary mode: starting WebSocket server and UNIX socket server")
    state = RelayState(connector_timeout=connector_timeout)
    server = build_mcp_server(state)
    init_opts = server.create_initialization_options()

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

    log.info(
        "Starting relay: stdio MCP server, connector WS on ws://%s:%d, connector-timeout=%.1fs",
        args.host,
        args.port,
        args.connector_timeout,
    )
    asyncio.run(run_relay(args.host, args.port, args.connector_timeout))


if __name__ == "__main__":
    main()
