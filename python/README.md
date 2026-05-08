# Chrome DevTools MCP — Python reverse-tunnel

Two small Python scripts that expose `chrome-devtools-mcp` to an MCP client
(Hermes, Claude Desktop, MCP Inspector, etc.) running on a server that
**cannot** reach the Windows PC directly.

## Network constraint

```
PC 32  ──► Server 33   (32 can call 33 ✓)
PC 32  ◄── Server 33   (33 cannot call 32 ✗)
```

## Architecture

```
MCP Client (33) ──stdio or HTTP/SSE──► relay_server.py (33)
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
```

* **`relay_server.py`** — runs on **Server 33**.  
  Presents a full MCP server interface (stdio **or** HTTP/SSE) to any MCP
  client. Listens on a WebSocket port for the reverse connection from PC 32.

* **`connector.py`** — runs on **PC 32** (Windows).  
  Dials the relay on 33, spawns `chrome-devtools-mcp` locally, and proxies
  every MCP request/response through the tunnel.

## Requirements

Python ≥ 3.11 on both machines.

```bash
pip install -r requirements.txt
# installs: websockets>=13  mcp>=1.23.0  starlette>=0.49.1  uvicorn>=0.34.2
```

## Quick start

### 1. Server 33 — start the relay

**stdio mode** (for Claude Desktop / Hermes — they spawn the relay as a
subprocess):

```bash
python relay_server.py --port 7000
```

Config entry for `claude_desktop_config.json` / `server.json`:

```json
{
  "mcpServers": {
    "chrome_devtools": {
      "command": "python",
      "args": ["/path/to/relay_server.py", "--port", "7000"]
    }
  }
}
```

**SSE/HTTP mode** (for MCP Inspector and any HTTP-based MCP client):

```bash
python relay_server.py --transport sse --http-port 7001 --port 7000
```

MCP Inspector: select **SSE** transport and enter
`http://server33:7001/sse` as the URL, then click **Connect**.

### 2. PC 32 — start the connector

First make sure `chrome-devtools-mcp` is installed:

```powershell
npm install -g chrome-devtools-mcp
```

Then start the connector:

```powershell
# Chrome open with --remote-debugging-port=9222
python connector.py --relay-url ws://33-host:7000 --browser-url http://127.0.0.1:9222

# Auto-connect to an existing Chrome session
python connector.py --relay-url ws://33-host:7000 --auto-connect

# Specify Chrome user-data-dir
python connector.py --relay-url ws://33-host:7000 `
    --user-data-dir "C:\Users\Me\AppData\Local\Google\Chrome\User Data"
```

The connector reconnects automatically if the relay restarts.

## Testing with MCP Inspector (single machine)

You can run both scripts on the same machine to verify the relay end-to-end.

**Terminal 1 — relay in SSE mode**

```bash
python relay_server.py --transport sse --http-port 7001 --port 7000
```

**Terminal 2 — connector**

```bash
python connector.py --relay-url ws://127.0.0.1:7000 --browser-url http://127.0.0.1:9222
```

**MCP Inspector**

```bash
npx @modelcontextprotocol/inspector
```

In the Inspector web UI:
1. Select transport **SSE**
2. Enter URL `http://127.0.0.1:7001/sse`
3. Click **Connect**

The Inspector will show the same tool list that `chrome-devtools-mcp` exposes
directly.

> **stdio mode with MCP Inspector**  
> Inspector can also spawn the relay directly. Select **stdio** transport,
> set command to `python` and args to `relay_server.py --port 7000`, then
> click **Connect**. Start the connector in a separate terminal as above.

## Options

### relay_server.py

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | `7000` | WebSocket port for connector connections from PC 32 |
| `--host` | `0.0.0.0` | Bind interface for the connector WebSocket server |
| `--transport` | `stdio` | MCP transport: `stdio` or `sse` |
| `--http-host` | `0.0.0.0` | Bind address for the HTTP/SSE server (`--transport sse`) |
| `--http-port` | `7001` | Port for the HTTP/SSE MCP server (`--transport sse`) |

### connector.py

| Option | Default | Description |
|--------|---------|-------------|
| `--relay-url` | *(required)* | WebSocket URL of relay, e.g. `ws://192.168.1.33:7000` |
| `--mcp-cmd` | `chrome-devtools-mcp` | Path to the MCP executable |
| `--browser-url` | — | Chrome DevTools HTTP URL |
| `--ws-endpoint` | — | Chrome DevTools WebSocket URL |
| `--auto-connect` | — | Auto-connect to running Chrome |
| `--user-data-dir` | — | Chrome user-data-dir path |
| `--reconnect-delay` | `5` | Seconds between reconnect attempts |
| `--tool-timeout` | `120` | Seconds to wait for a single tool call |

## Integration test

A self-contained integration test is included that starts Chrome, the relay, and
the connector automatically, then runs an MCP Inspector–style SSE client against
the full chain:

```bash
# Install test dependency
pip install 'httpx>=0.27'

# Run the test (auto-starts Chrome headlessly)
python test_integration.py

# Or point at an already-running Chrome instance
python test_integration.py --browser-url http://127.0.0.1:9222
```

Expected output:
```
✓ PASS  tools/list returns a list
✓ PASS  tools/list returns at least 1 tool
✓ PASS  tools have 'name' field
✓ PASS  tools have 'inputSchema' field
✓ PASS  navigate_page returns content
✓ PASS  second tools/list also works
Results: 6/6 passed
```

## Protocol notes

### Stdio framing

`chrome-devtools-mcp` uses `@modelcontextprotocol/sdk` (Node.js) which
serialises every JSON-RPC message as a **single UTF-8 line terminated with
`\n`** — plain newline-delimited JSON.  This is *not* the HTTP-style
Content-Length framing sometimes described in older MCP documentation.
`connector.py` uses the same newline-delimited format when communicating
with the subprocess.

### SSE routing

`relay_server.py` uses a pure ASGI router (not Starlette's `Mount`) for the
SSE transport.  Starlette's `Mount` strips path prefixes and adds them to
`scope["root_path"]`, which caused `SseServerTransport` to compute wrong
client POST URLs.  The hand-written ASGI router passes all request paths
unchanged, so the transport computes `/messages/?session_id=…` correctly.



1. The MCP client (Hermes / MCP Inspector) connects to `relay_server.py` over
   stdio or HTTP/SSE.  The relay uses the **official MCP Python SDK** to handle
   the initialize handshake and all protocol details correctly.

2. The connector on PC 32 dials `ws://33-host:7000` and stays connected.

3. When the client calls `tools/list` or `tools/call`, the relay forwards the
   request over the WebSocket to the connector, awaiting the response.

4. The connector proxies the request to the local `chrome-devtools-mcp`
   subprocess (stdio) and sends the result back through the WebSocket.

5. The relay delivers the result to the client as a normal MCP response.

All request/response correlation uses UUIDs so multiple concurrent tool calls
are handled safely.
