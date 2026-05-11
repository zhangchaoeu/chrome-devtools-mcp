# Chrome DevTools MCP — Python reverse-tunnel

Two small Python scripts that expose `chrome-devtools-mcp` to an MCP agent
running on a server that **cannot** reach the Windows PC directly.

## Network constraint

```
PC 32  ──► Server 33   (32 can call 33 ✓)
PC 32  ◄── Server 33   (33 cannot call 32 ✗)
```

## Architecture

```
MCP Agent (33) ──HTTP/SSE──► relay_server.py (33)
                                      │ tools/list & tools/call only
                              WebSocket server :7000
                                      ▲  ← 32 dials 33
                              WebSocket client
                                      │
                               connector.py (32)
                                      │ subprocess stdio (initialize + tools)
                             chrome-devtools-mcp (32)
                                      │
                                Chrome browser (32)
```

**Two independent MCP sessions**

There are two separate MCP sessions in play, each with its own full lifecycle:

| Session | Between | Who handles it |
|---------|---------|----------------|
| **Agent ↔ Relay** | MCP agent on Server 33 ↔ `relay_server.py` | **`relay_server.py`** is a fully compliant MCP server: it handles the MCP `initialize` request, returns the `initialize` response with server info and capabilities, and processes the `notifications/initialized` notification to mark the session ready |
| **Connector ↔ subprocess** | `connector.py` ↔ `chrome-devtools-mcp` | **`connector.py`** owns the full MCP `initialize` handshake with the subprocess before accepting any requests from the relay |

**What the relay sends to the connector (over WebSocket) is exactly two things:**

- `tools/list` — when the agent queries available tools
- `tools/call` — when the agent invokes a tool

Everything else in the agent-facing MCP session (`initialize`, `notifications/initialized`, capability negotiation, error handling) is managed by the relay itself using the MCP Python SDK. The connector is never involved in those steps.

## Requirements

Python ≥ 3.11 on both machines.

**PC 32 (connector only)**

```bash
pip install "websockets>=13"
```

The connector has a single lightweight dependency and is ready to use out of the box.

**Server 33 (relay)**

```bash
pip install -r requirements.txt
# installs: websockets>=13  mcp>=1.23.0  fastapi>=0.100.0  uvicorn>=0.34.2
```

## Quick start

### 1. Server 33 — start the relay

The relay always runs as an HTTP/SSE server (FastAPI + uvicorn):

```bash
python relay_server.py --port 7000 --http-port 7001
```

MCP Inspector or any HTTP-based MCP client: select **SSE** transport and enter
`http://server33:7001/sse` as the URL, then click **Connect**.

### 2. PC 32 — start the connector

Make sure `chrome-devtools-mcp` is installed and Chrome is running:

```powershell
npm install -g chrome-devtools-mcp
```

Then start the connector (only `websockets` needed):

```powershell
# Chrome open with --remote-debugging-port=9222
python connector.py --relay-url ws://33-host:7000 --browser-url http://127.0.0.1:9222

# Auto-connect to an existing Chrome session
python connector.py --relay-url ws://33-host:7000 --auto-connect

# Specify Chrome user-data-dir
python connector.py --relay-url ws://33-host:7000 `
    --user-data-dir "C:\Users\Me\AppData\Local\Google\Chrome\User Data"
```

The connector:
1. Spawns `chrome-devtools-mcp` as a subprocess
2. Completes the MCP `initialize` handshake with the subprocess
3. Dials the relay at `--relay-url` and stays connected
4. On each `tools/list` or `tools/call` from the relay, forwards the request
   to the subprocess and sends the result back — reconnecting automatically
   if the relay restarts

## Testing with MCP Inspector (single machine)

Run both scripts on the same machine to verify the relay end-to-end.

**Terminal 1 — relay**

```bash
python relay_server.py --http-port 7001 --port 7000 --http-host 127.0.0.1 --host 127.0.0.1
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

## Options

### relay_server.py

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | `7000` | WebSocket port for connector connections from PC 32 |
| `--host` | `0.0.0.0` | Bind interface for the connector WebSocket server |
| `--http-host` | `0.0.0.0` | Bind address for the HTTP/SSE server |
| `--http-port` | `7001` | Port for the HTTP/SSE MCP server |

### connector.py

| Option | Default | Description |
|--------|---------|-------------|
| `--relay-url` | *(required)* | WebSocket URL of relay, e.g. `ws://192.168.1.33:7000` |
| `--mcp-cmd` | `chrome-devtools-mcp` | Path to the `chrome-devtools-mcp` executable |
| `--browser-url` | — | Chrome DevTools HTTP URL, e.g. `http://127.0.0.1:9222` |
| `--ws-endpoint` | — | Chrome DevTools WebSocket URL |
| `--auto-connect` | — | Auto-connect to a running Chrome instance |
| `--user-data-dir` | — | Chrome user-data-dir path |
| `--reconnect-delay` | `5` | Seconds between WebSocket reconnect attempts |
| `--tool-timeout` | `120` | Seconds to wait for a single tool call response |

## Integration test

A self-contained integration test starts the relay and connector automatically,
then runs an MCP Inspector–style SSE client against the full chain:

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
`\n`** — plain newline-delimited JSON, not the HTTP-style Content-Length
framing sometimes described in older MCP documentation.

`connector.py` uses the same newline-delimited framing when talking to the
subprocess, so no special transport layer is needed.

### How the full flow works

**On connector startup (once, before any agent request):**

1. `connector.py` spawns `chrome-devtools-mcp` as a subprocess.
2. The connector sends an MCP `initialize` request to the subprocess.
3. The subprocess replies with its `initialize` response (capabilities, server info).
4. The connector sends `notifications/initialized` to mark the session ready.
5. The connector dials `ws://relay-host:7000` and holds the WebSocket open.

**When an agent connects to the relay:**

1. The agent sends an MCP `initialize` request to `relay_server.py` over HTTP/SSE.
2. The relay responds with its own `initialize` response (capabilities, server info) — handled entirely by the MCP Python SDK, without contacting the connector.
3. The agent sends `notifications/initialized`; the relay acknowledges — session with the agent is now ready.

**On each tool request (agent → relay → connector → subprocess):**

1. The agent calls `tools/list` or `tools/call` on the relay.
2. The relay assigns a UUID to the request and sends it to the connector over the WebSocket.
3. The connector writes the corresponding JSON-RPC request to the subprocess's stdin.
4. `chrome-devtools-mcp` executes the tool in Chrome and writes the result to stdout.
5. The connector reads the result, attaches the original UUID, and sends it back over the WebSocket.
6. The relay resolves the pending future and returns the result to the agent.

All request/response correlation uses UUIDs so multiple concurrent tool calls
are handled safely.

