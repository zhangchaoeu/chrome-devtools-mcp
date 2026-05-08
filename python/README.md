# Chrome DevTools MCP — Python reverse-tunnel

Two small Python scripts that expose `chrome-devtools-mcp` to an MCP client
(Hermes, Claude Desktop, etc.) running on a server that **cannot** reach the
Windows PC directly.

## Network constraint

```
PC 32  ──► Server 33   (32 can call 33 ✓)
PC 32  ◄── Server 33   (33 cannot call 32 ✗)
```

## Architecture

```
Hermes (33) ──stdio──► relay_server.py (33)
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
  Presents a standard MCP stdio interface to Hermes and listens on a WebSocket
  port for the connector from PC 32.

* **`connector.py`** — runs on **PC 32** (Windows).  
  Dials the relay on 33, spawns `chrome-devtools-mcp` locally, and proxies
  every MCP request/response through the tunnel.

## Requirements

Python ≥ 3.11 on both machines (uses `asyncio` from the standard library).

```bash
pip install -r requirements.txt   # installs websockets>=13
```

## Quick start

### 1. Server 33 — start the relay

```bash
python relay_server.py --port 7000
```

Tell your MCP client to spawn the relay.  
Example `claude_desktop_config.json` / `server.json` entry:

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

## Local testing on a single machine (MCP Inspector)

You can run both scripts on the same Windows PC to verify the relay without
needing two separate hosts.  This lets you point **MCP Inspector** (or any
other MCP client) at the relay and confirm it has identical capabilities to a
direct `chrome-devtools-mcp` connection.

**Terminal 1 — relay server (MCP Inspector will spawn this)**

MCP Inspector spawns the relay as a subprocess and talks to it over stdio, so
no extra terminal is needed for it; Inspector handles that automatically.

**Terminal 1 — connector**

```powershell
# Chrome already open with --remote-debugging-port=9222
python connector.py --relay-url ws://127.0.0.1:7000 --browser-url http://127.0.0.1:9222
```

The connector will keep retrying until the relay is up, so you can start it
before or after Inspector launches the relay.

**MCP Inspector config**

Point MCP Inspector at the relay script using the *stdio* transport:

```json
{
  "command": "python",
  "args": ["C:\\path\\to\\relay_server.py", "--port", "7000", "--host", "127.0.0.1"]
}
```

Once connected, MCP Inspector will show the same tool list that
`chrome-devtools-mcp` exposes directly.

## Options

### relay_server.py

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | `7000` | WebSocket port for connector connections |
| `--host` | `0.0.0.0` | Bind interface |

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

## How it works

1. Hermes spawns `relay_server.py` via stdio and sends `initialize`.  
   The relay responds immediately with its own capabilities — no connector
   needed for the handshake.

2. The connector on 32 dials `ws://33-host:7000` and stays connected.

3. When Hermes calls `tools/list` or `tools/call`, the relay forwards the
   request over the WebSocket to the connector, awaiting the response.

4. The connector proxies the request to the local `chrome-devtools-mcp`
   subprocess (stdio) and sends the result back through the WebSocket.

5. The relay delivers the result to Hermes as a normal MCP response.

All request/response correlation uses UUIDs so multiple concurrent tool calls
are handled safely.
