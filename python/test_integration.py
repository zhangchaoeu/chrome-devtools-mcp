#!/usr/bin/env python3
"""
Integration test for the chrome-devtools relay chain.

Tests the complete path:
  MCP Client (HTTP/SSE) → relay_server.py → connector.py → chrome-devtools-mcp → Chrome

The test starts all components, connects to the relay's SSE endpoint (exactly
as MCP Inspector would), calls tools/list and a real tool, then verifies the
results.

Usage
-----
  python test_integration.py [--mcp-cmd /path/to/chrome-devtools-mcp.js]
                              [--browser-url http://127.0.0.1:9222]

If --browser-url is not given the test starts Chrome itself using Chromium.

Exit codes
----------
  0  all tests passed
  1  one or more tests failed
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

# ── dependency checks ─────────────────────────────────────────────────────────

try:
    import httpx
except ImportError:
    print("ERROR: httpx not installed. Run: pip install 'httpx>=0.27'", file=sys.stderr)
    sys.exit(1)

try:
    import websockets  # noqa: F401  (indirectly needed: relay_server.py is launched as a subprocess)
except ImportError:
    print("ERROR: websockets not installed. Run: pip install 'websockets>=13'", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s [test] %(levelname)s %(message)s",
)
log = logging.getLogger("test")

HERE = os.path.dirname(os.path.abspath(__file__))
RELAY_SCRIPT = os.path.join(HERE, "relay_server.py")
CONNECTOR_SCRIPT = os.path.join(HERE, "connector.py")


# ── SSE client ────────────────────────────────────────────────────────────────


class SseMcpClient:
    """
    Minimal SSE MCP client that mirrors what MCP Inspector does:

    1. GET /sse  → receive SSE stream; first event gives the POST endpoint
    2. POST <endpoint>  → send JSON-RPC request
    3. Read SSE events for responses
    """

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._endpoint: Optional[str] = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._client: Optional[httpx.AsyncClient] = None
        self._sse_task: Optional[asyncio.Task[None]] = None

    async def connect(self, timeout: float = 30.0) -> None:
        """Open the SSE connection, read the endpoint event, and perform MCP handshake."""
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))
        loop = asyncio.get_running_loop()
        endpoint_ready: asyncio.Future[str] = loop.create_future()

        async def sse_reader() -> None:
            async with self._client.stream(  # type: ignore[union-attr]
                "GET",
                f"{self._base_url}/sse",
                headers={"Accept": "text/event-stream"},
            ) as resp:
                resp.raise_for_status()
                event_type = ""
                data_lines: list[str] = []

                async for raw_line in resp.aiter_lines():
                    if raw_line.startswith("event:"):
                        event_type = raw_line[len("event:"):].strip()
                    elif raw_line.startswith("data:"):
                        data_lines.append(raw_line[len("data:"):].strip())
                    elif raw_line == "":
                        # blank line → dispatch event
                        data = "\n".join(data_lines)
                        if event_type == "endpoint":
                            # data is relative path, e.g. /messages/?session_id=...
                            url = (
                                data if data.startswith("http")
                                else f"{self._base_url}{data}"
                            )
                            self._endpoint = url
                            if not endpoint_ready.done():
                                endpoint_ready.set_result(url)
                        elif event_type == "message" and data:
                            try:
                                msg = json.loads(data)
                                req_id = str(msg.get("id", ""))
                                fut = self._pending.pop(req_id, None)
                                if fut and not fut.done():
                                    fut.set_result(msg)
                            except Exception as exc:
                                log.warning("SSE message parse error: %s", exc)
                        event_type = ""
                        data_lines = []

        self._sse_task = asyncio.create_task(sse_reader())

        # Wait for the endpoint event.
        deadline = time.monotonic() + timeout
        while not endpoint_ready.done():
            await asyncio.sleep(0.1)
            if time.monotonic() > deadline:
                raise TimeoutError("SSE endpoint event not received in time")
            if self._sse_task.done():
                exc = self._sse_task.exception()
                raise RuntimeError(f"SSE reader failed: {exc}")

        log.info("SSE connected, endpoint: %s", self._endpoint)

        # MCP protocol requires initialize before any other call.
        init_result = await self._raw_request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "mcp-inspector-test", "version": "0.1.0"},
            },
            timeout=timeout,
        )
        log.info(
            "MCP handshake OK: server=%s",
            init_result.get("serverInfo", {}).get("name", "?"),
        )
        # Send notifications/initialized (no response expected)
        await self._notify("notifications/initialized")

    async def _raw_request(
        self, method: str, params: Optional[dict[str, Any]] = None, timeout: float = 30.0
    ) -> dict[str, Any]:
        """Internal: send a request and return the full result dict."""
        if self._client is None or self._endpoint is None:
            raise RuntimeError("Not connected — call connect() first")

        req_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[req_id] = fut

        payload = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params or {},
        }
        resp = await self._client.post(
            self._endpoint,
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()

        try:
            result_msg = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise TimeoutError(f"No SSE response for {method} within {timeout}s")

        if "error" in result_msg:
            raise RuntimeError(f"MCP error: {result_msg['error']}")
        return result_msg.get("result", {})

    async def _notify(self, method: str, params: Optional[dict[str, Any]] = None) -> None:
        """Send a notification (no id, no response expected)."""
        if self._client is None or self._endpoint is None:
            raise RuntimeError("Not connected")
        payload = {"jsonrpc": "2.0", "method": method, "params": params or {}}
        resp = await self._client.post(
            self._endpoint,
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()

    async def request(
        self, method: str, params: Optional[dict[str, Any]] = None, timeout: float = 30.0
    ) -> dict[str, Any]:
        """Send a JSON-RPC request and return the result."""
        return await self._raw_request(method, params, timeout=timeout)

    async def close(self) -> None:
        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._client:
            await self._client.aclose()


# ── process helpers ───────────────────────────────────────────────────────────


def start_process(cmd: list[str], name: str) -> subprocess.Popen[bytes]:
    log.info("Starting %s: %s", name, " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    return proc


async def wait_for_port(host: str, port: int, timeout: float = 30.0) -> None:
    """Spin until a TCP port accepts connections."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            _, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            if time.monotonic() > deadline:
                raise TimeoutError(f"{host}:{port} not reachable within {timeout}s")
            await asyncio.sleep(0.2)


async def wait_for_log(proc: subprocess.Popen[bytes], text: str, timeout: float = 30.0) -> None:
    """Block until *text* appears on a process's stderr."""
    deadline = time.monotonic() + timeout
    assert proc.stderr is not None
    loop = asyncio.get_running_loop()
    while True:
        if time.monotonic() > deadline:
            raise TimeoutError(f"'{text}' not seen in stderr within {timeout}s")
        line_bytes = await asyncio.wait_for(
            loop.run_in_executor(None, proc.stderr.readline), timeout=5.0
        )
        line = line_bytes.decode("utf-8", errors="replace").rstrip()
        if line:
            log.debug("[%s] %s", os.path.basename(str(proc.args[0])), line)
        if text in line:
            return


def kill_proc(proc: subprocess.Popen[bytes]) -> None:
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


# ── Chrome helper ─────────────────────────────────────────────────────────────


class ChromeManager:
    def __init__(self, port: int = 9226) -> None:
        self.port = port
        self._proc: Optional[subprocess.Popen[bytes]] = None

    def start(self) -> str:
        candidates = [
            "google-chrome",
            "chromium-browser",
            "chromium",
            "/usr/bin/google-chrome",
        ]
        executable = next((c for c in candidates if _which(c)), None)
        if not executable:
            raise RuntimeError("No Chrome/Chromium found in PATH")

        self._proc = subprocess.Popen(
            [
                executable,
                f"--remote-debugging-port={self.port}",
                "--headless=new",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--user-data-dir=/tmp/chrome-test-profile",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        if self._proc:
            kill_proc(self._proc)


def _which(cmd: str) -> str:
    import shutil
    return shutil.which(cmd) or ""


# ── test runner ───────────────────────────────────────────────────────────────


class TestSuite:
    def __init__(self) -> None:
        self._passed = 0
        self._failed = 0

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        if cond:
            self._passed += 1
            log.info("✓ PASS  %s", name)
        else:
            self._failed += 1
            log.error("✗ FAIL  %s  %s", name, detail)

    @property
    def all_passed(self) -> bool:
        return self._failed == 0

    def summary(self) -> None:
        total = self._passed + self._failed
        log.info("Results: %d/%d passed", self._passed, total)


# ── main test ─────────────────────────────────────────────────────────────────

RELAY_WS_PORT = 17100
RELAY_HTTP_PORT = 17101


async def run_tests(mcp_cmd: str, browser_url: str) -> bool:
    """Start all components and run the full integration test suite."""
    ts = TestSuite()
    relay_proc: Optional[subprocess.Popen[bytes]] = None
    connector_proc: Optional[subprocess.Popen[bytes]] = None
    chrome_mgr: Optional[ChromeManager] = None
    client: Optional[SseMcpClient] = None

    try:
        # ── 1. Start relay in SSE mode ────────────────────────────────────
        relay_proc = start_process(
            [
                sys.executable,
                RELAY_SCRIPT,
                "--transport", "sse",
                "--port", str(RELAY_WS_PORT),
                "--http-port", str(RELAY_HTTP_PORT),
                "--http-host", "127.0.0.1",
                "--host", "127.0.0.1",
            ],
            "relay",
        )

        await wait_for_log(relay_proc, "Starting relay in SSE mode", timeout=15)
        await wait_for_port("127.0.0.1", RELAY_HTTP_PORT, timeout=10)
        log.info("Relay is up on http://127.0.0.1:%d/sse", RELAY_HTTP_PORT)

        # ── 2. Optionally start Chrome ────────────────────────────────────
        if not browser_url:
            chrome_mgr = ChromeManager(port=9226)
            browser_url = chrome_mgr.start()
            await asyncio.sleep(3)  # Chrome startup
            log.info("Chrome started at %s", browser_url)

        # ── 3. Start connector ────────────────────────────────────────────
        connector_proc = start_process(
            [
                sys.executable,
                CONNECTOR_SCRIPT,
                "--relay-url", f"ws://127.0.0.1:{RELAY_WS_PORT}",
                "--mcp-cmd", mcp_cmd,
                "--browser-url", browser_url,
            ],
            "connector",
        )

        await wait_for_log(connector_proc, "Connected to relay", timeout=30)
        log.info("Connector registered with relay")

        # ── 4. Connect as MCP client (SSE) ───────────────────────────────
        client = SseMcpClient(f"http://127.0.0.1:{RELAY_HTTP_PORT}")
        await client.connect(timeout=10)

        # ── 5. tools/list ─────────────────────────────────────────────────
        log.info("Calling tools/list …")
        result = await client.request("tools/list", timeout=30)
        tools: list[dict[str, Any]] = result.get("tools", [])
        log.info("Received %d tools from relay", len(tools))

        ts.check("tools/list returns a list", isinstance(tools, list))
        ts.check("tools/list returns at least 1 tool", len(tools) >= 1)
        tool_names = [t["name"] for t in tools]
        log.info("Tool names: %s", tool_names)
        ts.check("tools have 'name' field", all("name" in t for t in tools))
        ts.check(
            "tools have 'inputSchema' field",
            all("inputSchema" in t for t in tools),
        )

        # ── 6. tools/call navigate_page ───────────────────────────────────
        nav_tool = next((n for n in tool_names if "navigate" in n), None)
        if nav_tool:
            log.info("Calling %s tool …", nav_tool)
            nav_result = await client.request(
                "tools/call",
                {"name": nav_tool, "arguments": {"url": "https://example.com"}},
                timeout=30,
            )
            content = nav_result.get("content", [])
            ts.check(f"{nav_tool} returns content", len(content) > 0)
            log.info("%s result: %s", nav_tool, str(content)[:200])
        else:
            log.warning("No navigate tool found — skipping tool call test")

        # ── 7. Verify relay is still healthy ─────────────────────────────
        result2 = await client.request("tools/list", timeout=15)
        ts.check(
            "second tools/list also works",
            len(result2.get("tools", [])) >= 1,
        )

    except Exception as exc:
        log.exception("Test failed with exception: %s", exc)
        ts.check("no unexpected exception", False, str(exc))

    finally:
        if client:
            await client.close()
        if connector_proc:
            kill_proc(connector_proc)
        if relay_proc:
            kill_proc(relay_proc)
        if chrome_mgr:
            chrome_mgr.stop()

    ts.summary()
    return ts.all_passed


def main() -> None:
    parser = argparse.ArgumentParser(description="Integration test for chrome-devtools relay chain")
    parser.add_argument(
        "--mcp-cmd",
        default=os.path.join(
            os.path.dirname(HERE),
            "build", "src", "bin", "chrome-devtools-mcp.js",
        ),
        help="Path to chrome-devtools-mcp JS entry (default: ../build/src/bin/chrome-devtools-mcp.js)",
    )
    parser.add_argument(
        "--browser-url",
        default="",
        help="Chrome DevTools HTTP URL (default: start Chrome automatically)",
    )
    args = parser.parse_args()

    # Resolve mcp_cmd: if it's a .js file, prepend 'node'
    if args.mcp_cmd.endswith(".js"):
        mcp_cmd = f"node {args.mcp_cmd}"
    else:
        mcp_cmd = args.mcp_cmd

    # connector.py needs the full command as a list; pass it as a string
    # that the connector will split — but actually connector just uses the cmd
    # as-is. We need to pass the node invocation correctly.
    # The connector's --mcp-cmd is passed directly to asyncio.create_subprocess_exec,
    # so we need to handle the node prefix here by wrapping in a shell script.
    # Simplest: set mcp_cmd to the full path and let connector handle it.
    if args.mcp_cmd.endswith(".js"):
        # Create a tiny wrapper so connector.py can exec it directly.
        import tempfile
        tmp_dir = tempfile.gettempdir()
        wrapper = os.path.join(tmp_dir, "chrome_devtools_mcp_wrapper.sh")
        with open(wrapper, "w") as f:
            f.write(f"#!/bin/sh\nexec node {args.mcp_cmd} --no-usage-statistics \"$@\"\n")
        os.chmod(wrapper, 0o755)
        mcp_cmd = wrapper
    else:
        mcp_cmd = args.mcp_cmd

    ok = asyncio.run(run_tests(mcp_cmd, args.browser_url))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
