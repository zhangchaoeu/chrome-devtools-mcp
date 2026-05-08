/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Bridge server (PC 32 side).
 *
 * Wraps the existing chrome-devtools-mcp MCP server and exposes it over HTTP
 * using the Streamable HTTP transport from the MCP SDK. The bridge listens for
 * incoming connections from the proxy running on Server 33 and forwards every
 * MCP message to a real Chrome browser that is already open on this machine.
 *
 * Each new MCP session (identified by the Mcp-Session-Id header) gets its own
 * McpServer instance so sessions are properly isolated. The underlying browser
 * connection is shared via the module-level singleton in browser.ts.
 */

import {randomUUID} from 'node:crypto';
import http from 'node:http';

import type {parseArguments} from '../bin/chrome-devtools-mcp-cli-options.js';
import {createMcpServer} from '../index.js';
import {logger} from '../logger.js';
import {StreamableHTTPServerTransport} from '../third_party/index.js';

type McpArgs = ReturnType<typeof parseArguments>;

interface Session {
  transport: StreamableHTTPServerTransport;
}

export async function startBridgeServer(
  serverArgs: McpArgs,
  port: number,
  host: string,
): Promise<http.Server> {
  const sessions = new Map<string, Session>();

  const httpServer = http.createServer(
    (req: http.IncomingMessage, res: http.ServerResponse) => {
      void handleRequest(req, res, serverArgs, sessions);
    },
  );

  await new Promise<void>((resolve, reject) => {
    httpServer.once('error', reject);
    httpServer.listen(port, host, () => {
      httpServer.off('error', reject);
      resolve();
    });
  });

  logger(`Bridge server listening on http://${host}:${port}`);
  return httpServer;
}

async function handleRequest(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  serverArgs: McpArgs,
  sessions: Map<string, Session>,
): Promise<void> {
  if (req.url !== '/mcp') {
    res.writeHead(404, {'Content-Type': 'text/plain'});
    res.end('Not Found – use POST /mcp');
    return;
  }

  const sessionHeader = req.headers['mcp-session-id'];
  const existingSessionId =
    typeof sessionHeader === 'string' ? sessionHeader : undefined;

  // Re-use an existing session if we already know about it.
  if (existingSessionId) {
    const session = sessions.get(existingSessionId);
    if (session) {
      await session.transport.handleRequest(req, res);
      return;
    }
    // The client claims a session ID we don't know about – it might have been
    // lost on a server restart. Fall through to create a new session.
  }

  // New session: create a fresh MCP server and transport.
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: randomUUID,
    onsessioninitialized: (sessionId: string) => {
      sessions.set(sessionId, {transport});
      logger(`Bridge: new session ${sessionId}`);
    },
  });

  transport.onclose = () => {
    const sid = transport.sessionId;
    if (sid) {
      sessions.delete(sid);
      logger(`Bridge: session ${sid} closed`);
    }
  };

  // createMcpServer connects to the browser on first tool call (lazy), so we
  // don't need to await a browser connection here.
  const {server} = await createMcpServer(serverArgs, {});
  await server.connect(transport);

  await transport.handleRequest(req, res);
}
