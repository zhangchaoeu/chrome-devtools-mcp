/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Proxy MCP server (Server 33 / Linux side).
 *
 * Presents a standard MCP server interface to Hermes over stdio, and forwards
 * every tool call transparently to the Bridge server running on PC 32 via the
 * Streamable HTTP transport.
 *
 * The tool list is fetched from the bridge at startup (and can be refreshed on
 * reconnect) so Hermes sees exactly the same tools as if it were talking
 * directly to chrome-devtools-mcp.
 */

import {logger} from '../logger.js';
import {
  Client,
  StreamableHTTPClientTransport,
  LowLevelServer,
  StdioServerTransport,
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '../third_party/index.js';
import {VERSION} from '../version.js';

export async function startProxyServer(bridgeUrl: string): Promise<void> {
  const url = new URL('/mcp', bridgeUrl);

  logger(`Proxy: connecting to bridge at ${url}`);

  const clientTransport = new StreamableHTTPClientTransport(url);

  const remoteClient = new Client(
    {name: 'chrome-devtools-proxy', version: VERSION},
    {capabilities: {}},
  );

  await remoteClient.connect(clientTransport);
  logger('Proxy: connected to bridge');

  // Create a low-level MCP Server that acts as the stdio endpoint for Hermes.
  // We handle tools/list and tools/call manually so we can forward them to the
  // remote client without needing to register Zod schemas for every tool.
  const proxyServer = new LowLevelServer(
    {name: 'chrome-devtools-proxy', version: VERSION},
    {capabilities: {tools: {}}},
  );

  proxyServer.setRequestHandler(ListToolsRequestSchema, async request => {
    logger('Proxy: forwarding tools/list');
    const result = await remoteClient.listTools(request.params);
    return result;
  });

  proxyServer.setRequestHandler(CallToolRequestSchema, async request => {
    logger(`Proxy: forwarding tools/call ${request.params.name}`);
    const result = await remoteClient.callTool(
      {
        name: request.params.name,
        arguments: request.params.arguments,
      },
      undefined,
    );
    return result;
  });

  const stdioTransport = new StdioServerTransport();
  await proxyServer.connect(stdioTransport);
  logger('Proxy: stdio transport connected, ready for Hermes');

  // Forward close signals so everything shuts down cleanly.
  stdioTransport.onclose = () => {
    void remoteClient.close();
  };
}
