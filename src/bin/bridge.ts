/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * CLI entry point for the Bridge server (runs on PC 32 / Windows machine with Chrome).
 *
 * Usage:
 *   npx chrome-devtools-bridge --port 3000 --browser-url http://127.0.0.1:9222
 *   npx chrome-devtools-bridge --port 3000 --user-data-dir "C:\\Users\\Me\\AppData\\Local\\Google\\Chrome\\User Data"
 *   npx chrome-devtools-bridge --port 3000 --auto-connect
 *
 * The bridge exposes the chrome-devtools-mcp MCP server over HTTP so that the
 * proxy server running on Server 33 (Linux) can forward Hermes tool calls to it.
 */

import '../polyfill.js';

import {startBridgeServer} from '../bridge/bridge-server.js';
import {logger} from '../logger.js';
import {yargs, hideBin} from '../third_party/index.js';
import {VERSION} from '../version.js';

import {cliOptions, parseArguments} from './chrome-devtools-mcp-cli-options.js';

const bridgeOnlyOptions = {
  port: {
    type: 'number' as const,
    description: 'Port to listen on for incoming proxy connections.',
    default: 3000,
  },
  host: {
    type: 'string' as const,
    description:
      'Host (interface) to bind to. Use 0.0.0.0 to accept connections from any network interface.',
    default: '0.0.0.0',
  },
};

// Parse all options together (bridge-specific + MCP browser options).
const rawArgs = yargs(hideBin(process.argv))
  .scriptName('chrome-devtools-bridge')
  .usage('$0 [options]')
  .options({...cliOptions, ...bridgeOnlyOptions})
  .help()
  .version(VERSION)
  .parseSync();

const port: number = rawArgs.port;
const host: string = rawArgs.host;

// Re-parse just the MCP options via the official helper so we get a correctly
// typed ParsedArguments object for createMcpServer.
const mcpArgs = parseArguments(VERSION);

logger(`Starting chrome-devtools-bridge v${VERSION}`);
logger(`Listening on http://${host}:${port}/mcp`);

console.error(
  `chrome-devtools-bridge v${VERSION}
Exposes chrome-devtools-mcp over HTTP so a remote proxy can forward requests.
Listening on http://${host}:${port}/mcp`,
);

await startBridgeServer(mcpArgs, port, host);
