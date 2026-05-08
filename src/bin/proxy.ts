/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * CLI entry point for the Proxy MCP server (runs on Server 33 / Linux).
 *
 * The proxy presents a standard MCP server interface (stdio) to Hermes and
 * forwards every request to the Bridge server that runs on PC 32 over HTTP.
 *
 * Usage (on Server 33):
 *   npx chrome-devtools-proxy --bridge-url http://192.168.1.32:3000
 *
 * MCP client config example (for Hermes / claude_desktop_config.json):
 *   {
 *     "mcpServers": {
 *       "chrome_devtools": {
 *         "command": "npx",
 *         "args": ["chrome-devtools-proxy", "--bridge-url", "http://192.168.1.32:3000"]
 *       }
 *     }
 *   }
 */

import '../polyfill.js';

import {logger} from '../logger.js';
import {startProxyServer} from '../proxy/proxy-server.js';
import {yargs, hideBin} from '../third_party/index.js';
import {VERSION} from '../version.js';

const args = yargs(hideBin(process.argv))
  .scriptName('chrome-devtools-proxy')
  .usage('$0 --bridge-url <url>')
  .option('bridge-url', {
    type: 'string',
    description:
      'HTTP URL of the Bridge server running on PC 32, e.g. http://192.168.1.32:3000',
    demandOption: true,
  })
  .help()
  .version(VERSION)
  .parseSync();

logger(`Starting chrome-devtools-proxy v${VERSION}`);

await startProxyServer(args.bridgeUrl);
