/**
 * NanoClaw Agent Runner
 * Runs inside a container, receives config via stdin, outputs result to stdout
 *
 * Single-shot mode (default):
 *   Stdin: Full ContainerInput JSON (read until EOF)
 *   IPC:   Follow-up messages at /workspace/ipc/input/ ; _close sentinel exits loop
 *   Output: Stdout markers (OUTPUT_START/END)
 *
 * Daemon mode:
 *   Stdin: GroupConfig JSON with mode: 'daemon'
 *   IPC:   Per-user dirs at /workspace/ipc/users/<senderJid>/input/ and output/
 *   Output: Per-user result files (no stdout markers)
 */

import fs from 'fs';
import path from 'path';
import { query, HookCallback, PreCompactHookInput } from '@anthropic-ai/claude-agent-sdk';
import { fileURLToPath } from 'url';

// Utility for atomic file writes (shared pattern across container/host)
function writeFileAtomic(filePath: string, content: string): void {
  const tempPath = `${filePath}.tmp`;
  fs.writeFileSync(tempPath, content);
  fs.renameSync(tempPath, filePath);
}

interface ContainerInput {
  prompt: string;
  sessionId?: string;
  groupFolder: string;
  chatJid: string;
  isMain: boolean;
  isScheduledTask?: boolean;
  assistantName?: string;
}

interface ContainerOutput {
  status: 'success' | 'error';
  result: string | null;
  newSessionId?: string;
  error?: string;
}

interface GroupConfig {
  mode: 'daemon';
  groupFolder: string;
  chatJid: string;
  isMain: boolean;
  assistantName?: string;
}

interface UserMessageFile {
  type: 'message';
  text: string;
  sessionId?: string;
  chatJid: string;
  requestId: string;
  senderJid: string;
}

interface UserState {
  stream: MessageStream | null;
  sessionId: string | undefined;
  busy: boolean;
  lastAssistantUuid: string | undefined;
}

interface SessionEntry {
  sessionId: string;
  fullPath: string;
  summary: string;
  firstPrompt: string;
}

interface SessionsIndex {
  entries: SessionEntry[];
}

interface SDKUserMessage {
  type: 'user';
  message: { role: 'user'; content: string };
  parent_tool_use_id: null;
  session_id: string;
}

const IPC_INPUT_DIR = '/workspace/ipc/input';
const IPC_INPUT_CLOSE_SENTINEL = path.join(IPC_INPUT_DIR, '_close');
const IPC_POLL_MS = 500;
const USERS_IPC_BASE = '/workspace/ipc/users';

/**
 * Push-based async iterable for streaming user messages to the SDK.
 * Keeps the iterable alive until end() is called, preventing isSingleUserTurn.
 */
class MessageStream {
  private queue: SDKUserMessage[] = [];
  private waiting: (() => void) | null = null;
  private done = false;

  push(text: string): void {
    this.queue.push({
      type: 'user',
      message: { role: 'user', content: text },
      parent_tool_use_id: null,
      session_id: '',
    });
    this.waiting?.();
  }

  end(): void {
    this.done = true;
    this.waiting?.();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<SDKUserMessage> {
    while (true) {
      while (this.queue.length > 0) {
        yield this.queue.shift()!;
      }
      if (this.done) return;
      await new Promise<void>(r => { this.waiting = r; });
      this.waiting = null;
    }
  }
}

async function readStdin(): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', chunk => { data += chunk; });
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });
}

const OUTPUT_START_MARKER = '---NANOCLAW_OUTPUT_START---';
const OUTPUT_END_MARKER = '---NANOCLAW_OUTPUT_END---';

function writeOutput(output: ContainerOutput): void {
  console.log(OUTPUT_START_MARKER);
  console.log(JSON.stringify(output));
  console.log(OUTPUT_END_MARKER);
}

function writeUserOutput(senderJid: string, requestId: string, seq: number, output: ContainerOutput): void {
  const outDir = path.join(USERS_IPC_BASE, senderJid, 'output', requestId);
  fs.mkdirSync(outDir, { recursive: true });
  const seqStr = seq.toString().padStart(6, '0');
  const finalPath = path.join(outDir, `${seqStr}.json`);
  writeFileAtomic(finalPath, JSON.stringify(output));
}

function log(message: string): void {
  console.error(`[agent-runner] ${message}`);
}

function getSessionSummary(sessionId: string, transcriptPath: string): string | null {
  const projectDir = path.dirname(transcriptPath);
  const indexPath = path.join(projectDir, 'sessions-index.json');

  if (!fs.existsSync(indexPath)) {
    log(`Sessions index not found at ${indexPath}`);
    return null;
  }

  try {
    const index: SessionsIndex = JSON.parse(fs.readFileSync(indexPath, 'utf-8'));
    const entry = index.entries.find(e => e.sessionId === sessionId);
    if (entry?.summary) {
      return entry.summary;
    }
  } catch (err) {
    log(`Failed to read sessions index: ${err instanceof Error ? err.message : String(err)}`);
  }

  return null;
}

/**
 * Archive the full transcript to conversations/ before compaction.
 */
function createPreCompactHook(assistantName?: string): HookCallback {
  return async (input, _toolUseId, _context) => {
    const preCompact = input as PreCompactHookInput;
    const transcriptPath = preCompact.transcript_path;
    const sessionId = preCompact.session_id;

    if (!transcriptPath || !fs.existsSync(transcriptPath)) {
      log('No transcript found for archiving');
      return {};
    }

    try {
      const content = fs.readFileSync(transcriptPath, 'utf-8');
      const messages = parseTranscript(content);

      if (messages.length === 0) {
        log('No messages to archive');
        return {};
      }

      const summary = getSessionSummary(sessionId, transcriptPath);
      const name = summary ? sanitizeFilename(summary) : generateFallbackName();

      const conversationsDir = '/workspace/group/conversations';
      fs.mkdirSync(conversationsDir, { recursive: true });

      const date = new Date().toISOString().split('T')[0];
      const filename = `${date}-${name}.md`;
      const filePath = path.join(conversationsDir, filename);

      const markdown = formatTranscriptMarkdown(messages, summary, assistantName);
      fs.writeFileSync(filePath, markdown);

      log(`Archived conversation to ${filePath}`);
    } catch (err) {
      log(`Failed to archive transcript: ${err instanceof Error ? err.message : String(err)}`);
    }

    return {};
  };
}

function sanitizeFilename(summary: string): string {
  return summary
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 50);
}

function generateFallbackName(): string {
  const time = new Date();
  return `conversation-${time.getHours().toString().padStart(2, '0')}${time.getMinutes().toString().padStart(2, '0')}`;
}

interface ParsedMessage {
  role: 'user' | 'assistant';
  content: string;
}

function parseTranscript(content: string): ParsedMessage[] {
  const messages: ParsedMessage[] = [];

  for (const line of content.split('\n')) {
    if (!line.trim()) continue;
    try {
      const entry = JSON.parse(line);
      if (entry.type === 'user' && entry.message?.content) {
        const text = typeof entry.message.content === 'string'
          ? entry.message.content
          : entry.message.content.map((c: { text?: string }) => c.text || '').join('');
        if (text) messages.push({ role: 'user', content: text });
      } else if (entry.type === 'assistant' && entry.message?.content) {
        const textParts = entry.message.content
          .filter((c: { type: string }) => c.type === 'text')
          .map((c: { text: string }) => c.text);
        const text = textParts.join('');
        if (text) messages.push({ role: 'assistant', content: text });
      }
    } catch {
    }
  }

  return messages;
}

function formatTranscriptMarkdown(messages: ParsedMessage[], title?: string | null, assistantName?: string): string {
  const now = new Date();
  const formatDateTime = (d: Date) => d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true
  });

  const lines: string[] = [];
  lines.push(`# ${title || 'Conversation'}`);
  lines.push('');
  lines.push(`Archived: ${formatDateTime(now)}`);
  lines.push('');
  lines.push('---');
  lines.push('');

  for (const msg of messages) {
    const sender = msg.role === 'user' ? 'User' : (assistantName || 'Assistant');
    const content = msg.content.length > 2000
      ? msg.content.slice(0, 2000) + '...'
      : msg.content;
    lines.push(`**${sender}**: ${content}`);
    lines.push('');
  }

  return lines.join('\n');
}

/**
 * Check for _close sentinel.
 */
function shouldClose(): boolean {
  if (fs.existsSync(IPC_INPUT_CLOSE_SENTINEL)) {
    try { fs.unlinkSync(IPC_INPUT_CLOSE_SENTINEL); } catch { /* ignore */ }
    return true;
  }
  return false;
}

/**
 * Generic drain function for JSON message files in a directory.
 * Reads, parses, deletes files, and returns results of type T.
 */
function drainJsonFiles<T extends { type: string }>(
  dir: string,
  logPrefix: string,
): T[] {
  try {
    fs.mkdirSync(dir, { recursive: true });
    const files = fs.readdirSync(dir).filter(f => f.endsWith('.json')).sort();

    const results: T[] = [];
    for (const file of files) {
      const filePath = path.join(dir, file);
      try {
        const data: T = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
        fs.unlinkSync(filePath);
        if (data.type === 'message') {
          results.push(data);
        }
      } catch (err) {
        log(`Failed to process ${logPrefix} file ${file}: ${err instanceof Error ? err.message : String(err)}`);
        try { fs.unlinkSync(filePath); } catch { /* ignore */ }
      }
    }
    return results;
  } catch (err) {
    log(`Drain error (${logPrefix}): ${err instanceof Error ? err.message : String(err)}`);
    return [];
  }
}

/**
 * Drain all pending IPC input messages.
 * Returns messages found, or empty array.
 */
function drainIpcInput(): string[] {
  const files = drainJsonFiles<{ type: string; text: string }>(IPC_INPUT_DIR, 'IPC');
  return files.map(f => f.text);
}

/**
 * Wait for a new IPC message or _close sentinel.
 * Returns the messages as a single string, or null if _close.
 */
function waitForIpcMessage(): Promise<string | null> {
  return new Promise((resolve) => {
    const poll = () => {
      if (shouldClose()) {
        resolve(null);
        return;
      }
      const messages = drainIpcInput();
      if (messages.length > 0) {
        resolve(messages.join('\n'));
        return;
      }
      setTimeout(poll, IPC_POLL_MS);
    };
    poll();
  });
}

/**
 * Drain messages from a user-specific input directory.
 */
function drainUserInput(senderJid: string): UserMessageFile[] {
  const inputDir = path.join(USERS_IPC_BASE, senderJid, 'input');
  if (!fs.existsSync(inputDir)) return [];
  return drainJsonFiles<UserMessageFile>(inputDir, `user/${senderJid}`);
}

/**
 * Run a single query and stream results via writeOutput or custom emitter.
 * Uses MessageStream (AsyncIterable) to keep isSingleUserTurn=false,
 * allowing agent teams subagents to run to completion.
 *
 * In single-shot mode (no opts.stream): creates stream, polls IPC, checks _close.
 * In daemon mode (opts.stream provided): uses external stream, skips IPC polling.
 */
async function runQuery(
  prompt: string,
  sessionId: string | undefined,
  mcpServerPath: string,
  containerInput: ContainerInput,
  sdkEnv: Record<string, string | undefined>,
  resumeAt?: string,
  opts?: {
    stream?: MessageStream;
    onResult?: (output: ContainerOutput) => void;
  },
): Promise<{ newSessionId?: string; lastAssistantUuid?: string; closedDuringQuery: boolean }> {
  const managedStream = !opts?.stream;
  const stream = opts?.stream ?? new MessageStream();
  const emitResult = opts?.onResult ?? writeOutput;

  if (managedStream) {
    stream.push(prompt);
  }

  // Poll IPC for follow-up messages and _close sentinel during the query (single-shot only)
  let ipcPolling = managedStream;
  let closedDuringQuery = false;

  if (managedStream) {
    const pollIpcDuringQuery = () => {
      if (!ipcPolling) return;
      if (shouldClose()) {
        log('Close sentinel detected during query, ending stream');
        closedDuringQuery = true;
        stream.end();
        ipcPolling = false;
        return;
      }
      const messages = drainIpcInput();
      for (const text of messages) {
        log(`Piping IPC message into active query (${text.length} chars)`);
        stream.push(text);
      }
      setTimeout(pollIpcDuringQuery, IPC_POLL_MS);
    };
    setTimeout(pollIpcDuringQuery, IPC_POLL_MS);
  }

  let newSessionId: string | undefined;
  let lastAssistantUuid: string | undefined;
  let messageCount = 0;
  let resultCount = 0;

  // Load global CLAUDE.md as additional system context (shared across all groups)
  const globalClaudeMdPath = '/workspace/global/CLAUDE.md';
  let globalClaudeMd: string | undefined;
  if (!containerInput.isMain && fs.existsSync(globalClaudeMdPath)) {
    globalClaudeMd = fs.readFileSync(globalClaudeMdPath, 'utf-8');
  }

  // Discover additional directories mounted at /workspace/extra/*
  // These are passed to the SDK so their CLAUDE.md files are loaded automatically
  const extraDirs: string[] = [];
  const extraBase = '/workspace/extra';
  if (fs.existsSync(extraBase)) {
    for (const entry of fs.readdirSync(extraBase)) {
      const fullPath = path.join(extraBase, entry);
      if (fs.statSync(fullPath).isDirectory()) {
        extraDirs.push(fullPath);
      }
    }
  }
  if (extraDirs.length > 0) {
    log(`Additional directories: ${extraDirs.join(', ')}`);
  }

  for await (const message of query({
    prompt: stream,
    options: {
      cwd: '/workspace/group',
      additionalDirectories: extraDirs.length > 0 ? extraDirs : undefined,
      resume: sessionId,
      resumeSessionAt: resumeAt,
      systemPrompt: globalClaudeMd
        ? { type: 'preset' as const, preset: 'claude_code' as const, append: globalClaudeMd }
        : undefined,
      allowedTools: [
        'Bash',
        'Read', 'Write', 'Edit', 'Glob', 'Grep',
        'WebSearch', 'WebFetch',
        'Task', 'TaskOutput', 'TaskStop',
        'TeamCreate', 'TeamDelete', 'SendMessage',
        'TodoWrite', 'ToolSearch', 'Skill',
        'NotebookEdit',
        'mcp__nanoclaw__*'
      ],
      env: sdkEnv,
      permissionMode: 'bypassPermissions',
      allowDangerouslySkipPermissions: true,
      settingSources: ['project', 'user'],
      mcpServers: {
        nanoclaw: {
          command: 'node',
          args: [mcpServerPath],
          env: {
            NANOCLAW_CHAT_JID: containerInput.chatJid,
            NANOCLAW_GROUP_FOLDER: containerInput.groupFolder,
            NANOCLAW_IS_MAIN: containerInput.isMain ? '1' : '0',
          },
        },
      },
      hooks: {
        PreCompact: [{ hooks: [createPreCompactHook(containerInput.assistantName)] }],
      },
    }
  })) {
    messageCount++;
    const msgType = message.type === 'system' ? `system/${(message as { subtype?: string }).subtype}` : message.type;
    log(`[msg #${messageCount}] type=${msgType}`);

    if (message.type === 'assistant' && 'uuid' in message) {
      lastAssistantUuid = (message as { uuid: string }).uuid;
    }

    if (message.type === 'system' && message.subtype === 'init') {
      newSessionId = message.session_id;
      log(`Session initialized: ${newSessionId}`);
    }

    if (message.type === 'system' && (message as { subtype?: string }).subtype === 'task_notification') {
      const tn = message as { task_id: string; status: string; summary: string };
      log(`Task notification: task=${tn.task_id} status=${tn.status} summary=${tn.summary}`);
    }

    if (message.type === 'result') {
      resultCount++;
      const textResult = 'result' in message ? (message as { result?: string }).result : null;
      log(`Result #${resultCount}: subtype=${message.subtype}${textResult ? ` text=${textResult.slice(0, 200)}` : ''}`);
      emitResult({
        status: 'success',
        result: textResult || null,
        newSessionId
      });
    }
  }

  ipcPolling = false;
  log(`Query done. Messages: ${messageCount}, results: ${resultCount}, lastAssistantUuid: ${lastAssistantUuid || 'none'}, closedDuringQuery: ${closedDuringQuery}`);
  return { newSessionId, lastAssistantUuid, closedDuringQuery };
}

/**
 * Run a single user's query session inside the daemon.
 * Manages per-user stream and writes results to user output dir.
 */
async function runUserSession(
  senderJid: string,
  requestId: string,
  firstMsg: UserMessageFile,
  state: UserState,
  config: GroupConfig,
  mcpServerPath: string,
  sdkEnv: Record<string, string | undefined>,
): Promise<void> {
  state.busy = true;
  const stream = new MessageStream();
  state.stream = stream;
  stream.push(firstMsg.text);
  // End the stream immediately so the SDK subprocess receives EOF and starts
  // processing. Each user message is its own query; follow-ups that arrive
  // while this query is running will be picked up by the daemon loop after
  // this session completes (state.busy = false).
  stream.end();
  state.stream = null; // No longer pipeable — each query is self-contained

  let seq = 0;

  const containerInput: ContainerInput = {
    prompt: firstMsg.text,
    sessionId: state.sessionId,
    groupFolder: config.groupFolder,
    chatJid: firstMsg.chatJid || config.chatJid,
    isMain: config.isMain,
    assistantName: config.assistantName,
  };

  const onResult = (output: ContainerOutput) => {
    writeUserOutput(senderJid, requestId, seq++, output);
    if (output.newSessionId) {
      state.sessionId = output.newSessionId;
    }
  };

  try {
    log(`[daemon] Starting query for ${senderJid} (request: ${requestId}, session: ${state.sessionId || 'new'})`);

    const result = await runQuery(
      firstMsg.text,
      state.sessionId,
      mcpServerPath,
      containerInput,
      sdkEnv,
      state.lastAssistantUuid,
      { stream, onResult },
    );

    if (result.newSessionId) state.sessionId = result.newSessionId;
    if (result.lastAssistantUuid) state.lastAssistantUuid = result.lastAssistantUuid;

    // Write done marker
    writeUserOutput(senderJid, requestId, seq++, {
      status: 'success',
      result: null,
      newSessionId: state.sessionId,
    });

    log(`[daemon] Query complete for ${senderJid} (request: ${requestId})`);
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    log(`[daemon] Query error for ${senderJid}: ${errorMessage}`);
    writeUserOutput(senderJid, requestId, seq++, {
      status: 'error',
      result: null,
      error: errorMessage,
    });
  } finally {
    state.stream = null;
    state.busy = false;
  }
}

/**
 * Daemon mode: persistent container handling multiple users concurrently.
 * Polls /workspace/ipc/users/ for new messages from any user.
 */
async function runDaemon(
  config: GroupConfig,
  mcpServerPath: string,
  sdkEnv: Record<string, string | undefined>,
): Promise<void> {
  const userStates = new Map<string, UserState>();

  log(`Daemon started for group: ${config.groupFolder}`);
  fs.mkdirSync(USERS_IPC_BASE, { recursive: true });

  while (true) {
    try {
      if (fs.existsSync(USERS_IPC_BASE)) {
        const entries = fs.readdirSync(USERS_IPC_BASE);

        for (const senderJid of entries) {
          const userDir = path.join(USERS_IPC_BASE, senderJid);
          if (!fs.statSync(userDir).isDirectory()) continue;

          const msgs = drainUserInput(senderJid);

          for (const msg of msgs) {
            let state = userStates.get(senderJid);
            if (!state) {
              state = {
                stream: null,
                sessionId: msg.sessionId,
                busy: false,
                lastAssistantUuid: undefined,
              };
              userStates.set(senderJid, state);
            } else if (msg.sessionId && !state.sessionId) {
              state.sessionId = msg.sessionId;
            }

            if (!state.busy) {
              // Start a new query for this user
              runUserSession(senderJid, msg.requestId, msg, state, config, mcpServerPath, sdkEnv)
                .catch(err =>
                  log(`[daemon] Unhandled session error for ${senderJid}: ${err instanceof Error ? err.message : String(err)}`)
                );
            } else {
              // User is busy — re-queue message to input dir; daemon will process it after current query ends
              log(`[daemon] User ${senderJid} busy, re-queuing message ${msg.requestId}`);
              const retryPath = path.join(USERS_IPC_BASE, senderJid, 'input', `${msg.requestId}.json`);
              try { fs.writeFileSync(retryPath, JSON.stringify(msg)); } catch { /* ignore */ }
            }
          }
        }
      }
    } catch (err) {
      log(`[daemon] Poll error: ${err instanceof Error ? err.message : String(err)}`);
    }

    await new Promise(resolve => setTimeout(resolve, IPC_POLL_MS));
  }
}

async function main(): Promise<void> {
  let rawInput: ContainerInput | GroupConfig;

  try {
    const stdinData = await readStdin();
    rawInput = JSON.parse(stdinData);
    try { fs.unlinkSync('/tmp/input.json'); } catch { /* may not exist */ }
  } catch (err) {
    writeOutput({
      status: 'error',
      result: null,
      error: `Failed to parse input: ${err instanceof Error ? err.message : String(err)}`
    });
    process.exit(1);
  }

  const sdkEnv: Record<string, string | undefined> = { ...process.env };

  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const mcpServerPath = path.join(__dirname, 'ipc-mcp-stdio.js');

  // Dispatch to daemon or single-shot mode based on input
  if ('mode' in rawInput && rawInput.mode === 'daemon') {
    const config = rawInput as GroupConfig;
    log(`Received daemon config for group: ${config.groupFolder}`);
    await runDaemon(config, mcpServerPath, sdkEnv);
    return;
  }

  // Single-shot mode (original behavior)
  const containerInput = rawInput as ContainerInput;
  log(`Received input for group: ${containerInput.groupFolder}`);

  let sessionId = containerInput.sessionId;
  fs.mkdirSync(IPC_INPUT_DIR, { recursive: true });

  // Clean up stale _close sentinel from previous container runs
  try { fs.unlinkSync(IPC_INPUT_CLOSE_SENTINEL); } catch { /* ignore */ }

  // Build initial prompt (drain any pending IPC messages too)
  let prompt = containerInput.prompt;
  if (containerInput.isScheduledTask) {
    prompt = `[SCHEDULED TASK - The following message was sent automatically and is not coming directly from the user or group.]\n\n${prompt}`;
  }
  const pending = drainIpcInput();
  if (pending.length > 0) {
    log(`Draining ${pending.length} pending IPC messages into initial prompt`);
    prompt += '\n' + pending.join('\n');
  }

  // Query loop: run query → wait for IPC message → run new query → repeat
  let resumeAt: string | undefined;
  try {
    while (true) {
      log(`Starting query (session: ${sessionId || 'new'}, resumeAt: ${resumeAt || 'latest'})...`);

      const queryResult = await runQuery(prompt, sessionId, mcpServerPath, containerInput, sdkEnv, resumeAt);
      if (queryResult.newSessionId) {
        sessionId = queryResult.newSessionId;
      }
      if (queryResult.lastAssistantUuid) {
        resumeAt = queryResult.lastAssistantUuid;
      }

      // If _close was consumed during the query, exit immediately.
      // Don't emit a session-update marker (it would reset the host's
      // idle timer and cause a 30-min delay before the next _close).
      if (queryResult.closedDuringQuery) {
        log('Close sentinel consumed during query, exiting');
        break;
      }

      // Emit session update so host can track it
      writeOutput({ status: 'success', result: null, newSessionId: sessionId });

      log('Query ended, waiting for next IPC message...');

      // Wait for the next message or _close sentinel
      const nextMessage = await waitForIpcMessage();
      if (nextMessage === null) {
        log('Close sentinel received, exiting');
        break;
      }

      log(`Got new message (${nextMessage.length} chars), starting new query`);
      prompt = nextMessage;
    }
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    log(`Agent error: ${errorMessage}`);
    writeOutput({
      status: 'error',
      result: null,
      newSessionId: sessionId,
      error: errorMessage
    });
    process.exit(1);
  }
}

main();
