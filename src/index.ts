import fs from 'fs';
import path from 'path';

import {
  ASSISTANT_NAME,
  CONTAINER_TIMEOUT,
  CREDENTIAL_PROXY_PORT,
  IDLE_TIMEOUT,
  POLL_INTERVAL,
  TIMEZONE,
  TRIGGER_PATTERN,
} from './config.js';
import { startCredentialProxy } from './credential-proxy.js';
import './channels/index.js';
import {
  getChannelFactory,
  getRegisteredChannelNames,
} from './channels/registry.js';
import {
  ContainerOutput,
  DaemonContainer,
  runContainerAgent,
  sendDaemonUserMessage,
  startDaemonContainer,
  watchDaemonUserOutput,
  writeGroupsSnapshot,
  writeTasksSnapshot,
} from './container-runner.js';
import {
  cleanupOrphans,
  ensureContainerRuntimeRunning,
  PROXY_BIND_HOST,
} from './container-runtime.js';
import {
  getAllChats,
  getAllGlobalFacts,
  getAllRegisteredGroups,
  getAllSessions,
  getAllTasks,
  getAllUserSessions,
  getMessagesSince,
  getNewMessages,
  getRouterState,
  getUserMemories,
  getTopicMemories,
  initDatabase,
  pruneExpiredTopicMemories,
  setRegisteredGroup,
  setRouterState,
  setSession,
  setUserSession,
  storeChatMetadata,
  storeMessage,
} from './db.js';
import { GroupQueue } from './group-queue.js';
import { resolveGroupFolderPath } from './group-folder.js';
import { startIpcWatcher } from './ipc.js';
import {
  findChannel,
  formatMessages,
  formatMessagesWithMemories,
  formatOutbound,
} from './router.js';
import { extractAndStoreMemories } from './memory-extractor.js';
import {
  restoreRemoteControl,
  startRemoteControl,
  stopRemoteControl,
} from './remote-control.js';
import {
  isSenderAllowed,
  isTriggerAllowed,
  loadSenderAllowlist,
  shouldDropMessage,
} from './sender-allowlist.js';
import { startSchedulerLoop } from './task-scheduler.js';
import { Channel, NewMessage, RegisteredGroup } from './types.js';
import { logger } from './logger.js';

// Re-export for backwards compatibility during refactor
export { escapeXml, formatMessages } from './router.js';

let lastTimestamp = '';
let sessions: Record<string, string> = {};
let userSessions: Record<string, string> = {}; // keyed "${groupFolder}:${senderJid}"
let registeredGroups: Record<string, RegisteredGroup> = {};
let lastAgentTimestamp: Record<string, string> = {};
let messageLoopRunning = false;

// Persistent daemon containers: one per group JID
const daemonContainers = new Map<string, DaemonContainer>();
// Track daemon container restarts to avoid immediate re-start loops
const daemonRestartAt = new Map<string, number>();

const channels: Channel[] = [];
const queue = new GroupQueue();

function loadState(): void {
  lastTimestamp = getRouterState('last_timestamp') || '';
  const agentTs = getRouterState('last_agent_timestamp');
  try {
    lastAgentTimestamp = agentTs ? JSON.parse(agentTs) : {};
  } catch {
    logger.warn('Corrupted last_agent_timestamp in DB, resetting');
    lastAgentTimestamp = {};
  }
  sessions = getAllSessions();
  userSessions = getAllUserSessions();
  registeredGroups = getAllRegisteredGroups();
  logger.info(
    { groupCount: Object.keys(registeredGroups).length },
    'State loaded',
  );
}

function saveState(): void {
  setRouterState('last_timestamp', lastTimestamp);
  setRouterState('last_agent_timestamp', JSON.stringify(lastAgentTimestamp));
}

function registerGroup(jid: string, group: RegisteredGroup): void {
  let groupDir: string;
  try {
    groupDir = resolveGroupFolderPath(group.folder);
  } catch (err) {
    logger.warn(
      { jid, folder: group.folder, err },
      'Rejecting group registration with invalid folder',
    );
    return;
  }

  registeredGroups[jid] = group;
  setRegisteredGroup(jid, group);

  // Create group folder
  fs.mkdirSync(path.join(groupDir, 'logs'), { recursive: true });

  logger.info(
    { jid, name: group.name, folder: group.folder },
    'Group registered',
  );
}

/**
 * Get available groups list for the agent.
 * Returns groups ordered by most recent activity.
 */
export function getAvailableGroups(): import('./container-runner.js').AvailableGroup[] {
  const chats = getAllChats();
  const registeredJids = new Set(Object.keys(registeredGroups));

  return chats
    .filter((c) => c.jid !== '__group_sync__' && c.is_group)
    .map((c) => ({
      jid: c.jid,
      name: c.name,
      lastActivity: c.last_message_time,
      isRegistered: registeredJids.has(c.jid),
    }));
}

/** @internal - exported for testing */
export function _setRegisteredGroups(
  groups: Record<string, RegisteredGroup>,
): void {
  registeredGroups = groups;
}

function getLastNonBotSender(messages: NewMessage[]): string {
  return [...messages].reverse().find((m) => !m.is_from_me)?.sender ?? '';
}

function buildMemoryContext(senderJid: string, groupFolder: string) {
  return {
    userMemories: senderJid ? getUserMemories(senderJid) : [],
    topicMemories: getTopicMemories(groupFolder),
    globalFacts: getAllGlobalFacts(),
  };
}

/**
 * Send typing indicator and keep refreshing it every 20 s until the returned
 * cancel function is called. WhatsApp expires the indicator after ~25 s, so
 * without refreshing the user sees silence during long agent runs.
 * Returns a stop function that clears the indicator and the interval.
 */
function keepTyping(channel: Channel, chatJid: string): () => void {
  logger.info({ chatJid, hasSetTyping: !!channel.setTyping }, 'keepTyping: starting');
  try {
    channel.setTyping?.(chatJid, true)?.catch((err) =>
      logger.warn({ chatJid, err }, 'keepTyping: setTyping failed'),
    );
  } catch (err) {
    logger.warn({ chatJid, err }, 'keepTyping: setTyping threw');
  }
  const interval = setInterval(async () => {
    logger.info({ chatJid }, 'keepTyping: refreshing');
    try {
      // WhatsApp won't reset the 25s timer if already in composing state.
      // Force a paused→composing transition to restart the clock.
      await channel.setTyping?.(chatJid, false);
      await channel.setTyping?.(chatJid, true);
    } catch (err) {
      logger.warn({ chatJid, err }, 'keepTyping: refresh threw');
    }
  }, 10_000);
  return () => {
    clearInterval(interval);
    logger.info({ chatJid }, 'keepTyping: stopped');
    channel.setTyping?.(chatJid, false)?.catch(() => {});
  };
}

/**
 * Process all pending messages for a group.
 * Called by the GroupQueue when it's this group's turn.
 */
async function processGroupMessages(chatJid: string): Promise<boolean> {
  const group = registeredGroups[chatJid];
  if (!group) return true;

  const channel = findChannel(channels, chatJid);
  if (!channel) {
    logger.warn({ chatJid }, 'No channel owns JID, skipping messages');
    return true;
  }

  const isMainGroup = group.isMain === true;

  const sinceTimestamp = lastAgentTimestamp[chatJid] || '';
  const missedMessages = getMessagesSince(
    chatJid,
    sinceTimestamp,
    ASSISTANT_NAME,
  );

  if (missedMessages.length === 0) return true;

  // For non-main groups, check if trigger is required and present
  if (!isMainGroup && group.requiresTrigger !== false) {
    const allowlistCfg = loadSenderAllowlist();
    const hasTrigger = missedMessages.some(
      (m) =>
        TRIGGER_PATTERN.test(m.content.trim()) &&
        (m.is_from_me || isTriggerAllowed(chatJid, m.sender, allowlistCfg)),
    );
    if (!hasTrigger) return true;
  }

  // Identify triggering sender (last non-bot message in the batch)
  const triggeringSender = getLastNonBotSender(missedMessages);
  const memoryCtx = buildMemoryContext(triggeringSender, group.folder);

  const prompt = formatMessagesWithMemories(
    missedMessages,
    TIMEZONE,
    memoryCtx,
  );

  // Advance cursor so the piping path in startMessageLoop won't re-fetch
  // these messages. Save the old cursor so we can roll back on error.
  const previousCursor = lastAgentTimestamp[chatJid] || '';
  lastAgentTimestamp[chatJid] =
    missedMessages[missedMessages.length - 1].timestamp;
  saveState();

  logger.info(
    { group: group.name, messageCount: missedMessages.length },
    'Processing messages',
  );

  // Track idle timer for closing stdin when agent is idle
  let idleTimer: ReturnType<typeof setTimeout> | null = null;

  const resetIdleTimer = () => {
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      logger.debug(
        { group: group.name },
        'Idle timeout, closing container stdin',
      );
      queue.closeStdin(chatJid);
    }, IDLE_TIMEOUT);
  };

  // Daemon mode: dispatch to background and release queue slot immediately.
  // The daemon handles concurrent users internally; holding the slot would
  // prevent other groups from running while one user waits for a response.
  if (triggeringSender) {
    const stopTyping = keepTyping(channel, chatJid);

    // Route to main group's daemon — all users share one container and global context
    const mainEntry = getMainGroup();
    const daemonGroup = mainEntry?.[1] ?? group;
    const daemonJid = mainEntry?.[0] ?? chatJid;

    // Ensure daemon is running (starts it if needed)
    if (!ensureDaemon(daemonGroup, daemonJid)) {
      // Throttled — roll back and retry
      stopTyping();
      lastAgentTimestamp[chatJid] = previousCursor;
      saveState();
      return false;
    }

    // Fire-and-forget: dispatch to daemon, release queue slot immediately
    let fullDaemonResponse = '';
    runAgent(group, prompt, chatJid, triggeringSender, async (result) => {
      if (result.result) {
        const raw = typeof result.result === 'string' ? result.result : JSON.stringify(result.result);
        const text = raw.replace(/<internal>[\s\S]*?<\/internal>/g, '').trim();
        logger.info({ group: group.name }, `Daemon agent output: ${raw.length} chars`);
        if (text) {
          fullDaemonResponse += (fullDaemonResponse ? '\n' : '') + text;
          await channel.sendMessage(chatJid, text);
        }
      }
      if (result.status === 'success' && fullDaemonResponse && triggeringSender) {
        extractAndStoreMemories({
          senderJid: triggeringSender,
          groupFolder: daemonGroup.folder,
          conversationText: prompt + '\n\nAssistant:\n' + fullDaemonResponse,
        });
        fullDaemonResponse = '';
      }
    }).then((status) => {
      stopTyping();
      if (status === 'error') {
        logger.warn({ group: group.name }, 'Daemon agent error (cursor already advanced)');
      }
    }).catch((err) => {
      logger.error({ group: group.name, err }, 'Daemon dispatch error');
      stopTyping();
    });

    return true; // Release queue slot immediately
  }

  // Single-shot path: scheduled tasks and messages without a senderJid
  const stopTyping = keepTyping(channel, chatJid);
  let hadError = false;
  let outputSentToUser = false;
  let fullAgentResponse = '';

  const output = await runAgent(group, prompt, chatJid, triggeringSender, async (result) => {
    if (result.result) {
      const raw =
        typeof result.result === 'string'
          ? result.result
          : JSON.stringify(result.result);
      const text = raw.replace(/<internal>[\s\S]*?<\/internal>/g, '').trim();
      logger.info({ group: group.name }, `Agent output: ${raw.length} chars`);
      if (text) {
        fullAgentResponse += (fullAgentResponse ? '\n' : '') + text;
        await channel.sendMessage(chatJid, text);
        outputSentToUser = true;
      }
      resetIdleTimer();
    }

    if (result.status === 'success') {
      queue.notifyIdle(chatJid);
      if (fullAgentResponse && triggeringSender) {
        extractAndStoreMemories({
          senderJid: triggeringSender,
          groupFolder: group.folder,
          conversationText: prompt + '\n\nAssistant:\n' + fullAgentResponse,
        });
        fullAgentResponse = '';
      }
    }

    if (result.status === 'error') {
      hadError = true;
    }
  });

  stopTyping();
  if (idleTimer) clearTimeout(idleTimer);

  if (output === 'error' || hadError) {
    // If we already sent output to the user, don't roll back the cursor —
    // the user got their response and re-processing would send duplicates.
    if (outputSentToUser) {
      logger.warn(
        { group: group.name },
        'Agent error after output was sent, skipping cursor rollback to prevent duplicates',
      );
      return true;
    }
    // Roll back cursor so retries can re-process these messages
    lastAgentTimestamp[chatJid] = previousCursor;
    saveState();
    logger.warn(
      { group: group.name },
      'Agent error, rolled back message cursor for retry',
    );
    return false;
  }

  return true;
}

/**
 * Ensure a daemon container is running for the group.
 * Starts one if not running or if previous one exited. Returns false if start is
 * throttled (recent crash).
 */
function ensureDaemon(group: RegisteredGroup, chatJid: string): boolean {
  if (daemonContainers.has(chatJid)) return true;

  // Throttle restart: wait 10s after last exit before restarting
  const lastRestart = daemonRestartAt.get(chatJid) ?? 0;
  if (Date.now() - lastRestart < 10_000) {
    logger.warn({ group: group.name }, 'Daemon container recently exited, throttling restart');
    return false;
  }

  daemonRestartAt.set(chatJid, Date.now());

  const daemon = startDaemonContainer(group, chatJid, ASSISTANT_NAME, (code) => {
    daemonContainers.delete(chatJid);
    logger.warn({ group: group.name, code }, 'Daemon container stopped');
  });

  daemonContainers.set(chatJid, daemon);
  logger.info({ group: group.name, container: daemon.name }, 'Daemon container started');
  return true;
}

/**
 * Returns the main group entry [jid, group], or undefined if none registered.
 */
function getMainGroup(): [string, RegisteredGroup] | undefined {
  return Object.entries(registeredGroups).find(([, g]) => g.isMain === true) as
    | [string, RegisteredGroup]
    | undefined;
}

async function runAgent(
  group: RegisteredGroup,
  prompt: string,
  chatJid: string,
  senderJid: string,
  onOutput?: (output: ContainerOutput) => Promise<void>,
): Promise<'success' | 'error'> {
  // All user messages route to the main group's daemon — single shared container,
  // global context, per-user sessions keyed by senderJid.
  const mainEntry = senderJid ? getMainGroup() : undefined;
  const daemonGroup = mainEntry?.[1] ?? group;
  const daemonJid = mainEntry?.[0] ?? chatJid;

  const isMain = daemonGroup.isMain === true;
  const sessionKey = senderJid
    ? `${daemonGroup.folder}:${senderJid}`
    : daemonGroup.folder;
  const sessionId = senderJid
    ? userSessions[sessionKey]
    : sessions[daemonGroup.folder];

  // Update tasks snapshot for container to read (filtered by group)
  const tasks = getAllTasks();
  writeTasksSnapshot(
    group.folder,
    isMain,
    tasks.map((t) => ({
      id: t.id,
      groupFolder: t.group_folder,
      prompt: t.prompt,
      schedule_type: t.schedule_type,
      schedule_value: t.schedule_value,
      status: t.status,
      next_run: t.next_run,
    })),
  );

  // Update available groups snapshot (main group only can see all groups)
  const availableGroups = getAvailableGroups();
  writeGroupsSnapshot(
    group.folder,
    isMain,
    availableGroups,
    new Set(Object.keys(registeredGroups)),
  );

  const saveSession = (newSessionId: string) => {
    if (senderJid) {
      userSessions[sessionKey] = newSessionId;
      setUserSession(daemonGroup.folder, senderJid, newSessionId);
    } else {
      sessions[daemonGroup.folder] = newSessionId;
      setSession(daemonGroup.folder, newSessionId);
    }
  };

  // User messages → daemon IPC path (persistent container, per-user sessions)
  // Scheduled tasks → single-shot container path (isolated, no daemon needed)
  if (senderJid) {
    if (!ensureDaemon(daemonGroup, daemonJid)) {
      return 'error';
    }

    const requestId = `${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    sendDaemonUserMessage(daemonGroup.folder, senderJid, {
      type: 'message',
      text: prompt,
      sessionId,
      chatJid,
      requestId,
      senderJid,
    });

    const configTimeout = group.containerConfig?.timeout || CONTAINER_TIMEOUT;

    try {
      await watchDaemonUserOutput(
        daemonGroup.folder,
        senderJid,
        requestId,
        async (output) => {
          if (output.newSessionId) {
            saveSession(output.newSessionId);
          }
          if (onOutput) await onOutput(output);
        },
        configTimeout,
      );
      return 'success';
    } catch (err) {
      logger.error({ group: group.name, err }, 'Daemon agent error');
      return 'error';
    }
  }

  // Single-shot path: scheduled tasks and fallback
  const wrappedOnOutput = onOutput
    ? async (output: ContainerOutput) => {
        if (output.newSessionId) {
          saveSession(output.newSessionId);
        }
        await onOutput(output);
      }
    : undefined;

  try {
    const output = await runContainerAgent(
      group,
      {
        prompt,
        sessionId,
        groupFolder: group.folder,
        chatJid,
        isMain,
        assistantName: ASSISTANT_NAME,
      },
      (proc, containerName) =>
        queue.registerProcess(chatJid, proc, containerName, group.folder),
      wrappedOnOutput,
    );

    if (output.newSessionId) {
      saveSession(output.newSessionId);
    }

    if (output.status === 'error') {
      logger.error(
        { group: group.name, error: output.error },
        'Container agent error',
      );
      return 'error';
    }

    return 'success';
  } catch (err) {
    logger.error({ group: group.name, err }, 'Agent error');
    return 'error';
  }
}

async function startMessageLoop(): Promise<void> {
  if (messageLoopRunning) {
    logger.debug('Message loop already running, skipping duplicate start');
    return;
  }
  messageLoopRunning = true;

  logger.info(`NanoClaw running (trigger: @${ASSISTANT_NAME})`);

  while (true) {
    try {
      const jids = Object.keys(registeredGroups);
      const { messages, newTimestamp } = getNewMessages(
        jids,
        lastTimestamp,
        ASSISTANT_NAME,
      );

      if (messages.length > 0) {
        logger.info({ count: messages.length }, 'New messages');

        // Advance the "seen" cursor for all messages immediately
        lastTimestamp = newTimestamp;
        saveState();

        // Deduplicate by group
        const messagesByGroup = new Map<string, NewMessage[]>();
        for (const msg of messages) {
          const existing = messagesByGroup.get(msg.chat_jid);
          if (existing) {
            existing.push(msg);
          } else {
            messagesByGroup.set(msg.chat_jid, [msg]);
          }
        }

        for (const [chatJid, groupMessages] of messagesByGroup) {
          const group = registeredGroups[chatJid];
          if (!group) continue;

          const channel = findChannel(channels, chatJid);
          if (!channel) {
            logger.warn({ chatJid }, 'No channel owns JID, skipping messages');
            continue;
          }

          const isMainGroup = group.isMain === true;
          const needsTrigger = !isMainGroup && group.requiresTrigger !== false;

          // For non-main groups, only act on trigger messages.
          // Non-trigger messages accumulate in DB and get pulled as
          // context when a trigger eventually arrives.
          if (needsTrigger) {
            const allowlistCfg = loadSenderAllowlist();
            const hasTrigger = groupMessages.some(
              (m) =>
                TRIGGER_PATTERN.test(m.content.trim()) &&
                (m.is_from_me ||
                  isTriggerAllowed(chatJid, m.sender, allowlistCfg)),
            );
            if (!hasTrigger) continue;
          }

          // Pull all messages since lastAgentTimestamp so non-trigger
          // context that accumulated between triggers is included.
          const allPending = getMessagesSince(
            chatJid,
            lastAgentTimestamp[chatJid] || '',
            ASSISTANT_NAME,
          );
          const messagesToSend =
            allPending.length > 0 ? allPending : groupMessages;
          const pipeSender = getLastNonBotSender(messagesToSend);
          const formatted = formatMessagesWithMemories(
            messagesToSend,
            TIMEZONE,
            buildMemoryContext(pipeSender, group.folder),
          );

          if (queue.sendMessage(chatJid, formatted)) {
            logger.debug(
              { chatJid, count: messagesToSend.length },
              'Piped messages to active container',
            );
            lastAgentTimestamp[chatJid] =
              messagesToSend[messagesToSend.length - 1].timestamp;
            saveState();
            // Show typing indicator while the container processes the piped message
            channel
              .setTyping?.(chatJid, true)
              ?.catch((err) =>
                logger.warn({ chatJid, err }, 'Failed to set typing indicator'),
              );
          } else {
            // No active container — enqueue for a new one
            queue.enqueueMessageCheck(chatJid);
          }
        }
      }
    } catch (err) {
      logger.error({ err }, 'Error in message loop');
    }
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL));
  }
}

/**
 * Startup recovery: check for unprocessed messages in registered groups.
 * Handles crash between advancing lastTimestamp and processing messages.
 */
function recoverPendingMessages(): void {
  for (const [chatJid, group] of Object.entries(registeredGroups)) {
    const sinceTimestamp = lastAgentTimestamp[chatJid] || '';
    const pending = getMessagesSince(chatJid, sinceTimestamp, ASSISTANT_NAME);
    if (pending.length > 0) {
      logger.info(
        { group: group.name, pendingCount: pending.length },
        'Recovery: found unprocessed messages',
      );
      queue.enqueueMessageCheck(chatJid);
    }
  }
}

function ensureContainerSystemRunning(): void {
  ensureContainerRuntimeRunning();
  cleanupOrphans();
}

async function main(): Promise<void> {
  ensureContainerSystemRunning();
  initDatabase();
  logger.info('Database initialized');
  loadState();
  // Prune expired topic memories daily
  pruneExpiredTopicMemories();
  setInterval(() => pruneExpiredTopicMemories(), 24 * 60 * 60 * 1000);
  restoreRemoteControl();

  // Start credential proxy (containers route API calls through this)
  const proxyServer = await startCredentialProxy(
    CREDENTIAL_PROXY_PORT,
    PROXY_BIND_HOST,
  );

  // Graceful shutdown handlers
  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Shutdown signal received');
    proxyServer.close();
    // Stop daemon containers
    for (const [jid, daemon] of daemonContainers) {
      logger.info({ jid, container: daemon.name }, 'Stopping daemon container');
      try { daemon.proc.kill('SIGTERM'); } catch { /* ignore */ }
    }
    await queue.shutdown(10000);
    for (const ch of channels) await ch.disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  // Handle /remote-control and /remote-control-end commands
  async function handleRemoteControl(
    command: string,
    chatJid: string,
    msg: NewMessage,
  ): Promise<void> {
    const group = registeredGroups[chatJid];
    if (!group?.isMain) {
      logger.warn(
        { chatJid, sender: msg.sender },
        'Remote control rejected: not main group',
      );
      return;
    }

    const channel = findChannel(channels, chatJid);
    if (!channel) return;

    if (command === '/remote-control') {
      const result = await startRemoteControl(
        msg.sender,
        chatJid,
        process.cwd(),
      );
      if (result.ok) {
        await channel.sendMessage(chatJid, result.url);
      } else {
        await channel.sendMessage(
          chatJid,
          `Remote Control failed: ${result.error}`,
        );
      }
    } else {
      const result = stopRemoteControl();
      if (result.ok) {
        await channel.sendMessage(chatJid, 'Remote Control session ended.');
      } else {
        await channel.sendMessage(chatJid, result.error);
      }
    }
  }

  // Channel callbacks (shared by all channels)
  const channelOpts = {
    onMessage: (chatJid: string, msg: NewMessage) => {
      // Remote control commands — intercept before storage
      const trimmed = msg.content.trim();
      if (trimmed === '/remote-control' || trimmed === '/remote-control-end') {
        handleRemoteControl(trimmed, chatJid, msg).catch((err) =>
          logger.error({ err, chatJid }, 'Remote control command error'),
        );
        return;
      }

      // Sender allowlist drop mode: discard messages from denied senders before storing
      if (!msg.is_from_me && !msg.is_bot_message && registeredGroups[chatJid]) {
        const cfg = loadSenderAllowlist();
        if (
          shouldDropMessage(chatJid, cfg) &&
          !isSenderAllowed(chatJid, msg.sender, cfg)
        ) {
          if (cfg.logDenied) {
            logger.debug(
              { chatJid, sender: msg.sender },
              'sender-allowlist: dropping message (drop mode)',
            );
          }
          return;
        }
      }
      storeMessage(msg);
    },
    onChatMetadata: (
      chatJid: string,
      timestamp: string,
      name?: string,
      channel?: string,
      isGroup?: boolean,
    ) => storeChatMetadata(chatJid, timestamp, name, channel, isGroup),
    registeredGroups: () => registeredGroups,
  };

  // Create and connect all registered channels.
  // Each channel self-registers via the barrel import above.
  // Factories return null when credentials are missing, so unconfigured channels are skipped.
  for (const channelName of getRegisteredChannelNames()) {
    const factory = getChannelFactory(channelName)!;
    const channel = factory(channelOpts);
    if (!channel) {
      logger.warn(
        { channel: channelName },
        'Channel installed but credentials missing — skipping. Check .env or re-run the channel skill.',
      );
      continue;
    }
    channels.push(channel);
    await channel.connect();
  }
  if (channels.length === 0) {
    logger.fatal('No channels connected');
    process.exit(1);
  }

  // Start subsystems (independently of connection handler)
  startSchedulerLoop({
    registeredGroups: () => registeredGroups,
    getSessions: () => sessions,
    queue,
    onProcess: (groupJid, proc, containerName, groupFolder) =>
      queue.registerProcess(groupJid, proc, containerName, groupFolder),
    sendMessage: async (jid, rawText) => {
      const channel = findChannel(channels, jid);
      if (!channel) {
        logger.warn({ jid }, 'No channel owns JID, cannot send message');
        return;
      }
      const text = formatOutbound(rawText);
      if (text) await channel.sendMessage(jid, text);
    },
  });
  startIpcWatcher({
    sendMessage: (jid, text) => {
      const channel = findChannel(channels, jid);
      if (!channel) throw new Error(`No channel for JID: ${jid}`);
      return channel.sendMessage(jid, text);
    },
    registeredGroups: () => registeredGroups,
    registerGroup,
    syncGroups: async (force: boolean) => {
      await Promise.all(
        channels
          .filter((ch) => ch.syncGroups)
          .map((ch) => ch.syncGroups!(force)),
      );
    },
    getAvailableGroups,
    writeGroupsSnapshot: (gf, im, ag, rj) =>
      writeGroupsSnapshot(gf, im, ag, rj),
    onTasksChanged: () => {
      const tasks = getAllTasks();
      const taskRows = tasks.map((t) => ({
        id: t.id,
        groupFolder: t.group_folder,
        prompt: t.prompt,
        schedule_type: t.schedule_type,
        schedule_value: t.schedule_value,
        status: t.status,
        next_run: t.next_run,
      }));
      for (const group of Object.values(registeredGroups)) {
        writeTasksSnapshot(group.folder, group.isMain === true, taskRows);
      }
    },
  });
  queue.setProcessMessagesFn(processGroupMessages);
  recoverPendingMessages();
  startMessageLoop().catch((err) => {
    logger.fatal({ err }, 'Message loop crashed unexpectedly');
    process.exit(1);
  });
}

// Guard: only run when executed directly, not when imported by tests
const isDirectRun =
  process.argv[1] &&
  new URL(import.meta.url).pathname ===
    new URL(`file://${process.argv[1]}`).pathname;

if (isDirectRun) {
  main().catch((err) => {
    logger.error({ err }, 'Failed to start NanoClaw');
    process.exit(1);
  });
}
