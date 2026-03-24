import { GlobalFact, TopicMemory, UserMemory } from './db.js';
import { Channel, NewMessage } from './types.js';
import { formatLocalTime } from './timezone.js';

export function escapeXml(s: string): string {
  if (!s) return '';
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

export function formatMessages(
  messages: NewMessage[],
  timezone: string,
): string {
  const lines = messages.map((m) => {
    const displayTime = formatLocalTime(m.timestamp, timezone);
    return `<message sender="${escapeXml(m.sender_name)}" time="${escapeXml(displayTime)}">${escapeXml(m.content)}</message>`;
  });

  const header = `<context timezone="${escapeXml(timezone)}" />\n`;

  return `${header}<messages>\n${lines.join('\n')}\n</messages>`;
}

export interface MemoryContext {
  userMemories: UserMemory[];
  topicMemories: TopicMemory[];
  globalFacts: GlobalFact[];
}

export function formatMessagesWithMemories(
  messages: NewMessage[],
  timezone: string,
  memory: MemoryContext,
): string {
  const parts: string[] = [];

  if (memory.globalFacts.length > 0) {
    const entries = memory.globalFacts
      .map((f) => `  <fact key="${escapeXml(f.key)}">${escapeXml(f.value)}</fact>`)
      .join('\n');
    parts.push(`<global_facts>\n${entries}\n</global_facts>`);
  }

  if (memory.userMemories.length > 0) {
    const entries = memory.userMemories
      .map((m) => `  <fact key="${escapeXml(m.key)}">${escapeXml(m.value)}</fact>`)
      .join('\n');
    parts.push(`<user_profile>\n${entries}\n</user_profile>`);
  }

  if (memory.topicMemories.length > 0) {
    const entries = memory.topicMemories
      .map(
        (m) =>
          `  <topic name="${escapeXml(m.topic)}" updated="${escapeXml(m.updated_at)}">${escapeXml(m.content)}</topic>`,
      )
      .join('\n');
    parts.push(`<topic_cache>\n${entries}\n</topic_cache>`);
  }

  const memoryBlock =
    parts.length > 0 ? `<memory>\n${parts.join('\n')}\n</memory>\n` : '';

  return memoryBlock + formatMessages(messages, timezone);
}

export function stripInternalTags(text: string): string {
  return text.replace(/<internal>[\s\S]*?<\/internal>/g, '').trim();
}

export function formatOutbound(rawText: string): string {
  const text = stripInternalTags(rawText);
  if (!text) return '';
  return text;
}

export function routeOutbound(
  channels: Channel[],
  jid: string,
  text: string,
): Promise<void> {
  const channel = channels.find((c) => c.ownsJid(jid) && c.isConnected());
  if (!channel) throw new Error(`No channel for JID: ${jid}`);
  return channel.sendMessage(jid, text);
}

export function findChannel(
  channels: Channel[],
  jid: string,
): Channel | undefined {
  return channels.find((c) => c.ownsJid(jid));
}
