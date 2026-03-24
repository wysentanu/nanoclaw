import { request as httpsRequest } from 'https';

import {
  upsertGlobalFact,
  upsertTopicMemory,
  upsertUserMemory,
} from './db.js';
import { readEnvFile } from './env.js';
import { logger } from './logger.js';

export interface MemoryExtractionContext {
  senderJid: string;
  groupFolder: string;
  conversationText: string;
}

interface ExtractedMemories {
  user?: Array<{ key: string; value: string }>;
  topics?: Array<{ topic: string; content: string }>;
  global?: Array<{ key: string; value: string }>;
}

const EXTRACTION_SYSTEM_PROMPT = `You are a memory extraction assistant for a financial AI agent used by multiple users.
Given a conversation, extract facts worth remembering across sessions in three categories.
Return ONLY valid JSON with this exact shape (no markdown fences):
{
  "user": [{"key": "risk_tolerance", "value": "moderate"}],
  "topics": [{"topic": "SBS Transit analysis", "content": "Revenue grew 8% YoY in Q3 2025, operating costs up due to diesel prices"}],
  "global": [{"key": "sg_overnight_rate", "value": "3.75% as of March 2026"}]
}

Rules:
- user: facts about this specific user's profile (risk tolerance, investment goals, preferences, sector interests). Keys in snake_case.
- topics: analysis summaries keyed by company or topic name. One concise paragraph per topic, max 300 chars.
- global: shared macro facts useful for all users (interest rates, index levels, regulatory changes, commodity prices). Keys in snake_case.
- Only extract clearly stated facts, not assumptions or inferences.
- Return empty arrays for categories with no extractable facts.
- Keep individual values concise (under 300 chars).
- Do not extract transient conversational details (greetings, acknowledgements).`;

let cachedApiKey: string | undefined;

function getApiKey(): string {
  if (!cachedApiKey) {
    const secrets = readEnvFile(['ANTHROPIC_API_KEY', 'CLAUDE_CODE_OAUTH_TOKEN']);
    cachedApiKey = secrets.ANTHROPIC_API_KEY || secrets.CLAUDE_CODE_OAUTH_TOKEN;
    if (!cachedApiKey) throw new Error('No API key found (ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN)');
  }
  return cachedApiKey;
}

async function callHaiku(conversationText: string): Promise<ExtractedMemories> {
  const apiKey = getApiKey();
  const body = JSON.stringify({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 1024,
    system: EXTRACTION_SYSTEM_PROMPT,
    messages: [{ role: 'user', content: conversationText }],
  });

  return new Promise((resolve, reject) => {
    const req = httpsRequest(
      {
        hostname: 'api.anthropic.com',
        port: 443,
        path: '/v1/messages',
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'anthropic-version': '2023-06-01',
          'x-api-key': apiKey,
          'content-length': Buffer.byteLength(body),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => {
          try {
            const json = JSON.parse(Buffer.concat(chunks).toString()) as {
              content?: Array<{ text?: string }>;
              error?: { message: string };
            };
            if (json.error) throw new Error(`Haiku API error: ${json.error.message}`);
            const raw = json?.content?.[0]?.text ?? '{}';
            const text = raw.replace(/^```json\s*/i, '').replace(/\s*```$/, '').trim();
            resolve(JSON.parse(text) as ExtractedMemories);
          } catch (err) {
            reject(err);
          }
        });
      },
    );
    req.setTimeout(30000, () => req.destroy(new Error('Request timeout')));
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

/**
 * Fire-and-forget: extract memories from a completed conversation and persist them.
 * Never awaited — must not delay user responses.
 */
export function extractAndStoreMemories(ctx: MemoryExtractionContext): void {
  void (async () => {
    try {
      const memories = await callHaiku(ctx.conversationText);

      let userCount = 0;
      let topicCount = 0;
      let globalCount = 0;

      for (const { key, value } of memories.user ?? []) {
        if (key && value) {
          upsertUserMemory(ctx.senderJid, key, value);
          userCount++;
        }
      }
      for (const { topic, content } of memories.topics ?? []) {
        if (topic && content) {
          upsertTopicMemory(topic, ctx.groupFolder, content);
          topicCount++;
        }
      }
      for (const { key, value } of memories.global ?? []) {
        if (key && value) {
          upsertGlobalFact(key, value);
          globalCount++;
        }
      }

      logger.debug(
        { sender: ctx.senderJid, group: ctx.groupFolder, userCount, topicCount, globalCount },
        'Memory extraction complete',
      );
    } catch (err) {
      logger.warn({ err, sender: ctx.senderJid }, 'Memory extraction failed (non-fatal)');
    }
  })();
}
