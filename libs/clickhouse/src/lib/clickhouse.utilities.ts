import { Logger } from '@nestjs/common';

export function isRetriableError(msg: string): boolean {
  return [
    'timeout',
    'memory',
    'overcommittracker',
    'eai_again',
    'socket hang up',
    'connection',
    'econnreset',
    'read econnreset',
    'ecconnreset',
    'tls',
    'temporary unavailable',
  ].some((s) => msg.includes(s));
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function runWithRetry<T>(
  fn: () => Promise<T>,
  desc: string,
  logger: Logger,
  maxRetries = 5,
  baseDelay = 2000
): Promise<T> {
  let lastErr: any;

  for (let i = 1; i <= maxRetries; i++) {
    try {
      return await fn();
    } catch (err: any) {
      lastErr = err;
      const msg = err?.message || err;
      const retriable = isRetriableError(msg.toString().toLowerCase());

      logger.error(`[ClickHouse] ${desc} failed (attempt ${i}): ${msg}`);

      if (!retriable || i === maxRetries) throw err;

      const backoff = baseDelay * 2 ** (i - 1);
      logger.warn(`[ClickHouse] Retrying ${desc} after ${backoff}ms...`);
      await sleep(backoff);
    }
  }

  throw lastErr;
}
