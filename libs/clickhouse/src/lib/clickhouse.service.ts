import {
  Inject,
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import {
  ClickHouseClient,
  createClient,
  ResponseJSON,
} from '@clickhouse/client';
import { ConfigService, ConfigType } from '@nestjs/config';
import { CLICKHOUSE_CONFIG } from './clickhouse.config';
import { runWithRetry } from './clickhouse.utilities';

@Injectable()
export class ClickhouseService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ClickhouseService.name);
  private client!: ClickHouseClient;

  constructor(
    @Inject(CLICKHOUSE_CONFIG.KEY)
    private readonly cfg: ConfigType<typeof CLICKHOUSE_CONFIG>,
    private readonly configService: ConfigService
  ) {}

  async onModuleInit() {
    const url = this.configService.getOrThrow<string>('CLICKHOUSE_URL');
    const username = this.configService.getOrThrow<string>(
      'CLICKHOUSE_USERNAME'
    );
    const password = this.configService.getOrThrow<string>(
      'CLICKHOUSE_PASSWORD'
    );
    const database = this.cfg.database;

    this.logger.log(`Connecting to ClickHouse at ${url} (db: ${database})`);

    this.client = createClient({
      url,
      username,
      password,
      database,
      request_timeout: this.cfg.request_timeout,
      compression: this.cfg.compression,
      keep_alive: this.cfg.keep_alive,
    });

    await this.healthCheck();
  }

  getClient(): ClickHouseClient {
    return this.client;
  }

  async query<T = any>(query: string, format: 'JSON'): Promise<T[]>;
  async query(query: string, format: 'CSV'): Promise<string>;
  async query<T = any>(
    query: string,
    format?: 'JSON' | 'CSV'
  ): Promise<T[] | string>;

  async query<T = any>(
    query: string,
    format: 'JSON' | 'CSV' = 'JSON'
  ): Promise<T[] | string> {
    return runWithRetry(
      async () => {
        const res = await this.client.query({ query, format });
        if (format === 'JSON') {
          const json = (await res.json()) as ResponseJSON<T>;
          return json.data;
        }
        return await res.text();
      },
      'query',
      this.logger,
      this.cfg.max_retries,
      this.cfg.retry_base_ms
    );
  }

  async command(query: string): Promise<void> {
    return runWithRetry(
      async () => {
        await this.client.command({ query });
      },
      'command',
      this.logger,
      this.cfg.max_retries,
      this.cfg.retry_base_ms
    );
  }

  async healthCheck() {
    try {
      await this.client.query({ query: 'SELECT 1', format: 'CSV' });
      this.logger.log('✅ ClickHouse connection healthy');
    } catch (err) {
      this.logger.error('❌ ClickHouse connection failed', err);
      throw err;
    }
  }

  async onModuleDestroy() {
    try {
      await this.client.close();
      this.logger.log('🧹 ClickHouse connection closed');
    } catch (err) {
      this.logger.error('Failed to close ClickHouse connection', err);
    }
  }
}
