import { registerAs } from '@nestjs/config';

export const CLICKHOUSE_CONFIG = registerAs('clickhouse', () => ({
  url: process.env['CLICKHOUSE_URL'],
  username: process.env['CLICKHOUSE_USERNAME'],
  password: process.env['CLICKHOUSE_PASSWORD'],
  database: process.env['CLICKHOUSE_DATABASE'] ?? 'zykrr_production',
  request_timeout: 900000,
  compression: { response: true, request: true },
  keep_alive: { enabled: false, idle_socket_ttl: 0 },
  max_retries: 10,
  retry_base_ms: 2000,
}));
