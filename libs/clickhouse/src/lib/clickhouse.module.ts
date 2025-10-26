import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { CLICKHOUSE_CONFIG } from './clickhouse.config';
import { ClickhouseService } from './clickhouse.service';

@Module({
  imports: [ConfigModule.forFeature(CLICKHOUSE_CONFIG)],
  providers: [ClickhouseService],
  exports: [ClickhouseService],
})
export class ClickhouseModule {}
