import { Module } from '@nestjs/common';
import { ClickhouseModule } from '@jobber/clickhouse';
import { PulsarModule } from '@jobber/pulsar';
import { JobClientsModule } from '../../job-clients.module';
import { ResponseAnalyticsEtlService } from './responseETL.service';
import { ClickhouseResponseConsumer } from './responseETL.consumer';

@Module({
  imports: [ClickhouseModule, PulsarModule, JobClientsModule],
  providers: [ResponseAnalyticsEtlService, ClickhouseResponseConsumer],
  exports: [ResponseAnalyticsEtlService, ClickhouseResponseConsumer],
})
export class ResponseAnalyticsModule {}
