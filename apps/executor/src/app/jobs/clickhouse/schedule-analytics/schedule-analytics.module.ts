import { Module } from '@nestjs/common';
import { ClickhouseModule } from '@jobber/clickhouse';
import { PulsarModule } from '@jobber/pulsar';
import { JobClientsModule } from '../../job-clients.module';
import { ScheduleAnalyticsEtlService } from './scheduleETL.service';
import { ClickhouseScheduleConsumer } from './scheduleETL.consumer';

@Module({
  imports: [ClickhouseModule, PulsarModule, JobClientsModule],
  providers: [ScheduleAnalyticsEtlService, ClickhouseScheduleConsumer],
  exports: [ScheduleAnalyticsEtlService, ClickhouseScheduleConsumer],
})
export class ScheduleAnalyticsModule {}
