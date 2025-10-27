import { Module } from '@nestjs/common';
import { PulsarModule } from '@jobber/pulsar';
import { LoadProductModule } from './products/load-products.module';
import { JobClientsModule } from './job-clients.module';
import { FibonacciConsumer } from './fibonacci/fibonacci.consumer';
import { ResponseAnalyticsModule } from './clickhouse/response-analytics/response-analytics.module';
import { ScheduleAnalyticsModule } from './clickhouse/schedule-analytics/schedule-analytics.module';

@Module({
  imports: [
    PulsarModule,
    LoadProductModule,
    JobClientsModule,
    ResponseAnalyticsModule,
    ScheduleAnalyticsModule,
  ],
  providers: [FibonacciConsumer],
})
export class JobsModule {}
