import { Module } from '@nestjs/common';
import { ClickhouseModule } from '@jobber/clickhouse';
import { PulsarModule } from '@jobber/pulsar'; // 👈 ADD THIS
import { JobClientsModule } from '../../job-clients.module';
import { ResponseAnalyticsEtlService } from './responseETL.service';
import { ClickhouseResponseConsumer } from './responseETL.consumer';

@Module({
  imports: [
    ClickhouseModule,
    PulsarModule, // 👈 provides PulsarClient
    JobClientsModule, // 👈 provides @Inject(Packages.JOBS)
  ],
  providers: [ResponseAnalyticsEtlService, ClickhouseResponseConsumer],
  exports: [ResponseAnalyticsEtlService, ClickhouseResponseConsumer],
})
export class ResponseAnalyticsModule {}
