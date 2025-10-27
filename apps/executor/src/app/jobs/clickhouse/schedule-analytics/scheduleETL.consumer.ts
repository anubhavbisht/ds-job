import { Inject, Injectable } from '@nestjs/common';
import { PulsarClient, ScheduleClickhouseMessage } from '@jobber/pulsar';
import { Jobs } from '@jobber/nestjs';
import { JobConsumer } from '../../job.consumer';
import { Packages } from '@jobber/grpc';
import { ClientGrpc } from '@nestjs/microservices';
import { ScheduleAnalyticsEtlService } from './scheduleETL.service';

@Injectable()
export class ClickhouseScheduleConsumer extends JobConsumer<ScheduleClickhouseMessage> {
  constructor(
    @Inject(Packages.JOBS) clientJobs: ClientGrpc,
    pulsarClient: PulsarClient,
    private readonly etl: ScheduleAnalyticsEtlService
  ) {
    super(Jobs.CLICKHOUSE_SCHEDULE, pulsarClient, clientJobs);
  }

  protected async execute(data: ScheduleClickhouseMessage): Promise<void> {
    this.logger.log(`Triggering Schedule ETL for campaign: ${data.campaignId}`);
    await this.etl.run(data.campaignId);
  }
}
