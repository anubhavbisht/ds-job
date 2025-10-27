import { Inject, Injectable } from '@nestjs/common';
import { ResponseClickhouseMessage, PulsarClient } from '@jobber/pulsar';
import { Jobs } from '@jobber/nestjs';
import { JobConsumer } from '../../job.consumer';
import { Packages } from '@jobber/grpc';
import { ClientGrpc } from '@nestjs/microservices';
import { ResponseAnalyticsEtlService } from './responseETL.service';

@Injectable()
export class ClickhouseResponseConsumer extends JobConsumer<ResponseClickhouseMessage> {
  constructor(
    @Inject(Packages.JOBS) clientJobs: ClientGrpc,
    pulsarClient: PulsarClient,
    private readonly etl: ResponseAnalyticsEtlService
  ) {
    super(Jobs.CLICKHOUSE_RESPONSE, pulsarClient, clientJobs);
  }

  protected async execute(data: ResponseClickhouseMessage): Promise<void> {
    this.logger.log(`Triggering Response ETL for campaign: ${data.campaignId}`);
    await this.etl.run(data.campaignId);
  }
}
