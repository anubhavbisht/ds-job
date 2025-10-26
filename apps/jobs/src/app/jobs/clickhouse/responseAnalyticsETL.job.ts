import { ResponseClickhouseMessage, PulsarClient } from '@jobber/pulsar';
import { Job } from '../../decorators/job.decorator';
import { AbstractJob } from '../abstract.job';
import { Jobs } from '@jobber/nestjs';
import { PrismaService } from '../../prisma/prisma.service';

@Job({
  name: Jobs.CLICKHOUSE_RESPONSE,
  description: 'Ingest data into clickhouse for response analytics',
})
export class ResponseETLJob extends AbstractJob<ResponseClickhouseMessage> {
  protected messageClass = ResponseClickhouseMessage;

  constructor(pulsarClient: PulsarClient, prismaService: PrismaService) {
    super(pulsarClient, prismaService);
  }
}
