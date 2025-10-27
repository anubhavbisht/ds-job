import { ScheduleClickhouseMessage, PulsarClient } from '@jobber/pulsar';
import { Job } from '../../decorators/job.decorator';
import { AbstractJob } from '../abstract.job';
import { Jobs } from '@jobber/nestjs';
import { PrismaService } from '../../prisma/prisma.service';

@Job({
  name: Jobs.CLICKHOUSE_SCHEDULE,
  description: 'Ingest data into clickhouse for schedule analytics',
})
export class ScheduleETLJob extends AbstractJob<ScheduleClickhouseMessage> {
  protected messageClass = ScheduleClickhouseMessage;

  constructor(pulsarClient: PulsarClient, prismaService: PrismaService) {
    super(pulsarClient, prismaService);
  }
}
