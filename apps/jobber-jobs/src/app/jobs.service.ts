import {
  DiscoveredClassWithMeta,
  DiscoveryService,
} from '@golevelup/nestjs-discovery';
import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  OnModuleInit,
} from '@nestjs/common';
import { JOB_METADATA_KEY } from './decorators/job.decorator';
import { AbstractJob } from './jobs/abstract.job';
import { JobMetadata } from './interfaces/job-metadata.interface';

@Injectable()
export class JobsService implements OnModuleInit {
  private jobs: DiscoveredClassWithMeta<JobMetadata>[] = [];
  constructor(private readonly discoveryService: DiscoveryService) {}

  async onModuleInit() {
    this.jobs = await this.discoveryService.providersWithMetaAtKey(
      JOB_METADATA_KEY
    );
  }

  async getJobs(): Promise<JobMetadata[]> {
    return this.jobs.map((job) => job.meta);
  }

  async executeJob(name: string, data: object): Promise<JobMetadata> {
    const job = this.jobs.find((job) => job.meta.name === name);
    if (!job) {
      throw new BadRequestException(`Job with name ${name} not found`);
    }
    if (!(job.discoveredClass.instance instanceof AbstractJob)) {
      throw new InternalServerErrorException(
        `Job with name ${name} is not an instance of AbstractJob`
      );
    }
    await job.discoveredClass.instance.execute(data, job.meta.name);
    return job.meta;
  }
}
