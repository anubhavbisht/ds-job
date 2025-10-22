import { Args, Mutation, Resolver } from '@nestjs/graphql';
import { Job } from './models/job.model';
import { Query } from '@nestjs/graphql';
import { JobsService } from './jobs.service';
import { ExecuteJobInput } from './dto/execute-job.input';
import { UseGuards } from '@nestjs/common';
import { GqlAuthGuard } from '@jobber/graphql';

@Resolver(() => Job)
export class JobsResolver {
  constructor(private readonly jobsService: JobsService) {}
  @UseGuards(GqlAuthGuard)
  @Query(() => [Job], { name: 'jobs' })
  async getJobs() {
    return this.jobsService.getJobs();
  }

  @UseGuards(GqlAuthGuard)
  @Mutation(() => Job)
  async executeJob(
    @Args('executeJobInput') executeJobInput: ExecuteJobInput
  ): Promise<Job> {
    return this.jobsService.executeJob(
      executeJobInput.name,
      executeJobInput.data
    );
  }
}
