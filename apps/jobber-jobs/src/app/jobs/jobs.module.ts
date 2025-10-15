import { Module } from '@nestjs/common';
import { FibonacciJob } from './fibonacci.job';

@Module({})
export class JobsModule {
  providers: [FibonacciJob];
}
