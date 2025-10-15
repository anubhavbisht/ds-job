import { Job } from '../decorators/job.decorator';
import { AbstractJob } from './abstract/abstract.job';

@Job({
  name: 'FibonacciJob',
  description: 'A job that calculates Fibonacci sequence and store it in a DB.',
})
export class FibonacciJob extends AbstractJob {}
