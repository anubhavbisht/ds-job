import { IsNotEmpty, IsNumber } from 'class-validator';

export class FibonacciData {
  @IsNumber()
  @IsNotEmpty()
  iteration: number;
}
