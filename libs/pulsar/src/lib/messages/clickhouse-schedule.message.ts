import { IsNotEmpty, IsUUID } from 'class-validator';
import { JobMessage } from './job.message';

export class ScheduleClickhouseMessage extends JobMessage {
  @IsUUID()
  @IsNotEmpty()
  campaignId: string;
}
