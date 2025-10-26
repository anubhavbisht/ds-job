import { IsNotEmpty, IsUUID } from 'class-validator';
import { JobMessage } from './job.message';

export class ResponseClickhouseMessage extends JobMessage {
  @IsUUID()
  @IsNotEmpty()
  campaignId: string;
}
