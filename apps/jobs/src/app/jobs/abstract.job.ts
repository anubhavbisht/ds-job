import { Producer } from 'pulsar-client';
import { plainToInstance } from 'class-transformer';
import { PulsarClient, serialize } from '@jobber/pulsar';
import { validate } from 'class-validator';
import { BadRequestException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { JobStatus } from '../models/job.status.enum';

export abstract class AbstractJob<T extends object> {
  private producer: Producer;
  protected abstract messageClass: new () => T;

  constructor(
    private readonly pulsarClient: PulsarClient,
    private readonly prismaService: PrismaService
  ) {}

  async execute(data: T | T[], name: string) {
    if (!this.producer) {
      this.producer = await this.pulsarClient.createProducer(name);
    }

    const job = await this.prismaService.job.create({
      data: {
        name,
        size: Array.isArray(data) ? data.length : 1,
        completed: 0,
        status: JobStatus.IN_PROGRESS,
      },
    });

    const messages = Array.isArray(data) ? data : [data];

    await Promise.all(
      messages.map((msg) => this.send({ ...msg, jobId: job.id }))
    );

    return job;
  }

  private async send(data: T & { jobId?: string | number }) {
    await this.validateData(data);

    const key =
      (data as any).campaignId ||
      (data as any).id ||
      (data as any).jobId?.toString() || // 👈 convert number → string
      'default';

    console.log(`[Producer] Sending key=${key}`);

    await this.producer.send({
      partitionKey: key,
      data: serialize(data),
    });
  }

  private async validateData(data: T) {
    const errors = await validate(plainToInstance(this.messageClass, data));
    if (errors.length) {
      throw new BadRequestException(
        `Job data is invalid: ${JSON.stringify(errors)}`
      );
    }
  }
}
