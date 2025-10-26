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

    // Wait for all sends to complete safely
    await Promise.all(
      messages.map((msg) => this.send({ ...msg, jobId: job.id }))
    );

    return job;
  }

  private async send(data: T & { jobId?: string | number }) {
    // ✅ Await validation
    await this.validateData(data);

    // ✅ Generate a deterministic key for KeyShared routing
    const key =
      (data as any).campaignId ||
      (data as any).id ||
      (data as any).jobId?.toString() || // 👈 convert number → string
      'default';

    console.log(`[Producer] Sending key=${key}`);

    // ✅ Await producer.send to ensure message is acknowledged by Pulsar
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
