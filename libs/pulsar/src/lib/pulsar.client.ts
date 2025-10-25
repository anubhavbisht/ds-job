import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Client, Producer, Consumer, Message } from 'pulsar-client';

@Injectable()
export class PulsarClient implements OnModuleDestroy {
  private readonly client: Client;
  private readonly producers: Producer[] = [];
  private readonly consumers: Consumer[] = [];

  constructor(private readonly configService: ConfigService) {
    this.client = new Client({
      serviceUrl: this.configService.getOrThrow<string>('PULSAR_SERVICE_URL'),
    });
  }

  async createProducer(topic: string) {
    const producer = await this.client.createProducer({
      blockIfQueueFull: true,
      topic,
    });
    this.producers.push(producer);
    return producer;
  }

  async createConsumer(topic: string, listener: (message: Message) => void) {
    const consumer = await this.client.subscribe({
      subscriptionType: 'Shared',
      topic,
      subscription: 'jobber',
      listener,
    });
    this.consumers.push(consumer);
    return consumer;
  }

  async onModuleDestroy() {
    for (const consumer of this.consumers) {
      try {
        await consumer.close();
      } catch (err) {
        console.error('Error closing consumer:', err);
      }
    }

    for (const producer of this.producers) {
      try {
        await producer.close();
      } catch (err) {
        console.error('Error closing producer:', err);
      }
    }

    try {
      await this.client.close();
    } catch (err) {
      console.error('Error closing Pulsar client:', err);
    }

    console.log('✅ Pulsar connections closed gracefully');
  }
}
