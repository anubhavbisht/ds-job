require('module-alias/register');
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app/app.module';
import { nestInit } from '@jobber/nestjs';
import { GrpcOptions, Transport } from '@nestjs/microservices';
import { ConfigService } from '@nestjs/config';
import { Packages } from '@jobber/grpc';
import { join } from 'path';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  await nestInit(app, 'Jobs-Init', 'jobs');
  app.connectMicroservice<GrpcOptions>({
    transport: Transport.GRPC,
    options: {
      url: app.get(ConfigService).getOrThrow('JOBS_GRPC_SERVICE_URL'),
      package: Packages.JOBS,
      protoPath: join(__dirname, '../../libs/grpc/proto/job.proto'),
    },
  });
  await app.startAllMicroservices();
}

bootstrap().catch((err) => console.error(err));
