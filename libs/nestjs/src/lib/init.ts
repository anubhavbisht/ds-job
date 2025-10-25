/**
 * This is not a production server yet!
 * This is only a minimal backend to get started.
 */

import { INestApplication, ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as cookieParser from 'cookie-parser';
import { Logger } from 'nestjs-pino';

export async function nestInit(
  app: INestApplication,
  appName: string,
  globalPrefix = 'api'
) {
  app.useGlobalPipes(new ValidationPipe({ whitelist: true }));
  app.setGlobalPrefix(globalPrefix);
  app.useLogger(app.get(Logger));
  app.use(cookieParser());
  const port = app.get(ConfigService).getOrThrow<number>('PORT');
  await app.listen(port);
  app
    .get(Logger)
    .log(
      `🚀 Application ${appName} is running on: http://localhost:${port}/${globalPrefix}`
    );
}
