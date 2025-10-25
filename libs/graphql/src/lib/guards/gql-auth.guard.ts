import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
  Logger,
  OnModuleInit,
} from '@nestjs/common';
import { GqlExecutionContext } from '@nestjs/graphql';
import { ClientGrpc } from '@nestjs/microservices';
import { catchError, map, Observable, of } from 'rxjs';
import { Packages, AUTH_SERVICE_NAME, AuthServiceClient } from '@jobber/grpc';

@Injectable()
export class GqlAuthGuard implements CanActivate, OnModuleInit {
  private readonly logger = new Logger(GqlAuthGuard.name);
  private authServiceClient: AuthServiceClient;
  constructor(@Inject(Packages.AUTH) private client: ClientGrpc) {}
  onModuleInit() {
    this.authServiceClient =
      this.client.getService<AuthServiceClient>(AUTH_SERVICE_NAME);
  }
  canActivate(
    context: ExecutionContext
  ): boolean | Promise<boolean> | Observable<boolean> {
    const token = this.getRequestContext(context).cookies?.Authentication;
    if (!token) {
      return false;
    }
    return this.authServiceClient.authenticate({ token }).pipe(
      map((res) => {
        this.getRequestContext(context).user = res;
        this.logger.log(res);
        return true;
      }),
      catchError((err) => {
        this.logger.error(err);
        return of(false);
      })
    );
  }

  private getRequestContext(context: ExecutionContext) {
    const ctx = GqlExecutionContext.create(context);
    return ctx.getContext().req;
  }
}
