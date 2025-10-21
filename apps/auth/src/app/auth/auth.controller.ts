import { Controller, UseGuards } from '@nestjs/common';
import { Observable } from 'rxjs';
import {
  AuthRequest,
  AuthServiceController,
  AuthServiceControllerMethods,
  User,
} from 'types/proto/auth';
import { JWTAuthGuard } from './guards/jwt-auth.guard';
import { UsersService } from '../users/users.service';
import { TokenPayload } from './interface/token.payload';

@Controller()
@AuthServiceControllerMethods()
export class AuthController implements AuthServiceController {
  constructor(private readonly userService: UsersService) {}
  @UseGuards(JWTAuthGuard)
  authenticate(
    request: AuthRequest & { user: TokenPayload }
  ): Promise<User> | Observable<User> | User {
    return this.userService.getUser({ id: request.user.userId });
  }
}
