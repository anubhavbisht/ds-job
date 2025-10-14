import { Injectable, UnauthorizedException } from '@nestjs/common';
import { LoginInput } from './dto/login.input';
import { Response } from 'express';
import { UsersService } from '../users/users.service';
import { compare } from 'bcryptjs';
import { User } from '../users/models/user.model';
import { ConfigService } from '@nestjs/config';
import { TokenPayload } from './interface/token.payload';
import { JwtService } from '@nestjs/jwt';

@Injectable()
export class AuthService {
  constructor(
    private readonly usersService: UsersService,
    private readonly configService: ConfigService,
    private readonly jwtService: JwtService
  ) {}
  async login({ email, password }: LoginInput, res: Response): Promise<any> {
    const user = await this.validateUser(email, password);
    const expires = new Date();
    expires.setMilliseconds(
      expires.getTime() +
        parseInt(this.configService.getOrThrow('JWT_EXPIRATION_TIME'), 10)
    );
    const tokenPayload: TokenPayload = { userId: user.id };
    const accessToken = this.jwtService.sign(tokenPayload);
    res.cookie('Authentication', accessToken, {
      httpOnly: true,
      expires,
      secure: this.configService.get('NODE_ENV') === 'production',
    });
    return user;
  }

  private async validateUser(email: string, password: string): Promise<User> {
    try {
      const user = await this.usersService.getUser({ email });
      if (!user) throw new UnauthorizedException('Invalid credentials');

      const isValid = await compare(password, user.password);
      if (!isValid) throw new UnauthorizedException('Invalid credentials');
      return user;
    } catch (err) {
      console.error(err);
      throw new UnauthorizedException('Invalid credentials');
    }
  }
}
