import { Injectable, UnauthorizedException } from '@nestjs/common';
import { LoginInput } from './dto/login.input';
import { Response } from 'express';
import { UsersService } from '../users/users.service';
import { compare } from 'bcryptjs';
import { User } from '../users/models/user.model';

@Injectable()
export class AuthService {
  constructor(private readonly usersService: UsersService) {}
  async login({ email, password }: LoginInput, res: Response): Promise<any> {
    const user = await this.validateUser(email, password);
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
