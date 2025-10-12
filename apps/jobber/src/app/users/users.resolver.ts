import { Resolver, Query, Mutation, Args } from '@nestjs/graphql';
import { User } from './models/user.model';
import { UsersService } from './users.service';
import { CreateUserInput } from './dto/create-user.input';

@Resolver(() => User)
export class UsersResolver {
    constructor(private readonly usersService: UsersService) { }

    @Query(() => [User], { name: 'users' })
    findAll() {
        return this.usersService.getUsers();
    }

    @Mutation(() => User)
    async createUser(@Args('createUserInput') createUserInput: CreateUserInput) {
        return this.usersService.createUser(createUserInput);
    }

}