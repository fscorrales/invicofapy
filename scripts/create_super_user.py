"""
This script requires the file ADMIN_USER_CONF to be in the same directory as the
script. Or you can set the username, email and password environment variables.
"""

import asyncio

from fastapi import HTTPException

from src.auth.repositories import UsersRepository
from src.auth.schemas import CreateUser
from src.auth.services import UsersService
from src.config import Database, settings


# -------------------------------------------------
async def main():
    Database.initialize()
    try:
        await Database.client.admin.command("ping")
        print("Connected to MongoDB")
    except Exception as e:
        print("Error connecting to MongoDB:", e)
        return

    try:
        with open("scripts/ADMIN_USER_CONF", "r") as f:
            print("Reading the config file...")
            lines = f.read().splitlines()
            data = dict(line.split("=") for line in lines)
    except FileNotFoundError:
        data = dict(
            email=settings.ADMIN_EMAIL,
            password=settings.ADMIN_PASSWORD,
        )

    # Assign role admin to user data
    try:
        data["role"] = "admin"

        insertion_user = CreateUser.model_validate(data)

        print("Creating super user...")
        users_service = UsersService(users=UsersRepository())
        result = await users_service.create_one(user=insertion_user)

        print(f"Super user with email: {data['email']} created with id: {result.id}")
    except HTTPException as e:
        if e.status_code == 409:
            print("Super user already exists. Skipping creation.")
        else:
            raise


# -------------------------------------------------
if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    loop.run_until_complete(main())
