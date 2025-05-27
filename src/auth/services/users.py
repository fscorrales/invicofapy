__all__ = ["UsersService", "UsersServiceDependency"]

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException, status

# from ...config import COLLECTIONS, Database
# from pydantic_mongo import PydanticObjectId
from ...utils import PyObjectId
from ..repositories import (
    UsersRepositoryDependency,
)
from ..schemas import (
    CreateUser,
    # UpdateUser,
    # FilterParamsUser,
    PrivateStoredUser,
    PublicStoredUser,
)

# from ..utils import validate_and_extract_data
from .auth import Authentication


# -------------------------------------------------
@dataclass
class UsersService:
    users: UsersRepositoryDependency

    # -------------------------------------------------
    async def create_one(self, user: CreateUser) -> PublicStoredUser:
        """Create a new user"""
        existing_user = await self.users.get_by_fields({"email": user.email})
        if existing_user is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail="User already exists"
            )

        hash_password = Authentication.get_password_hash(user.password)
        insert_user = user.model_dump(exclude={"password"}, exclude_unset=False)
        insert_user.update(hash_password=hash_password)

        new_user = await self.users.save(insert_user)
        # return new_user
        return PublicStoredUser.model_validate(
            await self.users.get_by_id(new_user.inserted_id)
        )

    # -------------------------------------------------
    async def get_one(
        self,
        *,
        id: PyObjectId | None = None,
        email: str | None = None,
        with_password: bool = False,
    ):
        if all(q is None for q in (id, email)):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No id, username or email provided",
            )
        # filter = {
        #     "$or": [
        #         {"_id": id},
        #         {"email": email},
        #     ]
        # }

        if db_user := await self.users.get_by_fields_or(
            {"_id": id, "email": email},
        ):
            return (
                PrivateStoredUser.model_validate(db_user).model_dump()
                if with_password
                else PublicStoredUser.model_validate(db_user).model_dump()
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
            )

    # @classmethod
    # def get_all(cls, query: FilterParamsUser) -> dict[str, list]:
    #     """Get all users"""
    #     cursor = query.query_collection(cls.collection)
    #     return validate_and_extract_data(cursor, PublicStoredUser)

    # @classmethod
    # def get_all_deleted(cls, query: FilterParamsUser) -> dict[str, list]:
    #     """Get all deleted users"""
    #     cursor = query.query_collection(cls.collection, get_deleted=True)
    #     return validate_and_extract_data(cursor, PublicStoredUser)

    # @classmethod
    # def get_all_active(cls, query: FilterParamsUser) -> dict[str, list]:
    #     """Get all active users"""
    #     cursor = query.query_collection(cls.collection, get_deleted=False)
    #     return validate_and_extract_data(cursor, PublicStoredUser)

    # @classmethod
    # def update_one(cls, id: PydanticObjectId, user: UpdateUser, is_admin: bool):
    #     exclude = {"password"} if is_admin else {"password", "role"}
    #     document = cls.collection.find_one_and_update(
    #         {"_id": id},
    #         {"$set": user.model_dump(exclude=exclude, exclude_unset=True)},
    #         return_document=True,
    #     )
    #     if document:
    #         try:
    #             return PublicStoredUser.model_validate(document).model_dump(
    #                 exclude_none=True
    #             )
    #         except ValidationError as e:
    #             raise HTTPException(
    #                 status_code=status.HTTP_204_NO_CONTENT, detail=str(e)
    #             )
    #     else:
    #         raise HTTPException(
    #             status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
    #         )

    # @classmethod
    # def delete_one(cls, id: PydanticObjectId):
    #     document = cls.collection.find_one_and_update(
    #         {"_id": id},
    #         {"$set": {"deactivated_at": datetime.now()}},
    #         return_document=True,
    #     )
    #     if document:
    #         try:
    #             validated_doc = PublicStoredUser.model_validate(document)
    #             return validated_doc.model_dump()
    #         except ValidationError as e:
    #             raise HTTPException(
    #                 status_code=status.HTTP_204_NO_CONTENT, detail=str(e)
    #             )
    #     else:
    #         raise HTTPException(
    #             status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
    #         )

    # @classmethod
    # def delete_one_hard(cls, id: PydanticObjectId):
    #     document = cls.collection.find_one_and_delete({"_id": id})
    #     if document:
    #         try:
    #             validated_doc = PublicStoredUser.model_validate(document)
    #             return validated_doc.model_dump()
    #         except ValidationError as e:
    #             raise HTTPException(
    #                 status_code=status.HTTP_204_NO_CONTENT, detail=str(e)
    #             )
    #     else:
    #         raise HTTPException(
    #             status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
    #         )


UsersServiceDependency = Annotated[UsersService, Depends()]