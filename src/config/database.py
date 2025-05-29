__all__ = ["Database", "COLLECTIONS", "BaseRepository"]

from typing import Generic, List, Optional, Type, TypeVar

from fastapi.encoders import jsonable_encoder
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel

from ..utils import BaseFilterParams, parse_filter_keys
from .__base_config import settings

ModelType = TypeVar("ModelType", bound=BaseModel)

MONGO_DB_NAME = "invico"
COLLECTIONS = [
    "users",
    "siif_rf602",
]


# -------------------------------------------------
class Database:
    client = None
    db = None

    @classmethod
    def initialize(cls):
        cls.client = AsyncIOMotorClient(settings.DB_URI)
        cls.db = cls.client[MONGO_DB_NAME]


# -------------------------------------------------
class BaseRepository(Generic[ModelType]):
    collection_name: str
    model: Type[ModelType]
    unique_field: Optional[str] = None

    # -------------------------------------------------
    def __init__(self):
        if not hasattr(self, "collection_name") or not hasattr(self, "model"):
            raise NotImplementedError("Repos must define 'collection_name' and 'model'")
        if self.collection_name not in COLLECTIONS:
            raise ValueError(f"'{self.collection_name}' not found in COLLECTIONS")

        self.collection = Database.db[self.collection_name]  # Motor async collection

    # -------------------------------------------------
    async def save(self, data: ModelType) -> ModelType:
        if not isinstance(data, self.model):
            raise TypeError(f"Expected instance of {self.model}, got {type(data)}")

        doc = jsonable_encoder(data, by_alias=True)

        if self.unique_field and doc.get(self.unique_field):
            existing = await self.collection.find_one(
                {self.unique_field: doc[self.unique_field]}
            )
            if existing:
                raise ValueError(
                    f"Duplicate entry for field '{self.unique_field}': {doc[self.unique_field]}"
                )

        result = await self.collection.insert_one(doc)
        # doc["_id"] = result.inserted_id  # agregamos el _id devuelto por Mongo

        # return self.model(**doc)  # devolvés el modelo reconstruido con _id incluido
        return result

    # -------------------------------------------------
    async def save_all(self, data: List[ModelType]) -> List[ModelType]:
        if isinstance(data, list):
            data = [jsonable_encoder(doc, by_alias=True) for doc in data]
        else:
            data = jsonable_encoder(data, by_alias=True)
        return await self.collection.insert_many(data)

    # -------------------------------------------------
    async def get_all(self, limit: Optional[int] = None) -> List[ModelType]:
        cursor = self.collection.find()
        if limit is not None:
            cursor = cursor.limit(limit)
        docs = await cursor.to_list(length=None if limit is None else limit)
        return docs

    # -------------------------------------------------
    async def get_by_id(self, id: str) -> Optional[ModelType]:
        doc = await self.collection.find_one({"_id": id})
        return doc if doc else None

    # -------------------------------------------------
    async def get_by_fields(self, fields: dict) -> Optional[ModelType]:
        """
        Find a document by one or more fields.

        Args:
            fields (dict): A dictionary where keys are field names and values are the values to match.

        Returns:
            Optional[ModelType]: The document that matches the fields, or None if not found.
        """
        if not fields:
            raise ValueError("Fields dictionary cannot be empty")

        doc = await self.collection.find_one(fields)
        return doc if doc else None

    # -------------------------------------------------
    async def get_by_fields_or(self, fields: dict) -> Optional[ModelType]:
        """
        Find a document by multiple fields using an $or filter.

        Args:
            fields (dict): A dictionary where keys are field names and values are the values to match.

        Returns:
            Optional[ModelType]: The document that matches the filter, or None if not found.
        """
        if not fields:
            raise ValueError("Fields dictionary cannot be empty")

        # Construir el filtro $or dinámicamente
        filter = {"$or": [{key: value} for key, value in fields.items()]}

        # Buscar el documento en la base de datos
        doc = await self.collection.find_one(filter)
        return doc if doc else None

    # -------------------------------------------------
    async def delete_by_id(self, id: str) -> bool:
        result = await self.collection.delete_one({"_id": id})
        return result.deleted_count == 1

    # -------------------------------------------------
    async def delete_by_fields(self, fields: dict) -> int:
        """
        Delete documents based on multiple fields (AND logic).

        Args:
            fields (dict): A dictionary where keys are field names and values are the values to match.

        Returns:
            int: The number of documents deleted.
        """
        if not fields:
            raise ValueError("Fields dictionary cannot be empty")

        # Construir el filtro basado en los campos proporcionados
        filter = {key: value for key, value in fields.items()}

        # Eliminar los documentos que coincidan con el filtro
        result = await self.collection.delete_many(filter)
        return result.deleted_count

    # -------------------------------------------------
    async def delete_all(self) -> int:
        result = await self.collection.delete_many({})
        return result.deleted_count

    # -------------------------------------------------
    async def get_paginated(self, skip: int = 0, limit: int = 20) -> List[ModelType]:
        cursor = self.collection.find().skip(skip).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [self.model(**doc) for doc in docs]

    # -------------------------------------------------
    async def find_with_filter_params(
        self, params: Optional[BaseFilterParams] = None
    ) -> list[ModelType]:
        if params is None:
            # Si no se pasan filtros, traer todos los documentos sin límite
            return await self.get_all()

        filter_dict = params.get_full_filter()
        sort_direction = 1 if params.sort_dir == "asc" else -1

        cursor = (
            self.collection.find(filter_dict)
            .skip(params.offset)
            .limit(params.limit)
            .sort(params.sort_by, sort_direction)
        )
        return await cursor.to_list(length=params.limit)
        # return [self.model(**doc) for doc in docs]

    # -------------------------------------------------
    async def count_by_fields(self, filters: dict) -> int:
        return await self.collection.count_documents(filters)

    # -------------------------------------------------
    async def find_by_filter(
        self,
        filters: dict,
        skip: int = 0,
        limit: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "asc",
    ) -> List[ModelType]:
        """
        Buscar documentos que coincidan con un filtro dinámico con operadores tipo __ne, __gt, etc.

        Args:
            filters (dict): Filtros de búsqueda (ej: {"estado__ne": "inactivo"}).
            skip (int): Cuántos documentos omitir.
            limit (Optional[int]): Máximo de documentos a devolver.
            sort_by (Optional[str]): Campo por el cual ordenar.
            sort_dir (str): Dirección de orden ("asc" o "desc").

        Returns:
            List[]: Lista de documentos encontrados.
        """
        mongo_filter = parse_filter_keys(filters or {})

        cursor = self.collection.find(mongo_filter).skip(skip)

        if sort_by:
            direction = 1 if sort_dir == "asc" else -1
            cursor = cursor.sort(sort_by, direction)

        # Si limit es None, usamos length=None para traer todos los documentos
        docs = await cursor.to_list(length=limit)
        # return [self.model(**doc) for doc in docs]
        return docs
