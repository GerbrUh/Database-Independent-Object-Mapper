from bson import ObjectId, errors as bson_errors
from pydantic import root_validator
from typing import Optional, Type, TypeVar
from core.template import TemplateClass
from front_end.connect import mongo_db as db
from typing import List
from pymongo import UpdateOne

import re
from typing import Union, Dict, Any

T = TypeVar('T', bound='MongoDocument')

class MongoDocument(TemplateClass):

    @classmethod
    def _collection(cls):
        return db[cls.__name__.lower()]
    
    @classmethod
    def _normalize_id(self, id_value):
        if id_value is None:
            return None
        try:
            return ObjectId(id_value)
        except (bson_errors.InvalidId, TypeError):
            return id_value

    def save(self):
        data = self.dict(by_alias=True, exclude_none=True)
        if self.id:
            data.pop("_id", None) 
            self._collection().update_one(
                {"_id": self._normalize_id(self.id)},
                {"$set": data},
                upsert=True  
            )
        else:
            result = self._collection().insert_one(data)
            self.id = str(result.inserted_id)

    def delete(self):
        if self.id:
            self._collection().delete_one({"_id": self._normalize_id(self.id)})

    @classmethod
    def find_by_id(cls, id: str) -> Optional[T]:
        data = cls._collection().find_one({"_id": cls._normalize_id(id)})
        return cls.parse_obj(data) if data else None

    @classmethod
    def find_all(cls) -> Optional[List[T]]:
        return [cls.parse_obj(doc) for doc in cls._collection().find()]

    @root_validator(pre=True)
    def convert_object_id(cls, values):
        _id = values.get("_id")
        if isinstance(_id, ObjectId):
            values["_id"] = str(_id)
        return values


    @classmethod
    def get_collection(cls):
        return db[cls.__name__.lower()]  

    @classmethod
    def find_by_field(
        cls: Type[T],
        query: Dict[str, Any],
        *,
        multiple: bool = True,
    ) -> Union[Optional[T], List[T], List[tuple[T, Optional["MongoDocument"]]]]:
        if not isinstance(query, dict):
            raise ValueError("Query must be a dict of field → condition/value")

        mongo_query: Dict[str, Any] = {}
        
        for field, condition in query.items():
            if isinstance(condition, dict):
                if "$sub" in condition:
                    val = condition["$sub"]
                    case = condition.get("case", True)
                    regex = re.escape(val)
                    mongo_query[field] = {"$regex": regex, "$options": "i" if case else ""}
                elif "$regex" in condition:
                    mongo_query[field] = {"$regex": condition["$regex"]}
                    if "$options" in condition:
                        mongo_query[field]["$options"] = condition["$options"]
                else:
                    mongo_query[field] = condition
            else:
                mongo_query[field] = condition

        collection = cls.get_collection()

        if multiple:
            cursor = collection.find(mongo_query)
            docs = list(cursor)
        else:
            doc = collection.find_one(mongo_query)
            docs = [doc] if doc else []

        parsed = [cls(**doc) for doc in docs]
        return parsed if multiple else (parsed[0] if parsed else None)


    @classmethod
    def mass_save(cls, rows: list[dict]) -> None:
        if not rows:
            return

        collection = cls._collection()

        inserts = []
        updates = []

        for row in rows:
            data = dict(row)

            _id = data.pop("_id", None) or data.pop("id", None)

            if _id:
                updates.append(
                    UpdateOne(
                        {"_id": cls._normalize_id(_id)},
                        {"$set": data},
                        upsert=True
                    )
                )
            else:
                inserts.append(data)

        if inserts:
            collection.insert_many(inserts)

        if updates:
            collection.bulk_write(updates, ordered=False)

    @classmethod
    def mass_delete(cls: Type[T], ids: List[str]) -> None:
        if not ids:
            return

        normalized_ids = [
            cls._normalize_id(id)
            for id in ids
            if id is not None
        ]

        if not normalized_ids:
            return

        cls._collection().delete_many(
            {"_id": {"$in": normalized_ids}}
        )


