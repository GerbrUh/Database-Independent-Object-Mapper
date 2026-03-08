from core.ra import Join
from datastores.mongo_store import MongoDocument
from datastores.yotta_store_string import YottaDocument
from datastores.sql_store import SQLDocument
from typing import Optional
from sqlmodel import Field

class User(MongoDocument):
    name: Optional[str] = None
    age: Optional[int] = None
     
class Address(YottaDocument):
    street: Optional[str] = None
    city: Optional[str] = None

class Information(SQLDocument, table=True):
    Key: str = Field(primary_key=True)
    email: Optional[str] = None
    phone_number: Optional[str] = None