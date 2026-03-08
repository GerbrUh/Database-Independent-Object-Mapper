from pymongo import MongoClient

mongo_client = MongoClient("mongodb://localhost:27017")

try:
    mongo_client.admin.command("ping")
    print("Successfully connected to local MongoDB!")
except Exception as e:
    print(e)

mongo_db = mongo_client["my_database"]


from sqlalchemy.orm import sessionmaker
from sqlmodel import create_engine

sql_engine = create_engine("sqlite:///app.db", echo=False)
session_maker = sessionmaker(bind=sql_engine)


import os
os.environ["ydb_gbldir"] = "..." #insert path
import yottadb
