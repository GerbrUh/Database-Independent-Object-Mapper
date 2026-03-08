from front_end.schemas import *
from typing import Type
from sqlmodel import SQLModel
from datastores.sql_store import engine
SQLModel.metadata.drop_all(engine)
SQLModel.metadata.create_all(engine)

user = User(name="Adriaan", email="adriaan@example.com")
address = Address(street="Main Street", city="Eindhoven")
info = Information(Key='1', email="Example@Mail.com", phone_number="+31 1234567")


user.save()

fetched = User.find_by_id(user.id)
print(f"Fetched: {fetched}")

fetched.name = "Updated Name"
fetched.save()

all_users = User.find_all()
for u in all_users:
    print(u)

fetched.delete()

info.save()