from front_end.schemas import *
from typing import Type
from sqlmodel import SQLModel
from datastores.sql_store import engine
SQLModel.metadata.drop_all(engine)
SQLModel.metadata.create_all(engine)

User.purge_data()
Address.purge_data()
Information.purge_data()

user = User(id='1', name="Max", email="max@example.com", age=28)
address = Address(street="Main Street", city="Eindhoven")
info = Information(Key='1', email="Example@Mail.com", phone_number="+31 1234567")

print("\nSave")
user.save()

data = [
    {"id": "2", "name": "Alex", "email":"alex@example.com", "age": 29},
    {"id": "3", "name": "Charles", "email": "charles@example.com", "age": 28}
]
users = User.mass_save(data)

print(User.find_all())

print("\nFind")
print(User.find_by_id("2"))
print(User.find_by_field({"name": "Alex"}))
print(User.find_by_field({"email": {"$sub": "max"}}))
print(User.find_by_field({"age": {"$gte": 28, "$lt": 29}}))

User.save_to_json('data')

print("\nDelete")
user.delete()
User.mass_delete(["2", "3"])
print(User.find_all())

print("\nJson")
User.load_from_json('data')
print(User.find_all())

print("\nRA")
rename = RA.Rename(User, {'name': 'u-name', 'email': 'u-email', 'age': 'u-age'})
print("Rename: " + str(rename))
projection = RA.Projection(rename, ['u-name', 'u-email', 'u-age'])
print("Projection: " + str(projection))
print("Cartesian Join: " + str(RA.Join(User, projection)))
print("Match Join: " + str(RA.Join(User, rename)))

select = RA.Select(User, {'age': {'$lt': 29}})
print("Select: " + str(select))
union = RA.Union_op(RA.Select(User, {'name': 'Max'}), RA.Select(User, {'name': 'Alex'}))
print("Union: " + str(union))
print("Difference: " + str(RA.Difference(select, union)))
print("Intersection: " + str(RA.Intersect(select, union)))

union2 = RA.Union_op(User, User)
print("Order: " + str(RA.Order(union2, ['age'])))
print("Distinct: " + str(RA.Distinct(RA.Order(union2, ['age']))))