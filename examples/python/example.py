import glob
import os
import sys

from doclite import Doclite

path = "example_py.doclite"
lib = sys.argv[1] if len(sys.argv) > 1 else None

with Doclite(path, lib_path=lib) as db:
    people = db.collection("people")
    people.create_index("age")
    for i in range(10):
        people.insert({"name": "person %d" % i, "age": 20 + i})

    print(people.find_one(3))
    people.delete_one(2)
    print(people.find_one(2))  # None
    people.update_one(1, {"name": "renamed", "age": 50})

    print(people.find({"age": {"$gte": 25}}))
    print("count:", people.count())
    print("deleted:", people.delete_many({"age": {"$lt": 25}}))
    print("count:", people.count())

for f in glob.glob(path + "*"):
    os.remove(f)
