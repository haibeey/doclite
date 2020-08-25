import pydoclite #install pydoclite
import os,glob


def removeFiles():
        for f in glob.glob("doclitetest.db*"):
            try:
                os.remove(f)
            except OSError:
                pass

removeFiles()

d = pydoclite.Doclite.Connect(b"doclitetest.db")
baseCollection = d.Base()

for i in range(10): 
    baseCollection.Insert({"a":1})

baseCollection.FindOne(1)
baseCollection.DeleteOne(2)

print(baseCollection.Find({"a":1}))
baseCollection.Delete({"a":1})

print(baseCollection.Find({"a":1}))
d.Close()
removeFiles()
