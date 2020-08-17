from ctypes import *
import json

deleted=b'deleted'
class GoString(Structure):
    _fields_ = [("p", c_char_p), ("n", c_longlong)]

lib = cdll.LoadLibrary("../../sharedlib/doclite.so")
lib.ConnectDB.argtypes = [GoString]
lib.Close.argtypes = []


lib.Insert.argtypes = [GoString]
lib.Insert.restype = GoString

lib.FindOne.argtypes = [c_longlong, GoString]
lib.FindOne.restype = GoString

lib.Find.argtypes = [GoString,GoString]
lib.Find.restype = GoString

lib.Delete.argtypes = [GoString,GoString]


class Collection:
    def __init__(self,name:str):
        self.name = name

    def GetCollection(self,name:str):
        if "|" in name:
            raise ValueError("name cannot contain alphanumeric")
        return Collection("{}|{}".format(self.name,name))
    
    def Insert(self,document:dict):
        document = json.dumps(document).encode()
        name = self.name.encode()
        return lib.Insert(
            GoString(document, len(document)),
            GoString(name, len(name))
        )

    def FindOne(self,id):
        name = self.name.encode()
        res = lib.FindOne(
            id,
            GoString(name, len(name)),
        ).p
        if not res or res==deleted:
            return {}
        return json.loads(res)

    def Find(self,filter):
        filter = json.dumps(filter).encode()
        name = self.name.encode()
        res=lib.Find(
            GoString(name, len(name)),
            GoString(filter, len(filter)),
        ).p
        if not res:
            return []
        return json.loads(res)

    def DeleteOne(self,id):
        name = self.name.encode()
        lib.DeleteOne(
            id,
            GoString(name, len(name)),
        )

    def Delete(self,filter):
        filter = json.dumps(filter).encode()
        name = self.name.encode()
        lib.Delete(
            GoString(name, len(name)),
            GoString(filter, len(filter)),
        )

class Doclite:
    @staticmethod
    def Connect(name:str):
        filename  = GoString(name, len(name))
        lib.ConnectDB(filename)
        return Doclite()
    
    def Close(self):
        lib.Close()

    def Base(self):
        return Collection("")


d = Doclite.Connect(b"doclitetest.db")
baseCollection = d.Base()
for i in range(10): 
    baseCollection.Insert({"a":1})
print(baseCollection.FindOne(1))
baseCollection.DeleteOne(2)
print(baseCollection.Find({"a":1}))
baseCollection.Delete({"a":1})

d.Close()

