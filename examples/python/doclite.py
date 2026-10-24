"""A small ctypes wrapper of the doclite C library.

Build the library first:

    go build -buildmode=c-shared -o libdoclite.so ./sharedlib
"""
import ctypes
import json
import os

_I64 = ctypes.c_int64
_STR = ctypes.c_char_p
_ERR = ctypes.POINTER(ctypes.c_void_p)


class DocliteError(Exception):
    pass


class _Lib:
    def __init__(self, path):
        lib = ctypes.CDLL(path)
        sig = {
            "doclite_open": (_I64, [_STR, ctypes.c_int, _ERR]),
            "doclite_close": (ctypes.c_int, [_I64, _ERR]),
            "doclite_insert": (_I64, [_I64, _STR, _STR, _ERR]),
            "doclite_find_one": (ctypes.c_void_p, [_I64, _STR, _I64, _ERR]),
            "doclite_find": (ctypes.c_void_p, [_I64, _STR, _STR, _ERR]),
            "doclite_update_one": (ctypes.c_int, [_I64, _STR, _I64, _STR, _ERR]),
            "doclite_delete_one": (ctypes.c_int, [_I64, _STR, _I64, _ERR]),
            "doclite_delete_many": (_I64, [_I64, _STR, _STR, _ERR]),
            "doclite_count": (_I64, [_I64, _STR, _STR, _ERR]),
            "doclite_create_index": (ctypes.c_int, [_I64, _STR, _STR, _ERR]),
            "doclite_free": (None, [ctypes.c_void_p]),
        }
        for name, (res, args) in sig.items():
            fn = getattr(lib, name)
            fn.restype = res
            fn.argtypes = args
        self.lib = lib

    def call(self, name, *args):
        err = ctypes.c_void_p()
        out = getattr(self.lib, name)(*args, ctypes.byref(err))
        if err.value:
            msg = ctypes.string_at(err.value).decode()
            self.lib.doclite_free(err.value)
            raise DocliteError(msg)
        return out

    def take_string(self, ptr):
        if not ptr:
            return None
        s = ctypes.string_at(ptr).decode()
        self.lib.doclite_free(ptr)
        return s


def _enc(s):
    return s.encode() if s is not None else None


def _filter(f):
    return _enc(json.dumps(f)) if f is not None else None


class Collection:
    def __init__(self, db, name):
        self._db = db
        self.name = name

    def _call(self, fn, *args):
        return self._db._lib.call(fn, self._db._h, _enc(self.name), *args)

    def collection(self, name):
        return Collection(self._db, self.name + "/" + name)

    def insert(self, doc):
        return self._call("doclite_insert", _enc(json.dumps(doc)))

    def find_one(self, doc_id):
        s = self._db._lib.take_string(self._call("doclite_find_one", doc_id))
        return json.loads(s) if s is not None else None

    def find(self, flt=None):
        s = self._db._lib.take_string(self._call("doclite_find", _filter(flt)))
        return json.loads(s)

    def update_one(self, doc_id, doc):
        return self._call("doclite_update_one", doc_id, _enc(json.dumps(doc))) == 0

    def delete_one(self, doc_id):
        return self._call("doclite_delete_one", doc_id) == 0

    def delete_many(self, flt=None):
        return self._call("doclite_delete_many", _filter(flt))

    def count(self, flt=None):
        return self._call("doclite_count", _filter(flt))

    def create_index(self, field):
        self._call("doclite_create_index", _enc(field))


class Doclite:
    def __init__(self, path, lib_path=None, read_only=False):
        lib_path = lib_path or os.path.join(os.path.dirname(__file__), "libdoclite.so")
        self._lib = _Lib(lib_path)
        self._h = self._lib.call("doclite_open", _enc(path), 1 if read_only else 0)

    def collection(self, name):
        return Collection(self, name)

    def close(self):
        if self._h:
            self._lib.call("doclite_close", self._h)
            self._h = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
