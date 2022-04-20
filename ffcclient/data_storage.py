from collections import defaultdict
from typing import Callable, Mapping, Optional

from ffcclient.category import Category
from ffcclient.interfaces import DataStorage
from ffcclient.utils.rwlock import ReadWriteLock


class InMemoryDataStorage(DataStorage):

    def __init__(self):
        self.__rw_lock = ReadWriteLock()
        self.__initialized = False
        self.__version = 0
        self.__storage = defaultdict(dict)

    def get(self,
            kind: Category,
            key: str,
            op: Callable[[Optional[dict]], Optional[dict]] = lambda x: x) -> Optional[dict]:
        try:
            self.__rw_lock.read_lock()
            keyItems = self.__storage[kind]
            item = keyItems.get(key, False)
            if (not item) or item['isArchived']:
                return op(None)
            return op(item)
        finally:
            self.__rw_lock.release_read_lock()

    def get_all(self,
                kind: Category,
                op: Callable[[Mapping[str, dict]], Mapping[str, dict]] = lambda x: x) -> Mapping[str, dict]:
        try:
            self.__rw_lock.read_lock()
            keyItems = self.__storage[kind]
            all_items = dict((k, v) for k, v in keyItems.items() if not v['isArchived'])
            return op(all_items)
        finally:
            self.__rw_lock.release_read_lock()

    def init(self, all_data: Mapping[Category, Mapping[str, dict]], version: int = 0):
        if (not all_data) or version <= self.__version:
            return
        try:
            self.__rw_lock.write_lock()
            self.__storage.clear()
            self.__storage.update(all_data)
            self.__initialized = True
            self.__version = version
        finally:
            self.__rw_lock.release_write_lock()

    def upsert(self, kind: Category, key: str, item: dict, version: int = 0):
        if (not item) or version <= self.__version:
            return
        try:
            self.__rw_lock.write_lock()
            keyItems = self.__storage[kind]
            v = keyItems.get(key, False)
            if (not v) or v['timestamp'] < version:
                keyItems[key] = item
                self.__version = version
                if not self.__initialized:
                    self.__initialized = True
        finally:
            self.__rw_lock.release_write_lock()

    @property
    def initialized(self) -> bool:
        try:
            self.__rw_lock.read_lock()
            return self.__initialized
        finally:
            self.__rw_lock.release_read_lock()

    @property
    def latest_version(self) -> int:
        try:
            self.__rw_lock.read_lock()
            return self.__version
        finally:
            self.__rw_lock.release_read_lock()


class NullDataStorage(DataStorage):

    def get(self,
            kind: Category,
            key: str,
            op: Callable[[Optional[dict]], Optional[dict]] = lambda x: x) -> Optional[dict]:
        return op(None)

    def get_all(self,
                kind: Category,
                op: Callable[[Mapping[str, dict]], Mapping[str, dict]] = lambda x: x) -> Mapping[str, dict]:
        return op(dict())

    def init(self, all_data: Mapping[Category, Mapping[str, dict]], version: int = 0):
        pass

    def upsert(self, kind: Category, key: str, item: dict, version: int = 0):
        pass

    @property
    def initialized(self) -> bool:
        return True

    @property
    def latest_version(self) -> int:
        return 0
