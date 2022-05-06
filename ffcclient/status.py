import threading
from time import time
from typing import Mapping

from ffcclient.category import Category
from ffcclient.interfaces import DataStorage, DataUpdateStatusProvider
from ffcclient.status_types import (DATA_STORAGE_INIT_ERROR,
                                    DATA_STORAGE_UPDATE_ERROR, ErrorInfo,
                                    State, StateType)
from ffcclient.utils import log


class DataUpdateStatusProviderIml(DataUpdateStatusProvider):

    def __init__(self, storage: DataStorage):
        self.__storage = storage
        self.__current_state = State(state_type=StateType.INITIALIZING, state_since=time(), error_info=None)
        self.__lock = threading.Condition(threading.Lock())

    def init(self, all_data: Mapping[Category, Mapping[str, dict]], version: int = 0) -> bool:
        try:
            self.__storage.init(all_data, version)
        except Exception as e:
            self.__handle_exception(e, ErrorInfo(DATA_STORAGE_INIT_ERROR, str(e)))
            return False
        return True

    def upsert(self, kind: Category, key: str, item: dict, version: int = 0) -> bool:
        try:
            self.__storage.upsert(kind, key, item, version)
        except Exception as e:
            self.__handle_exception(e, ErrorInfo(DATA_STORAGE_UPDATE_ERROR, str(e)))
            return False
        return True

    def __handle_exception(self, error: Exception, error_info: ErrorInfo):
        log.exception('FFC Python SDK: Data Storage error: %s, UpdateProcessor will attempt to receive the data' % str(error))
        self.update_state(StateType.INTERRUPTED, error_info)

    @property
    def initialized(self) -> bool:
        return self.__storage.initialized

    @property
    def latest_version(self) -> int:
        return self.__storage.latest_version

    @property
    def current_state(self) -> StateType:
        with self.__lock:
            return self.__current_state.state_type

    def update_state(self, new_state: StateType, error: ErrorInfo = None):
        if not new_state:
            return
        with self.__lock:
            old_state = self.__current_state.state_type
            new_state_copy = new_state
            # special case: if ``new_state`` is INTERRUPTED, but the previous state was INITIALIZING, the state will remain at INITIALIZING
            # INTERRUPTED is only meaningful after a successful startup
            if new_state_copy == StateType.INTERRUPTED and old_state == StateType.INITIALIZING:
                new_state_copy = StateType.INITIALIZING
            # normal case
            if new_state_copy != old_state or error:
                state_since = time() if new_state_copy != old_state else self.__current_state.state_since
                self.__current_state = State(state_type=new_state_copy, state_since=state_since, error_info=error)
                # wakes up all threads waiting for the ok state to check the new state
                self.__lock.notify_all()

    def wait_for_OKState(self, timeoutInSeconds: float = 0) -> bool:
        timeout = 0 if timeoutInSeconds is None or timeoutInSeconds <= 0 else timeoutInSeconds
        deadline = time() + timeout
        with self.__lock:
            while True:
                if StateType.OK == self.__current_state.state_type:
                    return True
                elif StateType.OFF == self.__current_state.state_type:
                    return False
                else:
                    if (timeout == 0):
                        self.__lock.wait()
                    else:
                        now = time()
                        if now >= deadline:
                            return False
                        else:
                            delay = deadline - now
                            self.__lock.wait(delay + 0.001)
