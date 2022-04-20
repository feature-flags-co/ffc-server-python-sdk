from ffcclient.client import FFCClient
from ffcclient.config import Config
from ffcclient.utils.rwlock import ReadWriteLock
from ffcclient.utils import log

"""Settings."""
start_wait = 15

__client = None
__config = None
__lock = ReadWriteLock()


def get() -> FFCClient:
    global __config
    global __client
    global __lock

    try:
        __lock.read_lock()
        if __client:
            return __client
        if not __config:
            raise Exception("config is not initialized")
    finally:
        __lock.release_read_lock()

    try:
        __lock.write_lock()
        if not __client:
            log.info("FFC Python Client is initializing...")
            __client = FFCClient(__config, start_wait)
        return __client
    finally:
        __lock.release_write_lock()


def set_config(config: Config):
    global __config
    global __client
    global __lock

    try:
        __lock.write_lock()
        if __client:
            __client.stop()
            log.info('FFC Python Client is reinitializing...')
            __client = FFCClient(config, start_wait)
    finally:
        __config = config
        __lock.release_write_lock()
