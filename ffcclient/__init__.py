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
    """Returns the singleton Python SDK client instance, using the current configuration.

    To use the SDK as a singleton, first make sure you have called :func:`ffcclient.set_config()`
    at startup time. Then ``get()`` will return the same shared :class:`ffcclient.client.FFCClient`
    instance each time. The client will be initialized if it runs first time.

    If you need to create multiple client instances with different environments, instead of this
    singleton approach you can call directly the :class:`ffcclient.client.FFCClient` constructor.
    """
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
            log.info("FFC Python SDK: FFC Python Client is initializing...")
            __client = FFCClient(__config, start_wait)
        return __client
    finally:
        __lock.release_write_lock()


def set_config(config: Config):
    """Sets the configuration for the shared SDK client instance.

    If this is called prior to :func:`ffcclient.get()`, it stores the configuration that will be used when the
    client is initialized. If it is called after the client has already been initialized, the client will be
    re-initialized with the new configuration.

    :param config: the client configuration
    """
    global __config
    global __client
    global __lock

    try:
        __lock.write_lock()
        if __client:
            __client.stop()
            log.info('FFC Python SDK: FFC Python Client is reinitializing...')
            __client = FFCClient(config, start_wait)
    finally:
        __config = config
        __lock.release_write_lock()
