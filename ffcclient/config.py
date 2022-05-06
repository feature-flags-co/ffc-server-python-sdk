from threading import Event
from typing import Callable, Optional, Tuple

from ffcclient.data_storage import InMemoryDataStorage
from ffcclient.interfaces import (DataStorage, DataUpdateStatusProvider,
                                  EventProcessor, UpdateProcessor)
from ffcclient.utils import is_base64

__all__ = ['Config', 'HTTPConfig', 'WebSocketConfig']

try:
    # https://websocket-client.readthedocs.io/en/latest/faq.html#why-is-this-library-slow
    import wsaccel  # noqa: F401

    def _skip_utf8_validation():
        return False
except ImportError:

    def _skip_utf8_validation():
        return True


class WebSocketConfig:

    def __init__(self,
                 timeout: float = 10.0,
                 ping_interval: float = 15.0,
                 ping_timeout: float = 10.0,
                 ping_payload: Optional[str] = 'ping',
                 sslopt: Optional[dict] = None,
                 proxy_type: Optional[str] = None,
                 proxy_host: Optional[str] = None,
                 proxy_port: Optional[str] = None,
                 proxy_auth: Optional[Tuple[str, str]] = None):

        self.__timeout = 10.0 if timeout is None or timeout <= 0 else min(timeout, 60.0)
        self.__ping_interval = 30.0 if ping_interval is None or ping_interval <= 0 else min(ping_interval, 60.0)
        self.__ping_timeout = 10.0 if ping_timeout is None or ping_timeout <= 0 else min(ping_timeout, 60.0)
        self.__ping_payload = ping_payload
        self.__sslopt = sslopt
        self.__proxy_type = proxy_type
        self.__proxy_host = proxy_host
        self.__proxy_port = proxy_port
        self.__proxy_auth = proxy_auth

    @property
    def skip_utf8_validation(self):
        return _skip_utf8_validation()

    @property
    def timeout(self):
        return self.__timeout

    @property
    def ping_interval(self):
        return self.__ping_interval

    @property
    def ping_timeout(self):
        return self.__ping_timeout

    @property
    def ping_payload(self):
        return self.__ping_payload

    @property
    def sslopt(self):
        return self.__sslopt

    @property
    def proxy_type(self):
        return self.__proxy_type

    @property
    def proxy_host(self):
        return self.__proxy_host

    @property
    def proxy_port(self):
        return self.__proxy_port

    @property
    def proxy_auth(self):
        return self.__proxy_auth


class HTTPConfig:

    def __init__(self,
                 connect_timeout: float = 10.0,
                 read_timeout: float = 15.0,
                 http_proxy: Optional[str] = None,
                 ca_certs: Optional[str] = None,
                 cert_file: Optional[str] = None,
                 disable_ssl_verification: bool = False):

        self.__connect_timeout = 10.0 if connect_timeout is None or connect_timeout <= 0 else connect_timeout
        self.__read_timeout = 15.0 if read_timeout is None or read_timeout <= 0 else read_timeout
        self.__http_proxy = http_proxy
        self.__ca_certs = ca_certs
        self.__cert_file = cert_file
        self.__disable_ssl_verification = disable_ssl_verification

    @property
    def connect_timeout(self) -> float:
        return self.__connect_timeout

    @property
    def read_timeout(self) -> float:
        return self.__read_timeout

    @property
    def http_proxy(self) -> Optional[str]:
        return self.__http_proxy

    @property
    def ca_certs(self) -> Optional[str]:
        return self.__ca_certs

    @property
    def cert_file(self) -> Optional[str]:
        return self.__cert_file

    @property
    def disable_ssl_verification(self) -> bool:
        return self.__disable_ssl_verification


class Config:

    __LATEST_FEATURE_FLAGS_PATH = '/api/public/sdk/latest-feature-flags'
    __STREAMING_PATH = '/streaming'
    __EVENTS_PATH = '/api/public/track'

    def __init__(self,
                 env_secret: str,
                 base_uri: str = 'https://api.featureflag.co',
                 events_max_in_queue: int = 10000,
                 events_flush_interval: float = 3.0,
                 events_retry_interval: float = 0.1,
                 events_max_retries: int = 1,
                 streaming_uri='wss://api.featureflag.co',
                 streaming_first_retry_delay: float = 1.0,
                 offline: bool = False,
                 data_storage: Optional[DataStorage] = None,
                 update_processor_imp: Optional[Callable[['Config', DataUpdateStatusProvider, Event], UpdateProcessor]] = None,
                 event_processor_imp: Optional[Callable[['Config'], EventProcessor]] = None,
                 http: HTTPConfig = HTTPConfig(),
                 websocket: WebSocketConfig = WebSocketConfig(),
                 defaults: dict = {}):

        self.__env_secret = env_secret
        self.__base_uri = base_uri.rstrip('/')
        self.__streaming_uri = streaming_uri.rstrip('/')
        self.__streaming_first_retry_delay = 1.0 if streaming_first_retry_delay is None or streaming_first_retry_delay <= 0 else min(
            streaming_first_retry_delay, 60.0)
        self.__offline = offline
        self.__data_storage = data_storage if data_storage else InMemoryDataStorage()
        self.__event_processor_imp = event_processor_imp
        self.__update_processor_imp = update_processor_imp
        self.__events_max_in_queue = 10000 if events_max_in_queue is None else max(events_max_in_queue, 10000)
        self.__events_flush_interval = 3.0 if events_flush_interval is None or events_flush_interval <= 0 else min(
            events_flush_interval, 10.0)
        self.__events_retry_interval = 0.1 if events_retry_interval is None or events_retry_interval <= 0 else min(
            events_retry_interval, 1)
        self.__events_max_retries = 1 if events_max_retries is None or events_max_retries <= 0 else min(
            events_max_retries, 3)
        self.__http = http
        self.__websocket = websocket
        self.__defaults = defaults

    def copy_config_in_a_new_env(self, env_secret: str, defaults={}) -> 'Config':
        return Config(env_secret,
                      base_uri=self.__base_uri,
                      events_max_in_queue=self.__events_max_in_queue,
                      events_flush_interval=self.__events_flush_interval,
                      events_retry_interval=self.__events_retry_interval,
                      events_max_retries=self.__events_max_retries,
                      streaming_uri=self.__streaming_uri,
                      streaming_first_retry_delay=self.__streaming_first_retry_delay,
                      offline=self.__offline,
                      data_store=self.__data_store,
                      update_processor_imp=self.__update_processor_imp,
                      event_processor_imp=self.__event_processor_imp,
                      http=self.__http,
                      websocket=self.__websocket,
                      defaults=defaults)

    def get_default_value(self, key, default=None):
        return self.__defaults.get(key, default)

    @property
    def env_secret(self) -> str:
        return self.__env_secret

    @property
    def events_max_in_queue(self) -> int:
        return self.__events_max_in_queue

    @property
    def events_flush_interval(self) -> float:
        return self.__events_flush_interval

    @property
    def events_retry_interval(self) -> float:
        return self.__events_retry_interval

    @property
    def events_max_retries(self) -> int:
        return self.__events_max_in_queue

    @property
    def events_url(self):
        return self.__base_uri + self.__EVENTS_PATH

    @property
    def streaming_url(self):
        return self.__streaming_uri + self.__STREAMING_PATH

    @property
    def streaming_first_retry_delay(self):
        return self.__streaming_first_retry_delay

    @property
    def is_offline(self):
        return self.__offline

    @property
    def data_storage(self):
        return self.__data_storage

    @property
    def update_processor_imp(self):
        return self.__update_processor_imp

    @property
    def event_processor_imp(self):
        return self.__event_processor_imp

    @property
    def http(self):
        return self.__http

    @property
    def websocket(self):
        return self.__websocket

    def validate(self):
        if not is_base64(self.__env_secret):
            raise ValueError('env secret is invalid')
