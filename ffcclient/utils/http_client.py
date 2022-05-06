from time import sleep
from typing import Mapping, Optional, Union

import certifi
import urllib3
from ffcclient.config import Config, HTTPConfig
from ffcclient.interfaces import Sender
from ffcclient.utils import build_headers, log


def build_http_factory(config: Config, headers={}):
    return HTTPFactory(build_headers(config.env_secret, headers), config.http)


class HTTPFactory:

    def __init__(self, headers, http_config: HTTPConfig):
        """
        :param override_read_timeout override default read timeout at streaming update
        """
        self.__headers = headers
        self.__http_config = http_config
        self.__timeout = urllib3.Timeout(connect=http_config.connect_timeout, read=http_config.read_timeout)

    @property
    def headers(self) -> Mapping[str, str]:
        return self.__headers

    @property
    def http_config(self) -> HTTPConfig:
        return self.__http_config

    @property
    def timeout(self) -> urllib3.Timeout:
        return self.__timeout

    def create_http_client(self, num_pools=1, max_size=10) -> Union[urllib3.PoolManager, urllib3.ProxyManager]:
        proxy_url = self.__http_config.http_proxy

        if self.__http_config.disable_ssl_verification:
            cert_reqs = 'CERT_NONE'
            ca_certs = None
        else:
            cert_reqs = 'CERT_REQUIRED'
            ca_certs = self.__http_config.ca_certs or certifi.where()

        if not proxy_url:
            return urllib3.PoolManager(num_pools=num_pools,
                                       maxsize=max_size,
                                       headers=self.__headers,
                                       timeout=self.__timeout,
                                       cert_reqs=cert_reqs,
                                       ca_certs=ca_certs)
        else:
            url = urllib3.util.parse_url
            proxy_headers = None
            if url.auth:
                proxy_headers = urllib3.util.make_headers(proxy_basic_auth=url.auth)

            return urllib3.ProxyManager(proxy_url,
                                        num_pools=num_pools,
                                        maxsize=max_size,
                                        headers=self.__headers,
                                        proxy_headers=proxy_headers,
                                        timeout=self.__timeout,
                                        cert_reqs=cert_reqs,
                                        ca_certs=ca_certs)


class DefaultSender(Sender):

    def __init__(self, config: Config, num_pools=1, max_size=10):
        self.__http = build_http_factory(config).create_http_client(num_pools, max_size)
        self.__retry_interval = config.events_retry_interval
        self.__max_retries = config.events_max_retries

    def postJson(self, url: str, json: str, fetch_response: bool = True) -> Optional[str]:
        for i in range(self.__max_retries + 1):
            try:
                if i > 0:
                    sleep(self.__retry_interval)
                response = self.__http.request('POST', url, body=json)
                if response.status == 200:
                    log.debug('sending ok')
                    resp = response.data.decode('utf-8')
                    return resp if fetch_response else None
            except Exception as e:
                log.exception('FFC Python SDK: sending error: %s' % str(e))
        return None

    def stop(self):
        self.__http.clear()
