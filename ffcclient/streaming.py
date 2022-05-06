import json
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Event, Thread
from time import sleep

import schedule
import websocket
from websocket._exceptions import (WebSocketConnectionClosedException,
                                   WebSocketException,
                                   WebSocketTimeoutException)

from ffcclient.category import FFC_FEATURE_FLAGS, FFC_SEGMENTS
from ffcclient.config import Config
from ffcclient.interfaces import DataUpdateStatusProvider, UpdateProcessor
from ffcclient.status_types import (DATA_INVALID_ERROR, NETWORK_ERROR,
                                    REQUEST_INVALID_ERROR, RUNTIME_ERROR,
                                    SYSTEM_ERROR, SYSTEM_QUIT,
                                    UNKNOWN_CLOSE_CODE, UNKNOWN_ERROR,
                                    WEBSOCKET_ERROR, ErrorInfo, StateType)
from ffcclient.utils import build_headers, build_token, log, valide_all_data
from ffcclient.utils.exponential_backoff_jitter_strategy import \
    FFCBackoffAndJitterStrategy

WS_NORMAL_CLOSE = 1000

WS_NORMAL_CLOSE_REASON = b'normal close'

WS_GOING_AWAY_CLOSE = 1001

WS_GOING_AWAY_CLOSE_REASON = b'client going away'

WS_INVALID_REQUEST_CLOSE = 4003

WS_EXCEPTION_TO_RECONN = (WebSocketTimeoutException, WebSocketConnectionClosedException)

JUST_RECONN_REASON_REGISTERED = 'reconn'


def get_error_details(error):
    if isinstance(error, WebSocketException):
        if isinstance(error, WS_EXCEPTION_TO_RECONN):
            return True, False, NETWORK_ERROR
        else:
            return False, False, WEBSOCKET_ERROR,
    if isinstance(error, OSError):
        return True, False, SYSTEM_ERROR,
    if isinstance(error, (KeyboardInterrupt, SystemExit)):
        return False, False, SYSTEM_QUIT,
    return True, True, RUNTIME_ERROR


# interal use: to record the details of close by client peer
CloseStatus = namedtuple('CloseStatus', ['close_status_code', 'close_msg', 'is_reconn', 'is_update_state', 'error_info'])


class Streaming(Thread, UpdateProcessor):

    def __init__(self, config: Config, dataUpdateStatusProvider: DataUpdateStatusProvider, ready: Event):
        super().__init__(daemon=True)
        self.__config = config
        self.__storage = dataUpdateStatusProvider
        self.__ready = ready
        self.__running = True
        self.__strategy = FFCBackoffAndJitterStrategy(config.streaming_first_retry_delay)
        self.__wsapp = None
        self.__close_status = None
        self.__pool = ThreadPoolExecutor(max_workers=1)
        self.__semaphore = BoundedSemaphore(value=20)
        schedule.every(config.websocket.ping_interval).seconds.do(self._on_ping)
        log.debug('Streaming ping thread is registered in schedule')

    def _init_wsapp(self):

        # authenfication and headers
        token = build_token(self.__config.env_secret)
        params = '?token=%s&type=server' % token
        url = self.__config.streaming_url + params
        headers = build_headers(self.__config.env_secret)

        # a timeout is triggered if no connection response is received
        websocket.setdefaulttimeout(self.__config.websocket.timeout)
        # init web socket app
        self.__wsapp = websocket.WebSocketApp(url,
                                              header=headers,
                                              on_open=self._on_open,
                                              on_message=self._on_message,
                                              on_close=self._on_close,
                                              on_error=self._on_error)
        # set the conn time
        self.__strategy.set_good_run()
        log.debug('Streaming WebSocket is connecting...')

    def run(self):
        while (self.__running):
            try:
                self._init_wsapp()
                self.__wsapp.run_forever(sslopt=self.__config.websocket.sslopt,
                                         ping_interval=self.__config.websocket.ping_interval,
                                         ping_timeout=self.__config.websocket.ping_timeout,
                                         ping_payload=self.__config.websocket.ping_payload,
                                         http_proxy_host=self.__config.websocket.proxy_host,
                                         http_proxy_port=self.__config.websocket.proxy_port,
                                         http_proxy_auth=self.__config.websocket.proxy_auth,
                                         proxy_type=self.__config.websocket.proxy_type,
                                         skip_utf8_validation=self.__config.websocket.skip_utf8_validation)
                if self.__running:
                    # calculate the delay for reconn
                    delay = self.__strategy.next_delay()
                    sleep(delay)
            except Exception as e:
                msg = str(e)
                log.exception('FFC Python SDK: Streaming unexpected error: %s', msg)
                self.__storage.update_state(StateType.INTERRUPTED, ErrorInfo(UNKNOWN_ERROR, msg))
            finally:
                # clear the last connection info
                self.__wsapp = None
                self.__close_status = None
        log.debug('Streaming WebSocket process is over')
        if not self.__ready.is_set():
            # if an no-reconn error occurs in the first attempt, set ready not to wait
            self.__ready.set()

    # handle websocket auto close issue
    def _on_ping(self):
        if self.__wsapp and self.__wsapp.sock and self.__wsapp.sock.connected:
            log.trace('ping')
            data_sync_msg = {'messageType': 'ping', 'data': None}
            json_str = json.dumps(data_sync_msg)
            self.__wsapp.send(json_str)

    def _on_close(self, wsapp, close_code, close_msg):
        if self.__close_status:
            # close by client
            self.__running = self.__close_status.is_reconn
            if self.__close_status.close_status_code == WS_NORMAL_CLOSE:
                # close code 1000
                state_type = StateType.OFF
            else:
                # close code 1001 because of some internal error
                state_type = StateType.INTERRUPTED if self.__close_status.is_reconn else StateType.OFF
            if self.__close_status.is_update_state:
                self.__storage.update_state(state_type, self.__close_status.error_info)
            log.debug('Streaming WebSocket close reason: %s' % self.__close_status.close_msg)
        elif close_code == WS_INVALID_REQUEST_CLOSE:
            # close by server with code 4003
            self.__running = False
            self.__storage.update_state(StateType.OFF, ErrorInfo(REQUEST_INVALID_ERROR, close_msg))
            log.debug('Streaming WebSocket close reason: %s' % close_msg)
        elif close_code:
            # close by server with an unknown close code, restart immediately
            msg = close_msg if close_msg else 'unexpected close'
            self.__storage.update_state(StateType.INTERRUPTED, ErrorInfo(UNKNOWN_CLOSE_CODE, msg))
            log.debug('Streaming WebSocket close reason: %s' % msg)

    def _on_error(self, wsapp: websocket.WebSocketApp, error):
        is_reconn, is_ws_close, error_type = get_error_details(error)
        error_info = ErrorInfo(error_type, str(error))
        log.warn('FFC Python SDK: Streaming WebSocket Failure, type=%s, msg=%s' % (error_type, str(error)))
        if is_ws_close:
            self.__close_status = CloseStatus(close_status_code=WS_GOING_AWAY_CLOSE,
                                              close_msg=WS_GOING_AWAY_CLOSE_REASON.decode(),
                                              is_reconn=is_reconn,
                                              is_update_state=True,
                                              error_info=error_info)
            wsapp.close(status=WS_GOING_AWAY_CLOSE, reason=WS_GOING_AWAY_CLOSE_REASON)
        else:
            self.__running = is_reconn
            state_type = StateType.INTERRUPTED if is_reconn else StateType.OFF
            self.__storage.update_state(state_type, ErrorInfo(error_type, str(error)))

    def _on_open(self, wsapp: websocket.WebSocketApp):
        log.debug('Asking Data updating on WebSocket')
        version = self.__storage.latest_version if self.__storage.initialized else 0
        data_sync_msg = {'messageType': 'data-sync', 'data': {'timestamp': version}}
        json_str = json.dumps(data_sync_msg)
        wsapp.send(json_str)

    def _on_process_data(self, data):

        def data_to_dict(data: dict) -> tuple[int, dict]:
            version = 0
            all_data = {}
            flags = {}
            segments = {}
            all_data[FFC_FEATURE_FLAGS] = flags
            all_data[FFC_SEGMENTS] = segments
            for flag in data['featureFlags']:
                flags[flag['id']] = {'id': flag['id'], 'timestamp': flag['timestamp'], 'isArchived': True} if flag['isArchived'] else flag
                version = max(version, flag['timestamp'])
            for segment in data['segments']:
                segments[segment['id']] = {'id': segment['id'], 'timestamp': segment['timestamp'], 'isArchived': True} if segment['isArchived'] else segment
                version = max(version, segment['timestamp'])
            return version, all_data

        version, all_data = data_to_dict(data)
        op_ok = False
        if 'patch' == data['eventType']:
            patch_ok = True
            for cat, items in all_data.items():
                for key, value in items.items():
                    self.__storage.upsert(cat, key, value, version)
            op_ok = patch_ok
        else:
            op_ok = self.__storage.init(all_data, version)
        if op_ok:
            if not self.__ready.is_set():
                # set ready when the initialization is complete.
                self.__ready.set()
            self.__storage.update_state(StateType.OK, None)
            log.debug("processing data is well done")
        else:
            if self.__wsapp:
                # state already updated in init or upsert, just reconn
                self.__close_status = CloseStatus(close_status_code=WS_GOING_AWAY_CLOSE,
                                                  close_msg=WS_GOING_AWAY_CLOSE_REASON.decode(),
                                                  is_reconn=True,
                                                  is_update_state=False,
                                                  error_info=None)
                self.__wsapp.close(status=WS_GOING_AWAY_CLOSE, reason=WS_GOING_AWAY_CLOSE_REASON)
        return op_ok

    def _on_message(self, wsapp: websocket.WebSocketApp, msg):
        log.trace('Streaming WebSocket data: %s' % msg)
        try:
            all_data = json.loads(msg)
            if valide_all_data(all_data):
                self.__semaphore.acquire()
                log.debug('Streaming WebSocket is processing data')
                self.__pool.submit(self._on_process_data,
                                   (all_data['data'])).add_done_callback(lambda x: self.__semaphore.release())
        except Exception as e:
            if isinstance(e, json.JSONDecodeError):
                self.__close_status = CloseStatus(close_status_code=WS_GOING_AWAY_CLOSE,
                                                  close_msg=WS_GOING_AWAY_CLOSE_REASON.decode(),
                                                  is_reconn=False,
                                                  is_update_state=True,
                                                  error_info=ErrorInfo(DATA_INVALID_ERROR, str(e)))
                wsapp.close(status=WS_GOING_AWAY_CLOSE, reason=WS_GOING_AWAY_CLOSE_REASON.decode())

    def stop(self):
        log.info('FFC Python SDK: Streaming is stopping...')
        if self.__running and self.__wsapp:
            self.__close_status = CloseStatus(close_status_code=WS_NORMAL_CLOSE,
                                              close_msg=WS_NORMAL_CLOSE_REASON.decode(),
                                              is_reconn=False,
                                              is_update_state=True,
                                              error_info=None)
            self.__wsapp.close(status=WS_NORMAL_CLOSE, reason=WS_NORMAL_CLOSE_REASON)
        log.debug('Streaming thread pool is stopping...')
        self.__pool.shutdown(wait=True)

    @property
    def initialized(self) -> bool:
        return self.__running and self.__ready.is_set() and self.__storage.initialized
