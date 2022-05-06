import json
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from typing import Iterable

import schedule

from ffcclient.common_types import FFCEvent
from ffcclient.config import Config
from ffcclient.event_types import (EventMessage, FlagEvent, MessageType,
                                   MetricEvent)
from ffcclient.interfaces import EventProcessor, Sender
from ffcclient.utils import log
from ffcclient.utils.http_client import DefaultSender


class DefaultEventProcessor(EventProcessor):
    def __init__(self, config: Config):
        self.__inbox = Queue(maxsize=config.events_max_in_queue)
        self.__closed = False
        self.__lock = threading.Lock()
        EventDispatcher(config, self.__inbox).start()
        schedule.every(config.events_flush_interval).seconds.do(self.flush)
        log.debug('event processor flush thread is registered in schedule')
        log.debug('event processor is ready')

    def __put_message_to_inbox(self, message: EventMessage) -> bool:
        try:
            self.__inbox.put_nowait(message)
            return True
        except:
            if message.type == MessageType.SHUTDOWN:
                # must put the shut down to inbox;
                self.__inbox.put(message, block=True, timeout=None)
                return True
            #  if it reaches here, it means the application is probably doing tons of flag
            #  evaluations across many threads-- so if we wait for a space in the inbox, we risk a very serious slowdown
            #  of the app. To avoid that, we'll just drop the event or you can increase the capacity of inbox
            log.warn('FFC Python SDK: Events are being produced faster than they can be processed; some events will be dropped')
            return False

    def __put_message_async(self, type: MessageType, event: FFCEvent):
        message = EventMessage(type, event, False)
        if self.__put_message_to_inbox(message):
            log.trace('put %s message to inbox' % str(type))

    def __put_message_and_wait_terminate(self, type: MessageType, event: FFCEvent):
        message = EventMessage(type, event, True)
        if self.__put_message_to_inbox(message):
            log.debug('put %s WaitTermination message to inbox' % str(type))
            message.waitForComplete()

    def send_event(self, event: FFCEvent):
        if not self.__closed and event:
            if isinstance(event, FlagEvent):
                self.__put_message_async(MessageType.FLAGS, event)
            elif isinstance(event, MetricEvent):
                self.__put_message_async(MessageType.METRICS, event)

    def flush(self):
        if not self.__closed:
            self.__put_message_async(MessageType.FLUSH, None)

    def stop(self):
        with self.__lock:
            if not self.__closed:
                log.info('FFC Python SDK: event processor is stopping')
                self.__closed = True
                self.flush()
                self.__put_message_and_wait_terminate(MessageType.SHUTDOWN, None)


class EventDispatcher(threading.Thread):

    __MAX_FLUSH_WORKERS_NUMBER = 5
    __BATCH_SIZE = 50

    def __init__(self, config: Config, inbox: "Queue[EventMessage]"):
        super().__init__(daemon=True)
        self.__inbox = inbox
        self.__config = config
        self.__closed = False
        self.__events_buffer_to_next_flush = []
        self.__sender = DefaultSender(config, max_size=max(self.__MAX_FLUSH_WORKERS_NUMBER, 10))
        self.__pool = ThreadPoolExecutor(max_workers=self.__MAX_FLUSH_WORKERS_NUMBER)
        self.__permits = threading.BoundedSemaphore(value=self.__MAX_FLUSH_WORKERS_NUMBER)

    def run(self):
        log.debug('event dispatcher is working...')
        while True:
            try:
                msgs = self.__drain_inbox(size=self.__BATCH_SIZE)
                for msg in msgs:
                    try:
                        if msg.type == MessageType.FLAGS or msg.type == MessageType.METRICS:
                            self.__put_events_to_buffer(msg.event)
                        elif msg.type == MessageType.FLUSH:
                            self.__trigger_flush()
                        elif msg.type == MessageType.SHUTDOWN:
                            self.__shutdown()
                            msg.completed()
                            return  # exit the loop
                        msg.completed()
                    except Exception as e1:
                        log.exception('FFC Python SDK: unexpected error in event dispatcher: %s' % str(e1))
            except Exception as e2:
                log.exception('FFC Python SDK: unexpected error in event dispatcher: %s' % str(e2))

    def __drain_inbox(self, size=50) -> Iterable[EventMessage]:
        msg = self.__inbox.get(block=True, timeout=None)
        msgs = [msg]
        for _ in range(size - 1):
            try:
                msg = self.__inbox.get_nowait()
                msgs.append(msg)
            except Empty:
                break
        return msgs

    def __put_events_to_buffer(self, event: FFCEvent):
        if not self.__closed and event.is_send_event:
            log.debug('put event to buffer')
            self.__events_buffer_to_next_flush.append(event)

    def __trigger_flush(self):
        if not self.__closed and self.__events_buffer_to_next_flush:
            log.debug('trigger flush')
            if self.__permits.acquire(blocking=False):
                payload = []
                payload.extend(self.__events_buffer_to_next_flush)
                self.__pool \
                    .submit(FlushPayloadExecutor(self.__config, self.__sender, payload).run) \
                    .add_done_callback(lambda x: self.__permits.release())
                # clear the buffer for the incoming flush
                self.__events_buffer_to_next_flush.clear()

    def __shutdown(self):
        if not self.__closed:
            try:
                log.debug('event dispatcher is cleaning up thread and conn pool')
                self.__closed = True
                log.debug('flush payload pool is stopping...')
                self.__pool.shutdown(wait=True)
                log.debug('event sender is stopping...')
                self.__sender.stop()
            except Exception as e:
                log.exception('FFC Python SDK: unexpected error when closing event dispatcher: %s' % str(e))


class FlushPayloadExecutor:
    def __init__(self, config: Config, sender: Sender, payload: Iterable[FFCEvent]):
        self.__config = config
        self.__sender = sender
        self.__payload = payload

    def run(self):
        try:
            json_payload = [event.to_json_dict() for event in self.__payload]
            json_str = json.dumps(json_payload)
            log.debug(json_str)
            self.__sender.postJson(self.__config.events_url, json_str, fetch_response=False)
        except Exception as e:
            log.exception('FFC Python SDK: unexpected error in sending payload: %s' % str(e))


class NullEventProcessor(EventProcessor):
    def __init__(self, config: Config):
        pass

    def send_event(self, event: FFCEvent):
        pass

    def flush(self):
        pass

    def stop(self):
        pass
