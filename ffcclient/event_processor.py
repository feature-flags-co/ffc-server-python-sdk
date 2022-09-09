import json
import threading
from queue import Empty, Full, Queue
from typing import Iterable

import atomics
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
        self.__sender = DefaultSender(config, max_size=max(self.__MAX_FLUSH_WORKERS_NUMBER, 10))
        self.__events_buffer_to_next_flush = []

        #  This queue only holds one payload, that should be immediately picked up by any free flush worker.
        #  if we try to push another one to this queue and then is refused,
        #  it means all the flush workers are busy, this payload will be consumed until a flush worker becomes free again.
        #  Events in the refused payload should be kept in buffer and try to be pushed to this queue in the next flush
        self.__outbox = Queue(maxsize=1)
        self.__busy_payload_thread_num = atomics.atomic(width=4, atype=atomics.INT)
        self.__payload_thread_cond = threading.Condition(threading.Lock())
        self.__payload_threads = []
        for _ in range(self.__MAX_FLUSH_WORKERS_NUMBER):
            t = FlushPayloadExecutor(self.__config,
                                     self.__sender,
                                     self.__outbox,
                                     self.__busy_payload_thread_num,
                                     self.__payload_thread_cond)
            self.__payload_threads.append(t)
            t.start()

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

            # get all the current events from event buffer
            payloads = []
            payloads.extend(self.__events_buffer_to_next_flush)
            # busy payload thread num + 1
            self.__busy_payload_thread_num.inc()
            try:
                #  put events to the next available flush worker
                self.__outbox.put_nowait(payloads)
                # clear the buffer for the incoming flush
                self.__events_buffer_to_next_flush.clear()
            except Full:
                log.debug('Skipped flushing because all workers are busy')
                with self.__payload_thread_cond:
                    #  All the workers are busy so we can't flush now;
                    #  the buffer should keep the events for the next flush
                    #  busy payload thread num - 1
                    self.__busy_payload_thread_num.dec()
                    self.__payload_thread_cond.notify_all()

    def __wait_util_flush_payload_worker_down(self):
        with self.__payload_thread_cond:
            while self.__busy_payload_thread_num.load() > 0:
                self.__busy_payload_thread_cond.wait()

    def __shutdown(self):
        if not self.__closed:
            try:
                log.debug('event dispatcher is cleaning up thread and conn pool')
                #  wait for all flush payload is well done
                self.__wait_util_flush_payload_worker_down()
                self.__closed = True
                for t in self.__payload_threads:
                    t.stop()
                log.debug('event sender is stopping...')
                self.__sender.stop()
            except Exception as e:
                log.exception('FFC Python SDK: unexpected error when closing event dispatcher: %s' % str(e))


class FlushPayloadExecutor(threading.Thread):
    __MAX_EVENT_SIZE_PER_REQUEST = 50

    def __init__(self,
                 config: Config,
                 sender: Sender,
                 payload_queue: "Queue[Iterable[FFCEvent]]",
                 busy_payload_thread_num: atomics.INTEGRAL,
                 payload_cond: threading.Condition):
        super().__init__(daemon=True)
        self.__config = config
        self.__sender = sender
        self.__payload_queue = payload_queue
        self.__busy_thread_num = busy_payload_thread_num
        self.__cond = payload_cond
        self.__running = True

    def run(self):
        def partition(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i : i + size]

        while self.__running:
            try:
                payloads = self.__payload_queue.get(block=True, timeout=1)
                for payload in list(partition(payloads, self.__MAX_EVENT_SIZE_PER_REQUEST)):
                    json_payload = [event.to_json_dict() for event in payload]
                    json_str = json.dumps(json_payload)
                    log.debug(json_str)
                    self.__sender.postJson(self.__config.events_url, json_str, fetch_response=False)
            except Empty:
                continue
            except Exception as e:
                log.exception('FFC Python SDK: unexpected error in sending payload: %s' % str(e))
            # // busy payload thread num - 1
            with self.__cond:
                self.__busy_thread_num.dec()
                self.__cond.notify_all()

    def stop(self):
        self.__running = False
        log.debug('flush payload worker is stopping...')


class NullEventProcessor(EventProcessor):
    def __init__(self, config: Config):
        pass

    def send_event(self, event: FFCEvent):
        pass

    def flush(self):
        pass

    def stop(self):
        pass
