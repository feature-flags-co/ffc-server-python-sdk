import threading
from distutils.util import strtobool
from typing import Any, Mapping, Optional

from ffcclient.category import FFC_FEATURE_FLAGS, FFC_SEGMENTS
from ffcclient.common_types import (AllFlagStates, EvalDetail, FFCUser,
                                    FlagState)
from ffcclient.config import Config
from ffcclient.evaluator import (REASON_CLIENT_NOT_READY, REASON_ERROR,
                                 REASON_FLAG_NOT_FOUND,
                                 REASON_USER_NOT_SPECIFIED, Evaluator)
from ffcclient.event_processor import DefaultEventProcessor, NullEventProcessor
from ffcclient.event_types import FlagEvent, Metric, MetricEvent
from ffcclient.interfaces import DataUpdateStatusProvider
from ffcclient.status import DataUpdateStatusProviderIml
from ffcclient.streaming import Streaming
from ffcclient.update_processor import NullUpdateProcessor
from ffcclient.utils import check_uwsgi, get_feature_flag_id, log
from ffcclient.utils.repeatable_task import RepeatableTaskSchedule


class FFCClient:
    """The featureflag.co Python SDK client object.

    Applications SHOULD instantiate a single instance for the lifetime of the application.
    In the case where an application needs to evaluate feature flags from different environments,
    you may create multiple clients, but they should still be retained for the lifetime of the application
    rather than created per request or per thread.

    Client instances are thread-safe.
    """

    def __init__(self, config: Config, start_wait: int = 15):
        """
        Creates a new client to connect to featureflag.co with a specified configuration.

        Unless client is configured in offline mode, this client try to connect to featureflag.co as soon as the constructor is called.

        The constructor will return when it successfully connects, or when the timeout (default: 15 seconds) expires, whichever comes first.
        ```
        client = FFCClient(Config(env_secret), start_wait=15)

        if client.initialize:
            # your code
        ```

        If it has not succeeded in connecting when the timeout elapses, you will receive the client in an uninitialized state where feature flags will return default values;
        it will still continue trying to connect in the background unless there has been an unrecoverable error or you close the client by :func:`stop`.
        You can detect whether initialization has succeeded by :func:`initialize`.

        If you prefer to have the constructor return immediately, and then wait for initialization to finish at some other point,
        you can use :func:`update_status_provider` as follows:
        ```
        client = FFCClient(Config(env_secret), start_wait=0)
        if client._update_status_provider.wait_for_OKState():
            # your code
        ```
        :param config: the client configuration
        :param start_wait: the max time to wait for initialization
        """
        check_uwsgi()

        self._config = config
        self._config.validate()

        # init scheduler
        self._scheduler = RepeatableTaskSchedule()
        self._scheduler.start()

        # init components
        # event processor
        self._event_processor = self._build_event_processor(config)
        self._event_handler = lambda ffc_event: self._event_processor.send_event(ffc_event)
        # data storage
        self._data_storage = config.data_storage
        # evaluator
        self._evaluator = Evaluator(lambda key: self._data_storage.get(FFC_FEATURE_FLAGS, key),
                                    lambda key: self._data_storage.get(FFC_SEGMENTS, key))
        # data updator and status provider
        self._update_status_provider = DataUpdateStatusProviderIml(config.data_storage)
        # update processor
        update_processor_ready = threading.Event()
        self._update_processor = self._build_update_processor(config, self._update_status_provider,
                                                              update_processor_ready)
        # data sync
        self._update_processor.start()
        if not self._config.is_offline and start_wait > 0:
            log.info("FFC Python SDK: Waiting for Client initialization in %s seconds" % str(start_wait))
            update_processor_ready.wait(start_wait)
            if self._config.is_offline:
                log.info('FFC Python SDK: Python SDK in offline mode')
            elif self._update_processor.initialized:
                log.info('FFC Python SDK: Python SDK Client initialization completed')
            else:
                log.warning('FFC Python SDK: Python SDK Client was not successfully initialized')
        else:
            log.info('FFC Python SDK: Python SDK Client initialization completed')

    def _build_event_processor(self, config: Config):
        if config.event_processor_imp:
            log.debug("Using user-specified event processor: %s" % str(config.event_processor_imp))
            return config.event_processor_imp(config)

        if config.is_offline:
            log.debug("Offline mode, SDK disable event processing")
            return NullEventProcessor(config)

        return DefaultEventProcessor(config)

    def _build_update_processor(self, config: Config, update_status_provider, update_processor_event):
        if config.update_processor_imp:
            log.debug("Using user-specified update processor: %s" % str(config.update_processor_imp))
            return config.update_processor_imp(config, update_status_provider, update_processor_event)

        if config.is_offline:
            log.debug("Offline mode, SDK disable streaming data updating")
            return NullUpdateProcessor(config, update_status_provider, update_processor_event)

        return Streaming(config, update_status_provider, update_processor_event)

    @property
    def initialize(self) -> bool:
        """Returns true if the client has successfully connected to featureflag.co.

        If this returns false, it means that the client has not yet successfully connected to featureflag.co.
        It might still be in the process of starting up, or it might be attempting to reconnect after an
        unsuccessful attempt, or it might have received an unrecoverable error and given up.
        """
        return self._update_processor.initialized

    @property
    def update_status_provider(self) -> DataUpdateStatusProvider:
        return self._update_status_provider

    def stop(self):
        """Releases all threads and network connections used by SDK.

        Do not attempt to use the client after calling this method.
        """
        log.info("FFC Python SDK: Python SDK client is closing...")
        self._update_processor.stop()
        self._event_processor.stop()
        self._scheduler.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()

    def is_offline(self) -> bool:
        """Returns true if the client is in offline mode.
        """
        return self._config.is_offline

    def _get_flag_internal(self, key: str) -> Optional[dict]:
        flag_id = get_feature_flag_id(self._config.env_secret, key)
        return self._data_storage.get(FFC_FEATURE_FLAGS, flag_id)

    def _evaluate_internal(self, key: str, user: dict, default: Any = None) -> EvalDetail:
        default_value = self._config.get_default_value(key, default)
        try:
            if not self.initialize:
                log.warn('FFC Python SDK: Evaluation called before Java SDK client initialized for feature flag, well using the default value')
                return EvalDetail.error(REASON_CLIENT_NOT_READY, default_value, key)

            if not key:
                log.warn('FFC Python SDK: null feature flag key; returning default value')
                return EvalDetail.error(REASON_FLAG_NOT_FOUND, default_value, key)

            flag = self._get_flag_internal(key)
            if not flag:
                log.warn('FFC Python SDK: Unknown feature flag %s; returning default value' % key)
                return EvalDetail.error(REASON_FLAG_NOT_FOUND, default_value, key)

            try:
                ffc_user = FFCUser.from_dict(user)
            except ValueError as ve:
                log.warn('FFC Python SDK: %s' % str(ve))
                return EvalDetail.error(REASON_USER_NOT_SPECIFIED, default_value, key)

            ffc_event = FlagEvent(ffc_user)
            ed = self._evaluator.evaluate(flag, ffc_user, ffc_event)
            self._event_handler(ffc_event)
            return ed

        except Exception as e:
            log.exception('FFC Python SDK: unexpected error in evaluation: %s' % str(e))
            return EvalDetail.error(REASON_ERROR, default_value, key)

    def variation(self, key: str, user: dict, default: Any = None) -> Any:
        """Return the variation of a feature flag for a given user.

        This method will send an event back to featureflag.co immediately if no error occurs.

        :param key: the unique key for the feature flag
        :param user:  the attributes of the user
        :param default: the default value of the flag, to be used if the return value is not available from featureflag.co
        :return: one of the flag's values, or the default value
        """
        return self._evaluate_internal(key, user, default).variation

    def variation_detail(self, key: str, user: dict, default: Any = None) -> FlagState:
        """"Return the variation of a feature flag for a given user, but also provides additional information
         about how this value was calculated, in the form of an :class:`ffcclient.common_types.EvalDetail` object.

        This method will send an event back to featureflag.co immediately if no error occurs.

        :param key: the unique key for the feature flag
        :param user: the attributes of the user
        :param default: the default value of the flag, to be used if the return value is not available from featureflag.co
        :return: an :class:`ffcclient.common_types.FlagState` object
        """
        return self._evaluate_internal(key, user, default).to_flag_state

    def is_enabled(self, key: str, user: dict) -> bool:
        """
        Return the bool variation of a feature flag for a given user.

        True values are 'y', 'yes', 't', 'true', 'on', and '1'; return false if
        'val' is anything else.

        This method will send an event back to featureflag.co immediately if no error occurs.

        :param key: the unique key for the feature flag
        :param user: the attributes of the user
        :return: True or False

        """
        try:
            value = self.variation(key, user, 'off')
            return strtobool(str(value))
        except ValueError:
            return False

    def get_all_latest_flag_variations(self, user: dict) -> AllFlagStates:
        """
        Returns an object that encapsulates the state of all feature flags for a given user

        This method does not send events back to featureflag.co immediately util calling :func:`ffcclient.common_types.AllFlagStates.get()`

        :param user: the attributes of the user
        :return: an :class:`ffcclient.common_types.AllFlagStates` object (will never be None; its `success` property will be False
         normally if SDK has not been initialized or the user invalid)
        """
        try:
            all_flag_details = {}
            message = None
            success = True
            if not self.initialize:
                log.warn('FFC Python SDK: Evaluation called before Java SDK client initialized for feature flag')
                message = REASON_CLIENT_NOT_READY
                success = False
                ed = EvalDetail.error(message)
                all_flag_details[ed] = None
            else:
                try:
                    ffc_user = FFCUser.from_dict(user)
                    all_flags = self._data_storage.get_all(FFC_FEATURE_FLAGS)
                    for flag in all_flags.values():
                        ffc_event = FlagEvent(ffc_user)
                        ed = self._evaluator.evaluate(flag, ffc_user, ffc_event)
                        all_flag_details[ed] = ffc_event
                except ValueError as ve:
                    log.warn('FFC Python SDK: %s' % str(ve))
                    message = REASON_USER_NOT_SPECIFIED
                    success = False
                    ed = EvalDetail.error(message)
                    all_flag_details[ed] = None
                except:
                    raise
        except Exception as e:
            log.exception('FFC Python SDK: unexpected error in evaluation: %s' % str(e))
            message = REASON_ERROR
            success = False
            ed = EvalDetail.error(message)
            all_flag_details[ed] = None
        return AllFlagStates(success, message, all_flag_details, self._event_handler)

    def is_flag_known(self, key: str) -> bool:
        """
        Checks if the given flag exists in the your environment

        :param key: The key name of the flag to check
        :return: True if the flag exists
        """
        try:
            if not self.initialize:
                log.warn('FFC Python SDK: isFlagKnown called before Java SDK client initialized for feature flag')
                return False
            return self._get_flag_internal(key) is not None
        except Exception as e:
            log.exception('FFC Python SDK: unexpected error in is_flag_known: %s' % str(e))
        return False

    def flush(self):
        """Flushes all pending events.

        Normally, batches of events are delivered in the background at intervals determined by the
        `events_flush_interval` property of :class:`ffcclient.config.Config`. Calling `flush()`
        schedules the next event delivery to be as soon as possible; however, the delivery still
        happens asynchronously on a thread, so this method will return immediately.
        """
        self._event_processor.flush()

    def track_metric(self, user: dict, event_name: str, metric_value: float = 1.0):
        """Tracks that a user performed a metric event.

        featureflag.co automatically tracks pageviews and clicks that are specified in the dashboard UI.
        This can be used to track custom metric.

        :param user: the attributes of the user
        :param event_name: the name of the event, which may correspond to a goal in A/B tests
        :param metric_value: a numeric value used by the experiment, default value is 1.0
        """
        if not event_name or metric_value <= 0:
            log.warn('FFC Python SDK: event/metric invalid')
            return
        try:
            ffc_user = FFCUser.from_dict(user)
        except ValueError:
            log.warn('FFC Python SDK: user invalid')
            return
        try:
            ffc_user = FFCUser.from_dict(user)
            metric_event = MetricEvent(ffc_user).add(Metric(event_name, metric_value))
            self._event_handler(metric_event)
        except Exception as e:
            log.exception('FFC Python SDK: unexpected error in track_metric: %s' % str(e))

    def track_metrics(self, user: dict, metrics: Mapping[str, float]):
        """Tracks that a user performed a map of metric events.

        if any event_name or metric_value is invalid, that metric will be ignored

        :param user: the attributes of the user
        :param metrics: the pairs of event_name and metric_value
        """
        if not isinstance(metrics, dict):
            log.warn('FFC Python SDK: metrics invalid')
            return
        try:
            ffc_user = FFCUser.from_dict(user)
        except ValueError:
            log.warn('FFC Python SDK: user invalid')
            return
        try:
            metric_event = MetricEvent(ffc_user)
            for event_name, metric_value in metrics.items():
                if event_name and metric_value > 0:
                    metric_event.add(Metric(event_name, metric_value))
            self._event_handler(metric_event)
        except Exception as e:
            log.exception('FFC Python SDK: unexpected error in track_metrics: %s' % str(e))
