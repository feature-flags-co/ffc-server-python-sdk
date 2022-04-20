import threading
from distutils.util import strtobool
from typing import Any, Iterable, Mapping, Optional

from ffcclient.category import FFC_FEATURE_FLAGS, FFC_SEGMENTS
from ffcclient.common_types import EvalDetail, FFCUser
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

    def __init__(self, config: Config, start_wait: int = 15):

        check_uwsgi()

        self._config = config
        self._config.validate()

        # init scheduler
        self._scheduler = RepeatableTaskSchedule()
        self._scheduler.start()

        # init components
        # event processor
        self._event_processor = self._build_event_processor(config)
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
            log.info("Waiting for Client initialization in %s seconds" % str(start_wait))
            update_processor_ready.wait(start_wait)

        if self._config.is_offline:
            log.info('Python SDK in offline mode')
        elif self._update_processor.initialized:
            log.info('Python SDK Client initialization completed')
        else:
            log.warning('Python SDK Client was not successfully initialized')

    def _build_event_processor(self, config: Config):
        if config.event_processor_imp:
            log.info("Using user-specified event processor: %s" % str(config.event_processor_imp))
            return config.event_processor_imp(config)

        if config.is_offline:
            log.info("Offline mode, SDK disable event processing")
            return NullEventProcessor(config)

        return DefaultEventProcessor(config)

    def _build_update_processor(self, config: Config, update_status_provider, update_processor_event):
        if config.update_processor_imp:
            log.info("Using user-specified update processor: %s" % str(config.update_processor_imp))
            return config.update_processor_imp(config, update_status_provider, update_processor_event)

        if config.is_offline:
            log.info("Offline mode, SDK disable streaming data updating")
            return NullUpdateProcessor(config, update_status_provider, update_processor_event)

        return Streaming(config, update_status_provider, update_processor_event)

    @property
    def initialize(self) -> bool:
        return self._update_processor.initialized

    @property
    def update_status_provider(self) -> DataUpdateStatusProvider:
        return self._update_status_provider

    def stop(self):
        log.info("Python SDK client is closing...")
        self._update_processor.stop()
        self._event_processor.stop()
        self._scheduler.stop()

    def is_offline(self) -> bool:
        return self._config.is_offline

    def _get_flag_internal(self, key: str) -> Optional[dict]:
        flag_id = get_feature_flag_id(self._config.env_secret, key)
        return self._data_storage.get(FFC_FEATURE_FLAGS, flag_id)

    def _evaluate_internal(self, key: str, user: dict, default: Any = None) -> EvalDetail:
        default_value = self._config.get_default_value(key, default)
        try:
            if not self.initialize:
                log.warn('Evaluation called before Java SDK client initialized for feature flag, well using the default value')
                return EvalDetail.error(REASON_CLIENT_NOT_READY, default_value, key)

            if not key:
                log.info('null feature flag key; returning default value')
                return EvalDetail.error(REASON_FLAG_NOT_FOUND, default_value, key)

            flag = self._get_flag_internal(key)
            if not flag:
                log.info('Unknown feature flag %s; returning default value' % key)
                return EvalDetail.error(REASON_FLAG_NOT_FOUND, default_value, key)

            try:
                ffc_user = FFCUser.from_dict(user)
            except ValueError as ve:
                log.warn(str(ve))
                return EvalDetail.error(REASON_USER_NOT_SPECIFIED, default_value, key)

            ffc_event = FlagEvent(ffc_user)
            ed = self._evaluator.evaluate(flag, ffc_user, ffc_event)
            self._event_processor.send_event(ffc_event)
            return ed

        except Exception as e:
            log.exception('unexpected error in evaluation: %s' % str(e))
            return EvalDetail.error(REASON_ERROR, default_value, key)

    def variation(self, key: str, user: dict, default: Any = None) -> Any:
        return self._evaluate_internal(key, user, default).variation()

    def variation_detail(self, key: str, user: dict, default: Any = None) -> EvalDetail:
        return self._evaluate_internal(key, user, default)

    def is_enabled(self, key: str, user: dict) -> bool:
        try:
            value = self.variation(key, user, 'off')
            return strtobool(str(value))
        except ValueError:
            return False

    def get_all_latest_flag_variations(self, user: dict) -> Iterable[EvalDetail]:
        all_flag_details = []
        try:
            if not self.initialize:
                log.warn('Evaluation called before Java SDK client initialized for feature flag')
                all_flag_details.append(EvalDetail.error(REASON_CLIENT_NOT_READY))
            else:
                try:
                    ffc_user = FFCUser.from_dict(user)
                    all_flags = self._data_storage.get_all(FFC_FEATURE_FLAGS)
                    all_flag_details.extend([self._evaluator.evaluate(flag, ffc_user) for flag in all_flags.values()])
                except ValueError as ve:
                    log.warn(str(ve))
                    all_flag_details.append(EvalDetail.error(REASON_CLIENT_NOT_READY))
                except:
                    raise
        except Exception as e:
            log.exception('unexpected error in evaluation: %s' % str(e))
            all_flag_details.append(EvalDetail.error(REASON_ERROR))
        return all_flag_details

    def is_flag_known(self, key: str) -> bool:
        try:
            if not self.initialize:
                log.warn('isFlagKnown called before Java SDK client initialized for feature flag')
                return False
            return self._get_flag_internal(key) is not None
        except Exception as e:
            log.exception('unexpected error in isFlagKnown: %s' % str(e))
        return False

    def flush(self):
        self._event_processor.flush()

    def track_metric(self, user: dict, event_name: str, metric_value: float = 1.0):
        if not user or not event_name or metric_value <= 0:
            log.warn('event/user/metric invalid')
            return
        try:
            ffc_user = FFCUser.from_dict(user)
            metric_event = MetricEvent(ffc_user).add(Metric(event_name, metric_value))
            self._event_processor.send_event(metric_event)
        except Exception as e:
            log.exception(str(e))

    def track_metrics(self, user: dict, metrics: Mapping[str, float]):
        if not user or not metrics:
            log.warn('user/metrics invalid')
            return
        try:
            ffc_user = FFCUser.from_dict(user)
            metric_event = MetricEvent(ffc_user)
            for event_name, metric_value in metrics.items():
                if event_name and metric_value > 0:
                    metric_event.add(Metric(event_name, metric_value))
            self._event_processor.send_event(metric_event)
        except Exception as e:
            log.exception(str(e))