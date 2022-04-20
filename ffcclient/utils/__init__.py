import base64
import binascii
import logging
import sys
from math import floor
from random import random
from time import time
from typing import Iterable, Optional

TRACE_LEVEL = logging.DEBUG - 5

logging.addLevelName(TRACE_LEVEL, 'TRACE')


class _MyLogger(logging.getLoggerClass()):
    def trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(TRACE_LEVEL):
            self._log(TRACE_LEVEL, msg, args, **kwargs)


logging.setLoggerClass(_MyLogger)

logging.getLogger("schedule").setLevel(logging.ERROR)

log = logging.getLogger(sys.modules[__name__].__name__)

ALPHABETS = {"0": "Q", "1": "B", "2": "W", "3": "S", "4": "P", "5": "H", "6": "D", "7": "X", "8": "Z", "9": "U"}


def build_headers(env_secret: str, extra_headers={}):

    def build_default_headers():
        return {
            'envSecret': env_secret or '',
            'User-Agent': 'ffc-python-server-sdk',
            'Content-Type': 'application/json'
        }

    headers = build_default_headers()
    headers.update(extra_headers)
    return headers


def build_token(env_secret: str) -> str:

    def encodeNumber(num, length):
        s = "000000000000" + str(num)
        return ''.join(list(map(lambda ch: ALPHABETS[ch], s[len(s) - length:])))

    text = env_secret.rstrip("=")
    now = unix_timestamp_in_milliseconds()
    timestampCode = encodeNumber(now, len(str(now)))
    start = max(floor(random() * len(text)), 2)
    part1 = encodeNumber(start, 3)
    part2 = encodeNumber(len(timestampCode), 2)
    part3 = text[0:start]
    part4 = timestampCode
    part5 = text[start:]
    return '%s%s%s%s%s' % (part1, part2, part3, part4, part5)


def unix_timestamp_in_milliseconds():
    return int(round(time() * 1000))


def valide_all_data(all_data={}) -> bool:
    return isinstance(all_data, dict) \
        and all_data.get('messageType', 'pong') == 'data-sync' \
        and 'data' in all_data and isinstance(all_data['data'], dict) \
        and all(k in all_data['data'] for k in ('eventType', 'featureFlags', 'segments')) \
        and any(k == all_data['data']['eventType'] for k in ('full', 'patch')) \
        and isinstance(all_data['data']['featureFlags'], Iterable) \
        and isinstance(all_data['data']['segments'], Iterable)


def is_base64(s):
    try:
        base64.b64decode(s, validate=True)
        return True
    except binascii.Error:
        return False
    except TypeError:
        return False


def get_feature_flag_id(env_secret: str, feature_flag_key_name: str) -> Optional[str]:
    # validate feature_flag_key_name
    if not isinstance(feature_flag_key_name, str) or not feature_flag_key_name.strip():
        return None
    # decode env_secret
    try:
        key_origin_text = base64 \
            .b64decode(env_secret, validate=True) \
            .decode('utf-8') \
            .split('__')
        account_id = key_origin_text[1]
        project_id = key_origin_text[2]
        env_id = key_origin_text[3]
        return 'FF__%s__%s__%s__%s' % (account_id, project_id, env_id, feature_flag_key_name)
    except:
        return None


def unpack_feature_flag_id(feature_flag_id: str, position: int) -> Optional[str]:
    if not isinstance(feature_flag_id, str) or \
       not feature_flag_id.strip() or \
       position < 0 or \
       position > 4:
        return None
    return feature_flag_id.split('__')[position]


def check_uwsgi():
    if 'uwsgi' in sys.modules:
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        import uwsgi
        if not hasattr(uwsgi, 'opt'):
            # means that we are not running under uwsgi
            return

        if uwsgi.opt.get('enable-threads'):
            return
        if uwsgi.opt.get('threads') is not None and int(uwsgi.opt.get('threads')) > 1:
            return
        raise ValueError("The Python Server SDK requires the 'enable-threads' or 'threads' option be passed to uWSGI.")


def is_numeric(value) -> bool:
    try:
        float(str(value))
        return True
    except ValueError:
        return False
