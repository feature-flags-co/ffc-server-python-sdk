import json
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, Mapping, Optional

from ffcclient.utils import is_numeric

__BUILTINS_MAPING__ = {'key': 'KeyId',
                       'email': 'Email',
                       'country': 'Country',
                       'name': 'Name',
                       'keyid': 'KeyId'}

__NO_VARIATION__ = -1

__FLAG_KEY_UNKNOWN__ = 'flag key unknown'

__FLAG_NAME_UNKNOWN__ = 'flag name unknown'

__FLAG_VALUE_UNKNOWN__ = 'flag value unknown'


class Jsonfy(ABC):

    @abstractmethod
    def to_json_dict(self) -> dict:
        pass

    def to_json_str(self) -> str:
        return json.dumps(self.to_json_dict())


class FFCUser(Jsonfy):

    def __init__(self,
                 key: str,
                 name: str = None,
                 email: str = None,
                 country: str = None,
                 **kwargs):
        self._check_argument(key, 'key is not valid')
        self._commons = {}
        self._commons['KeyId'] = key
        if name and self._check_argument(name, 'name is not valid'):
            self._commons['Name'] = name
        if email and self._check_argument(email, 'email is not valid'):
            self._commons['Email'] = email
        if country and self._check_argument(country, 'country is not valid'):
            self._commons['Country'] = country
        self._customs = {}
        if len(kwargs) > 0:
            self._customs \
                .update(dict((k, str(v)) for k, v in kwargs.items() if isinstance(k, str) and k.lower() not in __BUILTINS_MAPING__.keys() and (isinstance(v, str) or is_numeric(v))))

    @staticmethod
    def from_dict(user: dict):
        user_copy = {}
        if not isinstance(user, dict):
            raise ValueError('user is not valid')
        user_copy.update(user)
        name = user_copy.pop('name', None)
        email = user_copy.pop('email', None)
        country = user_copy.pop('country', None)
        key = user_copy.pop('key', None) or user_copy.pop('keyid', None)
        return FFCUser(key, name, email, country, **user_copy)

    def _check_argument(self, value, msg) -> bool:
        if isinstance(value, str) and value.strip():
            return True
        raise ValueError(msg)

    def get(self, prop: str, default=None) -> Optional[str]:
        if not isinstance(prop, str):
            return default

        if prop in self._commons:
            return self._commons[prop]

        if prop in __BUILTINS_MAPING__:
            return self._commons.get(__BUILTINS_MAPING__[prop.lower()], default)

        return self._customs.get(prop, default)

    def to_json_dict(self) -> dict:
        json_dict = {}

        json_dict['keyId'] = self._commons['KeyId']
        json_dict['userName'] = self._commons.get('Name', '')
        json_dict['email'] = self._commons.get('Email', '')
        json_dict['country'] = self._commons.get('Country', '')
        json_dict['customizedProperties'] = [{'name': k, 'value': v} for k, v in self._customs.items()]
        return json_dict


class EvalDetail(Jsonfy):
    def __init__(self,
                 id: int,
                 reason: str,
                 variation: Any,
                 key_name: Optional[str] = None,
                 name: Optional[str] = None):
        self._id = id
        self._reason = reason
        self._variation = variation
        self._key_name = key_name
        self._name = name

    @staticmethod
    def error(reason: str, variation: Any = None, key_name: Optional[str] = None) -> 'EvalDetail':
        return EvalDetail(__NO_VARIATION__,
                          reason,
                          __FLAG_VALUE_UNKNOWN__ if not variation else variation,
                          __FLAG_KEY_UNKNOWN__ if not key_name else key_name,
                          __FLAG_NAME_UNKNOWN__)

    @property
    def is_default_value(self) -> bool:
        return self._id == __NO_VARIATION__

    @property
    def is_success(self) -> bool:
        return self._id >= 0

    @property
    def id(self) -> int:
        return self._id

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def variation(self) -> Any:
        return self._variation

    @property
    def key_name(self) -> Optional[str]:
        return self._key_name

    @property
    def name(self) -> Optional[str]:
        return self._name

    def to_json_dict(self) -> dict:
        json_dict = {}
        json_dict['id'] = self.id
        json_dict['reason'] = self.reason
        json_dict['variation'] = self.variation
        json_dict['keyName'] = self.key_name
        json_dict['name'] = self.name
        return json_dict

    def to_flag_state(self):
        return FlagState(self.is_success, self._reason, self)


class BasicFlagState:
    def __init__(self, success: bool, message: str):
        self._success = success
        self._message = 'OK' if success else message

    @property
    def success(self) -> bool:
        return self._success

    @property
    def message(self) -> str:
        return self._message


class FlagState(BasicFlagState, Jsonfy):
    def __init__(self, success: bool, message: str, data: EvalDetail):
        super().__init__(success, message)
        self._data = data

    @property
    def data(self) -> EvalDetail:
        return self._data

    def to_json_dict(self) -> dict:
        return {'success': self.success,
                'message': self.message,
                'data': self._data.to_json_dict() if self._data else None}


class AllFlagStates(BasicFlagState, Jsonfy):
    def __init__(self, success: bool, message: str,
                 data: Mapping[EvalDetail, 'FFCEvent'],
                 event_handler: Callable[['FFCEvent'], None]):
        super().__init__(success, message)
        self._data = dict((ed.key_name, (ed, ffc_event)) for ed, ffc_event in data.items()) if data else {}
        self._event_handler = event_handler

    @property
    def key_names(self) -> Iterable[str]:
        return self._data.keys()

    def get(self, key_name: str) -> EvalDetail:
        ed, ffc_event = self._data.get(key_name, (None, False))
        if self._event_handler and ffc_event:
            self._event_handler(ffc_event)
        return ed

    def to_json_dict(self) -> dict:
        return {'success': self.success,
                'message': self.message,
                'data': [ed.to_json_dict() for ed, _ in self._data.values()] if self._data else []}


class FFCEvent(Jsonfy, ABC):
    def __init__(self, user: FFCUser):
        self._user = user

    @abstractmethod
    def add(self, *elements) -> 'FFCEvent':
        pass

    @property
    @abstractmethod
    def is_send_event(self) -> bool:
        pass
