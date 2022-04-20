from collections import namedtuple
from enum import Enum

ErrorInfo = namedtuple('ErrorInfo', ['error_type', 'message'])

State = namedtuple('State', ['state_type', 'state_since', 'error_info'])

DATA_STORAGE_INIT_ERROR = 'Data Storage init error'

DATA_STORAGE_UPDATE_ERROR = 'Data Storage update error'

REQUEST_INVALID_ERROR = 'Request invalid'

DATA_INVALID_ERROR = 'Received Data invalid'

NETWORK_ERROR = 'Network error'

RUNTIME_ERROR = 'Runtime error'

WEBSOCKET_ERROR = 'WebSocket error'

UNKNOWN_ERROR = 'Unknown error'

UNKNOWN_CLOSE_CODE = 'Unknown close code'

SYSTEM_ERROR = 'System error'

SYSTEM_QUIT = 'System quit'


class StateType(Enum):
    """
    The initial state of the update processing when the SDK is being initialized.
    If it encounters an error that requires it to retry initialization, the state will remain at
    INITIALIZING until it either succeeds and becomes OK, or permanently fails and becomes OFF.
    """
    INITIALIZING = 1
    """
    Indicates that the update processing is currently operational and has not had any problems since the
    last time it received data.
    In streaming mode, this means that there is currently an open stream connection and that at least
    one initial message has been received on the stream.
    """
    OK = 2
    """
    Indicates that the update processing encountered an error that it will attempt to recover from.
    In streaming mode, this means that the stream connection failed, or had to be dropped due to some
    other error, and will be retried after a backoff delay.
    """
    INTERRUPTED = 3
    """
    Indicates that the update processing has been permanently shut down.
    This could be because it encountered an unrecoverable error or because the SDK client was
    explicitly shut down.
    """
    OFF = 4
