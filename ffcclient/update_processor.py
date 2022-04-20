from threading import Event
from ffcclient.config import Config
from ffcclient.interfaces import DataUpdateStatusProvider, UpdateProcessor


class NullUpdateProcessor(UpdateProcessor):

    def __init__(self, config: Config, dataUpdateStatusProvider: DataUpdateStatusProvider, ready: Event):
        self.__ready = ready

    def start(self):
        self.__ready.set()

    def stop(self):
        pass

    @property
    def initialized(self) -> bool:
        return True
