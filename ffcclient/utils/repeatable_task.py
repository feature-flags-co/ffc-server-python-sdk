from time import sleep
import schedule
from threading import Event, Thread

from ffcclient.utils import log


class RepeatableTaskSchedule(Thread):
    """
    Continuously run, while executing pending jobs at each elapsed time interval.

    see `here <https://schedule.readthedocs.io/en/stable/background-execution.html>`
    """

    def __init__(self):
        super().__init__(daemon=True)
        self.__ready = Event()

    def stop(self):
        log.debug('repeatable task schedule is stopping...')
        schedule.clear()
        self.__ready.set()

    def run(self):
        while not self.__ready.is_set():
            schedule.run_pending()
            sleep(1)
