from typing import List, Optional

from aenum import enum

from volga.streaming.runtime.network_deprecated.channel import Channel


class StatsEvent(enum):
    MSG_SENT = 'MSG_SENT'
    MSG_RCVD = 'MSG_RCVD'
    ACK_SENT = 'ACK_SENT'
    ACK_RCVD = 'ACK_RCVD'


class Stats:
    def __init__(self):
        self._event_counters = {
            StatsEvent.MSG_SENT: {},
            StatsEvent.MSG_RCVD: {},
            StatsEvent.ACK_SENT: {},
            StatsEvent.ACK_RCVD: {},
        }

    def get_counter_for_event(self, event_type: StatsEvent):
        return self._event_counters[event_type]

    def inc(self, event_type: StatsEvent, key: str, num: Optional[int] = None):
        stats = self._event_counters[event_type]
        if key not in stats:
            stats[key] = 0
        if num is None:
            stats[key] += 1
        else:
            stats[key] += num

    def __repr__(self):
        return str(self.__dict__)