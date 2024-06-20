from typing import Any, Optional

from volga.streaming.runtime.network.channel import ChannelMessage

from decimal import Decimal


class Record:
    # Data record in data stream

    def __init__(self, value: Any, event_time: Optional[Decimal] = None):
        self.value = value
        self.stream_name = None
        self.event_time = event_time

    def __repr__(self):
        return f'Record(value={self.value}, stream_name={self.stream_name}, event_time={self.event_time})'

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.stream_name, self.value, self.event_time) == (other.stream_name, other.value, self.event_time)
        return False

    def __hash__(self):
        return hash((self.stream_name, self.value, self.event_time))

    def to_channel_message(self) -> ChannelMessage:
        return {
            'value': self.value,
            'stream_name': self.stream_name,
            'event_time': self.event_time
        }

    def set_stream_name(self, stream_name):
        self.stream_name = stream_name


class KeyRecord(Record):
    # Data record in a keyed data stream

    def __init__(self, key: Any, value: Any, event_time: Optional[Decimal] = None):
        super().__init__(value=value, event_time=event_time)
        self.key = key

    def __repr__(self):
        return f'KeyRecord(key={self.key}, value={self.value}, stream_name={self.stream_name}, event_time={self.event_time})'

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.stream_name, self.key, self.value, self.event_time) == (
                other.stream_name,
                other.key,
                other.value,
                other.event_time
            )
        return False

    def __hash__(self):
        return hash((self.stream_name, self.key, self.value, self.event_time))

    # TODO we should have proper ser/de
    def to_channel_message(self):
        return {
            'key': self.key,
            'value': self.value,
            'stream_name': self.stream_name,
            'event_time': self.event_time
        }


# TODO we should have proper ser/de
def record_from_channel_message(channel_message: ChannelMessage) -> Record:
    if 'key' in channel_message:
        record = KeyRecord(
            key=channel_message['key'],
            value=channel_message['value'],
            event_time=channel_message['event_time']
        )
    else:
        record = Record(
            value=channel_message['value'],
            event_time=channel_message['event_time']
        )

    record.set_stream_name(channel_message['stream_name'])
    return record
