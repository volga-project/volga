from typing import Any, Optional

from volga.streaming.runtime.network_deprecated.channel import ChannelMessage

from decimal import Decimal


class Record:
    # Data record in data stream

    def __init__(self, value: Any, event_time: Optional[Decimal] = None, source_emit_ts: Optional[int] = None):
        self.value = value
        self.stream_name = None
        self.event_time = event_time
        self.source_emit_ts = source_emit_ts

    def __repr__(self):
        return f'Record(value={self.value}, stream_name={self.stream_name}, event_time={self.event_time}, source_emit_ts={self.source_emit_ts})'

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.stream_name, self.value, self.event_time, self.source_emit_ts) == (other.stream_name, other.value, other.event_time, other.source_emit_ts)
        return False

    def __hash__(self):
        return hash((self.stream_name, self.value, self.event_time, self.source_emit_ts))

    def to_channel_message(self) -> ChannelMessage:
        return {
            'value': self.value,
            'stream_name': self.stream_name,
            'event_time': self.event_time,
            'source_emit_ts': self.source_emit_ts
        }

    def set_stream_name(self, stream_name):
        self.stream_name = stream_name


class KeyRecord(Record):
    # Data record in a keyed data stream

    def __init__(self, key: Any, value: Any, event_time: Optional[Decimal] = None, source_emit_ts: Optional[int] = None):
        super().__init__(value=value, event_time=event_time, source_emit_ts=source_emit_ts)
        self.key = key

    def __repr__(self):
        return f'KeyRecord(key={self.key}, value={self.value}, stream_name={self.stream_name}, event_time={self.event_time}, source_emit_ts={self.source_emit_ts})'

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.stream_name, self.key, self.value, self.event_time, self.source_emit_ts) == (
                other.stream_name,
                other.key,
                other.value,
                other.event_time,
                other.source_emit_ts
            )
        return False

    def __hash__(self):
        return hash((self.stream_name, self.key, self.value, self.event_time, self.source_emit_ts))

    # TODO we should have proper ser/de
    def to_channel_message(self):
        return {
            'key': self.key,
            'value': self.value,
            'stream_name': self.stream_name,
            'event_time': self.event_time,
            'source_emit_ts': self.source_emit_ts
        }


# TODO we should have proper ser/de
def record_from_channel_message(channel_message: ChannelMessage) -> Record:
    if 'key' in channel_message:
        record = KeyRecord(
            key=channel_message['key'],
            value=channel_message['value'],
            event_time=channel_message['event_time'],
            source_emit_ts=channel_message['source_emit_ts']
        )
    else:
        record = Record(
            value=channel_message['value'],
            event_time=channel_message['event_time'],
            source_emit_ts=channel_message['source_emit_ts']
        )

    record.set_stream_name(channel_message['stream_name'])
    return record
