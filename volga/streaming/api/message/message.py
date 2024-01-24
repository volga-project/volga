from typing import Any

from volga.streaming.runtime.transfer.channel import ChannelMessage


class Record:
    # Data record in data stream

    def __init__(self, value: Any):
        self.value = value
        self.stream_name = None

    def __repr__(self):
        return "Record({})".format(self.value)

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.stream_name, self.value) == (other.stream_name, other.value)
        return False

    def __hash__(self):
        return hash((self.stream_name, self.value))

    def to_channel_message(self) -> ChannelMessage:
        return {
            'value': self.value,
            'stream_name': self.stream_name
        }

    def set_stream_name(self, stream_name):
        self.stream_name = stream_name


class KeyRecord(Record):
    # Data record in a keyed data stream

    def __init__(self, key: Any, value: Any):
        super().__init__(value)
        self.key = key

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.stream_name, self.key, self.value) == (
                other.stream_name,
                other.key,
                other.value,
            )
        return False

    def __hash__(self):
        return hash((self.stream_name, self.key, self.value))

    def to_channel_message(self):
        return {
            'key': self.key,
            'value': self.value,
            'stream_name': self.stream_name
        }


def record_from_channel_message(channel_message: ChannelMessage) -> Record:
    if 'key' in channel_message:
        record = KeyRecord(
            key=channel_message['key'],
            value=channel_message['value']
        )
    else:
        record = Record(
            value=channel_message['value']
        )

    record.set_stream_name(channel_message['stream_name'])
    return record
