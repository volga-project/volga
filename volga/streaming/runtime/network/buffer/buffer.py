from typing import List

import simplejson
from pydantic import BaseModel

from volga.streaming.runtime.network.buffer.serialization.byte_utils import str_to_bytes

Buffer = bytes

# Buffer schema
# [16-channel_id][varint-buff_id][varint-buff_payload_size][[varint-msg_id_0][varint-msg_size_0][msg-payload-bytes]... * n_messages]

# buffer header flags
CHANNEL_ID_HEADER_LENGTH = 16  # 16 chars max for channel_id

DEFAULT_BUFFER_SIZE = 32 * 1024


# TODO unify acks and buffer serializations/formats
class AckMessage(BaseModel):
    buffer_id: int


class AckMessageBatch(BaseModel):
    channel_id: str
    acks: List[AckMessage]

    @staticmethod
    def de(raw: bytes) -> 'AckMessageBatch':
        # remove channel_id header
        payload = bytearray(raw)[CHANNEL_ID_HEADER_LENGTH:]
        return AckMessageBatch(**simplejson.loads(payload.decode()))

    def ser(self) -> bytes:
        s = simplejson.dumps(self.dict())
        data = s.encode('utf-8')

        # append channel_id header
        channel_id_bytes = str_to_bytes(self.channel_id, pad_to_size=CHANNEL_ID_HEADER_LENGTH)

        return channel_id_bytes + data