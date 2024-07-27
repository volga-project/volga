import time
import msgpack
import orjson

n = 1000000
msg_size = 1024
batch_size = 100

msgs = [{'k': i, 'v': msg_size*'a'} for i in range(n)]


# test json
for serializer in ['msgpack', 'orjson']:
    for mode in ['per_msg', 'batched']:
        ser = lambda m: msgpack.dumps(m) if serializer == 'msgpack' else orjson.dumps(m)
        de = lambda m: msgpack.loads(m) if serializer == 'msgpack' else orjson.loads(m)

        serd = []
        t1 = time.time()
        batch = []
        if mode == 'batched':
            for m in msgs:
                batch.append(m)
                if len(batch) == batch_size:
                    serd.append(ser(batch))
                    batch = []
            if len(batch) != 0:
                serd.append(ser(batch))
        else:
            for m in msgs:
                serd.append(ser(m))
        ser_t = time.time() - t1

        deser = []
        t2 = time.time()
        if mode == 'batched':
            for s in serd:
                deser.extend(de(s))
        else:
            for s in serd:
                deser.append(de(s))

        deser_t = time.time() - t2
        print(f'[{serializer}][{mode}]: Ser: {ser_t}, De: {deser_t}')

