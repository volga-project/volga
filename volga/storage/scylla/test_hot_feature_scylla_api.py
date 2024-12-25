import asyncio
import json

from volga.storage.scylla.api import ScyllaPyHotFeatureStorageApi, AcsyllaHotFeatureStorageApi

feature_name = 'test_feature'
keys1 = {'key1': '1', 'key2': '2'}
values1 = {'val1': '1', 'val2': '2'}

keys2 = {'key_1': '1', 'key_2': '2'}
values2 = {'val1': '1', 'val2': '2'}


async def run():
    api = AcsyllaHotFeatureStorageApi()
    # api = ScyllaHotFeatureStorageApi()
    await api.init()
    await asyncio.gather(*[
        api.insert(feature_name, keys1, values1),
        api.insert(feature_name, keys2, values2)
    ])
    res1 = await api.fetch_latest(feature_name, keys1)
    assert json.loads(res1['values_json']) == values1

    res2 = await api.fetch_latest(feature_name, keys2)
    assert json.loads(res2['values_json']) == values2

    print('assert ok')
    await api.close()

asyncio.run(run())