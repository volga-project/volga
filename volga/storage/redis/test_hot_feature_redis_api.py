import asyncio
import json

from volga.storage.redis.api import RedisHotFeatureStorageApi

feature_name = 'test_feature'
keys1 = {'key1': '1', 'key2': '2'}
values1 = {'val1': '1', 'val2': '2'}

keys2 = {'key_1': '1', 'key_2': '2'}
values2 = {'val1': '1', 'val2': '2'}


async def run():
    # Configure Redis connection - update these values as needed
    host = '127.0.0.1'
    port = 6379
    
    api = RedisHotFeatureStorageApi(host=host, port=port)
    await api.init()
    
    # Clean up any existing test data
    await api._delete_data()
    
    await asyncio.gather(*[
        api.insert(feature_name, keys1, values1),
        api.insert(feature_name, keys2, values2)
    ])
    
    # Test single key lookup (wrapped in a list)
    res1_list = await api.get_latest(feature_name, [keys1])
    assert len(res1_list) == 1
    assert json.loads(res1_list[0]['values_json']) == values1

    # Test multiple keys lookup
    res_multi = await api.get_latest(feature_name, [keys1, keys2])
    assert len(res_multi) == 2
    assert json.loads(res_multi[0]['values_json']) == values1
    assert json.loads(res_multi[1]['values_json']) == values2

    # Test with a non-existent key
    non_existent_key = {'key3': '3', 'key4': '4'}
    res_with_missing = await api.get_latest(feature_name, [keys1, non_existent_key, keys2])
    assert len(res_with_missing) == 3  # Should return 3 results, with empty dict for missing key
    assert json.loads(res_with_missing[0]['values_json']) == values1
    assert res_with_missing[1] == {}  # Empty dict for non-existent key
    assert json.loads(res_with_missing[2]['values_json']) == values2

    print('assert ok')
    await api.close()

if __name__ == "__main__":
    asyncio.run(run()) 