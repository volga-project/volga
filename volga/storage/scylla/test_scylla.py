from volga.storage.scylla.api import store_many, fetch_latest
from volga.storage.scylla.connection import create_session, sync_tables

create_session()
sync_tables()

feature_name = 'test_feature'
keys1 = {'key1': '1', 'key2': '2'}
values1 = {'val1': '1', 'val2': '2'}

keys2 = {'key_1': '1', 'key_2': '2'}
values2 = {'val1': '1', 'val2': '2'}

store_many(feature_name, [(keys1, values1), (keys2, values2)])
print(fetch_latest(feature_name, keys1))
print(fetch_latest(feature_name, keys2))