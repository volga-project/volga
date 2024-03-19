import unittest

from volga.storage.common.key_index import KeyIndex, compose_main_key


class TestKeyIndex(unittest.TestCase):

    def test(self):
        k = {'k1': 1, 'k2': 2, 'k3': 3}
        main_key = compose_main_key(k)
        index = KeyIndex()
        index.put(k)
        assert index.get({'k1': 1})[0] == main_key
        assert index.get({'k2': 2})[0] == main_key
        assert index.get({'k3': 3})[0] == main_key
        assert index.get({'k1': 1, 'k2': 2})[0] == main_key
        assert index.get({'k1': 1, 'k3': 3})[0] == main_key
        assert index.get({'k2': 2, 'k3': 3})[0] == main_key


if __name__ == '__main__':
    t = TestKeyIndex()
    t.test()