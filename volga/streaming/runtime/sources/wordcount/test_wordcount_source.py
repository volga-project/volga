import unittest

from volga.streaming.runtime.sources.source_splits_manager import SourceSplitType
from volga.streaming.runtime.sources.wordcount.split_source import WordCountSourceSplitEnumerator


class TestWordCountSource(unittest.TestCase):

    # tests limited by number of messages only, not limited by time
    def test_word_count_source_split_enumerator(self):
        count_per_word = 5
        num_msgs_per_split = 2
        dictionary = ['a', 'b', 'c', 'd', 'e']

        e = WordCountSourceSplitEnumerator(
            dictionary=dictionary,
            split_size=num_msgs_per_split,
            count_per_word=count_per_word
        )
        splits = []
        while True:
            split = e.poll_next_split(task_id=1)
            splits.append(split)
            if split.type == SourceSplitType.END_OF_INPUT:
                break
        counts = {}

        words_left = len(dictionary) * count_per_word
        for split in splits:
            if split.type != SourceSplitType.END_OF_INPUT:
                num_words = 0
                for word in split.data:
                    count = split.data[word]
                    num_words += count
                    if word in counts:
                        counts[word] += count
                    else:
                        counts[word] = count
                assert num_words == min(words_left, num_msgs_per_split)
                words_left -= num_words

        assert len(counts) == len(dictionary)
        for w in counts:
            assert counts[w] == count_per_word

        print('assert ok')


if __name__ == '__main__':
    t = TestWordCountSource()
    t.test_word_count_source_split_enumerator()