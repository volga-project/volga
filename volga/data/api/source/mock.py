from volga.data.api.source.source import Source


class MockBatchSource(Source):

    def get(*args, **kwargs) -> 'Source':
        return MockBatchSource()


class MockStreamingSource(Source):

    def get(*args, **kwargs) -> 'Source':
        return MockStreamingSource()


