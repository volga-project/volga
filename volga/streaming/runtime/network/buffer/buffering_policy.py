
class BufferingPolicy:
    pass


# Each message is in it's own buffer. Provides lowest latency, essentially disables buffering
class BufferPerMessagePolicy(BufferingPolicy):
    pass


# If a buffer is not full we still flush it after flush_period_s of inactivity.
# This makes sure we do not wait indefinitely on idle channels
class PeriodicPartialFlushPolicy(BufferingPolicy):

    DEFAULT_FLUSH_PERIOD_S = 0.1

    def __init__(self, flush_period_s: float = DEFAULT_FLUSH_PERIOD_S):
        self.flush_period_s = flush_period_s
