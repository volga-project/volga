import enum


class JobStatus(enum.Enum):
    # The initial state of a job
    SUBMITTING = 1

    # Job has been submitted and is long-running
    RUNNING = 2

    # Job is finished, clear worker by killing
    FINISHED = 3

    # Job is finished, clear worker by calling
    FINISHED_AND_CLEAN = 4

    # Job submission failed: passive cancel
    SUBMITTING_FAILED = 5

    # Job submission cancelled: active cancel
    SUBMITTING_CANCELLED = 6

    # Job wait for resubmit
    RESUBMITTING = 7
