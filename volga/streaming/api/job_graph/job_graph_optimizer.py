from volga.streaming.api.job_graph.job_graph import JobGraph


class JobGraphOptimizer:

    def __init__(self, job_graph: JobGraph):
        self.job_graph = job_graph

    def optimize(self) -> JobGraph:
        # TODO implement chaining and other optimizations
        return self.job_graph