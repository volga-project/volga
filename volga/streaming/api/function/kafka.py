from typing import Optional

from volga.streaming.api.function.function import SourceFunction, SourceContext


# TODO implement kafka source
class KafkaSourceFunction(SourceFunction):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        security_protocol: Optional[str],
        sasl_mechanism: Optional[str],
        sasl_plain_username: Optional[str],
        sasl_plain_password: Optional[str],
        verify_cert: Optional[bool],
    ):
        raise NotImplementedError('KafkaSource is not implemented yet, please use mock sources')

    def init(self, parallel, index):
        pass

    def fetch(self, ctx: SourceContext):
        pass