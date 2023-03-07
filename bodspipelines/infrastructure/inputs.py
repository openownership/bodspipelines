from bodspipelines.infrastructure.clients.kinesis_client import KinesisStream

class KinesisInput:
    """Read from Kinesis Stream"""
    def __init__(self, stream_arn=None):
        self.stream_arn = stream_arn
        self.stream = KinesisStream(self.stream_arn)

    def process(self):
        for records in self.stream.read_stream():
            for record in records:
                yield record
