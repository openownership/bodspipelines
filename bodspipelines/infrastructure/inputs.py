from bodspipelines.infrastructure.clients.kinesis_client import KinesisStream

class KinesisInput:
    """Read from Kinesis Stream"""
    def __init__(self, stream_name=None):
        self.stream_name = stream_name
        self.stream = KinesisStream(self.stream_name)

    def prepare(self, stage_dir):
        pass

    def process(self):
        for records in self.stream.read_stream():
            for record in records
                yield record
