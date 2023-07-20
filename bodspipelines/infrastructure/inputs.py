from bodspipelines.infrastructure.clients.kinesis_client import KinesisStream

class KinesisInput:
    """Read from Kinesis Stream"""
    def __init__(self, stream_name=None):
        self.stream_name = stream_name
        self.stream = KinesisStream(self.stream_name)

    async def process(self):
        async for record in self.stream.read_stream():
            #for record in records:
            #    if isinstance(record, dict):
            yield record

    async def setup(self):
        if hasattr(self.stream, 'setup'):
            await self.stream.setup()

