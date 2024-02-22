#from bodspipelines.infrastructure.clients.kinesis_client import KinesisStream
from bodspipelines.infrastructure.clients.kinesis_client import create_kinesis

class KinesisInput:
    """Read from Kinesis Stream"""
    def __init__(self, stream_name=None):
        self.stream_name = stream_name
        #self.stream = KinesisStream(self.stream_name)

    async def process(self):
        #async for record in self.stream.read_stream():
            #for record in records:
            #    if isinstance(record, dict):
        async for record in self.stream.get():
            yield record

    async def setup(self):
        self.kinesis = create_kinesis(self.stream_name)
        self.stream = await anext(self.kinesis)
#        if hasattr(self.stream, 'setup'):
#            await self.stream.setup()

    async def close(self):
        #if hasattr(self.stream, 'close'):
        #    await self.stream.close()
        await anext(self.kinesis)
