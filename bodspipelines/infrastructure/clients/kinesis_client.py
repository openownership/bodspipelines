import os
import asyncio
from types import SimpleNamespace
from kinesis import Producer, Consumer, JsonLineProcessor

async def read_stream(consumer):
    """Read records from stream"""
    found = False
    count = 0
    while True:
        async for item in consumer:
            print(item)
            yield item
            found = True
            count = 0
        print("Waiting for records...")
        if found: count += 1
        if found and count > 10: break
    print("No more records")

async def create_kinesis(stream_name):
    """Create Producer and Consumer for stream and put and get methods"""
    os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('BODS_AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('BODS_AWS_SECRET_ACCESS_KEY')
    async with (Producer(stream_name=stream_name, processor=JsonLineProcessor(),
                        region_name=os.getenv('BODS_AWS_REGION'),
                        put_bandwidth_limit_per_shard=800) as producer,
                Consumer(stream_name=stream_name, processor=JsonLineProcessor(),
                                 region_name=os.getenv('BODS_AWS_REGION')) as consumer):
        async def put(record):
            await producer.put(record)
        async def get():
            async for item in read_stream(consumer):
                yield item
        async def finish():
            await asyncio.sleep(5)
        yield SimpleNamespace(put=put, get=get, finish=finish)
    yield


#import os
#import json
#import asyncio

#from kinesis import Producer, Consumer, JsonLineProcessor

#class KinesisStream:
#    """Kinesis Stream client"""
#    def __init__(self, stream_name=None):
#        self.stream_name=stream_name
#        self.processor = JsonLineProcessor()
#        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('BODS_AWS_ACCESS_KEY_ID')
#        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('BODS_AWS_SECRET_ACCESS_KEY')
#
#    async def setup(self):
#        """Setup kinesis clients"""
#        self.producer = Producer(stream_name=self.stream_name,
#                                 processor=self.processor,
#                                 region_name=os.getenv('BODS_AWS_REGION'),
#                                 put_bandwidth_limit_per_shard=800)
#        self.consumer = Consumer(stream_name=self.stream_name,
#                                 processor=self.processor,
#                                 region_name=os.getenv('BODS_AWS_REGION'))
#        self.read = False
#
#    async def close(self):
#        """Close kinesis clients"""
#        await self.producer.close()
#        # Client only started by first read, so only close if has been read
#        if self.read: await self.consumer.close()
#
#    async def read_stream(self):
#        """Read records from stream"""
#        self.read = True
#        found = False
#        count = 0
#        while True:
#            async for item in self.consumer:
#                #print("Read:", item)
#                yield item
#                found = True
#                count = 0
#            print("Waiting for records...")
#            if found: count += 1
#            if found and count > 10: break
#        print("No more records")
#
#    async def add_record(self, record):
#        """Add record to stream"""
#        # Put item onto queue to be flushed via put_records()
#        await self.producer.put(record)
#
#    async def finish_write(self):
#        """Write any remaining records"""
#        #await self.producer.flush()
#        await asyncio.sleep(5)
