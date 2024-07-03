import os
import time
import json
import gzip

from kinesis import Producer, Consumer, JsonLineProcessor

class KinesisStream:
    """Kinesis Stream class"""
    def __init__(self, stream_name=None):
        """Initial setup"""
        self.producer = None
        self.consumer = None
        self.stream_name = stream_name
        self.count = 0

    async def setup(self):
        """Setup Kinesis clients"""
        os.environ["AWS_ACCESS_KEY_ID"] = os.environ["BODS_AWS_ACCESS_KEY_ID"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["BODS_AWS_SECRET_ACCESS_KEY"]
        self.producer = await Producer(stream_name=self.stream_name, processor=JsonLineProcessor(),
                        region_name=os.getenv('BODS_AWS_REGION'),
                        put_bandwidth_limit_per_shard=750).__aenter__()
        self.consumer = await Consumer(stream_name=self.stream_name, processor=JsonLineProcessor(),
                                 region_name=os.getenv('BODS_AWS_REGION')).__aenter__()

    async def add_record(self, record):
        """Add record to stream"""
        await self.producer.put(record)
        self.count += 1
        if self.count % 10000 == 0: await self.finish_write()

    async def finish_write(self):
        """Write any remaining records"""
        await self.producer.flush()

    async def read_stream(self):
        """Read records from stream"""
        found = False
        count = 0
        while True:
            async for item in self.consumer:
                yield item
                found = True
                count = 0
            if count % 10 == 0: print("Waiting for records...")
            if found: count += 1
            if found and count > 10: break
        print("No more records")

    async def close(self):
        """Close Kinesis clients"""
        if self.producer: await self.producer.close()
        if self.consumer: await self.consumer.close()
