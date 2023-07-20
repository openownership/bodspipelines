import os
import json
import asyncio

from kinesis import Producer, Consumer, JsonLineProcessor

class KinesisStream:
    """Kinesis Stream client"""
    def __init__(self, stream_name=None):
        self.stream_name=stream_name
        self.processor = JsonLineProcessor()
        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('BODS_AWS_ACCESS_KEY_ID')
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('BODS_AWS_SECRET_ACCESS_KEY')

    async def setup(self):
        self.producer = Producer(stream_name=self.stream_name, processor=self.processor, region_name=os.getenv('BODS_AWS_REGION'))
        self.consumer = Consumer(stream_name=self.stream_name, processor=self.processor, region_name=os.getenv('BODS_AWS_REGION'))

    async def read_stream(self):
        """Read records from stream"""
        found = False
        count = 0
        while True:
            async for item in self.consumer:
                print(item)
                yield item
                found = True
                count = 0
            print("Waiting for records...")
            if found: count += 1
            if found and count > 10: break
        print("No more records")

    async def add_record(self, record):
        """Add record to stream"""
        # Put item onto queue to be flushed via put_records()
        await self.producer.put(record)

    async def finish_write(self):
        """Write any remaining records"""
        await self.producer.flush()
