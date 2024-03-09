import os
import time
import json
import gzip
#import boto3
from aiobotocore.session import get_session

async def create_client(service):
     """Create AWS client for specified service"""
     session = get_session()
     return await session.create_client(service, region_name=os.getenv('BODS_AWS_REGION'),
                                  aws_access_key_id=os.environ.get('BODS_AWS_ACCESS_KEY_ID'),
                                  aws_secret_access_key=os.environ.get('BODS_AWS_SECRET_ACCESS_KEY')).__aenter__()
     #return boto3.client(service, region_name=os.getenv('BODS_AWS_REGION'), 
     #                             aws_access_key_id=os.environ.get('BODS_AWS_ACCESS_KEY_ID'),
     #                             aws_secret_access_key=os.environ.get('BODS_AWS_SECRET_ACCESS_KEY'))

async def get_stream_arn(client, stream_name):
    description = await client.describe_stream(StreamName=stream_name)
    return description["StreamDescription"]["StreamARN"]

async def shard_id(client, stream_arn):
     """Generate shard id"""
     response = await client.describe_stream(StreamARN=stream_arn)
     return response['StreamDescription']['Shards'][0]['ShardId']

def unpack_records(record_response):
    """Unpack records"""
    #print(record_response)
    records = []
    for record in record_response['Records']:
        #records.append(json.loads(gzip.decompress(record['Data']).decode('utf-8')))
        for line in record['Data'].decode('utf-8').split('\n'):
            #print(line)
            if line: records.append(json.loads(line))
    return records

class KinesisStream:
    """Kinesis Stream class"""
    def __init__(self, stream_name=None, shard_count=1):
        """Initial setup"""
        self.client = None
        self.stream_name = stream_name
        self.stream_arn = None
        self.shard_id = None
        self.shard_count = shard_count
        self.records = []
        self.waiting_bytes = 0

    async def setup(self):
        self.client = await create_client('kinesis')
        self.stream_arn = await get_stream_arn(self.client, self.stream_name)
        self.shard_id = await shard_id(self.client, self.stream_arn)

    async def send_records(self):
        """Send accumulated records"""
        print(f"Sending {len(self.records)} records to {self.stream_arn}")
        failed = len(self.records)
        while failed == len(self.records):
            response = await self.client.put_records(Records=self.records, StreamARN=self.stream_arn) #, StreamARN='string')
            failed = response['FailedRecordCount']
            if failed == len(self.records):
                time.sleep(1)
            elif failed > 0:
                batch = self.records
                self.records = []
                self.waiting_bytes = 0
                for i, record in enumerate(response['Records']):
                    if 'ErrorCode' in record:
                        self.records.append(batch[i])
                        self.waiting_bytes += len(batch[i]["Data"])
                break
            else:
                self.records = []
                self.waiting_bytes = 0
                break

    async def add_record(self, record):
        """Add record to stream"""
        json_data = json.dumps(record) + "\n"
        #encoded_data = bytes(json_data, 'utf-8')
        #compressed_data = gzip.compress(json_data.encode('utf-8'))
        self.records.append({"Data": json_data, "PartitionKey": str(self.shard_count)})
        num_bytes = len(json_data)
        self.waiting_bytes += num_bytes
        #print(f"Added {num_bytes} byte record ...")
        #print(f"Batched records {len(self.records)}")
        if self.waiting_bytes > 500000 or len(self.records) > 485: await self.send_records()

    async def finish_write(self):
        """Write any remaining records"""
        if len(self.records) > 0: await self.send_records()

    async def read_stream(self):
        """Read records from stream"""
        shard_iterator = await self.client.get_shard_iterator(StreamARN=self.stream_arn,
	                                                ShardId=self.shard_id,
                                                        ShardIteratorType='TRIM_HORIZON')
	                                                #ShardIteratorType='LATEST')
        shard_iterator = shard_iterator['ShardIterator']
        empty = 0
        while True:
            record_response = await self.client.get_records(ShardIterator=shard_iterator, Limit=100)
            #print(record_response)
            if len(record_response['Records']) == 0 and record_response['MillisBehindLatest'] == 0:
                empty += 1
            else:
                if len(record_response['Records']) > 0:
                    empty = 0
                    for item in unpack_records(record_response):
                        yield item
            if empty > 250:
                print(f"No records found in {self.stream_arn} after {empty} retries")
                break
            elif 'NextShardIterator' in record_response:
                shard_iterator = record_response['NextShardIterator']
            else:
                break

#    def read_stream(self):
#        """Read records from stream"""
#        stream = self.client.describe_stream(StreamName=self.stream_name)
#        shard_id = stream["StreamDescription"]["Shards"][0]["ShardId"]
#        print(f"Got {shard_id=}")
#        iterator = self.client.get_shard_iterator(
#            StreamName=self.stream_name,
#            ShardId=shard_id,
#            ShardIteratorType="TRIM_HORIZON"
#            )["ShardIterator"]
#        print(f"Reading data...")
#        response = self.client.get_records(ShardIterator=iterator, Limit=1)
#        while "NextShardIterator" in response:
#            data = response["Records"]
#            if len(data) < 1:
#                print("No data received")
#            else:
#                data = data[0]["Data"]
#                print(f"Received {data=}")
#            response = self.client.get_records(ShardIterator=response["NextShardIterator"], Limit=1)
