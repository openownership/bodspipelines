import os
import json
import gzip
import boto3

def create_client(service):
     """Create AWS client for specified service"""
     return boto3.client(service, region_name=os.getenv('BODS_AWS_REGION'), aws_access_key_id=os.environ.get('BODS_AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key=os.environ.get('BODS_AWS_SECRET_ACCESS_KEY'))

def shard_id(client, stream_name):
     """Generate shard id"""
     response = client.describe_stream(StreamName=stream_name)
     return response['StreamDescription']['Shards'][0]['ShardId']

def unpack_records(record_response):
    """Unpack records"""
    records = []
    for record in record_response['Records']:
        records.append(json.loads(gzip.decompress(record['Data']).decode('utf-8')))
    return records

class KinesisStream:
    """Kinesis Stream class"""
    def __init__(self, stream_name=None, shard_count=1):
        """Initial setup"""
        self.client = create_client('kinesis')
        self.stream_name = stream_name
        self.shard_id = shard_id(self.client, self.stream_name)
        self.shard_count = shard_count
        self.records = []
        self.waiting_bytes = 0

    def add_record(self, record):
        """Add record to stream"""
        json_data = json.dumps(record)
        #encoded_data = bytes(json_data, 'utf-8')
        compressed_data = gzip.compress(json_data.encode('utf-8'))
        self.records.append({"Data": compressed_data, "PartitionKey": str(self.shard_count)})
        num_bytes = len(compressed_data)
        self.waiting_bytes += num_bytes
        print(f"Added {num_bytes} byte record ...")
        if self.waiting_bytes > 50000 or len(self.records) == 500: self.send_records()

    def finish_write(self):
        """Write any remaining records"""
        if len(self.records) > 0: self.send_records()

    def read_stream(self):
        """Read records from stream"""
        shard_iterator = self.client.get_shard_iterator(StreamName=self.stream_name,
	                                                ShardId=self.shard_id,
                                                        ShardIteratorType='TRIM_HORIZON')
	                                                #ShardIteratorType='LATEST')
        shard_iterator = shard_iterator['ShardIterator']
        while True:
            record_response = self.client.get_records(ShardIterator=shard_iterator, Limit=100)
            yield unpack_records(record_response)
            if 'NextShardIterator' in record_response:
                shard_iterator = record_response['NextShardIterator']
            else:
                break
