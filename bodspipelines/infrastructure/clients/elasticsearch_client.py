import os
import json
#from elasticsearch import Elasticsearch
#from elasticsearch.helpers import bulk, streaming_bulk
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk

async def create_client():
    """Create Elasticsearch client"""
    protocol = os.getenv('ELASTICSEARCH_PROTOCOL')
    host = os.getenv('ELASTICSEARCH_HOST')
    port = os.getenv('ELASTICSEARCH_PORT')
    password = os.getenv('ELASTICSEARCH_PASSWORD')
    if password:
        return AsyncElasticsearch(f"{protocol}://{host}:{port}", basic_auth=('elastic', password), timeout=30, max_retries=10, retry_on_timeout=True)
    else:
        return AsyncElasticsearch(f"{protocol}://{host}:{port}", timeout=30, max_retries=10, retry_on_timeout=True) #, basic_auth=('elastic', password))

def index_definition(record, out):
    """Create index definition from record"""
    for key in record:
        if isinstance(record[key], dict):
            out2 = index_definition(record[key], {})
            out[key] = {"type":"object", "properties": out2}
        else:
            out[key] = {"type": "text"}
    return out

class ElasticsearchClient:
    """ElasticsearchClient class"""
    def __init__(self, indexes):
        """Initial setup"""
        self.client = None
        self.indexes = indexes
        self.index_name = None

    async def create_client(self):
        self.client = await create_client()

    def set_index(self, index_name):
        """Set index name"""
        self.index_name = index_name

    async def create_index(self, index_name, properties):
        """Create index"""
        self.set_index(index_name)
        # index settings
        settings = {"number_of_shards": 1,
                    "number_of_replicas": 0}
        mappings = {"dynamic": "strict",
                    "properties": properties}
        if not await self.client.indices.exists(index=self.index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            await self.client.options(ignore_status=400).indices.create(index=self.index_name, settings=settings, mappings=mappings)
            print('Elasticserach created Index')

    def delete_index(self):
        """Delete index"""
        self.client.options(ignore_status=[400, 404]).indices.delete(index=self.index_name)

    async def create_indexes(self):
        """Moved from storage"""
        for index_name in self.indexes:
            await self.create_index(index_name, self.indexes[index_name]['properties'])

    def stats(self, index_name):
        """Get index statistics"""
        return self.client.indices.stats(index=index_name)

    async def store_data(self, data):
        """Store data in index"""
        if isinstance(data, list):
            for d in data:
                await self.client.index(index=self.index_name, document=d)
        else:
            await self.client.index(index=self.index_name, document=data)

    def bulk_store_data(self, actions, index_name):
        """Store bulk data in index"""
        for ok, item in streaming_bulk(client=self.client, index=index_name, actions=actions):
            print(ok, item)
            if not ok:
                yield False
            else:
                yield item

    def batch_store_data(self, actions, index_name):
        """Store bulk data in index"""
        #ok, errors = bulk(client=self.client, index=index_name, actions=actions)
        errors = self.client.bulk(index=index_name, operations=actions)
        print("Bulk:", errors)
        return errors

    async def batch_store_data(self, actions, batch, index_name):
        """Store bulk data in index"""
        await self.create_client()
        record_count = 0
        new_records = 0
        async for ok, result in async_streaming_bulk(client=self.client, actions=actions, raise_on_error=False): #index=index_name,
            record_count += 1
            #print(ok, result)
            #print(batch[0])
            if ok:
                new_records += 1
                match = [i for i in batch if i['_id'] == result['create']['_id']]
                yield match[0]['_source']
            else:
                print(ok, result)
            #
            #if not ok:
            #    yield False
            #else:
            #    yield item
        if callable(index_name):
            print(f"Storing in {index_name(batch[0]['_source'])}: {record_count} records; {new_records} new records")
        else:
            print(f"Storing in {index_name}: {record_count} records; {new_records} new records")

    async def search(self, search):
        """Search index"""
        return await self.client.search(index=self.index_name, query=search)

    async def get(self, id):
        """Get by id"""
        match = await self.search({"query": {"match": {"_id": id}}})
        return match['hits']['hits']

    def list_indexes(self):
        """List indexes"""
        return self.client.indices.get_alias(index="*")

    def get_mapping(self, index_name):
        """Get index mapping"""
        return self.client.indices.get_mapping(index=index_name)

    def check_new(self, data):
        pass

    async def setup(self):
        self.client = await create_client()
