import os
import json
import asyncio
import elastic_transport
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk, async_scan

async def create_client():
    """Create Elasticsearch client"""
    protocol = os.getenv('ELASTICSEARCH_PROTOCOL')
    host = os.getenv('ELASTICSEARCH_HOST')
    port = os.getenv('ELASTICSEARCH_PORT')
    password = os.getenv('ELASTICSEARCH_PASSWORD')
    if password:
        return AsyncElasticsearch(f"{protocol}://{host}:{port}",
                                  basic_auth=('elastic', password),
                                  timeout=30,
                                  max_retries=10,
                                  retry_on_timeout=True)
    else:
        return AsyncElasticsearch(f"{protocol}://{host}:{port}",
                                  timeout=30,
                                  max_retries=10,
                                  retry_on_timeout=True)

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
            await self.client.options(ignore_status=400).indices.create(index=self.index_name,
                                                                        settings=settings,
                                                                        mappings=mappings)
            print('Elasticserach created Index')

    async def setup_indexes(self):
        """Setup indexes"""
        done = False
        if not self.client: await self.setup()
        while not done:
            try:
                await self.create_indexes()
                done = True
            except elastic_transport.ConnectionError:
                print("Waiting for Elasticsearch to start ...")
                asyncio.sleep(5)
        await self.close()

    def delete_index(self):
        """Delete index"""
        self.client.options(ignore_status=[400, 404]).indices.delete(index=self.index_name)

    async def create_indexes(self):
        """Moved from storage"""
        for index_name in self.indexes:
            await self.create_index(index_name, self.indexes[index_name]['properties'])

    async def statistics(self, index_name):
        """Get index statistics"""
        count = 0
        stats = {}
        for index_name in self.indexes:
            result = await self.client.indices.stats(index=index_name)
            stats[index_name] = result
            count += result
        stats['total'] = count
        return stats

    async def store_data(self, data, id=None):
        """Store data in index"""
        if isinstance(data, list):
            for d in data:
                await self.client.index(index=self.index_name, document=d)
        else:
            await self.client.index(index=self.index_name, document=data)

    async def update_data(self, data, id):
        """Update data in index"""
        await self.client.update(index=self.index_name, id=id, document=data)

    def bulk_store_data(self, actions, index_name):
        """Store bulk data in index"""
        for ok, item in streaming_bulk(client=self.client, index=index_name, actions=actions):
            #print(ok, item)
            if not ok:
                yield False
            else:
                yield item

    def batch_store_data(self, actions, index_name):
        """Store bulk data in index"""
        errors = self.client.bulk(index=index_name, operations=actions)
        print("Bulk:", errors)
        return errors

    async def batch_store_data(self, actions, batch, index_name):
        """Store bulk data in index"""
        await self.create_client()
        record_count = 0
        new_records = 0
        #for b in batch:
        #    print(b['_id'], b['_index'])
        async for ok, result in async_streaming_bulk(client=self.client, actions=actions, raise_on_error=False):
            record_count += 1
            #print(ok, result)
            if ok:
                new_records += 1
                match = [i for i in batch if i['_id'] == result['create']['_id']]
                yield match[0]['_source']
            else:
                print(ok, result)
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
        result = match['hits']['hits']
        if result:
            return result[0]
        else:
            return None

    async def delete(self, id):
        """Delete by id"""
        return await self.client.delete(self.index_name, id)

    async def delete_all(self, index):
        """Delete all documents in index"""
        await self.client.delete_by_query(index, {"query":{"match_all":{}}})

    async def scan_index(self, index):
        """Scan index"""
        async for doc in async_scan(client=self.client,
                                    query={"query": {"match_all": {}}},
                                    index=index):
            yield doc

    def list_indexes(self):
        """List indexes"""
        return self.client.indices.get_alias(index="*")

    def get_mapping(self, index_name):
        """Get index mapping"""
        return self.client.indices.get_mapping(index=index_name)

    def check_new(self, data):
        """Dummy method"""
        pass

    async def setup(self):
        """Setup Elasticsearch client"""
        self.client = await create_client()

    async def close(self):
        """Close Elasticsearch client"""
        await self.client.transport.close()
