import os
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, scan

def create_client():
    """Create Elasticsearch client"""
    protocol = os.getenv('ELASTICSEARCH_PROTOCOL')
    host = os.getenv('ELASTICSEARCH_HOST')
    port = os.getenv('ELASTICSEARCH_PORT')
    password = os.getenv('ELASTICSEARCH_PASSWORD')
    if password:
        return Elasticsearch(f"{protocol}://{host}:{port}", basic_auth=('elastic', password), timeout=30, max_retries=10, retry_on_timeout=True)
    else:
        return Elasticsearch(f"{protocol}://{host}:{port}", timeout=30, max_retries=10, retry_on_timeout=True) #, basic_auth=('elastic', password))

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
    def __init__(self):
        """Initial setup"""
        self.client = create_client()
        self.index_name = None

    def set_index(self, index_name):
        """Set index name"""
        self.index_name = index_name

    def create_index(self, index_name, properties):
        """Create index"""
        self.set_index(index_name)
        # index settings
        settings = {"number_of_shards": 1,
                    "number_of_replicas": 0}
        mappings = {"dynamic": "strict",
                    "properties": properties}
        if not self.client.indices.exists(index=self.index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            self.client.options(ignore_status=400).indices.create(index=self.index_name, settings=settings, mappings=mappings)
            print('Elasticserach created Index')

    def delete_index(self):
        """Delete index"""
        self.client.options(ignore_status=[400, 404]).indices.delete(index=self.index_name)

    def stats(self, index_name):
        """Get index statistics"""
        return self.client.indices.stats(index=index_name)

    def store_data(self, data, id=None):
        """Store data in index"""
        if isinstance(data, list):
            for d in data:
                self.client.index(index=self.index_name, document=d)
        else:
            self.client.index(index=self.index_name, document=data, id=id)

    def update_data(self, data, id):
        """Update data in index"""
        print(f"Updating {self.index_name} index: {id} {data}")
        self.client.update(index=self.index_name, id=id, body=data)

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

    def batch_store_data(self, actions, batch, index_name):
        """Store bulk data in index"""
        record_count = 0
        new_records = 0
        for ok, result in streaming_bulk(client=self.client, actions=actions, raise_on_error=False): #index=index_name,
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

    def search(self, search):
        """Search index"""
        return self.client.search(index=self.index_name, query=search)

    def get(self, id):
        """Get by id"""
        #match = self.search({"query": {"match": {"_id": id}}})
        match = self.search({"match": {"_id": id}})
        result = match['hits']['hits']
        if result:
            return result[0]
        else:
            return None

    def delete(self, id):
        """Delete by id"""
        return self.client.delete(self.index_name, id)

    def delete_all(self, index):
        """Delete all documents in index"""
        self.client.delete_by_query(index, {"query":{"match_all":{}}})

    def scan_index(self, index):
        """Scan index"""
        for doc in scan(client=self.client,
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
        pass

