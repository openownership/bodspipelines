import os
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk

def create_client():
    """Create Elasticsearch client"""
    protocol = os.getenv('ELASTICSEARCH_PROTOCOL')
    host = os.getenv('ELASTICSEARCH_HOST')
    port = os.getenv('ELASTICSEARCH_PORT')
    password = os.getenv('ELASTICSEARCH_PASSWORD')
    if password:
        return Elasticsearch(f"{protocol}://{host}:{port}", basic_auth=('elastic', password))
    else:
        return Elasticsearch(f"{protocol}://{host}:{port}") #, basic_auth=('elastic', password))

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

    def store_data(self, data):
        """Store data in index"""
        if isinstance(data, list):
            for d in data:
                self.client.index(index=self.index_name, document=d)
        else:
            self.client.index(index=self.index_name, document=data)

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
        for ok, result in streaming_bulk(client=self.client, index=index_name, actions=actions, raise_on_error=False):
            print(ok, item)
            if ok:
                item = [i for i in batch if i._id == result['create']['_id']][0]
                yield item
            #
            #if not ok:
            #    yield False
            #else:
            #    yield item

    def search(self, search):
        """Search index"""
        return self.client.search(index=self.index_name, query=search)

    def list_indexes(self):
        """List indexes"""
        return self.client.indices.get_alias(index="*")

    def get_mapping(self, index_name):
        """Get index mapping"""
        return self.client.indices.get_mapping(index=index_name)

    def check_new(self, data):
        pass

