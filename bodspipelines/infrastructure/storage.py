from typing import List, Union, Optional
from dataclasses import dataclass

from bodspipelines.infrastructure.clients.elasticsearch_client import ElasticsearchClient

class ElasticStorage:
    """Elasticsearch storage definition class"""
    def __init__(self, indexes):
        self.indexes = indexes
        self.storage = ElasticsearchClient()
        self.current_index = None

    def setup_indexes(self):
        for index_name in self.indexes:
            self.storage.create_index(index_name, self.indexes[index_name]['properties'])

    def list_indexes(self):
        return self.storage.list_indexes()

    def list_index_details(self, index_name):
        return self.storage.get_mapping(index_name)

    def set_index(self, index_name):
        self.current_index = index_name
        self.storage.set_index(index_name)

    def delete_index(self, index_name):
        self.current_index = index_name
        self.storage.set_index(index_name)
        self.storage.delete_index()

    def delete_all(self, index_name):
        self.current_index = index_name
        self.storage.set_index(index_name)
        self.storage.delete_index()
        self.storage.create_index(index_name, self.indexes[index_name])

    def add_item(self, item, item_type):
        print(item_type, self.indexes[item_type])
        query = self.indexes[item_type]['match'](item)
        print("Query:", query)
        match = self.storage.search(query)
        print(match)
        if not match['hits']['hits']:
            out = self.storage.store_data(item)
            print(out)
            return item
        else:
            return False

    def process(self, item, item_type):
        if item_type != self.current_index:
            self.set_index(item_type)
        return self.add_item(item, item_type)

    def query(self, index_name, query):
        self.storage.set_index(index_name)
        return self.storage.search({"match" : query})

