from typing import List, Union, Optional
from dataclasses import dataclass

from bodspipelines.infrastructure.clients.elasticsearch_client import ElasticsearchClient

class Storage:
    """Storage definition class"""
    def __init__(self, storage):
        self.storage = storage

    def setup(self):
        self.storage.setup()

    def list_indexes(self):
        return self.storage.list_indexes()

    def list_index_details(self, index_name):
        return self.storage.get_mapping(index_name)

    def stats(self):
        for index_name in self.indexes:
            print(f"Index {index_name}:", self.storage.stats(index_name))

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
        self.storage.create_index(index_name, self.indexes[index_name]['properties'])

    def create_action(self, index_name, item):
        if callable(index_name): index_name = index_name(item)
        return {"_id": self.storage.indexes[index_name]["id"](item), '_index': index_name, '_op_type': 'create', "_source": item}

    def get(self, id):
        return self.storage.get(id)

    #def add_item(self, item, item_type):
    #    query = self.storage.indexes[item_type]['match'](item)
    #    match = self.storage.search(query)
    #    if not match['hits']['hits']:
    #        out = self.storage.store_data(item)
    #        return item
    #    else:
    #        return False

    def add_item(self, item, item_type):
        id = self.storage.indexes[item_type]['id'](item)
        result = self.storage.get(id)
        if not result:
            action = self.create_action(item_type, item)
            out = self.storage.store_data(action)
            return item
        else:
            return False


    def process(self, item, item_type):
        if item_type != self.storage.index_name:
            self.storage.set_index(item_type)
        return self.add_item(item, item_type)

    def create_batch(self, batch):
        def func():
            for i in batch:
                yield i
        return func(), batch

    def batch_stream(self, stream, index_name):
        batch = []
        for item in stream:
            batch.append(self.create_action(index_name, item))
            if len(batch) > 485:
                yield self.create_batch(batch)
                batch = []
        if len(batch) > 0:
            yield self.create_batch(batch)

    def process_batch(self, stream, item_type):
        for actions, items in self.batch_stream(stream, item_type):
            for item in self.storage.batch_store_data(actions, items, item_type):
                yield item

