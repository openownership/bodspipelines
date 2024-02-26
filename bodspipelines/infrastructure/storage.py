from typing import List, Union, Optional
from dataclasses import dataclass

from bodspipelines.infrastructure.clients.elasticsearch_client import ElasticsearchClient

class ElasticStorage:
    """Elasticsearch storage definition class"""
    def __init__(self, indexes):
        self.indexes = indexes
        self.storage = ElasticsearchClient()
        self.current_index = None
        self.auto_batch = {}

    def setup_indexes(self):
        for index_name in self.indexes:
            if index_name in ("entity", "person"): self.storage.create_index(index_name, self.indexes[index_name]['properties'])

    def create_action(self, index_name, item):
        #print(index_name, item)
        if callable(index_name): index_name = index_name(item)
        #return {"create": { "_index" : index_name, "_id" : self.indexes[index_name]["id"](item)}}
        return {"_id": self.indexes[index_name]["id"](item), '_index': index_name, '_op_type': 'create', "_source": item}

    def action_stream(self, stream, index_name):
        for item in stream:
            yield self.create_action(index_name, item)

    def batch_stream(self, stream, index_name):
        for item in stream:
            yield self.create_action(index_name, item), item

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

    def add_item(self, item, item_type):
        #print(item_type, self.indexes[item_type])
        query = self.indexes[item_type]['match'](item)
        #print("Query:", query)
        match = self.storage.search(query)
        #print(match)
        if not match['hits']['hits']:
            out = self.storage.store_data(item)
            #print(out)
            return item
        else:
            return False

    def add_item(self, item, item_type, overwrite=False):
        """Add item to index"""
        self.storage.set_index(item_type)
        id = self.indexes[item_type]['id'](item)
        result = self.storage.get(id)
        #print("add_item", item_type, id, result)
        if overwrite or not result:
            if overwrite and result:
                #print(f"Updating: {item}")
                out = self.storage.update_data(item, id)
                return item
            else:
                #print(f"Creating: {item}")
                #action = self.create_action(item_type, item)
                out = self.storage.store_data(item, id=id)
                return item
        else:
            return False

    def flush_batch(self, item_type):
        items = self.auto_batch[item_type]
        actions = [self.create_action(item_type, current_item) for current_item in items]
        for current_item in self.storage.batch_store_data(actions, items, item_type):
            pass
        self.auto_batch[item_type] = []

    def add_item_auto_batch(self, item, item_type):
        if not item_type in self.auto_batch: self.auto_batch[item_type] = []
        self.auto_batch[item_type].append(item)
        if len(self.auto_batch[item_type]) < 485:
            self.flush_batch(item_type)

    def auto_batch_flush(self, item_type):
        if len(self.auto_batch[item_type]) > 0:
            self.flush_batch(item_type)

    def process(self, item, item_type):
        if item_type != self.current_index:
            self.set_index(item_type)
        return self.add_item(item, item_type)

    def process_stream(self, stream, item_type):
        for item in self.storage.bulk_store_data(self.action_stream(stream, item_type), item_type):
            yield item

    def process_batch(self, stream, item_type):
        batch = []
        for action, item in self.batch_stream(stream, item_type):
            batch.append(action)
            batch.append(item)
            if len(batch) > 499:
                for item in self.storage.batch_store_data(batch, item_type):
                    yield item
                batch = []

    def process_batch(self, stream, item_type):
        for actions, items in self.batch_stream(stream, item_type):
            for item in self.storage.batch_store_data(actions, items, item_type):
                yield item
            #top_mem()

    def query(self, index_name, query):
        self.storage.set_index(index_name)
        return self.storage.search(query)

    def get_all(self, index_name):
        self.storage.set_index(index_name)
        return self.storage.search({'match_all': {}})
