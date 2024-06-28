from typing import List, Union, Optional
from dataclasses import dataclass

class Storage:
    """Storage definition class"""

    def __init__(self, storage):
        """Initialise storage"""
        self.storage = storage

    async def setup(self):
        """Setup storage"""
        await self.storage.setup()

    def list_indexes(self):
        """List indexes"""
        return self.storage.list_indexes()

    def list_index_details(self, index_name):
        """List details for specified index"""
        return self.storage.get_mapping(index_name)

    async def statistics(self):
        """Print storage statistics"""
        print("Storage:")
        print("")
        statistics = await self.storage.statistics()
        for index_name in statistics:
            if index_name != "total":
                print(f"{index_name} items:", statistics[index_name])
        print("")
        print("Total items:", statistics["total"])

    def set_index(self, index_name):
        """Set current index"""
        self.current_index = index_name
        self.storage.set_index(index_name)

    def delete_index(self, index_name):
        """Delete index"""
        self.current_index = index_name
        self.storage.set_index(index_name)
        self.storage.delete_index()

    def delete_all(self, index_name):
        """Delete index"""
        self.current_index = index_name
        self.storage.set_index(index_name)
        self.storage.delete_index()
        self.storage.create_index(index_name, self.indexes[index_name]['properties'])

    def create_action(self, index_name, item):
        """Build create action for item"""
        if callable(index_name): index_name = index_name(item)
        return {"_id": self.storage.indexes[index_name]["id"](item), '_index': index_name, '_op_type': 'create', "_source": item}

    async def get_item(self, id, item_type):
        """Get item from index"""
        self.storage.set_index(item_type)
        return await self.storage.get(id)

    async def add_item(self, item, item_type, overwrite=False):
        """Add item to index"""
        self.storage.set_index(item_type)
        id = self.storage.indexes[item_type]['id'](item)
        result = await self.storage.get(id)
        #print(result)
        if overwrite or not result:
            if overwrite and result:
                #print(f"Updating: {item}")
                out = await self.storage.update_data(item, id)
                return item
            else:
                #print(f"Creating: {item}")
                #action = self.create_action(item_type, item)
                out = await self.storage.store_data(item, id=id)
                return item
        else:
            return False

    async def delete_item(self, id, item_type):
        """Delete item with id in index"""
        self.storage.set_index(item_type)
        await self.storage.delete(id)

    async def stream_items(self, index):
        """Stream items in index"""
        async for item in self.storage.scan_index(index):
            yield item['_source']

    async def process(self, item, item_type):
        """Add item to index"""
        if item_type != self.storage.index_name:
            self.storage.set_index(item_type)
        return await self.add_item(item, item_type)

    async def create_batch(self, batch):
        """Create iterator that yields batch"""
        async def func():
            for i in batch:
                yield i
        return func(), batch

    async def batch_stream(self, stream, index_name):
        """Create stream of batched actions"""
        batch = []
        async for item in stream:
            batch.append(self.create_action(index_name, item))
            if len(batch) > 485:
                yield await self.create_batch(batch)
                batch = []
        if len(batch) > 0:
            yield await self.create_batch(batch)

    async def process_batch(self, stream, item_type):
        """Store items from stream in batches"""
        async for actions, items in self.batch_stream(stream, item_type):
            async for item in self.storage.batch_store_data(actions, items, item_type):
                yield item

    async def setup_indexes(self):
        """Setup indexes"""
        await self.storage.setup_indexes()

    async def add_batch(self, item_type, items):
        print(f"Write batch of {len(items)} item for {item_type}")
        actions = [self.create_action(item_type, current_item[1], action_type=current_item[0])
                                                                    for current_item in items]
        async for current_item in self.storage.batch_store_data(actions, actions, item_type):
            pass
