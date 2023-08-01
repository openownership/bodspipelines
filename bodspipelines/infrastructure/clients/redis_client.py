import os
import json

from redis.asyncio import Redis, RedisError

def create_client():
    """Create redis client"""
    host = os.getenv('REDIS_HOST')
    port = os.getenv('REDIS_PORT')
    return Redis(host=host, port=port)

def get_key(index, id):
    return f"{index}-{id}"

class RedisClient:
    """RedisClient class"""
    def __init__(self, indexes):
        """Initial setup"""
        self.client = create_client()
        self.indexes = indexes
        self.index_name = None

    def set_index(self, index_name):
        """Set index name"""
        self.index_name = index_name

    async def batch_store_data(self, actions, batch, index_name, output_new=True):
        """Store bulk data in index"""
        record_count = 0
        new_records = 0
        async with self.client.pipeline() as pipe:
            async for item in actions:
                key = get_key(index_name, item['_id'])
                await pipe.setnx(key, json.dumps(item['_source']))
            results = await pipe.execute()
            for i, result in enumerate(results):
                if result is True and output_new:
                    new_records += 1
                    yield batch[i]['_source']
                else:
                    print(result, batch[i]['_id'])

    async def get(self, id):
        """Search index"""
        key = get_key(self.index_name, id)
        try:
            value = await self.client.get(key)
        except RedisError:
            return None
        return json.loads(value)

    async def store_data(self, data):
        """Store data in index"""
        if isinstance(data, list):
            for d in data:
                key = get_key(self.index_name, d['_id'])
                await self.client.set(key, d['_source'])
        else:
            key = get_key(self.index_name, data['_id'])
            await self.client.set(key, data['_source'])

    async def setup(self):
        """Dummy setup method"""
        pass

