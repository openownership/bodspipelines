import os
import json

import redis

def create_client():
    """Create redis client"""
    host = os.getenv('REDIS_HOST')
    port = os.getenv('REDIS_PORT')
    return redis.Redis(host=host, port=port)

def get_key(index, id):
    return f"{index}-{id}"

class RedisClient:
    """RedisClient class"""
    def __init__(self):
        """Initial setup"""
        self.client = create_client()
        self.index_name = None

    def batch_store_data(self, actions, batch, index_name, output_new=True):
        """Store bulk data in index"""
        record_count = 0
        new_records = 0
        with self.client.pipeline() as pipe:
            for item in actions:
                key = get_key(index_name, item['_id'])
                pipe.setnx(key, json.dumps(item['_source']))
            results = pipe.execute()
            for result in results:
                if result is True:
                    new_records += 1
