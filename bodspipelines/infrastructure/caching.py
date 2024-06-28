#from functools import wraps
import inspect

def get_id(storage, item_type, item):
    """Get item id given item and item_type"""
    return storage.storage.indexes[item_type]['id'](item)

class Caching():
    """Caching for updates"""
    def __init__(self):
        """Setup cache"""
        self.initialised = False
        self.cache = {"latest": {}, "references": {}, "exceptions": {}}
        self.batch = {"latest": {}, "references": {}, "exceptions": {}}

    async def load(self, storage):
        """Load data into cache"""
        for item_type in self.cache:
            print(f"Loading cache for {item_type}")
            async for item in storage.stream_items(item_type):
                #print(item_type, item)
                item_id = get_id(storage, item_type, item)
                self.cache[item_type][item_id] = item
        self.initialised = True

    def save(self, item_type, item, item_id):
        """Save item in cache"""
        self.cache[item_type][item_id] = item

    def batch_item(self, item_type, item, item_id, overwrite=False):
        """Batch to be written later"""
        if overwrite:
            self.batch[item_type][item_id] = ('index', item)
        else:
            self.batch[item_type][item_id] = ('create', item)

    def check_batch_item(self, item_type, item_id):
        """Check batch for item"""
        return self.batch[item_type].get(item_id)

    def unbatch_item(self, item_type, item_id):
        """Remove item from batch"""
        del self.batch[item_type][item_id]

    async def write_batch(self, storage, item_type):
        """Write batch to storage"""
        items = [self.batch[item_type][item_id] for item_id in self.batch[item_type]]
        await storage.add_batch(item_type, items)
        self.batch[item_type] = {}

    async def check_batch(self, storage):
        """Check if any batches need writing"""
        for item_type in self.batch:
            if len(self.batch[item_type]) > 485:
                await self.write_batch(storage, item_type)

    async def flush_batch(self, storage):
        """Check if any batches need writing"""
        for item_type in self.batch:
            if len(self.batch[item_type]) > 0:
                await self.write_batch(storage, item_type)

    def read(self, item_type, item_id):
        """Read item from cache"""
        return self.cache[item_type].get(item_id)

    def delete(self, item_type, item_id):
        """Delete item from cache"""
        del self.cache[item_type][item_id]

cache = Caching()

#def cached(func):
#    """Apply caching to function"""
#    @wraps(func)
#    def wrapper(*args, **kwargs):
#        storage = func.__self__
#        if not cache.initialised:
#            cache.load(storage)
#        if func.__name__ == "add_item":
#            item = args[0]
#            item_type = args[1]
#            item_id = get_id(storage, item_type, item)
#            cache.save(item_type, item, item_id)
#            return func(*args, **kwargs)
#        elif func.__name__ == "get_item":
#            item_id = args[0]
#            item_type = args[1]
#            item = cache.read(item_type, item_id)
#            if item: return item
#            return func(*args, **kwargs)
#    return wrapper

async def load_cache(storage):
    await cache.load(storage)

async def flush_cache(storage):
    await cache.flush_batch(storage)

async def cached(func, *args, **kwargs):
    """Apply caching to function call"""
    #if inspect.isclass(func):
    #    storage = func
    #else:
    storage = func.__self__
    #if "initialise" in kwargs and kwargs["initialise"]:
    #    cache.load(storage)
    if "batch" in kwargs:
        batch = kwargs["batch"]
        del kwargs["batch"]
    else:
        batch = False
    if "overwrite" in kwargs:
        overwrite = kwargs["overwrite"]
        del kwargs["overwrite"]
    else:
        overwrite = False
    #if batch == "finished":
    #    cache.flush_batch(storage)
    #    return
    #if not cache.initialised:
    #    cache.load(storage)
    out = None
    if func.__name__ == "add_item":
        item = args[0]
        item_type = args[1]
        item_id = get_id(storage, item_type, item)
        cache.save(item_type, item, item_id)
        if batch:
            cache.batch_item(item_type, item, item_id, overwrite=overwrite)
        else:
            out = await func(*args, **kwargs)
    elif func.__name__ == "get_item":
        item_id = args[0]
        item_type = args[1]
        item = cache.read(item_type, item_id)
        if item:
            out = item
        else:
            out = await func(*args, **kwargs)
    elif func.__name__ == "delete_item":
        item_id = args[0]
        item_type = args[1]
        cache.delete(item_type, item_id)
        if cache.check_batch_item(item_type, item_id):
            cache.unbatch_item(item_type, item_id)
        else:
            out = await func(*args, **kwargs)
    if batch: await cache.check_batch(storage)
    return out
