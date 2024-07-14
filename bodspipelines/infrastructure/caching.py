#from functools import wraps
import inspect

def get_id(storage, item_type, item):
    """Get item id given item and item_type"""
    return storage.indexes[item_type]['id'](item)

class Caching():
    """Caching for updates"""
    def __init__(self):
        """Setup cache"""
        self.initialised = False
        self.cache = {"latest": {}, "references": {}, "exceptions": {}}
        self.batch = {"latest": {}, "references": {}, "exceptions": {}}

    def load(self, storage):
        """Load data into cache"""
        for item_type in self.cache:
            for item in storage.stream_items(item_type):
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

    def write_batch(self, storage, item_type):
        """Write batch to storage"""
        items = [self.batch[item_type][item_id] for item_id in self.batch[item_type]]
        storage.add_batch(item_type, items)
        self.batch[item_type] = {}

    def check_batch(self, storage):
        """Check if any batches need writing"""
        for item_type in self.batch:
            if len(self.batch[item_type]) > 485:
                self.write_batch(storage, item_type)

    def flush_batch(self, storage):
        """Check if any batches need writing"""
        for item_type in self.batch:
            if len(self.batch[item_type]) > 0:
                self.write_batch(storage, item_type)

    def read(self, item_type, item_id):
        """Read item from cache"""
        return self.cache[item_type].get(item_id)

    def delete(self, item_type, item_id):
        """Delete item from cache"""
        del self.cache[item_type][item_id]

#cache = Caching()

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

#def load_cache(storage):
#    cache.load(storage)

#def flush_cache(storage):
#    cache.flush_batch(storage)

#def cached(func, *args, **kwargs):
#    """Apply caching to function call"""
#    #if inspect.isclass(func):
#    #    storage = func
#    #else:
#    storage = func.__self__
#    #if "initialise" in kwargs and kwargs["initialise"]:
#    #    cache.load(storage)
#    if "batch" in kwargs:
#        batch = kwargs["batch"]
#        del kwargs["batch"]
#    else:
#        batch = False
#    if "overwrite" in kwargs:
#        overwrite = kwargs["overwrite"]
#        del kwargs["overwrite"]
#    else:
#        overwrite = False
#    #if batch == "finished":
#    #    cache.flush_batch(storage)
#    #    return
#    #if not cache.initialised:
#    #    cache.load(storage)
#    out = None
#    if func.__name__ == "add_item":
#        item = args[0]
#        item_type = args[1]
#        item_id = get_id(storage, item_type, item)
#        cache.save(item_type, item, item_id)
#        if batch:
#            cache.batch_item(item_type, item, item_id, overwrite=overwrite)
#        else:
#            out = func(*args, **kwargs)
#    elif func.__name__ == "get_item":
#        item_id = args[0]
#        item_type = args[1]
#        item = cache.read(item_type, item_id)
#        if item:
#            out = item
#        else:
#            out = func(*args, **kwargs)
#    elif func.__name__ == "delete_item":
#        item_id = args[0]
#        item_type = args[1]
#        cache.delete(item_type, item_id)
#        if cache.check_batch_item(item_type, item_id):
#            cache.unbatch_item(item_type, item_id)
#        else:
#            out = func(*args, **kwargs)
#    if batch: cache.check_batch(storage)
#    return out

class Caching():
    """Caching for updates"""
    def __init__(self, storage, batching=False):
        """Setup cache"""
        self.initialised = False
        self.cache = {"latest": {}, "references": {}, "exceptions": {}, "updates": {}}
        self.batch = {"latest": {}, "references": {}, "exceptions": {}, "updates": {}} if batching else None
        self.batch_size = batching if batching else None
        self.memory_only = ["updates"]
        self.storage = storage

    def load(self):
        """Load data into cache"""
        for item_type in self.cache:
            if item_type in self.memory_only:
                print(f"Loading cache for {item_type}")
                for item in self.storage.stream_items(item_type):
                    #print(item_type, item)
                    item_id = get_id(self.storage, item_type, item)
                    self.cache[item_type][item_id] = item
        self.initialised = True

    def _save(self, item_type, item, item_id):
        """Save item in cache"""
        self.cache[item_type][item_id] = item

    def _batch_item(self, item_type, item, item_id, overwrite=False):
        """Batch to be written later"""
        if overwrite:
            self.batch[item_type][item_id] = ('index', item)
        else:
            self.batch[item_type][item_id] = ('create', item)

    def _check_batch_item(self, item_type, item_id):
        """Check batch for item"""
        return self.batch[item_type].get(item_id)

    def _unbatch_item(self, item_type, item_id):
        """Remove item from batch"""
        del self.batch[item_type][item_id]

    def _write_batch(self, item_type):
        """Write batch to storage"""
        items = [self.batch[item_type][item_id] for item_id in self.batch[item_type]]
        self.storage.add_batch(item_type, items)
        self.batch[item_type] = {}

    def _check_batch(self):
        """Check if any batches need writing"""
        for item_type in self.batch:
            if not item_type in self.memory_only:
                if len(self.batch[item_type]) > self.batch_size:
                    self._write_batch(item_type)

    def flush(self):
        """Check if any item need writing"""
        for item_type in self.batch:
            if not item_type in self.memory_only:
                if len(self.batch[item_type]) > 0:
                    self._write_batch(item_type)

    def _read(self, item_type, item_id):
        """Read item from cache"""
        return self.cache[item_type].get(item_id)

    def _delete(self, item_type, item_id):
        """Delete item from cache"""
        del self.cache[item_type][item_id]
        if not item_type in self.memory_only:
            self.batch[item_type][item_id] = None

    def add(self, item, item_type, overwrite=False):
        """Apply caching to function call"""
        item_id = get_id(self.storage, item_type, item)
        self._save(item_type, item, item_id)
        if not self.batch is None:
            self._batch_item(item_type, item, item_id, overwrite=overwrite)
        else:
            out = self.storage.add_item(*args, **kwargs)

    def get(self, item_id, item_type):
        """Get cached item"""
        #print(item_id, item_type)
        item = self._read(item_type, item_id)
        #if item:
        #    out = item
        #else:
        #    out = await self.storage.get_item(item, item_type)
        return item

    def delete(self, item_id, item_type):
        """Delete acched item"""
        self._delete(item_type, item_id)
        if self._check_batch_item(item_type, item_id):
            self._unbatch_item(item_type, item_id)
        else:
            out = self.storage.delete_item(item_id, item_type)

    def stream(self, item_type):
        """Get cached items"""
        for item_id in self.cache[item_type]:
            yield self.cache[item_type].get(item_id)

    def count(self, item_type):
        return len(self.cache[item_type])
