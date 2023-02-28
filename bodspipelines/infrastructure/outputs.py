from typing import List, Union

from bodspipelines.infrastructure.clients.kinesis_client import KinesisStream

class OutputConsole:
    """Output to console definition class"""
    def __init__(self, name):
        """Initial setup"""
        self.name = name

    def process(self, item, item_type):
        """Print item"""
        print(f"{self.name}: {item}")


class Output:
    """Data output definition class"""
    def __init__(self, name, target):
        """Initial setup"""
        self.name = name
        self.target = target

    def process(self, item, item_type):
        """Process item"""
        self.target.process(item, item_type)


class NewOutput:
    """Storage data and output if new definition class"""
    def __init__(self, storage, output):
        self.storage = storage
        self.output = output

    def process(self, item, item_type):
        item = self.storage.process(item, item_type)
        if item:
            self.storage.process(item)


class KinesisOutput:
    """Output to Kinesis Stream"""
    def __init__(self, stream_name):
        self.stream_name = stream_name
        self.stream = KinesisStream(self.stream_name)

    def process(self, item, item_type):
        self.stream.add_record(item)

    def __del__(self):
        self.stream.finish_write()
