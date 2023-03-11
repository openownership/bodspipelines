from typing import List, Union

from bodspipelines.infrastructure.clients.kinesis_client import KinesisStream

class OutputConsole:
    """Output to console definition class"""
    def __init__(self, name=None):
        """Initial setup"""
        self.streaming = False
        self.name = name

    def process(self, item, item_type):
        """Print item"""
        print(f"{self.name}: {item}")


class Output:
    """Data output definition class"""
    def __init__(self, name=None, target=None):
        """Initial setup"""
        self.streaming = False
        self.name = name
        self.target = target

    def process(self, item, item_type):
        """Process item"""
        self.target.process(item, item_type)


class NewOutput:
    """Storage data and output if new definition class"""
    def __init__(self, storage=None, output=None, identify=None):
        self.streaming = True
        self.storage = storage
        self.output = output
        self.identify = identify
        self.processed_count = 0
        self.new_count = 0

    def process(self, item, item_type):
        self.processed_count += 1
        #print(f"{item_type}: {item}")
        stored = self.storage.process(item, item_type)
        #print(f"NewOutput: {stored}")
        if stored:
            self.output.process(item, item_type)
            self.new_count += 1
        #print(f"Processed: {self.processed_count}, New: {self.new_count}")

    def process_stream(self, stream, item_type):
        if self.identify: item_type = self.identify
        for item in self.storage.process_batch(stream, item_type):
            if item:
                self.output.process(item, item_type)

class KinesisOutput:
    """Output to Kinesis Stream"""
    def __init__(self, stream_name=None):
        self.streaming = False
        self.stream_name = stream_name
        self.stream = KinesisStream(self.stream_name)

    def process(self, item, item_type):
        self.stream.add_record(item)

    def __del__(self):
        self.stream.finish_write()
