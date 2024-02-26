from typing import List, Union
from pathlib import Path

from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData

from bodspipelines.infrastructure.storage import ElasticStorage

class Source:
    """Data source definition class"""
    def __init__(self, name=None, origin=None, datatype=None):
        """Initial setup"""
        self.name = name
        self.origin = origin
        self.datatype = datatype

    def process(self, stage_dir, updates=False):
        """Iterate over source items"""
        if hasattr(self.origin, "prepare"):
            data = self.origin.prepare(stage_dir, self.name, updates=False)
            for header, item in self.datatype.process(data):
                yield header, item
        else:
            for item in self.origin.process():
                header, item = self.datatype.process(item)
                yield header, item

class Stage:
    """Pipeline stage definition class"""

    def __init__(self, name=None, sources=None, processors=None, outputs=None):
        """Initial setup"""
        self.name = name
        self.sources = sources
        self.processors = processors
        self.outputs = outputs 

    def directory(self, parent_dir) -> Path:
        """Return subdirectory path after ensuring exists"""
        path = Path(parent_dir) / self.name
        path.mkdir(exist_ok=True)
        return path

    def source_processing(self, source, stage_dir, updates=False):
        """Iterate over items from source, with processing"""
        for header, item in source.process(stage_dir, updates=False):
            if self.processors:
                items = [item]
                for processor in self.processors:
                    new_items = []
                    for current_item in items:
                        for out in processor.process(current_item, source.name, header, updates=updates):
                            #print(out)
                            new_items.append(out)
                    items = new_items
                for current_item in items:
                    yield current_item
            else:
                yield item
        for processor in self.processors:
            print("Processor:", hasattr(processor, "finish_updates"), updates)
            if hasattr(processor, "finish_updates") and updates:
                for out in processor.finish_updates():
                    yield out

    #def process_source(self, source, stage_dir):
    #    """Iterate over items from source, with processing and output"""
    #    for item in source.process(stage_dir):
    #        for processor in self.processors:
    #            item = processor.process(item, source.name)

    def process_source(self, source, stage_dir, updates=False):
        """Iterate over items from source, and output"""
        if len(self.outputs) > 1 or not self.outputs[0].streaming:
            for item in self.source_processing(source, stage_dir, updates=False):
                for output in self.outputs:
                    output.process(item, source.name)
        else:
            self.outputs[0].process_stream(self.source_processing(source, stage_dir, updates=False), source.name)

    def process(self, pipeline_dir, updates=False):
        """Process all sources for stage"""
        print(f"Running {self.name} pipeline stage")
        stage_dir = self.directory(pipeline_dir)
        for source in self.sources:
            print(f"Processing {source.name} source")
            self.process_source(source, stage_dir, updates=False)
        print(f"Finished {self.name} pipeline stage")

class Pipeline:
    """Pipeline definition class"""
    def __init__(self, name=None, stages=None):
        """Initial setup"""
        self.name = name
        self.stages = stages

    def directory(self) -> Path:
        """Return subdirectory path after ensuring exists"""
        path = Path("data") / self.name
        path.mkdir(exist_ok=True)
        return path

    def get_stage(self, name):
        """Get pipeline stage by name"""
        for stage in self.stages:
            if stage.name == name:
                return stage
        return None

    def process(self, stage_name, updates=False):
        """Process specified pipeline stage"""
        stage = self.get_stage(stage_name)
        pipeline_dir = self.directory()
        stage.process(pipeline_dir, updates=False)
