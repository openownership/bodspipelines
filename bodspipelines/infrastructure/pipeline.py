from typing import List, Union
#from dataclasses import dataclass
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

    def process(self, stage_dir):
        """Iterate over source items"""
        if hasattr(self.origin, "prepare"):
            data = self.origin.prepare(stage_dir)
            for item in self.datatype.process(data):
                yield item
        else:
            for item in self.origin.process():
                yield self.datatype.process(item)

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

    def source_processing(self, source, stage_dir):
        """Iterate over items from source, with processing"""
        for item in source.process(stage_dir):
            if self.processors:
                for processor in self.processors:
                    for out in processor.process(item, source.name):
                        print(out)
                        yield out
            else:
                yield item

    #def process_source(self, source, stage_dir):
    #    """Iterate over items from source, with processing and output"""
    #    for item in source.process(stage_dir):
    #        for processor in self.processors:
    #            item = processor.process(item, source.name)

    def process_source(self, source, stage_dir):
        """Iterate over items from source, and output"""
        if len(self.outputs) > 1 or not self.outputs[0].streaming:
            for item in self.source_processing(source, stage_dir):
                for output in self.outputs:
                    output.process(item, source.name)
        else:
            self.outputs[0].process_stream(self.source_processing(source, stage_dir), source.name)

    def process(self, pipeline_dir):
        """Process all sources for stage"""
        print(f"Running {self.name} pipeline stage")
        stage_dir = self.directory(pipeline_dir)
        for source in self.sources:
            self.process_source(source, stage_dir)

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

    def process(self, stage_name):
        """Process specified pipeline stage"""
        stage = self.get_stage(stage_name)
        pipeline_dir = self.directory()
        stage.process(pipeline_dir)
