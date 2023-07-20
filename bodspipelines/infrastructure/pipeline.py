import time
import asyncio
from typing import List, Union
from pathlib import Path

from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData

#from bodspipelines.infrastructure.storage import ElasticStorage

class Source:
    """Data source definition class"""
    def __init__(self, name=None, origin=None, datatype=None):
        """Initial setup"""
        self.name = name
        self.origin = origin
        self.datatype = datatype

    async def process(self, stage_dir):
        """Iterate over source items"""
        if hasattr(self.origin, "prepare"):
            data = self.origin.prepare(stage_dir, self.name)
            async for item in self.datatype.process(data):
                yield item
        else:
            async for item in self.origin.process():
                yield self.datatype.process(item)

    async def setup(self):
        if hasattr(self.origin, 'setup'):
            await self.origin.setup()


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

    async def source_processing(self, source, stage_dir):
        """Iterate over items from source, with processing"""
        async for item in source.process(stage_dir):
            if self.processors:
                for processor in self.processors:
                    for out in processor.process(item, source.name):
                        #print(out)
                        yield out
            else:
                yield item

    #def process_source(self, source, stage_dir):
    #    """Iterate over items from source, with processing and output"""
    #    for item in source.process(stage_dir):
    #        for processor in self.processors:
    #            item = processor.process(item, source.name)

    async def process_source(self, source, stage_dir):
        """Iterate over items from source, and output"""
        if len(self.outputs) > 1 or not self.outputs[0].streaming:
            async for item in self.source_processing(source, stage_dir):
                for output in self.outputs:
                    output.process(item, source.name)
        else:
            await self.outputs[0].process_stream(self.source_processing(source, stage_dir), source.name)

    async def process(self, pipeline_dir):
        """Process all sources for stage"""
        print(f"Running {self.name} pipeline stage")
        stage_dir = self.directory(pipeline_dir)
        for source in self.sources:
            print(f"Processing {source.name} source")
            await self.process_source(source, stage_dir)
        print(f"Finished {self.name} pipeline stage")

    async def setup(self):
        for source in self.sources:
            if hasattr(source, 'setup'):
                await source.setup()
        for processor in self.processors:
            if hasattr(processor, 'setup'):
                await processor.setup()
        for output in self.outputs:
            if hasattr(output, 'setup'):
                await output.setup()

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

    async def process_stage(self, stage_name):
        """Process specified pipeline stage"""
        stage = self.get_stage(stage_name)
        pipeline_dir = self.directory()
        await stage.setup()
        await stage.process(pipeline_dir)

    def process(self, stage_name):
        """Process specified pipeline stage"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.process_stage(stage_name))
        #try:
        #    asyncio.run(self.process_stage(stage_name))
        #except RuntimeError:
        #    #loop = asyncio.get_event_loop()
        #    #loop = asyncio.new_event_loop()
        #    #loop.run_until_complete(self.process_stage(stage_name))
        #    task = asyncio.create_task(self.process_stage(stage_name))
        #    while not task.done():
        #        time.sleep(1)
        #    #asyncio.wait_for(self.process_stage(stage_name), timeout=10000)
