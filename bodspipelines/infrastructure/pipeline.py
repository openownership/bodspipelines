import time
import asyncio
from typing import List, Union
from pathlib import Path

#from bodspipelines.infrastructure.processing.bulk_data import BulkData
#from bodspipelines.infrastructure.processing.xml_data import XMLData

#from bodspipelines.infrastructure.storage import ElasticStorage

#from .memory_debugging import log_memory

class Source:
    """Data source definition class"""
    def __init__(self, name=None, origin=None, datatype=None):
        """Initial setup"""
        self.name = name
        self.origin = origin
        self.datatype = datatype

    async def process(self, stage_dir, updates=False):
        """Iterate over source items"""
        if hasattr(self.origin, "prepare"):
            for data in self.origin.prepare(stage_dir, self.name, updates=updates):
                async for header, item in self.datatype.process(data):
                    yield header, item
        else:
            async for item in self.origin.process():
                header, item = self.datatype.process(item)
                yield header, item

    async def setup(self):
        """Run origin setup"""
        if hasattr(self.origin, 'setup'):
            await self.origin.setup()

    async def close(self):
        """Close origin"""
        if hasattr(self.origin, 'close'):
            await self.origin.close()


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

    async def source_processing(self, source, stage_dir, updates=False):
        """Iterate over items from source, with processing"""
        #count = 0
        async for header, item in source.process(stage_dir, updates=updates):
            #print(header, item)
            if self.processors:
                items = [item]
                for processor in self.processors:
                    new_items = []
                    for current_item in items:
                    #print("Processor:", processor)
                        async for out in processor.process(current_item, source.name, header, updates=updates):
                            #print(out)
                            #yield out
                            new_items.append(out)
                    items = new_items
                for current_item in items:
                    yield current_item
            else:
                yield item
            #count += 1
            #if count % 100000 == 0:
            #    log_memory()
        for processor in self.processors:
            print("Processor:", hasattr(processor, "finish_updates"), updates)
            if hasattr(processor, "finish_updates") and updates:
                async for out in processor.finish_updates(updates=updates):
                    yield out

    async def process_source(self, source, stage_dir, updates=False):
        """Iterate over items from source, and output"""
        print("Process source:", len(self.outputs) > 1, not self.outputs[0].streaming)
        if len(self.outputs) > 1 or not self.outputs[0].streaming:
            print("Interating:")
            async for item in self.source_processing(source, stage_dir, updates=updates):
                for output in self.outputs:
                    output.process(item, source.name)
        else:
            print("Streaming:")
            await self.outputs[0].process_stream(self.source_processing(source, stage_dir, updates=updates), source.name)

    async def process(self, pipeline_dir, updates=False):
        """Process all sources for stage"""
        print(f"Running {self.name} pipeline stage")
        stage_dir = self.directory(pipeline_dir)
        for source in self.sources:
            print(f"Processing {source.name} source")
            await self.process_source(source, stage_dir, updates=updates)
        print(f"Finished {self.name} pipeline stage")

    async def setup(self):
        """Setup stage components"""
        for components in (self.sources, self.processors, self.outputs):
            for component in components:
                if hasattr(component, 'setup'):
                    await component.setup()

    async def close(self):
        """Shutdown stage components"""
        for components in (self.sources, self.processors, self.outputs):
            for component in components:
                if hasattr(component, 'close'):
                    await component.close()


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

    async def process_stage(self, stage_name, updates=False):
        """Process specified pipeline stage"""
        stage = self.get_stage(stage_name)
        pipeline_dir = self.directory()
        await stage.setup()
        await stage.process(pipeline_dir, updates=updates)
        await stage.close()

    def process(self, stage_name, updates=False):
        """Process specified pipeline stage"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.process_stage(stage_name, updates=updates))
