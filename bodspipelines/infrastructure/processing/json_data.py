import aiofiles
import ijson
import json
import os
import pathlib

def get_last_line(filename):
    with open(filename, 'rb') as f:
        try:  # catch OSError in case of a one line file
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        last_line = f.readline().decode()
    return last_line

def get_first_line(filename):
    with open(filename) as f:
        return f.readline()

class JSONData:
    """JSON data definition class"""

    def __init__(self, header=None, exclude=None, sample=False):
        """Initial setup"""
        self.header = header # 1: take first line, -1: take last
        self.exclude = exclude
        self.sample = sample

    async def extract_header(self, filename):
        """Extract header"""
        if self.header == -1:
            line = get_last_line(filename)
        elif self.header == 1:
            line = get_first_line(filename)
        return json.loads(line)

    async def process(self, data):
        """Open and parse if file, else return data directly"""
        if isinstance(data, str) or isinstance(data, pathlib.PurePath):
            if self.header:
                header = await self.extract_header(data)
            else:
                header = None
            count = 0
            async with aiofiles.open(data, mode="r", encoding="utf-8") as file:
                async for item in ijson.items(file, '', multiple_values=True):
                    count += 1
                    if self.sample and not count % self.sample == 0: continue
                    if self.exclude:
                        if not self.exclude(item):
                            yield header, item
                    else:
                        yield header, item
        else:
            # Dummy (past through data if reading from Kinesis stream)
            yield None, data
