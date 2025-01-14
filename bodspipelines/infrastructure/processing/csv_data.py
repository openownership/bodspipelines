import aiofiles
import re

from aiocsv import AsyncDictReader

class AsyncDictReaderStrip(AsyncDictReader):
    @property
    def fieldnames(self):
        if self._fieldnames is None:
            # Initialize self._fieldnames
            # Note: DictReader is an old-style class, so can't use super()
            AsyncDictReader.fieldnames.fget(self)
            if self._fieldnames is not None:
                self._fieldnames = [name.strip() for name in self._fieldnames]
        return self._fieldnames

def get_fieldnames(filename):
    with open(filename) as file:
        return [col.strip() for col in file.readline().split(",")]

def get_file_date(filename):
    match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
    if match:
        return match[0]
    return None

def get_header(filename):
    return {"ContentDate": get_file_date(filename)}

class CSVData:
    """CSV data parser configuration"""

    def __init__(self):
        """Initial setup"""
        pass

    async def process(self, filename):
        """Iterate over processed items from file"""
        fieldnames = get_fieldnames(filename)
        header = get_header(filename)
        async with aiofiles.open(filename, mode="r", encoding="utf-8") as afp:
            await afp.readline()
            async for row in AsyncDictReader(afp, fieldnames=fieldnames):
                yield None, row

