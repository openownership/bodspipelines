import aiofiles
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

class CSVData:
    """CSV data parser configuration"""

    def __init__(self):
        """Initial setup"""
        pass

    async def process(self, filename):
        """Iterate over processed items from file"""
        fieldnames = get_fieldnames(filename)
        async with aiofiles.open(filename, mode="r", encoding="utf-8") as afp:
            async for row in AsyncDictReader(afp, fieldnames=fieldnames):
                yield None, row

