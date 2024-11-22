import aiofiles
from aiocsv import AsyncDictReader

class CSVData:
    """CSV data parser configuration"""

    def __init__(self):
        """Initial setup"""
        pass

    async def process(self, filename):
        """Iterate over processed items from file"""
        async with aiofiles.open(filename, mode="r", encoding="utf-8") as afp:
            async for row in AsyncDictReader(afp):
                yield None, row

