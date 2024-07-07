import aiofiles
#from lxml import etree
import xml.etree.ElementTree as etree


async def stream_file(filename):
    async with aiofiles.open(filename, mode='r') as f:
        while True:
            data = await f.read(65536)
            if data:
                yield data
            else:
                yield ""
                break


def is_plural(tag, child_tag):
    """Is tag name plural"""
    if tag == child_tag + "s":
        return True
    elif tag == child_tag + "es":
        return True
    elif tag == child_tag[:-1] + 'ies':
        return True
    return False


def is_plural(tag, child_tag):
    """Is tag name plural"""
    if tag.endswith(child_tag + "s"):
        return True
    elif tag.endswith(child_tag + "es"):
        return True
    elif tag.endswith(child_tag[:-1] + 'ies'):
        return True
    return False


def get_tag(element, pos):
    """Return tag name without namespace"""
    for ns in pos:
        if ns in element.tag:
            return element.tag[pos[ns]:]


def handle_event(event, element, tag_name, skip, stack, pos, filter):
    out = None
    tag = get_tag(element, pos)
    if event == 'start':
        if skip:
            pass
        elif tag in filter:
            skip = True
        elif element.tag == tag_name or stack:
            stack.append([element.tag, {}])
    elif event == 'end':
        if skip:
            if tag in filter:
                skip = False
        elif element.tag == tag_name:
            element.clear()
            # Also eliminate now-empty references from the root node to elem
            #for ancestor in element.xpath('ancestor-or-self::*'):
            #    while ancestor.getprevious() is not None:
            #        del ancestor.getparent()[0]
            elem = stack.pop()
            out = elem[1]
        elif stack:
            elem = stack.pop()
            if elem[1]:
                val = elem[1]
            else:
                val = element.text
            if stack[-1][1]:
                if isinstance(stack[-1][1], list):
                    if 'type' in element.attrib:
                        if isinstance(val, dict):
                            val['type'] = element.attrib['type']
                            stack[-1][1].append(val)
                        else:
                            stack[-1][1].append({'type': element.attrib['type'], tag: val})
                    else:
                        stack[-1][1].append(val)
                else:
                    stack[-1][1][tag] = val
            else:
                if is_plural(stack[-1][0], tag):
                    if 'type' in element.attrib:
                        if isinstance(val, dict):
                            val['type'] = element.attrib['type']
                            stack[-1][1] = [val]
                        else:
                            stack[-1][1] = [{'type': element.attrib['type'], tag: val}]
                    else:
                        if isinstance(stack[-1][1], list):
                            stack[-1][1].append(val)
                        else:
                            stack[-1][1] = [val]
                else:
                    stack[-1][1][tag] = val
    return out, skip


async def data_stream(filename, tag_name, namespaces, filter=[]):
    """Stream items from XML file"""
    skip = False
    stack = []
    pos = {namespaces[ns]: len(namespaces[ns])+2 for ns in namespaces}
    parser = etree.XMLPullParser(('start', 'end',))
    async for chunk in stream_file(filename):
        parser.feed(chunk)
        for event, element in parser.read_events():
            out, skip = handle_event(event, element, tag_name, skip, stack, pos, filter)
            if out: yield out


class XMLData:
    """XML data parser configuration"""

    def __init__(self, item_tag=None, header_tag=None, namespace=None, filter=[]):
        """Initial setup"""
        self.item_tag = item_tag
        self.header_tag = header_tag
        self.namespace = namespace
        self.filter = filter

    async def extract_header(self, filename):
        """Extract header"""
        if self.header_tag:
            tag_name = f"{{{self.namespace[next(iter(self.namespace))]}}}{self.header_tag}"
            async for item in data_stream(filename, tag_name, self.namespace, filter=self.filter):
                return item
        else:
            return None

    async def process(self, filename):
        """Iterate over processed items from file"""
        header = await self.extract_header(filename)
        tag_name = f"{{{self.namespace[next(iter(self.namespace))]}}}{self.item_tag}"
        async for item in data_stream(filename, tag_name, self.namespace, filter=self.filter):
            yield header, item
