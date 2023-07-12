from lxml import etree

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


def data_stream(filename, tag_name, namespaces, filter=[]):
    skip = False
    stack = []
    pos = {namespaces[ns]: len(namespaces[ns])+2 for ns in namespaces}
    for event, element in etree.iterparse(filename, events=('start', 'end',)):
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
                elem = stack.pop()
                yield elem[1]
            elif stack:
                elem = stack.pop()
                if elem[1]:
                    val = elem[1]
                else:
                    val = element.text
                if stack[-1][1]:
                    if isinstance(stack[-1][1], list):
                        if 'type' in element.attrib:
                            stack[-1][1].append({'type': element.attrib['type'], tag: val})
                        else:
                            stack[-1][1].append(val)
                    else:
                        stack[-1][1][tag] = val
                else:
                    if is_plural(stack[-1][0], tag):
                        if 'type' in element.attrib:
                            if isinstance(stack[-1][1], list):
                                stack[-1][1].append({'type': element.attrib['type'], tag: val})
                            else:
                                stack[-1][1] = [{'type': element.attrib['type'], tag: val}]
                            #stack[-1][1][element.attrib['type']] = val
                        else:
                            if isinstance(stack[-1][1], list):
                                stack[-1][1].append(val)
                            else:
                                stack[-1][1] = [val]
                    else:
                        stack[-1][1][tag] = val


class XMLData:
    """XML data parser configuration"""

    def __init__(self, item_tag=None, namespace=None, filter=[]):
        """Initial setup"""
        self.item_tag = item_tag
        self.namespace = namespace
        self.filter = filter

    def process(self, filename):
        """Iterate over processed items from file"""
        tag_name = f"{{{self.namespace[next(iter(self.namespace))]}}}{self.item_tag}"
        for item in data_stream(filename, tag_name, self.namespace, filter=self.filter):
            yield item
