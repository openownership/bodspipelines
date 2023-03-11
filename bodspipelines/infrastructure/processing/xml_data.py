from lxml import etree

def is_plural(tag, child_tag):
    if tag == child_tag + "s":
        return True
    elif tag == child_tag + "es":
        return True
    elif tag == child_tag[:-1] + 'ies':
        return True
    return False

class XMLData:
    """XML data definition class"""

    def __init__(self, item_tag=None, namespace=None, filter=[]):
        """Initial setup"""
        self.item_tag = item_tag
        self.namespace = namespace
        self.filter = filter

    def data_stream(self, filename):
        """Stream parsed XML elements from file"""
        print(f"Parsing {filename}")
        ns = self.namespace[next(iter(self.namespace))]
        tag_name = f"{{{ns}}}{self.item_tag}"
        for event, element in etree.iterparse(filename, events=('end',), tag=tag_name):
            yield element

    def is_array(self, tag, child):
        """Check if is array """
        child_tag = etree.QName(child[0]).localname
        if is_plural(tag, child_tag):
            #print("Array!!!!")
            return True
        else:
            return False

    def add_element(self, out, tag, value):
        if not tag in self.filter and value:
            out[tag] = value

    def process_item(self, item, out):
        """Process XML item to dict"""
        for child in item:
            tag = etree.QName(child).localname
            #print(tag, len(child))
            if len(child) > 0:
                if self.is_array(tag, child):
                    parent_data = []
                else:
                    parent_data = {}
                child_data = self.process_item(child, parent_data)
                if isinstance(out, list):
                    out.append(child_data)
                else:
                    self.add_element(out, tag, child_data)
                    #out[tag] = child_data
            else:
                try:
                    child_value = child.xpath("./text()", namespaces=self.namespace)[0]
                except IndexError:
                    child_value = False
                if isinstance(out, list):
                    out.append(child_value)
                else:
                    self.add_element(out, tag, child_value)
                    #out[tag] = child_value
        return out

    def process(self, filename):
        """Iterate over processed items from file"""
        for element in self.data_stream(filename):
            item = self.process_item(element, {})
            element.clear()
            #print(item)
            yield item
            #break # Remove
