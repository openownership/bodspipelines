from lxml import etree

class XMLData:
    """XML data definition class"""

    def __init__(self, item_tag=None, namespace=None):
        """Initial setup"""
        self.item_tag = item_tag
        self.namespace = namespace

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
        if tag == child_tag + "s":
            #print("Array!!!!")
            return True
        else:
            return False

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
                    out[tag] = child_data
            else:
                try:
                    child_value = child.xpath("./text()", namespaces=self.namespace)[0]
                except IndexError:
                    child_value = ""
                if isinstance(out, list):
                    out.append(child_value)
                else:
                    out[tag] = child_value
        return out

    def process(self, filename):
        """Iterate over processed items from file"""
        for element in self.data_stream(filename):
            item = self.process_item(element, {})
            #print(item)
            yield item
            #break # Remove
