import time
from collections import OrderedDict


class Node(object):
    def __init__(self, data):
        self.data = data
        self.name = data['name']
        self.children = {}
        self.heats = {}

    def add_child(self, obj):
        if obj.name not in self.children:
            self.children[obj.name] = obj

    def get_child_by_name(self, name):
        return self.children[name]

    def get_total_heat(self):
        heat = 0
        for k, v in self.heats.iteritems():
            heat += v
        return heat

    def get_data(self, data_type):
        if data_type in self.heats:
            return self.heats[data_type]
        return 0

class NodeHelper(object):
    @staticmethod
    def add_path_and_data_to_tree(node, parts, data):
        if len(parts) > 0:
            part = parts.pop(0)
            if part not in node.children:
                node.add_child(Node({'name':part}))
            if data['type'] not in node.heats:
                node.heats[data['type']] = 0
            node.heats[data['type']] += data['rate']
            NodeHelper.add_path_and_data_to_tree(node.get_child_by_name(part), parts, data)
        else:
            if data['type'] not in node.heats:
                node.heats[data['type']] = 0
            node.heats[data['type']] += data['rate']

    @staticmethod
    def iternodes(node):
        stack = [node]
        while stack:
            node = stack.pop()
            yield node
            for child in node.get('children', []):
                stack.append(child)

    @staticmethod
    def node_recurse_generator(node):
        yield node.value
        for n in node.ChildElements:
            for rn in node_recurse_generator(n):
                yield rn            

    @staticmethod
    def walk_tree(ts, parent_path, node, level=0, max_level=3):
        if level > 0:
            data = OrderedDict()
            data["ts"] = ts
            data["ts_latest"] = time.time()
            data["path"] = parent_path + node.name
            data["level"] = level
            data["count"] = 1
            data["read_data"] = int(node.get_data("file-throughput-read"))
            data["write_data"] = int(node.get_data("file-throughput-write"))
            data["total_data"] = data["read_data"] + data["write_data"]
            data["read_iops"] = int(node.get_data("metadata-iops-read") + node.get_data("file-iops-read"))
            data["write_iops"] = int(node.get_data("metadata-iops-write") + node.get_data("file-iops-write"))
            data["total_iops"] = data["read_iops"] + data["write_iops"]
            if (data["total_data"] > 10000 or data["total_iops"] > 5):
                yield data
        for name,child in node.children.iteritems():
            if level <= max_level:
                for rn in NodeHelper.walk_tree(ts, parent_path + node.name + ('/' if level > 1 else ''), child, level+1, max_level):
                    yield rn
