class Record:
    def __init__(self, key: int, x: int, y: int):
        self.key = key
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Record(key={self.key}, x={self.x}, y={self.y})"


class BTreeNode:
    def __init__(self, t, leaf=True, node_id=None):
        self.t = t
        self.leaf = leaf
        self.keys = []
        self.children = []
        self.records = []
        self.node_id = node_id

    def is_full(self):
        return len(self.keys) == (2 * self.t) - 1



class BTree:
    def __init__(self, t):
        self.root = BTreeNode(t, True)
        self.t = t
