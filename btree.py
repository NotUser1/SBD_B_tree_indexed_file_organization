import struct

RECORD_COUNT = 15
RECORDS_PER_NODE = 10
RECORD_STRUCT = struct.Struct('<dd')  # 2x 64b float (x, y)
RECORD_SIZE = RECORD_STRUCT.size


class Record:
    def __init__(self, x: float, y: float):
        self.x = float(x)
        self.y = float(y)

    def pack(self) -> bytes:
        return RECORD_STRUCT.pack(self.x, self.y)

    @classmethod
    def unpack(cls, data: bytes):
        x, y = RECORD_STRUCT.unpack(data)
        return cls(x, y)

    def __repr__(self):
        return f"Record(x={self.x}, y={self.y})"


class BTreeNode:

    # node structure:
    #    RECORDS_PER_NODE x 32b keys
    #    RECORDS_PER_NODE x 64b record pointers
    #    (RECORDS_PER_NODE + 1) x 64b child pointers

    KEYS_STRUCT = struct.Struct('<' + 'I' * RECORDS_PER_NODE)
    RECORD_POINTERS_STRUCT = struct.Struct('<' + 'Q' * RECORDS_PER_NODE)
    CHILD_POINTERS_STRUCT = struct.Struct('<' + 'Q' * (RECORDS_PER_NODE + 1))

    NODE_SIZE = KEYS_STRUCT.size + RECORD_POINTERS_STRUCT.size + CHILD_POINTERS_STRUCT.size

    def __init__(self):
        self.is_leaf = True
        self.keys = [0] * RECORDS_PER_NODE
        self.children_pointers = [0] * (RECORDS_PER_NODE + 1)   # child pointer will be a file offset in the B-Tree file
        self.record_pointers = [0] * RECORDS_PER_NODE   # record pointer will be a file offset in the records file

    def pack(self) -> bytes:
        keys_data = self.KEYS_STRUCT.pack(*self.keys)
        record_pointers_data = self.RECORD_POINTERS_STRUCT.pack(*self.record_pointers)
        children_pointers_data = self.CHILD_POINTERS_STRUCT.pack(*self.children_pointers)

        return keys_data + record_pointers_data + children_pointers_data

    @classmethod
    def unpack(cls, data: bytes):
        node = cls()
        offset = 0

        keys_size = cls.KEYS_STRUCT.size
        record_pointers_size = cls.RECORD_POINTERS_STRUCT.size
        children_pointers_size = cls.CHILD_POINTERS_STRUCT.size

        node.keys = list(cls.KEYS_STRUCT.unpack(data[offset:offset + keys_size]))
        offset += keys_size

        node.record_pointers = list(cls.RECORD_POINTERS_STRUCT.unpack(data[offset:offset + record_pointers_size]))
        offset += record_pointers_size

        node.children_pointers = list(cls.CHILD_POINTERS_STRUCT.unpack(data[offset:offset + children_pointers_size]))

        return node


class BTree:
    def __init__(self):
        self.root = BTreeNode()
