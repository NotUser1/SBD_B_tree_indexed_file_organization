import struct
from conf import *
from typing import Optional, Tuple


class Record:
    def __init__(self, key: int, x: float, y: float):
        self.key = int(key)
        self.x = float(x)
        self.y = float(y)

    def pack(self) -> bytes:
        return RECORD_STRUCT.pack(self.key, self.x, self.y)

    @classmethod
    def unpack(cls, data: bytes):
        key, x, y = RECORD_STRUCT.unpack(data)
        return cls(int(key), float(x), float(y))

    def __repr__(self):
        return f"Record(key={self.key}, x={self.x}, y={self.y})"


class BTreeNode:
    # node structure:
    #    RECORDS_PER_NODE x 64b record pointers that are effectively keys
    #    (RECORDS_PER_NODE + 1) x 64b child pointers
    RECORD_POINTERS_STRUCT = struct.Struct('<' + 'Q' * RECORDS_PER_NODE)
    CHILD_POINTERS_STRUCT = struct.Struct('<' + 'Q' * (RECORDS_PER_NODE + 1))
    NODE_SIZE = RECORD_POINTERS_STRUCT.size + CHILD_POINTERS_STRUCT.size

    def __init__(self):
        self.is_leaf = True
        self.children_pointers = [0] * (RECORDS_PER_NODE + 1)  # child pointer will be a file offset in the B-Tree file
        self.record_pointers = [0] * RECORDS_PER_NODE  # record pointer will be a file offset in the records file

    def pack(self) -> bytes:
        record_pointers_data = self.RECORD_POINTERS_STRUCT.pack(*self.record_pointers)
        children_pointers_data = self.CHILD_POINTERS_STRUCT.pack(*self.children_pointers)

        return record_pointers_data + children_pointers_data

    @classmethod
    def unpack(cls, data: bytes):
        node = cls()

        record_pointers_size = cls.RECORD_POINTERS_STRUCT.size
        children_pointers_size = cls.CHILD_POINTERS_STRUCT.size

        node.record_pointers = list(cls.RECORD_POINTERS_STRUCT.unpack(data[0:record_pointers_size]))
        node.children_pointers = list(
            cls.CHILD_POINTERS_STRUCT.unpack(data[record_pointers_size:record_pointers_size + children_pointers_size]))
        node.is_leaf = all(ptr == 0 for ptr in node.children_pointers)

        return node

    def get_key_list(self, reader):
        keys = []
        for rp in self.record_pointers:
            if rp == 0:
                break
            rec = reader(rp)
            keys.append((rec.key, rp))
        return keys

    def get_data_from_node(self):
        data = []
        for i in range(RECORDS_PER_NODE):
            if self.record_pointers[i] != 0:
                data.append((self.record_pointers[i]))
                # need to implement getting real data from record pointer
        return data


class BTree:
    def __init__(self):
        self.root = BTreeNode()
        self.root_offset = 0

    def read_page(self, file_offset: int) -> BTreeNode:
        with open(BTREE_FILE, 'rb') as f:
            f.seek(file_offset)
            data = f.read(BTreeNode.NODE_SIZE)
        return BTreeNode.unpack(data)

    def write_page(self, node: BTreeNode, file_offset: int):
        with open(BTREE_FILE, 'r+b') as f:
            f.seek(file_offset)
            f.write(node.pack())
            f.flush()

    def read_record(self, record_pointer: int) -> Record:
        with open(DATA_FILE, 'rb') as f:
            f.seek(record_pointer)
            data = f.read(RECORD_STRUCT.size)
        return Record.unpack(data)

    def append_record(self, record: Record) -> int:
        with open(DATA_FILE, 'ab') as f:
            f.seek(0, 2)  # Move to the end of the file
            record_pointer = f.tell()
            f.write(record.pack())
            f.flush()
        return record_pointer

    def search(self, key: int) -> Tuple[Optional[Record], Optional[BTreeNode], int, int]:
        return self.search_node(self.root_offset, key)

    def search_node(self, node_offset: int, key: int) -> Tuple[Optional[Record], Optional[BTreeNode], int, int]:
        if node_offset is None:
            return None, None, 0, 0
        node = self.read_page(node_offset)
        keys = node.get_key_list(self.read_record)
        n = len(keys)
        i = 0
        while i < n and key > keys[i][0]:
            i += 1
        if i < n and key == keys[i][0]:
            record =  self.read_record(keys[i][1])
            return record, node, node_offset, i
        elif node.is_leaf:
            return None, node, node_offset, i

        child_offset = node.children_pointers[i]
        if child_offset == 0:
            return None, node, node_offset, i
        return self.search_node(child_offset, key)

    def _count_occupied_rps(self, node: BTreeNode) -> int:
        count = 0
        for rp in node.record_pointers:
            if rp == 0:
                break
            count += 1
        return count

    def _collect_path_for_key(self, key: int):
        """
        Zwraca listę (node, offset) od korzenia do liścia, decyzje podejmowane wg klucza.
        """
        path = []
        off = self.root_offset
        while True:
            node = self.read_page(off)
            path.append((node, off))
            keys = node.get_key_list(self.read_record)
            n = len(keys)
            i = 0
            while i < n and key > keys[i][0]:
                i += 1
            if node.is_leaf:
                break
            next_off = node.children_pointers[i]
            if next_off == 0:
                # no child present => stop here (will be treated as leaf)
                break
            off = next_off
        return path

    def insert(self, record: Record):
        found, node, node_offset, index = self.search(record.key)
        if found is not None:
            return {"status": "already_exists"}

        new_record_offset = self.append_record(record)
        path = self._collect_path_for_key(record.key)
        leaf_node, leaf_offset = path[-1]

        insert_index = index
        occupied = self._count_occupied_rps(leaf_node)
        if occupied < RECORDS_PER_NODE:
            # Insert into leaf node
            for j in range(occupied, insert_index, -1):
                leaf_node.record_pointers[j] = leaf_node.record_pointers[j - 1]
            leaf_node.record_pointers[insert_index] = new_record_offset
            self.write_page(leaf_node, leaf_offset)
            return {"status": "inserted"}

        # OVERFLOW
        # handle overflow tomorrow



