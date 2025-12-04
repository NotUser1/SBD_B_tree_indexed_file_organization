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
    #    RECORDS_PER_NODE x 32b record pointers
    #    (RECORDS_PER_NODE + 1) x 32b child pointers
    RECORD_POINTERS_STRUCT = struct.Struct('<' + 'I' * RECORDS_PER_NODE)
    CHILD_POINTERS_STRUCT = struct.Struct('<' + 'I' * (RECORDS_PER_NODE + 1))
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
        self.height = 0

    def read_page(self, file_offset: int) -> BTreeNode:
        with open(BTREE_FILE, 'rb') as f:
            f.seek(file_offset)
            data = f.read(BTreeNode.NODE_SIZE)
            if len(data) < BTreeNode.NODE_SIZE:
                raise ValueError("Failed to read a complete B-Tree node from file.")
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

    def create_root(self, record: Record):
        new_record_offset = self.append_record(record)
        self.root.record_pointers[0] = new_record_offset
        # create a node
        root = BTreeNode()
        root.record_pointers[0] = new_record_offset
        root_packed = root.pack()
        with open(BTREE_FILE, 'ab') as f:
            f.seek(0)
            self.root_offset = f.tell()
            f.write(root_packed)
            f.flush()
        self.height = 1

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
            record = self.read_record(keys[i][1])
            return record, node, node_offset, i
        elif node.is_leaf:
            return None, node, node_offset, i

        child_offset = node.children_pointers[i]
        if child_offset == 0:
            return None, node, node_offset, i
        return self.search_node(child_offset, key)

    def _count_occupied_rps(self, node: BTreeNode) -> int:
        if node is None:
            return 0

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

    def split_node(self, path, new_record_offset, leaf_offset=None, current_node=None, new_child_offset=None):
        print("split_node called")
        # we need to handle root split here also because split_node is called from handle_overflow recursively ?
        # no need because handle_overflow checks for root split first
        # #sanity_check
        if current_node is None or leaf_offset is None:
            current_node, leaf_offset = path[-1]

        if len(path) < 2:
            print("splitting root from split_node")
            return self.split_root(new_record_offset, leaf_offset, current_node)

        parent_node, parent_offset = path[-2]

        temp = [rp for rp in current_node.record_pointers if rp != 0]
        temp.append(new_record_offset)
        temp.sort(key=lambda rec_ptr: self.read_record(rec_ptr).key)

        total = len(temp)
        mid_index = total // 2
        mid_value = temp[mid_index]
        left_rps = temp[:mid_index]
        right_rps = temp[mid_index + 1:]

        child_index = parent_node.children_pointers.index(leaf_offset)

        # save left node to current node
        for i in range(RECORDS_PER_NODE):
            if i < len(left_rps):
                current_node.record_pointers[i] = left_rps[i]
            else:
                current_node.record_pointers[i] = 0
        left_children = current_node.children_pointers[:mid_index + 1]
        current_node.children_pointers = left_children + [0] * (RECORDS_PER_NODE + 1 - len(left_children))
        self.write_page(current_node, leaf_offset)

        # create and save right node to the end of btree file
        right_node = BTreeNode()
        for i in range(RECORDS_PER_NODE):
            if i < len(right_rps):
                right_node.record_pointers[i] = right_rps[i]
            else:
                right_node.record_pointers[i] = 0
        right_children = current_node.children_pointers[mid_index + 1:]
        right_node.children_pointers = right_children + [0] * (RECORDS_PER_NODE + 1 - len(right_children))
        # right_node.is_leaf = all(ptr == 0 for ptr in right_node.children_pointers)
        with open(BTREE_FILE, 'ab') as f:
            f.seek(0, 2)
            right_node_offset = f.tell()
            f.write(right_node.pack())
            f.flush()

        # print(self._count_occupied_rps(parent_node))

        # insert mid_value into parent node
        if self._count_occupied_rps(parent_node) >= RECORDS_PER_NODE:
            # parent overflow
            return self.handle_overflow(path[:-1], mid_value, parent_offset, parent_node, right_node_offset)
            # print(f"parent offset {parent_offset}, children pointers: {parent_node.children_pointers}")
        else:
            parent_occupied = self._count_occupied_rps(parent_node)
            for i in range(parent_occupied, child_index, -1):
                parent_node.record_pointers[i] = parent_node.record_pointers[i - 1]
            parent_node.record_pointers[child_index] = mid_value

            # adjust child pointers
            for i in range(parent_occupied + 1, child_index + 1, -1):
                parent_node.children_pointers[i] = parent_node.children_pointers[i - 1]
            parent_node.children_pointers[child_index + 1] = new_child_offset if new_child_offset is not None else right_node_offset

            self.write_page(parent_node, parent_offset)

        return {"status": "node_split_done"}

    def split_root(self, new_record_offset, leaf_offset=None, current_node=None, new_child_offset=None):
        print("split_root called")
        # print(f"asd {new_record_offset}")
        # self.print_tree()

        if current_node is None:
            current_node = self.read_page(self.root_offset)
            leaf_offset = self.root_offset

        org_children = list(current_node.children_pointers)
        print(new_child_offset, leaf_offset)

        temp = [rp for rp in current_node.record_pointers if rp != 0]
        temp.append(new_record_offset)
        temp.sort(key=lambda rec_ptr: self.read_record(rec_ptr).key)

        total = len(temp)
        mid_index = total // 2
        mid_value = temp[mid_index]
        left_rps = temp[:mid_index]
        right_rps = temp[mid_index + 1:]

        temp_new_children = list(current_node.children_pointers)
        if new_child_offset is not None:
            try:
                index = temp.index(new_record_offset)
                insert_pos = index + 1
                temp_new_children.insert(insert_pos, new_child_offset)
            except ValueError:
                pass

        # save left node (current root) to current node
        left_node = BTreeNode()
        for i in range(RECORDS_PER_NODE):
            if i < len(left_rps):
                left_node.record_pointers[i] = left_rps[i]
            else:
                left_node.record_pointers[i] = 0

        left_children = temp_new_children[:mid_index + 1]
        left_node.children_pointers = left_children + [0] * (RECORDS_PER_NODE + 1 - len(left_children))
        with open(BTREE_FILE, 'ab') as f:
            f.seek(0, 2)
            left_offset = f.tell()
            f.write(left_node.pack())
            f.flush()

        # create and save right node to the end of btree file
        right_node = BTreeNode()
        for i in range(RECORDS_PER_NODE):
            if i < len(right_rps):
                right_node.record_pointers[i] = right_rps[i]
            else:
                right_node.record_pointers[i] = 0
        right_children = temp_new_children[mid_index + 1:]
        right_node.children_pointers = right_children + [0] * (RECORDS_PER_NODE + 1 - len(right_children))
        with open(BTREE_FILE, 'ab') as f:
            f.seek(0, 2)
            right_offset = f.tell()
            f.write(right_node.pack())
            f.flush()

        # create new root, add mid_value and 2 children to it to offset 0
        new_root = BTreeNode()
        new_root.is_leaf = False
        new_root.record_pointers[0] = mid_value
        new_root.children_pointers[0] = left_offset
        new_root.children_pointers[1] = right_offset
        with open(BTREE_FILE, 'r+b') as f:
            f.seek(self.root_offset)
            f.write(new_root.pack())
            f.flush()
        self.height = max(1, self.height)+1

        return {"status": "root_split_done"}

    def handle_overflow(self, path, new_record_offset, leaf_offset=None, current_node=None, new_child_offset=None):
        print("handle_overflow called")

        print(f"sdasd {new_child_offset}")

        if len(path) < 2:
            print("splitting root")
            return self.split_root(new_record_offset, leaf_offset, current_node, new_child_offset)

        parent_node, parent_offset = path[-2]
        try:
            child_index = parent_node.children_pointers.index(leaf_offset)
        except ValueError:
            print("splitting a root 2")
            return self.split_root(new_record_offset, leaf_offset, current_node, new_child_offset)

        # now we check if compensation is possible:
        # 1. start with left sibling
        # count the amount of occupied rps in sibling node
        left_sibling = None
        left_sibling_offset = None
        if child_index > 0:
            left_sibling_offset = parent_node.children_pointers[child_index - 1]
            left_sibling = self.read_page(left_sibling_offset)
        left_occupied = self._count_occupied_rps(left_sibling) if left_sibling else 0
        if left_sibling_offset == 0:
            left_sibling = None

        # 2. check if compensation is possible for the left sibling first
        if left_sibling and RECORDS_PER_NODE > left_occupied > 0:
            print("compensating with left sibling")
            # generate a temp long array of node, left sibling, record and the parent record between them
            # left + current + new + parent
            temp = []
            for rp in left_sibling.record_pointers:
                if rp != 0:
                    temp.append(rp)
            for rp in current_node.record_pointers:
                if rp != 0:
                    temp.append(rp)
            temp.append(new_record_offset)
            temp.append(parent_node.record_pointers[child_index - 1])
            # sort the temp array by keys
            temp.sort(key=lambda rec_ptr: self.read_record(rec_ptr).key)

            # middle record goes to parent
            total = len(temp)
            mid_index = total // 2
            # left sibling
            for i in range(RECORDS_PER_NODE):
                if i < mid_index:
                    left_sibling.record_pointers[i] = temp[i]
                else:
                    left_sibling.record_pointers[i] = 0
            self.write_page(left_sibling, left_sibling_offset)
            # parent
            parent_node.record_pointers[child_index - 1] = temp[mid_index]
            self.write_page(parent_node, parent_offset)
            # current node
            for i in range(RECORDS_PER_NODE):
                if i < total - mid_index - 1:
                    current_node.record_pointers[i] = temp[mid_index + 1 + i]
                else:
                    current_node.record_pointers[i] = 0
            self.write_page(current_node, leaf_offset)
            return {"status": "compensated_left"}

        # 3. if left sibling out try right sibling
        right_sibling = None
        right_sibling_offset = None
        if child_index < RECORDS_PER_NODE:
            right_sibling_offset = parent_node.children_pointers[child_index + 1]
            right_sibling = self.read_page(right_sibling_offset)
        if right_sibling_offset == 0:
            right_sibling = None
        right_occupied = self._count_occupied_rps(right_sibling) if right_sibling else 0
        print("right occupied:", right_occupied)
        print(right_sibling_offset)
        if right_sibling and RECORDS_PER_NODE > right_occupied > 0:
            print("compensating with right sibling")
            temp = []
            for rp in current_node.record_pointers:
                if rp != 0:
                    temp.append(rp)
            for rp in right_sibling.record_pointers:
                if rp != 0:
                    temp.append(rp)
            temp.append(new_record_offset)
            temp.append(parent_node.record_pointers[child_index])
            temp.sort(key=lambda rec_ptr: self.read_record(rec_ptr).key)
            total = len(temp)
            mid_index = total // 2
            # current node
            for i in range(RECORDS_PER_NODE):
                if i < mid_index:
                    current_node.record_pointers[i] = temp[i]
                else:
                    current_node.record_pointers[i] = 0
            self.write_page(current_node, leaf_offset)
            # parent
            parent_node.record_pointers[child_index] = temp[mid_index]
            self.write_page(parent_node, parent_offset)
            # right sibling
            for i in range(RECORDS_PER_NODE):
                if i < total - mid_index - 1:
                    right_sibling.record_pointers[i] = temp[mid_index + 1 + i]
                else:
                    right_sibling.record_pointers[i] = 0
            self.write_page(right_sibling, right_sibling_offset)
            return {"status": "compensated_right"}
        #   4. if compensation is not possible, perform split
        print("splitting node")
        return self.split_node(path, new_record_offset, leaf_offset, current_node, new_child_offset)

    def insert(self, record: Record):
        found, node, node_offset, index = self.search(record.key)
        if found is not None:
            print("Record with the same key already exists.")
            return {"status": "already_exists"}

        new_record_offset = self.append_record(record)
        path = self._collect_path_for_key(record.key)
        leaf_node, leaf_offset = path[-1]

        keys = leaf_node.get_key_list(self.read_record)
        insert_index = 0
        while insert_index < len(keys) and record.key > keys[insert_index][0]:
            insert_index += 1

        print("Inserting record at leaf node offset:", leaf_offset)
        print(path)
        print(insert_index)

        occupied = self._count_occupied_rps(leaf_node)
        if occupied < RECORDS_PER_NODE:
            # Insert into leaf node
            for j in range(occupied, insert_index, -1):
                leaf_node.record_pointers[j] = leaf_node.record_pointers[j - 1]
            leaf_node.record_pointers[insert_index] = new_record_offset
            self.write_page(leaf_node, leaf_offset)
            return {"status": "inserted"}
        print("Overflow detected")
        # OVERFLOW
        self.handle_overflow(path, new_record_offset, leaf_offset, leaf_node)
        return {"status": "overflow_handled"}

    def print_tree(self):
        self._print_subtree(self.root_offset, 0)

    def _print_subtree(self, node_offset: int, level: int):
        node = self.read_page(node_offset)
        keys = node.get_key_list(self.read_record)
        print("    " * level + f"Level {level} | Node Offset: {node_offset} | Keys: {[k[0] for k in keys]}")
        if not node.is_leaf:
            for i in range(len(keys) + 1):
                child_offset = node.children_pointers[i]
                if child_offset != 0:
                    self._print_subtree(child_offset, level + 1)
