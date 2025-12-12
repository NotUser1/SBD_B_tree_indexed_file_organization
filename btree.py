import os.path

from conf import *
from typing import Tuple


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
    #    RECORDS_PER_NODE x 32b record keys
    #    RECORDS_PER_NODE x 32b record pointers
    #    (RECORDS_PER_NODE + 1) x 32b child pointers
    KEYS_STRUCT = struct.Struct('<' + 'I' * RECORDS_PER_NODE)
    RECORD_POINTERS_STRUCT = struct.Struct('<' + 'I' * RECORDS_PER_NODE)
    CHILD_POINTERS_STRUCT = struct.Struct('<' + 'I' * (RECORDS_PER_NODE + 1))
    NODE_SIZE = KEYS_STRUCT.size + RECORD_POINTERS_STRUCT.size + CHILD_POINTERS_STRUCT.size

    def __init__(self):
        self.is_leaf = True
        # keys will be stored in records, but this will allow us to sert the tree without reading the data file
        self.keys = [0] * RECORDS_PER_NODE
        self.children_pointers = [0] * (RECORDS_PER_NODE + 1)  # child pointer will be a file offset in the B-Tree file
        self.record_pointers = [0] * RECORDS_PER_NODE  # record pointer will be a file offset in the records file

    def pack(self) -> bytes:
        keys_data = self.KEYS_STRUCT.pack(*self.keys)
        record_pointers_data = self.RECORD_POINTERS_STRUCT.pack(*self.record_pointers)
        children_pointers_data = self.CHILD_POINTERS_STRUCT.pack(*self.children_pointers)

        return keys_data + record_pointers_data + children_pointers_data

    @classmethod
    def unpack(cls, data: bytes):
        node = cls()

        keys_size = cls.KEYS_STRUCT.size
        record_pointers_size = cls.RECORD_POINTERS_STRUCT.size
        children_pointers_size = cls.CHILD_POINTERS_STRUCT.size

        node.keys = list(cls.KEYS_STRUCT.unpack(data[0:keys_size]))
        node.record_pointers = list(
            cls.RECORD_POINTERS_STRUCT.unpack(data[keys_size:keys_size + record_pointers_size]))
        node.children_pointers = list(
            cls.CHILD_POINTERS_STRUCT.unpack(
                data[keys_size + record_pointers_size:keys_size + record_pointers_size + children_pointers_size]))
        node.is_leaf = all(ptr == 0 for ptr in node.children_pointers)

        return node

    def get_key_list(self):
        # returns list of (key, record_pointer)
        tuples = []
        for i in range(RECORDS_PER_NODE):
            rp = self.record_pointers[i]
            if rp == 0:
                break
            tuples.append((self.keys[i], rp))
        return tuples

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
        self.operation_page_reads = 0
        self.operation_page_writes = 0
        self.operation_page_appends = 0
        self.freed_pages = []
        self.data_page = None
        self.data_page_offset = None
        self.deletion_holes = []

    def read_page(self, file_offset: int) -> BTreeNode:
        self.operation_page_reads += 1
        with open(BTREE_FILE, 'rb') as f:
            f.seek(file_offset)
            data = f.read(BTreeNode.NODE_SIZE)
            if len(data) < BTreeNode.NODE_SIZE:
                raise ValueError("Failed to read a complete B-Tree node from file.")
        return BTreeNode.unpack(data)

    def write_page(self, node: BTreeNode, file_offset: int):
        self.operation_page_writes += 1
        with open(BTREE_FILE, 'r+b') as f:
            f.seek(file_offset)
            f.write(node.pack())
            f.flush()

    def append_page(self, node: BTreeNode) -> int:
        if self.freed_pages:
            reused_offset = self.freed_pages.pop()
            self.write_page(node, reused_offset)
            return reused_offset
        self.operation_page_appends += 1
        with open(BTREE_FILE, 'ab') as f:
            f.seek(0, 2)
            node_pointer = f.tell()
            f.write(node.pack())
            f.flush()
        return node_pointer

    @staticmethod
    def append_record(record: Record) -> int:
        with open(DATA_FILE, 'ab') as f:
            f.seek(0, 2)  # Move to the end of the file
            record_pointer = f.tell()
            f.write(record.pack())
            f.flush()
        return record_pointer

    def create_root(self, record: Record):
        new_record_offset = self.append_record(record)
        self.root.record_pointers[0] = new_record_offset
        root = BTreeNode()
        root.keys[0] = record.key
        root.record_pointers[0] = new_record_offset
        self.root_offset = self.append_page(root)
        self.height = 1

    def search(self, key: int):
        return self.search_node(self.root_offset, key)

    def search_node(self, node_offset: int, key: int):
        if node_offset is None:
            return False, None, 0, 0

        path = []
        offset = node_offset
        while True:
            node = self.read_page(offset)
            path.append((node, offset))
            keys = node.get_key_list()
            n = len(keys)
            i = 0
            while i < n and key > keys[i][0]:
                i += 1
            if i < n and keys[i][0] == key:
                return True, path, node, offset, i
            if node.is_leaf:
                return False, path, node, offset, i
            next_off = node.children_pointers[i]
            if next_off == 0:
                return False, path, node, offset, i
            offset = next_off

    @staticmethod
    def count_occupied_rps(node: BTreeNode) -> int:
        if node is None:
            return 0

        count = 0
        for rp in node.record_pointers:
            if rp == 0:
                break
            count += 1
        return count

    # TODO: divide this into smaller sections and clean up (a lot of repetition inside the code)
    def split_node(self, path, new_record_offset, new_record_key, leaf_offset=None,
                   current_node=None, new_child_offset=None):
        print("split_node called")

        # we need to handle root split here also because split_node is called from handle_overflow recursively ?
        # no need because handle_overflow checks for root split first
        # #sanity_check

        if current_node is None or leaf_offset is None:
            current_node, leaf_offset = path[-1]

        if len(path) < 2:
            # print("splitting root from split_node")
            return self.split_root(new_record_offset, new_record_key, leaf_offset, current_node)

        parent_node, parent_offset = path[-2]

        temp = current_node.get_key_list()
        temp.append((new_record_key, new_record_offset))
        temp.sort(key=lambda kp: kp[0])

        total = len(temp)
        mid_index = total // 2
        mid_key, mid_pointer = temp[mid_index]
        left_rps = temp[:mid_index]
        right_rps = temp[mid_index + 1:]

        child_index = parent_node.children_pointers.index(leaf_offset)
        children_pointers = current_node.children_pointers
        if new_child_offset is not None:
            try:
                # print("Trying to insert new child offset into split children")
                index = next(i for i, entry in enumerate(temp) if entry[1] == new_record_offset)
                insert_pos = index + 1
                children_pointers.insert(insert_pos, new_child_offset)
                new_child_offset = None  # prevent double insertion
            except ValueError:
                pass

        # save left node to current node
        for i in range(RECORDS_PER_NODE):
            if i < len(left_rps):
                current_node.keys[i] = left_rps[i][0]
                current_node.record_pointers[i] = left_rps[i][1]
            else:
                current_node.keys[i] = 0
                current_node.record_pointers[i] = 0
        left_children = children_pointers[:mid_index + 1] + [0] * (RECORDS_PER_NODE + 1 - len(children_pointers[:mid_index + 1]))
        current_node.children_pointers = left_children
        self.write_page(current_node, leaf_offset)

        # create and save right node to the end of btree file
        right_node = BTreeNode()
        for i in range(RECORDS_PER_NODE):
            if i < len(right_rps):
                right_node.keys[i] = right_rps[i][0]
                right_node.record_pointers[i] = right_rps[i][1]
            else:
                right_node.keys[i] = 0
                right_node.record_pointers[i] = 0
        right_node_children = children_pointers[mid_index + 1:] + [0] * (RECORDS_PER_NODE + 1 - len(children_pointers[mid_index + 1:]))
        right_node.children_pointers = right_node_children
        # right_node.is_leaf = all(ptr == 0 for ptr in right_node.children_pointers)
        right_node_offset = self.append_page(right_node)

        # insert mid_value into parent node
        if self.count_occupied_rps(parent_node) >= RECORDS_PER_NODE:
            # print("parent overflow")
            return self.handle_overflow(path[:-1], mid_pointer, mid_key, parent_offset, parent_node, right_node_offset)
        else:
            parent_occupied = self.count_occupied_rps(parent_node)
            for i in range(parent_occupied, child_index, -1):
                parent_node.keys[i] = parent_node.keys[i - 1]
                parent_node.record_pointers[i] = parent_node.record_pointers[i - 1]
            parent_node.keys[child_index] = mid_key
            parent_node.record_pointers[child_index] = mid_pointer

            # adjust child pointers
            for i in range(parent_occupied + 1, child_index + 1, -1):
                parent_node.children_pointers[i] = parent_node.children_pointers[i - 1]
            parent_node.children_pointers[
                child_index + 1] = new_child_offset if new_child_offset is not None else right_node_offset

            self.write_page(parent_node, parent_offset)

        return {"status": "node_split_done"}

    def split_root(self, new_record_offset, new_record_key, leaf_offset=None, current_node=None, new_child_offset=None):
        print("split_root called")

        if current_node is None:
            current_node = self.read_page(self.root_offset)

        # new_key = self.read_record(new_record_offset).key
        temp = current_node.get_key_list()
        temp.append((new_record_key, new_record_offset))
        temp.sort(key=lambda kp: kp[0])

        total = len(temp)
        mid_index = total // 2
        mid_key, mid_value = temp[mid_index]
        left_rps = temp[:mid_index]
        right_rps = temp[mid_index + 1:]

        # manual inserting of this one stupid missing right child pointer after recursive split
        temp_new_children = list(current_node.children_pointers)
        if new_child_offset is not None:
            try:
                # print("Trying to insert new child offset into root split children")
                index = next(i for i, entry in enumerate(temp) if entry[1] == new_record_offset)
                insert_pos = index + 1
                temp_new_children.insert(insert_pos, new_child_offset)
            except ValueError:
                pass

        # save left node (current root) to end of btree file
        left_node = BTreeNode()
        for i in range(RECORDS_PER_NODE):
            if i < len(left_rps):
                left_node.keys[i] = left_rps[i][0]
                left_node.record_pointers[i] = left_rps[i][1]
            else:
                left_node.keys[i] = 0
                left_node.record_pointers[i] = 0
        left_children = temp_new_children[:mid_index + 1]
        left_node.children_pointers = left_children + [0] * (RECORDS_PER_NODE + 1 - len(left_children))
        left_offset = self.append_page(left_node)

        # create and save right node to the end of btree file
        right_node = BTreeNode()
        for i in range(RECORDS_PER_NODE):
            if i < len(right_rps):
                right_node.keys[i] = right_rps[i][0]
                right_node.record_pointers[i] = right_rps[i][1]
            else:
                right_node.keys[i] = 0
                right_node.record_pointers[i] = 0
        right_children = temp_new_children[mid_index + 1:]
        right_node.children_pointers = right_children + [0] * (RECORDS_PER_NODE + 1 - len(right_children))
        right_offset = self.append_page(right_node)

        # create new root, add mid_value and 2 children to it to offset 0
        new_root = BTreeNode()
        new_root.is_leaf = False
        new_root.keys[0] = mid_key
        new_root.record_pointers[0] = mid_value
        new_root.children_pointers[0] = left_offset
        new_root.children_pointers[1] = right_offset
        with open(BTREE_FILE, 'r+b') as f:
            f.seek(self.root_offset)
            f.write(new_root.pack())
            f.flush()
        self.height = max(1, self.height) + 1

        return {"status": "root_split_done"}

    # TODO: divide this into smaller sections and clean up (a lot of repetition inside the code)
    def handle_compensation(self, new_record_offset, new_record_key, leaf_offset=None, current_node=None,
                            child_index=None, parent_node=None, parent_offset=None, new_child_offset=None):
        print("handle_compensation called")

        # now we check if compensation is possible:
        # 1. start with left sibling
        # count the amount of occupied rps in sibling node
        left_sibling = None
        left_sibling_offset = None
        if child_index > 0:
            left_sibling_offset = parent_node.children_pointers[child_index - 1]
            left_sibling = self.read_page(left_sibling_offset)
        left_occupied = self.count_occupied_rps(left_sibling) if left_sibling else 0
        if left_sibling_offset == 0:
            left_sibling = None

        # 2. check if compensation is possible for the left sibling first
        if left_sibling and RECORDS_PER_NODE > left_occupied > 0:
            print("compensating with left sibling")
            # generate a temp long array of node, left sibling, record and the parent record between them
            temp = left_sibling.get_key_list()
            temp += current_node.get_key_list()
            temp.append((new_record_key, new_record_offset))
            temp.append((parent_node.keys[child_index - 1], parent_node.record_pointers[child_index - 1]))
            temp.sort(key=lambda kp: kp[0])

            if new_child_offset is not None and current_node.children_pointers[0] != 0:
                record_inserted_at_index = next(i for i, entry in enumerate(temp) if entry[1] == new_record_offset)
                idx = record_inserted_at_index + 1
                combined_children = left_sibling.children_pointers + current_node.children_pointers
                while 0 in combined_children:
                    combined_children.remove(0)
                combined_children.insert(idx, new_child_offset)
            else:
                combined_children = left_sibling.children_pointers + current_node.children_pointers

            # combined_children = left_sibling.children_pointers + current_node.children_pointers
            while 0 in combined_children:
                combined_children.remove(0)
            print(combined_children)

            total = len(temp)
            mid_index = total // 2
            # left sibling
            for i in range(RECORDS_PER_NODE):
                if i < mid_index:
                    left_sibling.keys[i] = temp[i][0]
                    left_sibling.record_pointers[i] = temp[i][1]
                else:
                    left_sibling.keys[i] = 0
                    left_sibling.record_pointers[i] = 0

            # children pointers adjustment
            left_sibling.children_pointers = combined_children[:mid_index + 1] + [0] * (RECORDS_PER_NODE + 1 - len(combined_children[:mid_index + 1]))
            self.write_page(left_sibling, left_sibling_offset)

            # parent gets middle record
            parent_node.keys[child_index - 1] = temp[mid_index][0]
            parent_node.record_pointers[child_index - 1] = temp[mid_index][1]
            self.write_page(parent_node, parent_offset)

            # current node gets the rest
            for i in range(RECORDS_PER_NODE):
                if i < total - mid_index - 1:
                    current_node.keys[i] = temp[mid_index + 1 + i][0]
                    current_node.record_pointers[i] = temp[mid_index + 1 + i][1]
                else:
                    current_node.keys[i] = 0
                    current_node.record_pointers[i] = 0

            current_node.children_pointers = combined_children[mid_index + 1:] + [0] * (RECORDS_PER_NODE + 1 - len(combined_children[mid_index + 1:]))
            print(current_node.children_pointers)
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
        right_occupied = self.count_occupied_rps(right_sibling) if right_sibling else 0

        if right_sibling and RECORDS_PER_NODE > right_occupied > 0:
            print("compensating with right sibling")
            temp = current_node.get_key_list()
            temp += right_sibling.get_key_list()
            temp.append((new_record_key, new_record_offset))
            temp.append((parent_node.keys[child_index], parent_node.record_pointers[child_index]))
            temp.sort(key=lambda kp: kp[0])

            if new_child_offset is not None and current_node.children_pointers[0] != 0:
                record_inserted_at_index = next(i for i, entry in enumerate(temp) if entry[1] == new_record_offset)
                idx = record_inserted_at_index + 1
                combined_children = current_node.children_pointers + right_sibling.children_pointers
                while 0 in combined_children:
                    combined_children.remove(0)
                combined_children.insert(idx, new_child_offset)
            else:
                combined_children = current_node.children_pointers + right_sibling.children_pointers

            total = len(temp)
            mid_index = total // 2
            # current node
            for i in range(RECORDS_PER_NODE):
                if i < mid_index:
                    current_node.keys[i] = temp[i][0]
                    current_node.record_pointers[i] = temp[i][1]
                else:
                    current_node.keys[i] = 0
                    current_node.record_pointers[i] = 0

            current_node.children_pointers = combined_children[:mid_index + 1] + [0] * (RECORDS_PER_NODE + 1 - len(combined_children[:mid_index + 1]))
            self.write_page(current_node, leaf_offset)

            # parent gets middle record
            parent_node.keys[child_index] = temp[mid_index][0]
            parent_node.record_pointers[child_index] = temp[mid_index][1]
            self.write_page(parent_node, parent_offset)

            # right sibling gets the rest
            for i in range(RECORDS_PER_NODE):
                if i < total - mid_index - 1:
                    right_sibling.keys[i] = temp[mid_index + 1 + i][0]
                    right_sibling.record_pointers[i] = temp[mid_index + 1 + i][1]
                else:
                    right_sibling.keys[i] = 0
                    right_sibling.record_pointers[i] = 0
            right_sibling.children_pointers = combined_children[mid_index + 1:] + [0] * (RECORDS_PER_NODE + 1 - len(combined_children[mid_index + 1:]))
            self.write_page(right_sibling, right_sibling_offset)
            return {"status": "compensated_right"}

    def handle_overflow(self, path, new_record_offset, new_record_key, leaf_offset=None, current_node=None,
                        new_child_offset=None):
        print("handle_overflow called")

        if len(path) < 2:
            # print("splitting root")
            return self.split_root(new_record_offset, new_record_key, leaf_offset, current_node, new_child_offset)

        parent_node, parent_offset = path[-2]
        try:
            child_index = parent_node.children_pointers.index(leaf_offset)
        except ValueError:
            # print("splitting a root 2")
            return self.split_root(new_record_offset, new_record_key, leaf_offset, current_node, new_child_offset)

        #   1-3. handle compensation if possible
        compensation_result = self.handle_compensation(new_record_offset, new_record_key, leaf_offset, current_node,
                                                       child_index, parent_node, parent_offset, new_child_offset)
        if compensation_result is not None:
            # print("compensation done")
            return compensation_result

        #   4. if compensation is not possible, perform split
        # print("splitting node")
        return self.split_node(path, new_record_offset, new_record_key, leaf_offset, current_node, new_child_offset)

    def insert(self, record: Record):
        print("----------------NEW INSERT OPERATION----------------")
        print(f"Inserting record with key: {record.key}")

        found, path, node, node_offset, index = self.search(record.key)

        if found:
            print("Record with the same key already exists.")
            return {"status": "already_exists"}

        new_record_offset = self.append_record(record)
        new_record_key = record.key

        if not path:
            print("Error: Path is empty during insertion.")
            return {"status": "error_no_path"}

        leaf_node, leaf_offset = path[-1]

        keys = leaf_node.get_key_list()
        insert_index = 0
        while insert_index < len(keys) and record.key > keys[insert_index][0]:
            insert_index += 1

        occupied = self.count_occupied_rps(leaf_node)
        if occupied < RECORDS_PER_NODE:
            # Insert into leaf node
            for j in range(occupied, insert_index, -1):
                leaf_node.keys[j] = leaf_node.keys[j - 1]
                leaf_node.record_pointers[j] = leaf_node.record_pointers[j - 1]
            leaf_node.keys[insert_index] = record.key
            leaf_node.record_pointers[insert_index] = new_record_offset
            self.write_page(leaf_node, leaf_offset)
            return {"status": "inserted"}
        # print("Overflow detected")
        # OVERFLOW
        self.handle_overflow(path, new_record_offset, new_record_key, leaf_offset, leaf_node)
        return {"status": "overflow_handled"}

    def find_predecessor(self, path, node, index) -> Tuple[int, int, BTreeNode, int, int, list]:
        pred_path = list(path)
        offset = node.children_pointers[index]
        while True:
            n = self.read_page(offset)
            pred_path.append((n, offset))
            keys = n.get_key_list()
            if n.is_leaf:
                if len(keys) == 0:
                    raise ValueError("No predecessor found in leaf node.")
                pred_index = len(keys) - 1
                return keys[pred_index][0], keys[pred_index][1], n, offset, pred_index, pred_path
            next_off = n.children_pointers[len(keys)]
            if next_off == 0:
                for i in range(len(node.children_pointers) - 1, -1, -1):
                    if n.children_pointers[i] != 0:
                        next_off = node.children_pointers[i]
                        break
            offset = next_off

    def delete_from_leaf(self, leaf_node: BTreeNode, leaf_offset: int, delete_index: int):
        # usuń z liścia i przesuń wszystko w lewo
        # print("delete_from_leaf called")
        occupied = self.count_occupied_rps(leaf_node)
        for i in range(delete_index, occupied - 1):
            leaf_node.keys[i] = leaf_node.keys[i + 1]
            leaf_node.record_pointers[i] = leaf_node.record_pointers[i + 1]
        leaf_node.keys[occupied - 1] = 0
        leaf_node.record_pointers[occupied - 1] = 0
        self.write_page(leaf_node, leaf_offset)

    def compensation_on_delete(self, path, leaf_node, leaf_offset, delete_index) -> bool:
        print("compensation_on_delete called")
        if len(path) < 2:
            # cannot compensate on root (chyba)
            return False

        parent_node, parent_offset = path[-2]
        try:
            child_index = parent_node.children_pointers.index(leaf_offset)
        except ValueError:
            return False

        # 1. try left sibling
        if child_index > 0:
            left_sibling_offset = parent_node.children_pointers[child_index - 1]
            if left_sibling_offset != 0:
                left_sibling = self.read_page(left_sibling_offset)
                left_occupied = self.count_occupied_rps(left_sibling)
                if left_occupied > (RECORDS_PER_NODE + 1) // 2:
                    print("compensating delete with left sibling")
                    # same as with normal compensation
                    # create temp array
                    # middle goes to parent
                    # the rest gets split between left sibling and current node
                    temp = left_sibling.get_key_list()
                    temp += leaf_node.get_key_list()
                    temp.append((parent_node.keys[child_index - 1], parent_node.record_pointers[child_index - 1]))
                    temp.sort(key=lambda kp: kp[0])

                    for i in range(RECORDS_PER_NODE):
                        if i < len(temp) // 2:
                            left_sibling.keys[i] = temp[i][0]
                            left_sibling.record_pointers[i] = temp[i][1]
                        else:
                            left_sibling.keys[i] = 0
                            left_sibling.record_pointers[i] = 0
                    self.write_page(left_sibling, left_sibling_offset)

                    mid_index = len(temp) // 2
                    parent_node.keys[child_index - 1] = temp[mid_index][0]
                    parent_node.record_pointers[child_index - 1] = temp[mid_index][1]
                    self.write_page(parent_node, parent_offset)

                    for i in range(RECORDS_PER_NODE):
                        if i < len(temp) - 1 - mid_index:
                            leaf_node.keys[i] = temp[mid_index + 1 + i][0]
                            leaf_node.record_pointers[i] = temp[mid_index + 1 + i][1]
                        else:
                            leaf_node.keys[i] = 0
                            leaf_node.record_pointers[i] = 0
                    self.write_page(leaf_node, leaf_offset)
                    return True
        # 2. try right sibling
        if child_index < RECORDS_PER_NODE:
            right_sibling_offset = parent_node.children_pointers[child_index + 1]
            if right_sibling_offset != 0:
                right_sibling = self.read_page(right_sibling_offset)
                right_occupied = self.count_occupied_rps(right_sibling)
                if right_occupied > (RECORDS_PER_NODE + 1) // 2:
                    # print("compensating delete with right sibling")
                    # same as with normal compensation
                    temp = leaf_node.get_key_list()
                    temp += right_sibling.get_key_list()
                    temp.append((parent_node.keys[child_index], parent_node.record_pointers[child_index]))
                    temp.sort(key=lambda kp: kp[0])

                    for i in range(RECORDS_PER_NODE):
                        if i < len(temp) // 2:
                            leaf_node.keys[i] = temp[i][0]
                            leaf_node.record_pointers[i] = temp[i][1]
                        else:
                            leaf_node.keys[i] = 0
                            leaf_node.record_pointers[i] = 0
                    self.write_page(leaf_node, leaf_offset)

                    mid_index = len(temp) // 2
                    parent_node.keys[child_index] = temp[mid_index][0]
                    parent_node.record_pointers[child_index] = temp[mid_index][1]
                    self.write_page(parent_node, parent_offset)

                    for i in range(RECORDS_PER_NODE):
                        if i < len(temp) - 1 - mid_index:
                            right_sibling.keys[i] = temp[mid_index + 1 + i][0]
                            right_sibling.record_pointers[i] = temp[mid_index + 1 + i][1]
                        else:
                            right_sibling.keys[i] = 0
                            right_sibling.record_pointers[i] = 0
                    self.write_page(right_sibling, right_sibling_offset)
                    return True
        return False

    def merge_nodes(self, path, leaf_node, leaf_offset, child_index):
        print("merge_nodes called")
        if len(path) < 2:
            # cannot merge on root (chyba)
            return {"status": "cannot_merge_root"}

        # merge is -> node + key from parent + right
        # or left + key from parent + node

        parent_node, parent_offset = path[-2]
        parent_occupied = self.count_occupied_rps(parent_node)

        def remove_parent_key(index):
            for idx in range(index, parent_occupied - 1):
                parent_node.keys[idx] = parent_node.keys[idx + 1]
                parent_node.record_pointers[idx] = parent_node.record_pointers[idx + 1]
            parent_node.keys[parent_occupied - 1] = 0
            parent_node.record_pointers[parent_occupied - 1] = 0
            # adjust child pointers
            for idx in range(index + 1, parent_occupied + 1):
                parent_node.children_pointers[idx - 1] = parent_node.children_pointers[idx]
            parent_node.children_pointers[parent_occupied] = 0

        left_sibling_offset = parent_node.children_pointers[child_index - 1] if child_index > 0 else 0
        if left_sibling_offset != 0:
            left_sibling = self.read_page(left_sibling_offset)
            # print("merging with left sibling")
            # merge left + parent key + current
            temp = left_sibling.get_key_list()
            temp.append((parent_node.keys[child_index - 1], parent_node.record_pointers[child_index - 1]))
            temp += leaf_node.get_key_list()
            temp.sort(key=lambda kp: kp[0])

            for i in range(RECORDS_PER_NODE):
                if i < len(temp):
                    left_sibling.keys[i] = temp[i][0]
                    left_sibling.record_pointers[i] = temp[i][1]
                else:
                    left_sibling.keys[i] = 0
                    left_sibling.record_pointers[i] = 0

            lc = [p for p in left_sibling.children_pointers if p != 0]
            rc = [p for p in leaf_node.children_pointers if p != 0]
            new_children = lc + rc
            left_sibling.children_pointers = new_children + [0] * (RECORDS_PER_NODE + 1 - len(new_children))
            left_sibling.is_leaf = all(ptr == 0 for ptr in left_sibling.children_pointers)
            self.write_page(left_sibling, left_sibling_offset)

            remove_parent_key(child_index - 1)
            parent_node.children_pointers[child_index - 1] = left_sibling_offset
            self.write_page(parent_node, parent_offset)

            if self.count_occupied_rps(parent_node) == 0 and parent_offset == self.root_offset:
                # print("merged root down to left sibling and set off to 0")
                self.write_page(left_sibling, 0)
                if left_sibling_offset != self.root_offset:
                    self.freed_pages.append(left_sibling_offset)
                if leaf_offset != self.root_offset:
                    self.freed_pages.append(leaf_offset)
                if parent_offset != self.root_offset:
                    self.freed_pages.append(parent_offset)
                self.root_offset = 0
                self.root = left_sibling
                self.height = max(1, self.height - 1)

            return {"status": "merged_left"}

        right_sibling_offset = parent_node.children_pointers[child_index + 1] if child_index < RECORDS_PER_NODE else 0
        if right_sibling_offset != 0:
            right_sibling = self.read_page(right_sibling_offset)
            # print("merging with right sibling")
            # merge current + parent key + right
            temp = leaf_node.get_key_list()
            temp.append((parent_node.keys[child_index], parent_node.record_pointers[child_index]))
            temp += right_sibling.get_key_list()
            temp.sort(key=lambda kp: kp[0])
            print(temp)

            for i in range(RECORDS_PER_NODE):
                if i < len(temp):
                    leaf_node.keys[i] = temp[i][0]
                    leaf_node.record_pointers[i] = temp[i][1]
                else:
                    leaf_node.keys[i] = 0
                    leaf_node.record_pointers[i] = 0

            lc = [p for p in leaf_node.children_pointers if p != 0]
            rc = [p for p in right_sibling.children_pointers if p != 0]
            new_children = lc + rc
            leaf_node.children_pointers = new_children + [0] * (RECORDS_PER_NODE + 1 - len(new_children))
            leaf_node.is_leaf = all(ptr == 0 for ptr in leaf_node.children_pointers)
            print(leaf_offset)
            self.write_page(leaf_node, leaf_offset)

            remove_parent_key(child_index)
            parent_node.children_pointers[child_index] = leaf_offset
            self.write_page(parent_node, parent_offset)

            if self.count_occupied_rps(parent_node) == 0 and parent_offset == self.root_offset:
                # print("merged root down to right sibling")
                self.write_page(leaf_node, 0)
                if right_sibling_offset != self.root_offset:
                    self.freed_pages.append(right_sibling_offset)
                if leaf_offset != self.root_offset:
                    self.freed_pages.append(leaf_offset)
                if parent_offset != self.root_offset:
                    self.freed_pages.append(parent_offset)
                self.root_offset = 0
                self.root = leaf_node
                self.height = max(1, self.height - 1)

            return {"status": "merged_right"}

    def handle_underflow(self, path, leaf_node, leaf_offset):
        print("handle_underflow called")
        if len(path) < 2:
            return {"status": "cannot_handle_underflow_root"}

        parent_node, parent_offset = path[-2]
        try:
            child_index = parent_node.children_pointers.index(leaf_offset)
        except ValueError:
            return {"status": "child_not_found"}

        # 1. try compensation
        if self.compensation_on_delete(path, leaf_node, leaf_offset, 0):
            return {"status": "compensated"}

        # 2. if compensation not possible, merge nodes
        merge_res = self.merge_nodes(path, leaf_node, leaf_offset, child_index)
        print(merge_res)

        parent_node = self.read_page(parent_offset)
        parent_occupied = self.count_occupied_rps(parent_node)

        # if parent is root and after merge is empty, make the merged node the new root (already did in merge_nodes)
        if parent_offset == self.root_offset:
            return {"status": "merged_into_root"}

        # handle parent underflow recursively
        if parent_occupied < (RECORDS_PER_NODE + 1) // 2:
            return self.handle_underflow(path[:-1], parent_node, parent_offset)

        return {"status": "merged_no_underflow"}

    def delete(self, record_key: int):
        print("----------------NEW DELETE OPERATION----------------")
        print(f"Deleting record with key: {record_key}")

        found, path, node, node_offset, index = self.search(record_key)

        if not found:
            # print("Record with this key does not exist.")
            return {"status": "not_found"}

        if not node.is_leaf:
            # the biggest form left child
            pred_key, pred_ptr, pred_leaf_node, pred_leaf_offset, pred_index, pred_path = self.find_predecessor(path,
                                                                                                                node,
                                                                                                                index)
            # print("predecessor found:", pred_key)
            deleted_record_offset = node.record_pointers[index]
            node.keys[index] = pred_key
            node.record_pointers[index] = pred_ptr
            self.write_page(node, node_offset)
            # now delete predecessor from leaf
            leaf_node = pred_leaf_node
            leaf_offset = pred_leaf_offset
            delete_index = pred_index
            path = pred_path
        else:
            leaf_node = node
            leaf_offset = node_offset
            delete_index = index
            deleted_record_offset = leaf_node.record_pointers[delete_index]

        # print(deleted_record_offset, delete_index)

        self.delete_from_leaf(leaf_node, leaf_offset, delete_index)

        if deleted_record_offset is not None:
            self.deletion_holes.append(deleted_record_offset)

        # TODO: uncomment when ready
        if len(self.deletion_holes) >= RECORDS_PER_NODE:
            print("Automatic data file compaction triggered.")
            self.reorganize_data_file()

        occupied = self.count_occupied_rps(leaf_node)
        if occupied >= (RECORDS_PER_NODE + 1) // 2 or leaf_offset == self.root_offset:
            # print("no underflow while deleting")
            return {"status": "key_deleted"}

        # UNDERFLOW
        result = self.handle_underflow(path, leaf_node, leaf_offset)
        return {"status": "underflow_handled", "detail": result}

    def print_tree(self):
        self._print_subtree(self.root_offset, 0)

    def _print_subtree(self, node_offset: int, level: int):
        node = self.read_page(node_offset)
        self.operation_page_reads -= 1  # avoid counting reads for printing
        keys = node.get_key_list()
        print("    " * level + f"Level {level} | Node Offset: {node_offset} | Keys: {[k[0] for k in keys]}")
        if not node.is_leaf:
            for i in range(len(keys) + 1):
                child_offset = node.children_pointers[i]
                if child_offset != 0:
                    self._print_subtree(child_offset, level + 1)

    def print_operation_stats(self):
        # return
        self.print_tree()
        print("Operation statistics (for btree file):")
        print(f"tree height: {self.height}")
        print(f"Page reads: {self.operation_page_reads}")
        print(f"Page writes: {self.operation_page_writes}")
        print(f"Page appends: {self.operation_page_appends}")
        print(f"Length of freed pages list: {len(self.freed_pages)}")
        self.operation_page_reads = 0
        self.operation_page_writes = 0
        self.operation_page_appends = 0

    def read_record_from_data_file(self, record_offset: int = None, key: int = None) -> None | Record:
        if record_offset is None and key is None:
            return None

        if key is not None and record_offset is None:
            found, path, node, node_offset, index = self.search(key)
            if not found:
                return None
            record_offset = node.record_pointers[index]

        page_size = RECORD_STRUCT.size * RECORDS_PER_PAGE
        page_start = (record_offset // page_size) * page_size  # this way we may check if we have buffered a right page

        if self.data_page is not None and self.data_page_offset == page_start:
            # print("Using buffered data page")
            rel = record_offset - page_start
            if rel >= 0 and rel + RECORD_STRUCT.size <= len(self.data_page):
                record_data = self.data_page[rel:rel + RECORD_STRUCT.size]
                return Record.unpack(record_data)

        # nothing in the buffer or wrong page, read from file
        with open(DATA_FILE, 'rb') as f:
            f.seek(page_start)
            self.data_page = f.read(page_size)
            self.data_page_offset = page_start
            rel = record_offset - page_start
            if rel >= 0 and rel + RECORD_STRUCT.size <= len(self.data_page):
                record_data = self.data_page[rel:rel + RECORD_STRUCT.size]
                return Record.unpack(record_data)

        # fallback: bezpośredni odczyt (np. gdy strona krótsza niż oczekiwano)
        with open(DATA_FILE, 'rb') as f:
            f.seek(record_offset)
            data = f.read(RECORD_STRUCT.size)
            if len(data) < RECORD_STRUCT.size:
                return None
        return Record.unpack(data)

    def print_keys_in_order(self):
        counter = 0
        print("Printing whole B-Tree by key order:")

        # if self.root_offset in None:
        #     print("B-Tree is empty.")
        #     return

        def traverse(offset):
            nonlocal counter
            if offset == 0:
                pass
            node = self.read_page(offset)
            keys = node.get_key_list()
            if node.is_leaf:
                for k, rp in keys:
                    record = self.read_record_from_data_file(record_offset=rp)
                    print(f"Record: {record}")
                    counter += 1
            else:
                for i in range(len(keys)):
                    child_offset = node.children_pointers[i]
                    traverse(child_offset)
                    k, rp = keys[i]
                    record = self.read_record_from_data_file(record_offset=rp)
                    print(f"Record: {record}")
                    counter += 1
                # traverse last child
                last_child_offset = node.children_pointers[len(keys)]
                traverse(last_child_offset)

        traverse(self.root_offset)
        print(f"Total records printed: {counter}")
        self.data_page = None
        self.data_page_offset = None

    def reorganize_data_file(self):
        if not self.deletion_holes:
            print("No deletion holes to compact.")
            return

        page_size = RECORD_STRUCT.size * RECORDS_PER_PAGE
        data_file_size = os.path.getsize(DATA_FILE)
        holes_sorted = sorted(set(self.deletion_holes), reverse=True)
        print(holes_sorted)

        with open(DATA_FILE, 'r+b') as f:
            while holes_sorted:
                hole = holes_sorted[0]
                if hole >= data_file_size:
                    holes_sorted.pop(0)
                    continue
                last_record_offset = data_file_size - RECORD_STRUCT.size
                if last_record_offset < 0:
                    break

                if last_record_offset == hole:
                    holes_sorted.pop(0)
                    data_file_size -= RECORD_STRUCT.size
                    f.truncate(data_file_size)
                    continue

                # read last record
                f.seek(last_record_offset)
                last_record_data = f.read(RECORD_STRUCT.size)
                if len(last_record_data) < RECORD_STRUCT.size:
                    break
                f.seek(hole)
                f.write(last_record_data)
                f.flush()

                # update B-Tree pointer
                last_record = Record.unpack(last_record_data)
                found, path, node, node_offset, index = self.search(last_record.key)
                if found:
                    node.record_pointers[index] = hole
                    self.write_page(node, node_offset)
                holes_sorted.pop(0)
                data_file_size -= RECORD_STRUCT.size
                f.truncate(data_file_size)

        self.deletion_holes = []
