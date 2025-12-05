import random
import sys

import IO
from IO import *
from btree import *
from conf import *


def init_files():
    with open(DATA_FILE, 'wb') as f:
        pass  # Initialize an empty data file

    with open(BTREE_FILE, 'wb') as f:
        pass  # Initialize an empty B-Tree file

def main():
    print("xd")
    init_files()
    btree = BTree()
    # print_root(btree)
    # IO.generate_random_records(RECORD_COUNT)

    # test_data = [10, 20, 5, 6, 12, 30, 7, 17, 10, 8, 9, 11, 13]
    # test_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0, 14]
    test_data = [5, 10, 12, 13, 15, 16, 19, 21, 26, 30, 39, 50, 7, 17, 27, 35, 45, 55, 60, 70, 80, 55, 3]
    for key in test_data:
        btree.operation_page_reads = 0
        btree.operation_page_writes = 0
        btree.operation_page_appends = 0
        record = Record(key, round(random.uniform(0.0, 1000.0)/100, 2), round(random.uniform(0.0, 1000.0)/100, 2))
        print(f"Inserting record with key: {key}")
        # input("Press Enter to continue...")
        if btree.height == 0:
            # First insertion
            btree.create_root(record)
        btree.insert(record)
        btree.print_tree()
        print(f"Page Reads: {btree.operation_page_reads}")
        print(f"Page Writes: {btree.operation_page_writes}")
        print(f"Page Appends: {btree.operation_page_appends}")


if __name__ == "__main__":
    main()
