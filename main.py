import sys
import IO
from btree import *
from conf import *


def initialize_files():
    open(BTREE_FILE, 'wb').close()
    with open(DATA_FILE, 'wb') as f:
        f.write(b'\xff' * 4)


def main():
    initialize_files()
    btree = BTree()

    print("Select mode of operation:")
    print("1. Read operations and data from keyboard")
    print("2. Read sequence of operations from file")
    choice = input("Enter your choice (1/2): ")
    if choice == '1':
        IO.handle_keyboard_operations(btree)
    elif choice == '2':
        IO.handle_file_operations(btree, 'operations1.txt')
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
