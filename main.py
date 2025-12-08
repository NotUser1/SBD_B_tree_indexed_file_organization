import sys
import IO
from btree import *


def main():
    print("xd")
    btree = BTree()

    print("Select mode of operation:")
    print("1. Read operations and data from keyboard")
    print("3. Read sequence of operations from file")
    choice = input("Enter your choice (1/2): ")
    if choice == '1':
        IO.handle_keyboard_operations(btree)
    elif choice == '2':
        IO.handle_file_operations(btree, 'operations2.txt')
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
