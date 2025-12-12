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
    print("3. Generate x lines of input")
    choice = input("Enter your choice (1/2/3): ")
    if choice == '1':
        IO.handle_keyboard_operations(btree)
    elif choice == '2':
        IO.handle_file_operations(btree, 'operations4.txt')
    elif choice == '3':
        num_lines = int(input("Enter number of lines to generate: "))
        IO.generate_input_file('operations_temp.txt', num_lines)
        IO.handle_file_operations(btree, 'operations_temp.txt')
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
