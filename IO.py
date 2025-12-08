from btree import *
import random


def generate_data():
    print("xd")


def read_data_from_keyboard():
    print("xd")


def read_data_from_file(file_path):
    print(file_path)


def generate_random_records(num_records):
    with open(DATA_FILE, 'wb') as f:
        for _ in range(num_records):
            key = random.randint(0, 1000)
            x = round(random.uniform(0.0, 1000.0) / 100, 2)
            y = round(random.uniform(0.0, 1000.0) / 100, 2)
            f.write(RECORD_STRUCT.pack(key, x, y))


def handle_keyboard_operations(btree):
    while True:
        print("Select operation:")
        print("1. Insert record")
        print("2. Search record")
        print("3. Delete record")
        print("4. Exit")
        choice = input("Enter your choice (1/2/3/4): ")
        if choice == '1':
            key = int(input("Enter key (integer): "))
            x = random.uniform(0.0, 1000.0)
            y = random.uniform(0.0, 1000.0)
            print(f"key: {key}, x: {x}, y: {y}")
            if btree.height == 0:
                # First insertion
                btree.create_root(Record(key, x, y))
            else:
                btree.insert(Record(key, x, y))
            btree.print_operation_stats()
        elif choice == '2':
            key = int(input("Enter key to search for: "))
            result = btree.search(key)
            if not result:
                print("Record not found.")
            else:
                print("need to implement offset to record retrieval")
                print(f"found stuff: {result}")
        elif choice == '3':
            key = int(input("Enter key to delete (integer): "))
            btree.delete(key)
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")


def handle_file_operations(btree, file_path):
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 0:
                continue
            operation = parts[0].lower()
            if operation == 'insert' and len(parts) == 4:
                key = int(parts[1])
                x = float(parts[2])
                y = float(parts[3])
                if btree.height == 0:
                    btree.create_root(Record(key, x, y))
                else:
                    btree.insert(Record(key, x, y))
                btree.print_operation_stats()
            elif operation == 'search' and len(parts) == 2:
                key = int(parts[1])
                result = btree.search(key)
                if not result:
                    print("Record not found.")
                else:
                    print("need to implement offset to record retrieval")
                    print(f"found stuff: {result}")
            elif operation == 'delete' and len(parts) == 2:
                key = int(parts[1])
                btree.delete(key)
                btree.print_operation_stats()
            else:
                print(f"Invalid operation or parameters in line: {line.strip()}")
