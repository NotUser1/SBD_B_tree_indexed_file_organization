from btree import *
import random


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
        print("5. Print B-Tree by key")
        print("6. Update record in data file")
        choice = input("Enter your choice: ")
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
        elif choice == '5':
            btree.print_keys_in_order()
        elif choice == '6':
            key = int(input("Enter key to update (integer): "))
            new_x = float(input("Enter new x value (float): "))
            new_y = float(input("Enter new y value (float): "))
            update_record_in_data_file(key, new_x, new_y)
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
    print("-----------EOF - SWITCHING TO KEYBOARD OPERATIONS-----------")
    handle_keyboard_operations(btree)


def update_record_in_data_file(key, new_x, new_y):
    with open(DATA_FILE, 'r+b') as f:
        while True:
            bytes_read = f.read(RECORD_SIZE * RECORDS_PER_PAGE)
            if not bytes_read:
                print("Record not found for update.")
                return
            page_off = f.tell()
            for i in range(0, len(bytes_read), RECORD_SIZE):
                record_data = bytes_read[i:i + RECORD_SIZE]
                record_key, x, y = RECORD_STRUCT.unpack(record_data)
                if record_key == key:
                    f.seek(page_off - len(bytes_read) + i)
                    f.write(RECORD_STRUCT.pack(key, new_x, new_y))
                    print(f"Record with key {key} updated to x: {new_x}, y: {new_y}")
                    return
