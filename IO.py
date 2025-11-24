from conf import *
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
            x = round(random.uniform(0.0, 1000.0)/100, 2)
            y = round(random.uniform(0.0, 1000.0)/100, 2)
            f.write(RECORD_STRUCT.pack(key, x, y))

def create_btree_file():
    with open(BTREE_FILE, 'wb') as f:
        pass  # Initialize an empty B-Tree file


