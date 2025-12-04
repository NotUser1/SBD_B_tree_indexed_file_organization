import struct

RECORD_COUNT = 15
RECORDS_PER_NODE = 4
RECORD_STRUCT = struct.Struct('<Idd')  # 1x int (key) + 2x 64b float (x, y)
RECORD_SIZE = RECORD_STRUCT.size

DATA_FILE = 'records.bin'
BTREE_FILE = 'btree.bin'
