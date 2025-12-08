import struct

RECORDS_PER_PAGE = 4    # in data file page
RECORDS_PER_NODE = 4    # per B-Tree node
RECORD_STRUCT = struct.Struct('<Idd')  # 1x int (key) + 2x 64b float (x, y)
RECORD_SIZE = RECORD_STRUCT.size

DATA_FILE = 'records.bin'
BTREE_FILE = 'btree.bin'

DATA_PAGE_READS = 0
DATA_PAGE_WRITES = 0
