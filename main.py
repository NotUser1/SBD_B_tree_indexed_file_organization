import random
import sys

import IO
from IO import *
from btree import *
from conf import *


def main():
    print("xd")
    IO.create_btree_file()
    IO.generate_random_records(RECORD_COUNT)


if __name__ == "__main__":
    main()
