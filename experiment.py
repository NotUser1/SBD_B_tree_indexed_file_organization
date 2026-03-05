import os
import random

from btree import BTree, Record, BTreeNode
from conf import BTREE_FILE, DATA_FILE, RECORDS_PER_NODE
import matplotlib.pyplot as plt

TRIALS = 10


def initialize_files():
    open(BTREE_FILE, 'wb').close()
    with open(DATA_FILE, 'wb') as f:
        f.write(b'\xff' * 4)


def run_fill_experiment(max_n=10000):
    initialize_files()
    b = BTree()
    percentages = []
    keys = set()
    xs = []
    for n in range(1, max_n + 1):
        while True:
            key = random.randint(1, 1_000_000)
            if key not in keys:
                keys.add(key)
                break
        r = Record(key, float(1), float(1))
        if b.height == 0:
            b.create_root(r)
        else:
            b.insert(r)

        size = os.path.getsize(BTREE_FILE)
        node_count = size // BTreeNode.NODE_SIZE if size > 0 else 0
        capacity = node_count * RECORDS_PER_NODE
        pct = n / capacity if capacity > 0 else 0.0

        xs.append(n)
        percentages.append(pct)

        if n % 1000 == 0 or n == max_n:
            print(f"Inserted {n} / {max_n} - node_count: {node_count}, capacity: {capacity}, fill: {pct:.3f}")

    return xs, percentages


def key_print_read_count(n):
    keys = set()
    b = BTree()
    values = []
    for n in range(n):
        while True:
            key = random.randint(1, 1_000_000)
            if key not in keys:
                keys.add(key)
                break
        r = Record(key, float(1), float(1))
        if b.height == 0:
            b.create_root(r)
        else:
            b.insert(r)

        if n % 100 == 0:
            b.print_keys_in_order()
            values.append((n, b.operation_data_page_reads + b.operation_page_reads))
            b.operation_data_page_reads = 0
    return values


def plot_and_save(xs, percentages, out_path='fill_percentage.png'):
    plt.figure(figsize=(10, 4))
    plt.plot(xs, percentages, linewidth=0.8)
    plt.xlabel('N (number of records inserted)')
    plt.ylabel('Fill percentage (N / tree_capacity)')
    plt.title('B-Tree fill percentage vs N; d = 2')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)


def run_trial(n, op="insert"):
    initialize_files()
    b = BTree()

    keys = set()
    for k in range(n):
        while True:
            key = random.randint(1, 1000000)
            if key not in keys:
                keys.add(key)
                break
        r = Record(key, float(k), float(k) * 2.0)
        if b.height == 0:
            b.create_root(r)
        else:
            b.insert(r)

    base_reads = b.operation_page_reads
    base_writes = b.operation_page_writes
    base_appends = b.operation_page_appends

    if op == "insert":
        while True:
            key = random.randint(1, 1000000)
            if key not in keys:
                break
        r = Record(key, float(n), float(n) * 2.0)
        b.insert(r)
    elif op == "search":
        search_key = random.choice(list(keys))
        b.search(search_key)
    elif op == "delete":
        del_key = random.choice(list(keys))
        b.delete(del_key)

    reads_delta = b.operation_page_reads - base_reads
    writes_delta = b.operation_page_writes - base_writes
    appends_delta = b.operation_page_appends - base_appends
    tree_height = b.height

    return reads_delta, writes_delta, appends_delta, tree_height


def measure_average(n=50, trials=TRIALS, op="insert"):
    results = []
    for i in range(trials):
        delta = run_trial(n, op)
        results.append(delta)

    if not results:
        print("No trials run.")
        return (0, 0, 0, 0)

    total_reads = sum(r[0] for r in results)
    total_writes = sum(r[1] for r in results)
    total_appends = sum(r[2] for r in results)
    total_heights = sum(r[3] for r in results)

    avg_reads = total_reads / len(results)
    avg_writes = total_writes / len(results)
    avg_appends = total_appends / len(results)
    avg_height = total_heights / len(results)

    print(f"Trials: {len(results)}")
    print(f"Individual (reads, writes, appends, height) per trial: {results}")
    print(
        f"Average for {n}-th insert -> reads: {avg_reads:.2f}, writes: {avg_writes:.2f}, appends: {avg_appends:.2f}, height: {avg_height:.2f}")

    return avg_reads, avg_writes, avg_appends, avg_height


if __name__ == "__main__":
    # xs, pcts = run_fill_experiment(10000)
    # plot_and_save(xs, pcts)

    key_print_read_count(1000)

    # print("INSERTS")
    # measure_average(50, op="insert")
    # measure_average(100, op="insert")
    # measure_average(500, op="insert")
    # measure_average(1000, op="insert")
    # measure_average(5000, op="insert")
    # measure_average(10000, op="insert")

    # print("\nDELETES")
    # measure_average(50, op="delete")
    # measure_average(100, op="delete")
    # measure_average(500, op="delete")
    # measure_average(1000, op="delete")
    # measure_average(5000, op="delete")
    # measure_average(10000, op="delete")

    # print("\nSEARCHES")
    # measure_average(50, op="search")
    # measure_average(100, op="search")
    # measure_average(500, op="search")
    # measure_average(1000, op="search")
    # measure_average(5000, op="search")
    # measure_average(10000, op="search")
