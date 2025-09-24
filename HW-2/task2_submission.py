import sys
import time
import argparse
import json
from itertools import combinations
from math import ceil
import os
import pyspark
from pyspark import SparkContext

def validateCandidates(iterator, flat_candidates):
    """
    Count occurrences of candidate itemsets in the data
    """
    baskets = list(iterator)
    local_counts = {}
    for basket in baskets:
        basket_set = set(str(item) for item in basket)
        for candidate in flat_candidates:
            if set(candidate).issubset(basket_set):
                local_counts[candidate] = local_counts.get(candidate, 0) + 1
    return local_counts.items()

def getCandidatesForNextSize(freq, nextSize, hashTable, hashSize):
    """
    Generate candidate itemsets for the next iteration:
    1. Take frequent itemsets from the current iteration
    2. Join pairs of frequent itemsets to create larger candidates
    3. Apply the a-priori property to prune invalid candidates
    4. Return valid candidate itemsets of the specified size
    """
    freq = sorted(freq)
    new_candidates = []
    for i in range(len(freq)):
        for j in range(i + 1, len(freq)):
            l1, l2 = list(freq[i]), list(freq[j])
            if l1[:-1] == l2[:-1]:
                union = tuple(sorted(set(l1).union(set(l2))))
                if len(union) == nextSize:
                    subsets = list(combinations(union, nextSize - 1))
                    if all(subset in freq for subset in subsets):
                        new_candidates.append(union)
    return new_candidates

def pcy(iterator, totalCount, threshold, hashSize):
    """
    Implement the PCY (Park-Chen-Yu) algorithm:
    1. Process baskets to calculate partition-specific threshold
    2. For singleton pass, hash item pairs into a hash table
    3. Use hash table to identify potentially frequent pairs
    4. Generate candidates with a-priori pruning and hash-based filtering
    5. Validate candidates against data in each iteration
    6. Continue until no new frequent itemsets are found
    7. Return all frequent itemsets discovered
    """
    baskets = list(iterator)
    local_threshold = ceil(threshold * len(baskets) / totalCount)

    singleton_count = {}
    hash_table = [0] * hashSize

    for basket in baskets:
        for item in basket:
            singleton_count[item] = singleton_count.get(item, 0) + 1
        for pair in combinations(sorted(basket), 2):
            index = hash(pair) % hashSize
            hash_table[index] += 1

    freq_singletons = sorted([item for item, count in singleton_count.items() if count >= local_threshold])
    yield from [(item,) for item in freq_singletons]

    size = 2
    current_freq = [(item,) for item in freq_singletons]

    while current_freq:
        candidates = []
        candidate_count = {}
        bitmap = set(i for i in range(hashSize) if hash_table[i] >= local_threshold)
        if size == 2:
            for basket in baskets:
                items = sorted(set(basket).intersection(freq_singletons))
                for pair in combinations(items, 2):
                    index = hash(pair) % hashSize
                    if index in bitmap:
                        candidate_count[pair] = candidate_count.get(pair, 0) + 1
            current_freq = [pair for pair, count in candidate_count.items() if count >= local_threshold]
            yield from current_freq
        else:
            candidates = getCandidatesForNextSize(current_freq, size, hash_table, hashSize)
            for basket in baskets:
                basket_set = set(basket)
                for candidate in candidates:
                    candidate_key = tuple(candidate)
                    if set(candidate_key).issubset(basket_set):
                        candidate_count[candidate_key] = candidate_count.get(candidate_key, 0) + 1
            current_freq = [cand for cand, count in candidate_count.items() if count >= local_threshold]
            yield from current_freq
        size += 1

def main(rdd, filter_threshold, support_threshold, outputJson, hashSize):
    """
    Main function controlling the workflow and implementing the SON algo:
    1. Preprocess data (remove header, etc.)
    2. Build Case 1 market-basket model (user -> businesses)
    3. Filter users who reviewed more than filter_threshold businesses
    4. Apply the SON algorithm with PCY
    5. Output results
    """
    out = {}

    header = rdd.first()
    data = rdd.filter(lambda x: x != header) \
              .map(lambda x: x.split(',')) \
              .map(lambda x: (x[0], x[1])) \
              .groupByKey() \
              .mapValues(set) \
              .filter(lambda x: len(x[1]) > filter_threshold) \
              .map(lambda x: sorted(set(map(str, x[1])))) \
              .persist()

    totalCount = data.count()

    candidates = data.mapPartitions(lambda it: pcy(it, totalCount, support_threshold, hashSize)) \
                     .distinct() \
                     .sortBy(lambda x: (len(x), x)) \
                     .collect()
    print(f"Number of candidate itemsets: {len(candidates)}")

    def group_output(lsts):
        grouped = {}
        for x in lsts:
            grouped.setdefault(len(x), []).append(x)
        return [sorted(grouped[k]) for k in sorted(grouped)]

    out["Candidates"] = group_output([tuple(map(str, c)) for c in candidates])

    # Phase 2: Frequent itemset validation
    flat_candidates = list(candidates)
    raw_freq_itemsets = data.mapPartitions(lambda it: validateCandidates(it, flat_candidates)).collect()
    print('check1: ', len(raw_freq_itemsets))

    raw_freq_itemsets = data.mapPartitions(lambda it: validateCandidates(it, flat_candidates)) \
                            .reduceByKey(lambda x, y: x + y) \
                            .filter(lambda x: x[1] >= support_threshold) \
                            .collect()
    print(f"Number of frequent itemsets: {len(raw_freq_itemsets)}")

    formatted_freq_itemsets = [{"itemset": list(map(str, k)), "support": v} for k, v in raw_freq_itemsets]
    grouped_freq_itemsets = {}
    for entry in formatted_freq_itemsets:
        k = len(entry["itemset"])
        grouped_freq_itemsets.setdefault(k, []).append(entry)

    out["Frequent Itemsets"] = [sorted(grouped_freq_itemsets[k], key=lambda x: x["itemset"]) for k in sorted(grouped_freq_itemsets)]

    time1 = time.time()
    out['Runtime'] = time1 - time0

    output_dir = os.path.dirname(outputJson)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(outputJson, 'w') as f:
        json.dump(out, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Task 2: SON algorithm on Yelp data')
    parser.add_argument('--f', type=int, help='Filter threshold for users')
    parser.add_argument('--t', type=int, help='Support threshold for frequent itemsets')
    parser.add_argument('--input_file', type=str, help='Input file path')
    parser.add_argument('--output_file', type=str, help='Output file path')

    args = parser.parse_args()

    filter_threshold = args.f
    support_threshold = args.t
    input_file = args.input_file
    output_file = args.output_file

    hashSize = 30000000
    time0 = time.time()

    sc_conf = pyspark.SparkConf() \
                .setAppName('task2') \
                .setMaster('local[*]') \
                .set('spark.driver.memory', '12g') \
                .set('spark.executor.memory', '8g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    rdd = sc.textFile(input_file)
    main(rdd, filter_threshold, support_threshold, output_file, hashSize)
# Stop the spark context
    sc.stop()

