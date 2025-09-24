import sys
import time
import argparse
import json
from itertools import combinations
from math import ceil
import pyspark
from pyspark import SparkContext

def getCandidatesForNextSize(freq, nextSize):
    """
    Generate candidate itemsets for the next iteration:
    1. Take frequent itemsets from the current iteration
    2. Join pairs of frequent itemsets to create larger candidates
    3. Apply the a-priori property to prune invalid candidates
    4. Return valid candidate itemsets of the specified size
    """
    freq = sorted(freq)
    candidate_set = set()
    for i in range(len(freq)):
        for j in range(i + 1, len(freq)):
            l1 = list(freq[i])
            l2 = list(freq[j])
            if l1[:-1] == l2[:-1]:
                union = tuple(sorted(set(l1).union(set(l2))))
                if len(union) == nextSize:
                    subsets = list(combinations(union, nextSize - 1))
                    if all(tuple(sorted(sub)) in freq for sub in subsets):
                        candidate_set.add(union)
    return sorted(candidate_set)

def removeheader(partitionIndex, iterator):
    return iter(list(iterator)[1:] if partitionIndex == 0 else iterator)


def aPriori(iterator, threshold, totalCount):
    """
    Implement the A-Priori algorithm:
    1. Process baskets to calculate partition-specific threshold
    2. Generate singleton itemsets from the data
    3. Identify frequent itemsets by counting and comparing to threshold
    4. Generate candidates for next iteration using frequent itemsets
    5. Validate each new set of candidates against the data
    6. Repeat until no new frequent itemsets are found
    7. Return the complete set of frequent itemsets
    """
    baskets = list(iterator)
    local_thresh = ceil(threshold * len(baskets) / totalCount)
    item_count = {}

    for basket in baskets:
        for item in basket:
            item_count[item] = item_count.get(item, 0) + 1

    freq_items = sorted([tuple([item]) for item, count in item_count.items() if count >= local_thresh])
    all_candidates = freq_items.copy()
    k = 2
    current_freq = freq_items

    while current_freq:
        candidate_count = {}
        candidates = getCandidatesForNextSize(current_freq, k)
        for basket in baskets:
            basket_set = set(basket)
            for cand in candidates:
                if set(cand).issubset(basket_set):
                    candidate_count[cand] = candidate_count.get(cand, 0) + 1
        current_freq = [cand for cand, count in candidate_count.items() if count >= local_thresh]
        all_candidates.extend(current_freq)
        k += 1

    return all_candidates

def validateCandidates(iterator, flat_candidates):
    """
    Count occurrences of candidate itemsets in the data
    """
    baskets = list(iterator)
    local_counts = {}
    for basket in baskets:
        basket_set = set(basket)
        for cand in flat_candidates:
            if set(cand).issubset(basket_set):
                local_counts[cand] = local_counts.get(cand, 0) + 1
    return local_counts.items()

def group_output(lst):
    from collections import defaultdict
    grouped = defaultdict(list)
    for item in lst:
        grouped[len(item)].append(item)
    return [sorted(grouped[k]) for k in sorted(grouped)]

def main(rdd, case, threshold, outputJson, year_filter, hashSize=None):
    """
    Main function controlling the workflow and implementing the SON algo:
    1. Preprocess data (remove header, filter by year, ect)
    2. Convert data into baskets based on case
    3. Apply the SON algorithm with A-Priori
    4. Output results
    """
    out = {}

    rdd = rdd.mapPartitionsWithIndex(removeheader)
    rdd = rdd.map(lambda x: x.strip().split(","))
    rdd = rdd.filter(lambda row: row[0] == str(year_filter))

    if case == 1:
        basket_rdd = rdd.map(lambda x: (x[1], x[2])).groupByKey().map(lambda x: sorted(set(x[1])))
    else:
        basket_rdd = rdd.map(lambda x: (x[2], x[1])).groupByKey().map(lambda x: sorted(set(x[1])))

    basket_rdd = basket_rdd.persist()
    basket_count = basket_rdd.count()

    # Phase 1: Candidate Generation
    candidates = basket_rdd.mapPartitions(lambda it: aPriori(it, threshold, basket_count)) \
                           .distinct() \
                           .sortBy(lambda x: (len(x), x)) \
                           .collect()
    out["Candidates"] = group_output([tuple(map(str, x)) for x in candidates])

    # Phase 2: Count Candidates
    freq_items = basket_rdd.mapPartitions(lambda it: validateCandidates(it, candidates)) \
                           .reduceByKey(lambda x, y: x + y) \
                           .filter(lambda x: x[1] >= threshold) \
                           .map(lambda x: x[0]) \
                           .sortBy(lambda x: (len(x), x)) \
                           .collect()
    out["Frequent Itemsets"] = group_output([tuple(map(str, x)) for x in freq_items])

    time1 = time.time()
    out["Runtime"] = round(time1 - time0, 2)

    with open(outputJson, "w") as f:
        json.dump(out, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HW2 Task 1')
    parser.add_argument('--y', type=int, help='Filter year')
    parser.add_argument('--c', type=int, help='Case number: 1 or 2')
    parser.add_argument('--t', type=int, help='Support threshold')
    parser.add_argument('--input_file', type=str, help='Input file path')
    parser.add_argument('--output_file', type=str, help='Output file path')

    args = parser.parse_args()
    year_filter = args.y
    case = args.c
    threshold = args.t
    inputJson = args.input_file
    outputJson = args.output_file

    time0 = time.time()

    sc = SparkContext()
    sc.setLogLevel("ERROR")
    rdd = sc.textFile(inputJson)

    main(rdd, case, threshold, outputJson, year_filter)

    sc.stop()

