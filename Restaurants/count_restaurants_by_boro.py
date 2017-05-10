from __future__ import print_function, division
import sys
from csv import reader
from operator import add
from pyspark import SparkContext
from itertools import islice

# Before running the file using spark-submit, please run the following commands:
# module load python/gnu/3.4.4
# export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
# export PYTHONHASHSEED=0
# export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

# The file passed as an argument should be the restaurant inspection dataset
# available from https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j/data
# but filtered using the NYC OpenData interactive filtering tool to only include the following inspection types:
# Pre-permit (Operational) / Initial Inspection
# Pre-permit (Non-operational) / Initial Inspection

# The filtered view is available for download here:
# https://data.cityofnewyork.us/Health/Pre-Permit-Restaurant-Inspections/jzz4-5r78/data

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_restaurants_by_boro.py <filename of restaurant dataset>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()

    # Load the restaurant inspections dataset
    # Skip header
    lines = sc.textFile(sys.argv[1], 1)
    restaurants = lines.mapPartitionsWithIndex(lambda i, iter: islice(iter, 1, None) if i==0 else iter) \
        .mapPartitions(lambda x: reader(x))

    # Map to (CAMIS, BORO) key-value pairs
    # Reduce by key to get rid of any duplicate CAMIS ids
    restaurants = restaurants.map(lambda line: (line[0], line[2])) \
        .reduceByKey(lambda a, b: a)

    # Map to (BORO, 1) key-value pairs
    # Reduce by key to get counts by borough
    # Alphabetize by borough
    restaurants = restaurants.map(lambda x: (x[1], 1)) \
        .reduceByKey(add) \
        .sortByKey()

    # Save output
    restaurants.saveAsTextFile("new_restaurants_by_boro.out")

    sc.stop()