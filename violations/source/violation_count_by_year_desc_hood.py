from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import os
from csv import reader

#Count the total violation complaint descriptions by year and neighborhood in bk
#output: key = year_desc_hood, value = count


if __name__ == "__main__":
    
    def filter_validHood(x):
        try:
	    x[11]
	    return True
	except Exception as e:
	    return False

    if len(sys.argv) != 2:
        print("Usage: violation_count_by_year_desc_hood.py <file1> ", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    data= sc.textFile(sys.argv[1], 1)
    rdd = data.mapPartitions(lambda x: reader(x))
    mapping = rdd.filter(lambda x: filter_validHood(x)).map(lambda line: (line[0], line[2], line[11])).map(lambda x: (str(x[0])+'_'+str(x[1])+'_'+str(x[2]), 1))
    joined = mapping.reduceByKey(lambda x,y: x+y).sortByKey(ascending=True).saveAsTextFile("violation_count_by_year_desc_hood.out")
