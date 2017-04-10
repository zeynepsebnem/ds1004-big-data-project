from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import os
from csv import reader

#Count the total felony complaints by time of day from 2006 - 2016
#output: key = tod, value = count


if __name__ == "__main__":
    def filter_date(x):
        try:
            x.split('/')[0]
            return True
        except Exception as e:
            return False

    def filter_felony(x):
        if 'FELONY' in x:
            return True
        else:
            return False

    def tod(x):
        try:
            hour = int(x.split(':')[0])
            minute = int(x.split(':')[1])
            hm = hour*100 + minute

            if ((hm >= 600) and (hm < 1200)):
                s = '6am_to_noon'
            elif ((hm >= 1200) and (hm < 1800)):
                s = 'noon_to_6pm'
            elif ((hm >= 1800) and (hm <= 2400)):
                s = '6pm_to_midnight'
            elif ((hm >= 0) and (hm < 600)):
                s = 'midnight_to_6am'
            else:
                s = '[blank]_'+str(hm)
            return s
        except Exception:
            s = '[blank]'
            return s
       except Exception:
            s = '[blank]'
            return s

    if len(sys.argv) != 2:
        print("Usage: felony_count_by_tod.py <file1> ", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    data= sc.textFile(sys.argv[1], 1)
    rdd = data.mapPartitions(lambda x: reader(x))
    mapping = rdd.map(lambda line: (line[2], line[11])).filter(lambda x: filter_date(x[0])).filter(lambda x: filter_felony(x[1]))
    mapping = mapping.map(lambda x: (tod(x[0]), 1))
    joined = mapping.reduceByKey(lambda x,y: x+y).sortByKey(ascending=True).saveAsTextFile("felony_count_by_tod.out")

