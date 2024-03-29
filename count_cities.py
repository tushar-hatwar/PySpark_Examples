"""
Problem Statement:

> Find distinct cities based on their first letter
Delhi
Dehradun
Banglore
Pune
Patna
Pondicherry

Output:
D 2
B 1
P 3
"""

import os
import sys
from pyspark import SparkContext

# Common lines
sc = SparkContext("local[*]", "wordcount")

input = sc.textFile("D:\\bdata\\w9\\file2.txt")

# one input row will give one output row only
word_counts = input.map(lambda x: (x[0], 1))

# take two rows , and does aggregation and returns one row
final_count = word_counts.reduceByKey(lambda x, y: x + y)

# action
result = final_count.collect()

for a in result:
    print(a)

#Databricks Code using parallelize
input = sc.parallelize(["Delhi",
"Dehradun",
"Banglore",
"Pune",
"Patna",
"Pondicherry"])

# one input row will give one output row only
word_counts = input.map(lambda x: (x[0], 1))

# take two rows , and does aggregation and returns one row
final_count = word_counts.reduceByKey(lambda x, y: x + y)

# action
result = final_count.collect()

for a in result:
    print(a)
