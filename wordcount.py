import os
import sys
from pyspark import SparkContext

#os.environ['PYSPARK_PYTHON'] = sys.executable
#os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Common lines
sc = SparkContext("local[*]", "wordcount")
# above line is not required in terminal pyspark
#use the path of text file which contains data to be used
input = sc.textFile("D:\\bdata\\w9\\file1.txt")

# one input row will give multiple output rows
words = input.flatMap(lambda x: x.split(" "))

# one input row will give one output row only
word_counts = words.map(lambda x: (x, 1))

# take two rows , and does aggregation and returns one row
final_count = word_counts.reduceByKey(lambda x, y: x + y)

# action
result = final_count.collect()

for a in result:
    print(a)