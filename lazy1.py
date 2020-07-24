from pyspark import SparkContext
from pyspark.sql import SparkSession
import os

os.environ["PYSPARK_PYTHON"] = '/usr/bin/python3'
# create a sample list
my_list = [i for i in range(1, 10000000)]
sc = SparkContext()
sparkSession = SparkSession(sc)
# parallelize the data by creating 3 partitions
rdd_0 = sc.parallelize(my_list, 3)
print(rdd_0)
print(os.linesep)

# add value 4 to each number
print("Adding 4 to each number:")
rdd_1 = rdd_0.map(lambda x: x + 4)
# RDD object
print(rdd_1)
# get the RDD Lineage
print(rdd_1.toDebugString())
print(os.linesep)

# add value 20 each number
print("Adding 24 to each number:")
rdd_2 = rdd_1.map(lambda x: x + 20)
# RDD Object
print(rdd_2)
# get the RDD Lineage
print(rdd_2.toDebugString())
