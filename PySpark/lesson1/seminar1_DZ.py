import pyspark
from pyspark import SparkContext, SparkConf

spark_conf = SparkConf().setMaster("local").setAppName("Longest sub")
sc = SparkContext.getOrCreate(conf=spark_conf)

def my_reducer(a, b):
    """  a or b: tuple = (6, 1, 5) """
    seq = 1
    max_seq = a[2]
    if b[0] >= a[0]:
        seq = b[1] + a[1]
        if seq > max_seq:
            max_seq = b[2] + a[2]
    return b[0], seq, max_seq

    
numbers_list = [9, 11, 7, 8, 4, 14, 15, 2, 5, 16, 21, 30, 30, 19, 5, 17]
number_rdd = sc.parallelize(numbers_list)
# asc v1 reducer function
number_pairs = number_rdd.map(lambda x: (x, 1, 1)).reduce(my_reducer)
print(f"maximum asc sequence lenght {number_pairs[2]}")

# asc v2 lambda
number_pairs1 = number_rdd.map(lambda x: (x, 1, 1)).reduce(lambda a,b: (b[0], b[1]+a[1] if b[0]>=a[0] else 1,  b[2] + a[2] if (b[0]>=a[0]) & ((b[1] + a[1]) > a[2])  else a[2] ))
print(f"maximum asc sequence lenght {number_pairs1[2]}")

# desc v1 lambda
number_pairs2 = number_rdd.map(lambda x: (x, 1, 1)).reduce(lambda a,b: (b[0], b[1]+a[1] if b[0]<=a[0] else 1,  b[2] + a[2] if (b[0]<=a[0]) & ((b[1] + a[1]) > a[2])  else a[2] ))
print(f"maximum desc sequence lenght {number_pairs2[2]}")

sc.stop()