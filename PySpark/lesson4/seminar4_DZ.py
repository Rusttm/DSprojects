# Условие: используйте источник rate, напишите код, 
# который создаст дополнительный столбец, 
# который будет выводить сумму только нечётных чисел.


from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, sum

# create spark session
spark = SparkSession.builder.appName("FilterOddNumbersSum").getOrCreate()
# stream rate
df = spark.readStream.format("rate").load()
# filter odds and add sum column
df_even = df.filter("value % 2 == 1").agg(f.sum('value'))
query = df_even.writeStream.outputMode("update").format("console").start()
query.awaitTermination()
# spark.stop()