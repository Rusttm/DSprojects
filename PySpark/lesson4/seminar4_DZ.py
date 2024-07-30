from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# create spark session
spark = SparkSession.builder.appName("FilterOddNumbersSum").getOrCreate()
# stream rate
df = spark.readStream.format("rate").load()
# filter odds and add sum column
df_even = df.filter("value % 2 == 1").agg(f.sum('value'))
query = df_even.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
# spark.stop()