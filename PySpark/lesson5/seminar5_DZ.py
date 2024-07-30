# — Используя Spark прочитайте данные из файла csv.
# — Фильтруйте данные, чтобы оставить только книги, продажи которых превышают 3000 экземпляров.
# — Сгруппируйте данные по жанру и вычислите общий объем продаж для каждого жанра.
# — Отсортируйте данные по общему объему продаж в порядке убывания.
# — Выведите результаты на экран. 

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Создаем SparkSession
spark=SparkSession.builder.appName('DZ5').getOrCreate()

#загружаем csv файл
df_pyspark=spark.read.load('PySpark/lesson5/music.csv', format="csv", sep=",", header="true", Infer_schema=True)

df_pyspark = df_pyspark.withColumn("title", df_pyspark["title"].cast('string'))
df_pyspark = df_pyspark.withColumn("author", df_pyspark["author"].cast('string'))
df_pyspark = df_pyspark.withColumn("genre", df_pyspark["genre"].cast('string'))
df_pyspark = df_pyspark.withColumn("sales", df_pyspark["sales"].cast('int'))
"""
+--------------------+--------------------+------------------+-----+------+
|               title|              author|             genre|sales| year |
+--------------------+--------------------+------------------+-----+------+
|                1984|     "George Orwell"| "Science Fiction"| 5000|  1949|
|The Lord of the R...|    "J.R.R. Tolkien"|         "Fantasy"| 3000|  1954|
|To Kill a Mocking...|        "Harper Lee"| "Southern Gothic"| 4000|  1960|
|The Catcher in th...|     "J.D. Salinger"|           "Novel"| 2000|  1951|
|    The Great Gatsby| "F. Scott Fitzge...|           "Novel"| 4500| 1925 |
+--------------------+--------------------+------------------+-----+------+
"""
df_pyspark.show()

# — Фильтруйте данные, чтобы оставить только книги, продажи которых превышают 3000 экземпляров.
"""
+--------------------+--------------------+------------------+-----+------+
|               title|              author|             genre|sales| year |
+--------------------+--------------------+------------------+-----+------+
|                1984|     "George Orwell"| "Science Fiction"| 5000|  1949|
|To Kill a Mocking...|        "Harper Lee"| "Southern Gothic"| 4000|  1960|
|    The Great Gatsby| "F. Scott Fitzge...|           "Novel"| 4500| 1925 |
+--------------------+--------------------+------------------+-----+------+
"""
df_filtered_pyspark = df_pyspark.filter("sales > 3000")
print("Filtered dataframe:")
df_filtered_pyspark.show()


# — Сгруппируйте данные по жанру и вычислите общий объем продаж для каждого жанра.
"""
+------------------+------------+
|             genre|sum_of_sales|
+------------------+------------+
| "Southern Gothic"|        4000|
| "Science Fiction"|        5000|
|           "Novel"|        6500|
|         "Fantasy"|        3000|
+------------------+------------+
"""
df_pyspark_grouped = df_pyspark.groupBy("genre").agg(F.sum("sales").alias("sum_of_sales"))
print("Grouped dataframe:")
df_pyspark_grouped.show()

# — Отсортируйте данные по общему объему продаж в порядке убывания.
"""
+------------------+------------+
|             genre|sum_of_sales|
+------------------+------------+
|           "Novel"|        6500|
| "Science Fiction"|        5000|
| "Southern Gothic"|        4000|
|         "Fantasy"|        3000|
+------------------+------------+
"""
df_pyspark_grouped_sorted = df_pyspark_grouped.sort(F.desc("sum_of_sales"))
print("Sorted dataframe:")
df_pyspark_grouped_sorted.show()

# — Выведите результаты на экран. 
"""
+------------------+------------+
|             genre|sum_of_sales|
+------------------+------------+
| "Science Fiction"|        5000|
|           "Novel"|        4500|
| "Southern Gothic"|        4000|
+------------------+------------+
"""
print("Filtered-Grouped-Sorted dataframe:")
df_pyspark_res = df_pyspark.filter("sales > 3000").groupBy("genre").agg(F.sum("sales").alias("sum_of_sales")).sort(F.desc("sum_of_sales"))
df_pyspark_res.show()