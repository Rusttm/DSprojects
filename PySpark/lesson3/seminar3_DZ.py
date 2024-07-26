
# Используя Apache Spark, загрузите предоставленный набор данных в DataFrame (пример данных ниже).
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import pyspark.sql.functions as F
from pyspark.sql import Window

spark = SparkSession.builder.appName("seminar2_dz").getOrCreate()

data_list = [("2023-11-20", "Electronics", 100, 12000),
("2023-11-21", "Electronics", 110, 13000),
("2023-11-22", "Electronics", 105, 12500),
("2023-11-20", "Clothing", 300, 15000),
("2023-11-21", "Clothing", 280, 14000),
("2023-11-22", "Clothing", 320, 16000),
("2023-11-20", "Books", 150, 9000),
("2023-11-21", "Books", 200, 12000),
("2023-11-22", "Books", 180, 10000)]

sdf = spark.createDataFrame(data_list, schema=["date", "category", "quantity", "revenue"])
sdf = sdf.withColumn("quantity", sdf["quantity"].cast("int"))
sdf = sdf.withColumn("revenue", sdf["revenue"].cast("double"))
sdf = sdf.withColumn("date", F.to_date("date"))
# sdf.show()

# С использованием оконных функций, рассчитайте среднее выручки от продаж для каждой категории продукта.
"""  
+----------+-----------+--------+-------+------------------+
|      date|   category|quantity|revenue|    cat_avg_profit|
+----------+-----------+--------+-------+------------------+
|2023-11-20|      Books|     150| 9000.0|10333.333333333334|
|2023-11-21|      Books|     200|12000.0|10333.333333333334|
|2023-11-22|      Books|     180|10000.0|10333.333333333334|
|2023-11-20|   Clothing|     300|15000.0|           15000.0|
|2023-11-21|   Clothing|     280|14000.0|           15000.0|
|2023-11-22|   Clothing|     320|16000.0|           15000.0|
|2023-11-20|Electronics|     100|12000.0|           12500.0|
|2023-11-21|Electronics|     110|13000.0|           12500.0|
|2023-11-22|Electronics|     105|12500.0|           12500.0|
+----------+-----------+--------+-------+------------------+
"""
avg_window = Window.partitionBy("category")
revenue_avg_cat = sdf.withColumn("cat_avg_profit", F.avg("revenue").over(avg_window))
# revenue_avg_cat.show()


# Примените операцию pivot для того, чтобы преобразовать полученные данные таким образом, 
# чтобы в качестве строк были категории продуктов, в качестве столбцов были дни, 
# а значениями были средние значения выручки от продаж за соответствующий день
""" 
согласно заданию, должно быть чтото вроде
+-----------+----------+----------+------------------+
|   category|2023-11-20|2023-11-21|        2023-11-22|
+-----------+----------+----------+------------------+
|Electronics|   12000.0|   13000.0|12833.333333333334|
|   Clothing|   12000.0|   13000.0|12833.333333333334|
|      Books|   12000.0|   13000.0|12833.333333333334|
+-----------+----------+----------+------------------+
у каждого дня по всем позициям одинаковая средняя величина
"""
# код следующий:
avg_window = Window.partitionBy("date")
revenue_avg_date = sdf.withColumn("date_avg_profit", F.avg("revenue").over(avg_window))
pivot_df1 = revenue_avg_date.groupBy("category").pivot("date").agg(F.max("date_avg_profit"))
print("Вариант 1:")
pivot_df1.show()



"""
хотя логика подсказвает, что средние продажи за день должны выглядеть так:
+-----------+----------+----------+----------+
|   category|2023-11-20|2023-11-21|2023-11-22|
+-----------+----------+----------+----------+
|Electronics|   12000.0|   13000.0|   12500.0|
|   Clothing|   15000.0|   14000.0|   16000.0|
|      Books|    9000.0|   12000.0|   10000.0|
+-----------+----------+----------+----------+
т.к. продавался один экземпляр товара за день
"""
avg_window = Window.partitionBy("category")
revenue_avg_cat = sdf.withColumn("cat_avg_profit", F.avg("revenue").over(avg_window))
pivot_df2 = revenue_avg_cat.groupBy("category").pivot("date").agg(F.avg("revenue"))
print("Вариант 2:")
pivot_df2.show()


""" 
но если мы хотим получить ответ как в презентации:
+-----------+----------+----------+------------------+
|   category|2023-11-20|2023-11-21|        2023-11-22|
+-----------+----------+----------+------------------+
|      Books|    9000.0|   10500.0|10333.333333333334|
|   Clothing|   15000.0|   14500.0|           15000.0|
|Electronics|   12000.0|   12500.0|           12500.0|
+-----------+----------+----------+------------------+
тогда, наверное, стоит перефразировать 'за соответствующий день' в 'за все предыдущие дни' или 'продажи на этот день'
"""
avg_window = Window.partitionBy("category").rowsBetween(-3, 0)
revenue_avg_cat = sdf.withColumn("cat_avg_profit", F.avg("revenue").over(avg_window))
pivot_df3 = revenue_avg_cat.groupBy("category").pivot("date").agg(F.first_value("cat_avg_profit"))
print("Вариант 3:")
pivot_df3.show()

