import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data = {'id': [1, 2, 3, 4], 'Name': ['Joe', 'Henry', 'Sam', 'Max'],
        'Salary':['70', '80', '60', '90'], 'ManagerId':['3', '4', 'Null', 'Null']}
df = pd.DataFrame(data)
ss = SparkSession.builder.master("local[1]").appName("pandas df").getOrCreate()
""" 
создаем spark dataframe вида:
+---+-----+------+---------+
| id| Name|Salary|ManagerId|
+---+-----+------+---------+
|  1|  Joe|    70|        3|
|  2|Henry|    80|        4|
|  3|  Sam|    60|     Null|
|  4|  Max|    90|     Null|
+---+-----+------+---------+

"""
sdf = ss.createDataFrame(df)
# т.к. в pd type(Salary)=string необходимо поменять тип колонки в spark dataframe на int
sdf = sdf.withColumn("Salary", sdf["Salary"].cast("int"))
""" 
приводим исходную таблицу к виду:
+-----+------+-------------+-----------+
| Name|Salary|ManagerSalary|ManagerName|
+-----+------+-------------+-----------+
|  Joe|    70|           60|        Sam|
|Henry|    80|           90|        Max|
+-----+------+-------------+-----------+
"""
sdf1 = sdf.alias("a") \
    .join(sdf.alias("b"), col("a.ManagerId") == col("b.id"), "inner")  \
    .select(col("a.Name"), col("a.Salary"), col("b.Salary").alias("ManagerSalary"), col("b.Name").alias("ManagerName"))

"""  
фильтруем таблицу по заданию (Salary сотрудника больше Salary менеджера) и выбираем столбец 'Name'.
должны получить таблицу вида:
+----+
|Name|
+----+
| Joe|
+----+
"""
sdf_filtered = sdf1.filter(sdf1["Salary"]>sdf1["ManagerSalary"]).select("Name")
sdf_filtered.show()
