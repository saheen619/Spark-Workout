abc@2fb3326b9643:~/workspace$ pyspark
Python 3.9.13 (main, Oct 13 2022, 21:15:33) 
[GCC 11.2.0] :: Anaconda, Inc. on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/05/24 18:35:59 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.2.1
      /_/

Using Python version 3.9.13 (main, Oct 13 2022 21:15:33)
Spark context Web UI available at http://2fb3326b9643:4040
Spark context available as 'sc' (master = local[*], app id = local-1684933560739).
SparkSession available as 'spark'.
>>> from pyspark.sql.types import StructType,StructField, StringType, IntegerType
>>> person_list = [("Berry","","Allen",1,"M"),
...         ("Oliver","Queen","",2,"M"),
...         ("Robert","","Williams",3,"M"),
...         ("Tony","","Stark",4,"F"),
...         ("Rajiv","Mary","Kumar",5,"F")
...     ]
>>> schema = StructType([ \
...         StructField("firstname",StringType(),True), \
...         StructField("middlename",StringType(),True), \
...         StructField("lastname",StringType(),True), \
...         StructField("id", IntegerType(), True), \
...         StructField("gender", StringType(), True), \
...       
...     ])
>>> df = spark.createDataFrame(data=person_list,schema=schema)
>>> df.printSchema()
root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- id: integer (nullable = true)
 |-- gender: string (nullable = true)

>>> df.show
<bound method DataFrame.show of DataFrame[firstname: string, middlename: string, lastname: string, id: int, gender: string]>
>>> df.show(truncate=False)
+---------+----------+--------+---+------+                                      
|firstname|middlename|lastname| id|gender|
+---------+----------+--------+---+------+
|    Berry|          |   Allen|  1|     M|
|   Oliver|     Queen|        |  2|     M|
|   Robert|          |Williams|  3|     M|
|     Tony|          |   Stark|  4|     F|
|    Rajiv|      Mary|   Kumar|  5|     F|
+---------+----------+--------+---+------+