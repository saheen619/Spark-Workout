# To read a file in pyspark

dataFrame = spark.read.option("header",True).option("inferSchema",True).csv("/path/test.csv")


>>> # to write a .csv file to HDFS using pyspark
>>> 
>>> from pyspark.sql.functions import *
>>> resultDf = df2.join(broadcast(df1), (df2.DEPARTMENT_ID == df1.DEPARTMENT_ID) & (df1.LOCATION_ID == 1700), "inner").select(df2.EMPLOYEE_ID,df2.FIRST_NAME, df2.DEPARTMENT_ID, df1.DEPARTMENT_NAME, df1.LOCATION_ID)
>>> resultDf.write.option("header",True).csv("/output")
>>> 
>>> # to write a file with PIPE SEPERATED VALUES to HDFS using pyspark
>>> 
>>> resultDf.write.option("header",True).option("delimiter","|").csv("/output/psv/")
>>> 
>>> 
>>> # To OVERWRITE a file with COMMA SEPERATED VALUES to HDFS using pyspark
>>> 
>>> resultDf.write.option("header",True).mode("overwrite").csv("/output/")
>>>                                                                             
>>> # To APPEND a file with COMMA SEPERATED VALUES to HDFS using pyspark
>>>
>>> resultDf.write.option("header",True).mode("append").csv("/output/")
>>> 
>>> # By default, the file gets saved in parquet format in the HDFS, so trying the SAVE option, you could see that                                                                            
>>> 
>>> resultDf.write.option("header",True).save("/output/parquet/")
>>>                                                                             
>>> # Save the file in ORC format or any other format using the below method .format
>>> 
>>> resultDf.write.option("header",True).format("orc").save("/output/orc/")
>>> 
# The file could be accessed in HDFS as below:

abc@2020c62febc4:~/workspace$ hdfs dfs -ls /output/orc/
Found 2 items
-rw-r--r--   1 abc supergroup          0 2023-05-26 21:58 /output/orc/_SUCCESS
-rw-r--r--   1 abc supergroup       1018 2023-05-26 21:58 /output/orc/part-00000-996aa2eb-aa12-493c-90ef-b72c1b217f75-c000.snappy.orc

>>> # Save the file in JSON format using the method .format
>>> 
>>> resultDf.write.option("header",True).format("json").save("/output/json/")
>>> 
abc@2020c62febc4:~/workspace$ hdfs dfs -ls /output/json/
Found 2 items
-rw-r--r--   1 abc supergroup          0 2023-05-26 22:16 /output/json/_SUCCESS
-rw-r--r--   1 abc supergroup       1984 2023-05-26 22:16 /output/json/part-00000-544b5fda-bab1-4ded-bc78-f91b43f67edb-c000.json

abc@2020c62febc4:~/workspace$ hdfs dfs -cat /output/json/part-00000-544b5fda-bab1-4ded-bc78-f91b43f67edb-c000.json
2023-05-26 22:16:39,917 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
{"EMPLOYEE_ID":200,"FIRST_NAME":"Jennifer","DEPARTMENT_ID":10,"DEPARTMENT_NAME":"Administration","LOCATION_ID":1700}
{"EMPLOYEE_ID":205,"FIRST_NAME":"Shelley","DEPARTMENT_ID":110,"DEPARTMENT_NAME":"Accounting","LOCATION_ID":1700}
{"EMPLOYEE_ID":206,"FIRST_NAME":"William","DEPARTMENT_ID":110,"DEPARTMENT_NAME":"Accounting","LOCATION_ID":1700}
{"EMPLOYEE_ID":100,"FIRST_NAME":"Steven","DEPARTMENT_ID":90,"DEPARTMENT_NAME":"Executive","LOCATION_ID":1700}
{"EMPLOYEE_ID":101,"FIRST_NAME":"Neena","DEPARTMENT_ID":90,"DEPARTMENT_NAME":"Executive","LOCATION_ID":1700}
{"EMPLOYEE_ID":102,"FIRST_NAME":"Lex","DEPARTMENT_ID":90,"DEPARTMENT_NAME":"Executive","LOCATION_ID":1700}
{"EMPLOYEE_ID":108,"FIRST_NAME":"Nancy","DEPARTMENT_ID":100,"DEPARTMENT_NAME":"Finance","LOCATION_ID":1700}
{"EMPLOYEE_ID":109,"FIRST_NAME":"Daniel","DEPARTMENT_ID":100,"DEPARTMENT_NAME":"Finance","LOCATION_ID":1700}
{"EMPLOYEE_ID":110,"FIRST_NAME":"John","DEPARTMENT_ID":100,"DEPARTMENT_NAME":"Finance","LOCATION_ID":1700}
{"EMPLOYEE_ID":111,"FIRST_NAME":"Ismael","DEPARTMENT_ID":100,"DEPARTMENT_NAME":"Finance","LOCATION_ID":1700}
{"EMPLOYEE_ID":112,"FIRST_NAME":"Jose Manuel","DEPARTMENT_ID":100,"DEPARTMENT_NAME":"Finance","LOCATION_ID":1700}
{"EMPLOYEE_ID":113,"FIRST_NAME":"Luis","DEPARTMENT_ID":100,"DEPARTMENT_NAME":"Finance","LOCATION_ID":1700}
{"EMPLOYEE_ID":114,"FIRST_NAME":"Den","DEPARTMENT_ID":30,"DEPARTMENT_NAME":"Purchasing","LOCATION_ID":1700}
{"EMPLOYEE_ID":115,"FIRST_NAME":"Alexander","DEPARTMENT_ID":30,"DEPARTMENT_NAME":"Purchasing","LOCATION_ID":1700}
{"EMPLOYEE_ID":116,"FIRST_NAME":"Shelli","DEPARTMENT_ID":30,"DEPARTMENT_NAME":"Purchasing","LOCATION_ID":1700}
{"EMPLOYEE_ID":117,"FIRST_NAME":"Sigal","DEPARTMENT_ID":30,"DEPARTMENT_NAME":"Purchasing","LOCATION_ID":1700}
{"EMPLOYEE_ID":118,"FIRST_NAME":"Guy","DEPARTMENT_ID":30,"DEPARTMENT_NAME":"Purchasing","LOCATION_ID":1700}
{"EMPLOYEE_ID":119,"FIRST_NAME":"Karen","DEPARTMENT_ID":30,"DEPARTMENT_NAME":"Purchasing","LOCATION_ID":1700}
abc@2020c62febc4:~/workspace$ 



# Because of distributed computing, different tasks store one file in different parts or partitions. When we try to save a file,
# or try to append a file to the HDFS storage, The file gets stored in different parts.

>>> 
>>> resultDf.write.option("header",True).format("csv").save("/output/append")
>>> resultDf.write.option("header",True).mode("append").format("csv").save("/output/append")
>>> resultDf.write.option("header",True).mode("append").format("csv").save("/output/append")
>>> resultDf.write.option("header",True).mode("append").format("csv").save("/output/append")
>>> 
# in HDFS
abc@2020c62febc4:~/workspace$ hdfs dfs -ls /output/append/
Found 5 items
-rw-r--r--   1 abc supergroup          0 2023-05-26 23:45 /output/append/_SUCCESS
-rw-r--r--   1 abc supergroup        591 2023-05-26 23:45 /output/append/part-00000-0d6d2215-528c-4f03-add2-7c32ecdd4ac1-c000.csv
-rw-r--r--   1 abc supergroup        591 2023-05-26 23:45 /output/append/part-00000-88ce37c6-1928-4c94-8098-2b52a8c86478-c000.csv
-rw-r--r--   1 abc supergroup        591 2023-05-26 23:45 /output/append/part-00000-ddbada81-6fec-43cb-a3bf-cf8be8940229-c000.csv
-rw-r--r--   1 abc supergroup        591 2023-05-26 23:45 /output/append/part-00000-e2983971-a60e-4084-a295-9620c3ea4da2-c000.csv
abc@2020c62febc4:~/workspace$ 




# PartitionBy()
# To have data partitioned by a particular field charecters,
# This partition is done to prevent full table stance
# When you partition a column, you are essentially creating a separate directory for each unique value in the column. This makes it easier to find and access data, and it can also improve performance.

>>> 
>>> resultDf.write.option("header",True).partitionBy("DEPARTMENT_NAME").format("csv").save("/output/partitioned_by_department_name/")
>>>             

# in HDFS
abc@2020c62febc4:~/workspace$ hdfs dfs -ls /output/partitioned_by_department_name/
Found 6 items
drwxr-xr-x   - abc supergroup          0 2023-05-26 23:54 /output/partitioned_by_department_name/DEPARTMENT_NAME=Accounting
drwxr-xr-x   - abc supergroup          0 2023-05-26 23:54 /output/partitioned_by_department_name/DEPARTMENT_NAME=Administration
drwxr-xr-x   - abc supergroup          0 2023-05-26 23:54 /output/partitioned_by_department_name/DEPARTMENT_NAME=Executive
drwxr-xr-x   - abc supergroup          0 2023-05-26 23:54 /output/partitioned_by_department_name/DEPARTMENT_NAME=Finance
drwxr-xr-x   - abc supergroup          0 2023-05-26 23:54 /output/partitioned_by_department_name/DEPARTMENT_NAME=Purchasing
-rw-r--r--   1 abc supergroup          0 2023-05-26 23:54 /output/partitioned_by_department_name/_SUCCESS
abc@2020c62febc4:~/workspace$ 
abc@2020c62febc4:~/workspace$ hdfs dfs -ls /output/partitioned_by_department_name/DEPARTMENT_NAME=Purchasing/
Found 1 items
-rw-r--r--   1 abc supergroup        158 2023-05-26 23:54 /output/partitioned_by_department_name/DEPARTMENT_NAME=Purchasing/part-00000-10a7f875-a9b1-4300-95ee-b12f5ef9e523.c000.csv

abc@2020c62febc4:~/workspace$ hdfs dfs -cat /output/partitioned_by_department_name/DEPARTMENT_NAME=Purchasing/part-00000-10a7f875-a9b1-4300-95ee-b12f5ef9e523.c000.csv
2023-05-26 23:58:15,218 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
EMPLOYEE_ID,FIRST_NAME,DEPARTMENT_ID,LOCATION_ID
114,Den,30,1700
115,Alexander,30,1700
116,Shelli,30,1700
117,Sigal,30,1700
118,Guy,30,1700
119,Karen,30,1700
abc@2020c62febc4:~/workspace$ 




