>>> 
>>> from pyspark.sql.functions import *
>>> 
>>> # A traditional join will be queried as below:
>>> df2.join(df1, (df2.DEPARTMENT_ID == df1.DEPARTMENT_ID) & (df1.LOCATION_ID == 1700), "inner").select(df2.EMPLOYEE_ID,df2.FIRST_NAME, df2.DEPARTMENT_ID, df1.DEPARTMENT_NAME, df1.LOCATION_ID).show(100)
+-----------+-----------+-------------+---------------+-----------+             
|EMPLOYEE_ID| FIRST_NAME|DEPARTMENT_ID|DEPARTMENT_NAME|LOCATION_ID|
+-----------+-----------+-------------+---------------+-----------+
|        200|   Jennifer|           10| Administration|       1700|
|        205|    Shelley|          110|     Accounting|       1700|
|        206|    William|          110|     Accounting|       1700|
|        100|     Steven|           90|      Executive|       1700|
|        101|      Neena|           90|      Executive|       1700|
|        102|        Lex|           90|      Executive|       1700|
|        108|      Nancy|          100|        Finance|       1700|
|        109|     Daniel|          100|        Finance|       1700|
|        110|       John|          100|        Finance|       1700|
|        111|     Ismael|          100|        Finance|       1700|
|        112|Jose Manuel|          100|        Finance|       1700|
|        113|       Luis|          100|        Finance|       1700|
|        114|        Den|           30|     Purchasing|       1700|
|        115|  Alexander|           30|     Purchasing|       1700|
|        116|     Shelli|           30|     Purchasing|       1700|
|        117|      Sigal|           30|     Purchasing|       1700|
|        118|        Guy|           30|     Purchasing|       1700|
|        119|      Karen|           30|     Purchasing|       1700|
+-----------+-----------+-------------+---------------+-----------+


>>> # A broadcast join will be queried as below:
>>> df2.join(broadcast(df1), (df2.DEPARTMENT_ID == df1.DEPARTMENT_ID) & (df1.LOCATION_ID == 1700), "inner").select(df2.EMPLOYEE_ID,df2.FIRST_NAME, df2.DEPARTMENT_ID, df1.DEPARTMENT_NAME, df1.LOCATION_ID).show(100)
+-----------+-----------+-------------+---------------+-----------+
|EMPLOYEE_ID| FIRST_NAME|DEPARTMENT_ID|DEPARTMENT_NAME|LOCATION_ID|
+-----------+-----------+-------------+---------------+-----------+
|        200|   Jennifer|           10| Administration|       1700|
|        205|    Shelley|          110|     Accounting|       1700|
|        206|    William|          110|     Accounting|       1700|
|        100|     Steven|           90|      Executive|       1700|
|        101|      Neena|           90|      Executive|       1700|
|        102|        Lex|           90|      Executive|       1700|
|        108|      Nancy|          100|        Finance|       1700|
|        109|     Daniel|          100|        Finance|       1700|
|        110|       John|          100|        Finance|       1700|
|        111|     Ismael|          100|        Finance|       1700|
|        112|Jose Manuel|          100|        Finance|       1700|
|        113|       Luis|          100|        Finance|       1700|
|        114|        Den|           30|     Purchasing|       1700|
|        115|  Alexander|           30|     Purchasing|       1700|
|        116|     Shelli|           30|     Purchasing|       1700|
|        117|      Sigal|           30|     Purchasing|       1700|
|        118|        Guy|           30|     Purchasing|       1700|
|        119|      Karen|           30|     Purchasing|       1700|
+-----------+-----------+-------------+---------------+-----------+

>>> 
>>> # To set a treshold limit for the size of broadcasted dataFrame, (The value is in bytes ~ 104 Mb in this case)
>>> spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)
>>> # OR
>>> 
>>> 
>>> # To desable the autoBroadcast for a dataFrame
>>> spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
>>> 
>>> 
>>> 
>>> 
>>> 
>>> 
