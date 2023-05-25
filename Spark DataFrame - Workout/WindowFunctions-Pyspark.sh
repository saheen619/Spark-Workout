>>> 
>>> # Window Functions in pyspark
>>> 
>>> # to use window functions in pyspark, we have to import the class Window from pyspark.sql.window library
>>> from pyspark.sql.window import Window
>>> 
>>> # Here in below WindowSpec command and the next line is equivalent to 
>>> # RANK() OVER( PARTITION BY department_id ORDER BY salary)
>>> 
>>> windowSpec = Window.partitionBy("DEPARTMENT_ID").orderBy("SALARY")
>>> df2.withColumn("SALARY_RANK", rank().over(windowSpec)).show()
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+-----------+
|EMPLOYEE_ID|FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|  JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|SALARY_RANK|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+-----------+
|        200|  Jennifer|     Whalen| JWHALEN|515.123.4444|17-SEP-03| AD_ASST|  4400|            - |       101|           10|          1|
|        202|       Pat|        Fay|    PFAY|603.123.6666|17-AUG-05|  MK_REP|  6000|            - |       201|           20|          1|
|        201|   Michael|  Hartstein|MHARTSTE|515.123.5555|17-FEB-04|  MK_MAN| 13000|            - |       100|           20|          2|
|        119|     Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|PU_CLERK|  2500|            - |       114|           30|          1|
|        118|       Guy|     Himuro| GHIMURO|515.127.4565|15-NOV-06|PU_CLERK|  2600|            - |       114|           30|          2|
|        117|     Sigal|     Tobias| STOBIAS|515.127.4564|24-JUL-05|PU_CLERK|  2800|            - |       114|           30|          3|
|        116|    Shelli|      Baida|  SBAIDA|515.127.4563|24-DEC-05|PU_CLERK|  2900|            - |       114|           30|          4|
|        115| Alexander|       Khoo|   AKHOO|515.127.4562|18-MAY-03|PU_CLERK|  3100|            - |       114|           30|          5|
|        114|       Den|   Raphaely|DRAPHEAL|515.127.4561|07-DEC-02|  PU_MAN| 11000|            - |       100|           30|          6|
|        203|     Susan|     Mavris| SMAVRIS|515.123.7777|07-JUN-02|  HR_REP|  6500|            - |       101|           40|          1|
|        132|        TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|ST_CLERK|  2100|            - |       121|           50|          1|
|        128|    Steven|     Markle| SMARKLE|650.124.1434|08-MAR-08|ST_CLERK|  2200|            - |       120|           50|          2|
|        136|     Hazel| Philtanker|HPHILTAN|650.127.1634|06-FEB-08|ST_CLERK|  2200|            - |       122|           50|          2|
|        127|     James|     Landry| JLANDRY|650.124.1334|14-JAN-07|ST_CLERK|  2400|            - |       120|           50|          4|
|        135|        Ki|        Gee|    KGEE|650.127.1734|12-DEC-07|ST_CLERK|  2400|            - |       122|           50|          4|
|        131|     James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|ST_CLERK|  2500|            - |       121|           50|          6|
|        140|    Joshua|      Patel|  JPATEL|650.121.1834|06-APR-06|ST_CLERK|  2500|            - |       123|           50|          6|
|        198|    Donald|   OConnell|DOCONNEL|650.507.9833|21-JUN-07|SH_CLERK|  2600|            - |       124|           50|          8|
|        199|   Douglas|      Grant|  DGRANT|650.507.9844|13-JAN-08|SH_CLERK|  2600|            - |       124|           50|          8|
|        126|     Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|ST_CLERK|  2700|            - |       120|           50|         10|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+-----------+
only showing top 20 rows

>>> 
>>> df2.withColumn("SALARY_RANK", rank().over(windowSpec)).select(col("DEPARTMENT_ID"), col("SALARY"), col("SALARY_RANK")).show()
+-------------+------+-----------+
|DEPARTMENT_ID|SALARY|SALARY_RANK|
+-------------+------+-----------+
|           10|  4400|          1|
|           20|  6000|          1|
|           20| 13000|          2|
|           30|  2500|          1|
|           30|  2600|          2|
|           30|  2800|          3|
|           30|  2900|          4|
|           30|  3100|          5|
|           30| 11000|          6|
|           40|  6500|          1|
|           50|  2100|          1|
|           50|  2200|          2|
|           50|  2200|          2|
|           50|  2400|          4|
|           50|  2400|          4|
|           50|  2500|          6|
|           50|  2500|          6|
|           50|  2600|          8|
|           50|  2600|          8|
|           50|  2700|         10|
+-------------+------+-----------+
only showing top 20 rows


>>> # Partitioning and displaying RUNNING SUM of salary in descending order
>>> 
>>> windowSpec = Window.partitionBy("DEPARTMENT_ID").orderBy(col("SALARY").desc())
>>> df2.withColumn("SUM_OF_SALARY", sum("SALARY").over(windowSpec)).select(col("DEPARTMENT_ID"), col("SALARY"), col("SUM_OF_SALARY")).show()
+-------------+------+-------------+
|DEPARTMENT_ID|SALARY|SUM_OF_SALARY|
+-------------+------+-------------+
|           10|  4400|         4400|
|           20| 13000|        13000|
|           20|  6000|        19000|
|           30| 11000|        11000|
|           30|  3100|        14100|
|           30|  2900|        17000|
|           30|  2800|        19800|
|           30|  2600|        22400|
|           30|  2500|        24900|
|           40|  6500|         6500|
|           50|  8200|         8200|
|           50|  8000|        16200|
|           50|  7900|        24100|
|           50|  6500|        30600|
|           50|  5800|        36400|
|           50|  3600|        40000|
|           50|  3300|        46600|
|           50|  3300|        46600|
|           50|  3200|        53000|
|           50|  3200|        53000|
+-------------+------+-------------+
only showing top 20 rows

>>> 
>>>
>>> # ROLLING SUM
>>> windowSpec = Window.partitionBy("DEPARTMENT_ID")
>>> df2.withColumn("ROLLING_SUM", sum("SALARY").over(windowSpec)).select(col("DEPARTMENT_ID"), col("SALARY"), col("ROLLING_SUM")).show()
+-------------+------+-----------+
|DEPARTMENT_ID|SALARY|ROLLING_SUM|
+-------------+------+-----------+
|           10|  4400|       4400|
|           20| 13000|      19000|
|           20|  6000|      19000|
|           30| 11000|      24900|
|           30|  3100|      24900|
|           30|  2900|      24900|
|           30|  2800|      24900|
|           30|  2600|      24900|
|           30|  2500|      24900|
|           40|  6500|       6500|
|           50|  2600|      85600|
|           50|  2600|      85600|
|           50|  8000|      85600|
|           50|  8200|      85600|
|           50|  7900|      85600|
|           50|  6500|      85600|
|           50|  5800|      85600|
|           50|  3200|      85600|
|           50|  2700|      85600|
|           50|  2400|      85600|
+-------------+------+-----------+
only showing top 20 rows
