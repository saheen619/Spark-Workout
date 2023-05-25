abc@b21941d8ee62:~/workspace$ pyspark
"""
Python 3.9.13 (main, Oct 13 2022, 21:15:33) 
[GCC 11.2.0] :: Anaconda, Inc. on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/05/24 22:46:01 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ '/ __/  _/
   /__ / .__/\_,_/_/ /_/\_\   version 3.2.1
      /_/

Using Python version 3.9.13 (main, Oct 13 2022 21:15:33)
Spark context Web UI available at http://b21941d8ee62:4040
Spark context available as 'sc' (master = local[*], app id = local-1684948562892).
SparkSession available as 'spark'.
"""
>>> df1 = spark.read.option("header",True).csv("/input_data/departments.csv")
>>> df1.printSchema()
"""
root
 |-- DEPARTMENT_ID: string (nullable = true)
 |-- DEPARTMENT_NAME: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- LOCATION_ID: string (nullable = true)
"""
>>> df1.show(truncate=True)
+-------------+--------------------+----------+-----------+
|DEPARTMENT_ID|     DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-------------+--------------------+----------+-----------+
|           10|      Administration|       200|       1700|
|           20|           Marketing|       201|       1800|
|           30|          Purchasing|       114|       1700|
|           40|     Human Resources|       203|       2400|
|           50|            Shipping|       121|       1500|
|           60|                  IT|       103|       1400|
|           70|    Public Relations|       204|       2700|
|           80|               Sales|       145|       2500|
|           90|           Executive|       100|       1700|
|          100|             Finance|       108|       1700|
|          110|          Accounting|       205|       1700|
|          120|            Treasury|        - |       1700|
|          130|       Corporate Tax|        - |       1700|
|          140|  Control And Credit|        - |       1700|
|          150|Shareholder Services|        - |       1700|
|          160|            Benefits|        - |       1700|
|          170|       Manufacturing|        - |       1700|
|          180|        Construction|        - |       1700|
|          190|         Contracting|        - |       1700|
|          200|          Operations|        - |       1700|
+-------------+--------------------+----------+-----------+
only showing top 20 rows

# inferSchema commands spark to logically assign datatypes for the attributes
# to add more parameters, use .option(<parameter>)

>>> df1 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/departments.csv")
>>> df1.printSchema()
root
 |-- DEPARTMENT_ID: integer (nullable = true)
 |-- DEPARTMENT_NAME: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- LOCATION_ID: integer (nullable = true)

>>> df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/employees.csv")
>>> df2.printSchema()
root
 |-- EMPLOYEE_ID: integer (nullable = true)
 |-- FIRST_NAME: string (nullable = true)
 |-- LAST_NAME: string (nullable = true)
 |-- EMAIL: string (nullable = true)
 |-- PHONE_NUMBER: string (nullable = true)
 |-- HIRE_DATE: string (nullable = true)
 |-- JOB_ID: string (nullable = true)
 |-- SALARY: integer (nullable = true)
 |-- COMMISSION_PCT: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- DEPARTMENT_ID: integer (nullable = true)

>>> df2.show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
only showing top 20 rows


#Select all columns
>>> df2.select("*").show()    
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
only showing top 20 rows

# Selecting few columns from the dataFrame
>>> df2.select("EMPLOYEE_ID","FIRST_NAME").show()
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows

# Another way of selecting few columns
>>> df2.select(df2.EMPLOYEE_ID, df2.FIRST_NAME).show() 
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows

 # Another way of selecting few columns for select
>>> df2.select(df2["EMPLOYEE_ID","FIRS],df2["FIRST_NAME"]).show() 
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows


# Using col functions to select few columns for select
>>> from pyspark.sql.functions import col
>>> df2.select(col("EMPLOYEE_ID"),col("FIRST_NAME")).show()      
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows


# giving different alias to columns in select
>>> df2.select(col("EMPLOYEE_ID").alias("EMP_ID"),col("FIRST_NAME").alias("F_NAME")).show()   
+------+---------+
|EMP_ID|   F_NAME|
+------+---------+
|   198|   Donald|
|   199|  Douglas|
|   200| Jennifer|
|   201|  Michael|
|   202|      Pat|
|   203|    Susan|
|   204|  Hermann|
|   205|  Shelley|
|   206|  William|
|   100|   Steven|
|   101|    Neena|
|   102|      Lex|
|   103|Alexander|
|   104|    Bruce|
|   105|    David|
|   106|    Valli|
|   107|    Diana|
|   108|    Nancy|
|   109|   Daniel|
|   110|     John|
+------+---------+
only showing top 20 rows

# Using Count function
>>> df2.select(col("EMPLOYEE_ID")).count()
50


>>> # To increase each employee salary by 1000 rupees
>>> df2.select("EMPLOYEE_ID","FIRST_NAME","SALARY").withColumn("NEW_SALARY",col("SALARY") + 1000).show()
+-----------+----------+------+----------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|NEW_SALARY|
+-----------+----------+------+----------+
|        198|    Donald|  2600|      3600|
|        199|   Douglas|  2600|      3600|
|        200|  Jennifer|  4400|      5400|
|        201|   Michael| 13000|     14000|
|        202|       Pat|  6000|      7000|
|        203|     Susan|  6500|      7500|
|        204|   Hermann| 10000|     11000|
|        205|   Shelley| 12008|     13008|
|        206|   William|  8300|      9300|
|        100|    Steven| 24000|     25000|
|        101|     Neena| 17000|     18000|
|        102|       Lex| 17000|     18000|
|        103| Alexander|  9000|     10000|
|        104|     Bruce|  6000|      7000|
|        105|     David|  4800|      5800|
|        106|     Valli|  4800|      5800|
|        107|     Diana|  4200|      5200|
|        108|     Nancy| 12008|     13008|
|        109|    Daniel|  9000|     10000|
|        110|      John|  8200|      9200|
+-----------+----------+------+----------+
only showing top 20 rows


>>> # Another way of sequential column ordering for an update function
>>> df2.withColumn("NEW_SALARY",col("SALARY") + 1000).select("EMPLOYEE_ID","FIRST_NAME","NEW_SALARY").show()
+-----------+----------+----------+
|EMPLOYEE_ID|FIRST_NAME|NEW_SALARY|
+-----------+----------+----------+
|        198|    Donald|      3600|
|        199|   Douglas|      3600|
|        200|  Jennifer|      5400|
|        201|   Michael|     14000|
|        202|       Pat|      7000|
|        203|     Susan|      7500|
|        204|   Hermann|     11000|
|        205|   Shelley|     13008|
|        206|   William|      9300|
|        100|    Steven|     25000|
|        101|     Neena|     18000|
|        102|       Lex|     18000|
|        103| Alexander|     10000|
|        104|     Bruce|      7000|
|        105|     David|      5800|
|        106|     Valli|      5800|
|        107|     Diana|      5200|
|        108|     Nancy|     13008|
|        109|    Daniel|     10000|
|        110|      John|      9200|
+-----------+----------+----------+
only showing top 20 rows


>>> # Updating new values or making changes in the exising data
>>> df2.select("EMPLOYEE_ID","FIRST_NAME","SALARY").withColumn("SALARY",col("SALARY") - 500).show()
+-----------+----------+------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|
+-----------+----------+------+
|        198|    Donald|  2100|
|        199|   Douglas|  2100|
|        200|  Jennifer|  3900|
|        201|   Michael| 12500|
|        202|       Pat|  5500|
|        203|     Susan|  6000|
|        204|   Hermann|  9500|
|        205|   Shelley| 11508|
|        206|   William|  7800|
|        100|    Steven| 23500|
|        101|     Neena| 16500|
|        102|       Lex| 16500|
|        103| Alexander|  8500|
|        104|     Bruce|  5500|
|        105|     David|  4300|
|        106|     Valli|  4300|
|        107|     Diana|  3700|
|        108|     Nancy| 11508|
|        109|    Daniel|  8500|
|        110|      John|  7700|
+-----------+----------+------+
only showing top 20 rows


>>> # Renaming a column in the DataFrame
>>> df2.withColumnRenamed("SALARY","EMP_SALARY").show()
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|EMP_SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|      2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|      2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|      4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN|     13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|      6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|      6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP|     10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR|     12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|      8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES|     24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP|     17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP|     17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|      9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|      6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|      4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|      4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|      4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR|     12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|      9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|      8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
only showing top 20 rows


>>> # Deleting the COMMISSION_PCT Column from the dataFrame
>>> df2.drop("COMMISSION_PCT").show()
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
only showing top 20 rows


>>> # finding all the employees whose salaries are below 5000 
>>> df2.filter(col("SALARY") < 5000).show()
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|  JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|        198|    Donald|   OConnell|DOCONNEL|650.507.9833|21-JUN-07|SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|      Grant|  DGRANT|650.507.9844|13-JAN-08|SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|     Whalen| JWHALEN|515.123.4444|17-SEP-03| AD_ASST|  4400|            - |       101|           10|
|        105|     David|     Austin| DAUSTIN|590.423.4569|25-JUN-05| IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|  Pataballa|VPATABAL|590.423.4560|05-FEB-06| IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|    Lorentz|DLORENTZ|590.423.5567|07-FEB-07| IT_PROG|  4200|            - |       103|           60|
|        115| Alexander|       Khoo|   AKHOO|515.127.4562|18-MAY-03|PU_CLERK|  3100|            - |       114|           30|
|        116|    Shelli|      Baida|  SBAIDA|515.127.4563|24-DEC-05|PU_CLERK|  2900|            - |       114|           30|
|        117|     Sigal|     Tobias| STOBIAS|515.127.4564|24-JUL-05|PU_CLERK|  2800|            - |       114|           30|
|        118|       Guy|     Himuro| GHIMURO|515.127.4565|15-NOV-06|PU_CLERK|  2600|            - |       114|           30|
|        119|     Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|PU_CLERK|  2500|            - |       114|           30|
|        125|     Julia|      Nayer|  JNAYER|650.124.1214|16-JUL-05|ST_CLERK|  3200|            - |       120|           50|
|        126|     Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|ST_CLERK|  2700|            - |       120|           50|
|        127|     James|     Landry| JLANDRY|650.124.1334|14-JAN-07|ST_CLERK|  2400|            - |       120|           50|
|        128|    Steven|     Markle| SMARKLE|650.124.1434|08-MAR-08|ST_CLERK|  2200|            - |       120|           50|
|        129|     Laura|     Bissot| LBISSOT|650.124.5234|20-AUG-05|ST_CLERK|  3300|            - |       121|           50|
|        130|     Mozhe|   Atkinson|MATKINSO|650.124.6234|30-OCT-05|ST_CLERK|  2800|            - |       121|           50|
|        131|     James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|ST_CLERK|  2500|            - |       121|           50|
|        132|        TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|ST_CLERK|  2100|            - |       121|           50|
|        133|     Jason|     Mallin| JMALLIN|650.127.1934|14-JUN-04|ST_CLERK|  3300|            - |       122|           50|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
only showing top 20 rows


>>> df2.filter(col("SALARY") < 5000).show(150)
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|  JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|        198|    Donald|   OConnell|DOCONNEL|650.507.9833|21-JUN-07|SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|      Grant|  DGRANT|650.507.9844|13-JAN-08|SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|     Whalen| JWHALEN|515.123.4444|17-SEP-03| AD_ASST|  4400|            - |       101|           10|
|        105|     David|     Austin| DAUSTIN|590.423.4569|25-JUN-05| IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|  Pataballa|VPATABAL|590.423.4560|05-FEB-06| IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|    Lorentz|DLORENTZ|590.423.5567|07-FEB-07| IT_PROG|  4200|            - |       103|           60|
|        115| Alexander|       Khoo|   AKHOO|515.127.4562|18-MAY-03|PU_CLERK|  3100|            - |       114|           30|
|        116|    Shelli|      Baida|  SBAIDA|515.127.4563|24-DEC-05|PU_CLERK|  2900|            - |       114|           30|
|        117|     Sigal|     Tobias| STOBIAS|515.127.4564|24-JUL-05|PU_CLERK|  2800|            - |       114|           30|
|        118|       Guy|     Himuro| GHIMURO|515.127.4565|15-NOV-06|PU_CLERK|  2600|            - |       114|           30|
|        119|     Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|PU_CLERK|  2500|            - |       114|           30|
|        125|     Julia|      Nayer|  JNAYER|650.124.1214|16-JUL-05|ST_CLERK|  3200|            - |       120|           50|
|        126|     Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|ST_CLERK|  2700|            - |       120|           50|
|        127|     James|     Landry| JLANDRY|650.124.1334|14-JAN-07|ST_CLERK|  2400|            - |       120|           50|
|        128|    Steven|     Markle| SMARKLE|650.124.1434|08-MAR-08|ST_CLERK|  2200|            - |       120|           50|
|        129|     Laura|     Bissot| LBISSOT|650.124.5234|20-AUG-05|ST_CLERK|  3300|            - |       121|           50|
|        130|     Mozhe|   Atkinson|MATKINSO|650.124.6234|30-OCT-05|ST_CLERK|  2800|            - |       121|           50|
|        131|     James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|ST_CLERK|  2500|            - |       121|           50|
|        132|        TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|ST_CLERK|  2100|            - |       121|           50|
|        133|     Jason|     Mallin| JMALLIN|650.127.1934|14-JUN-04|ST_CLERK|  3300|            - |       122|           50|
|        134|   Michael|     Rogers| MROGERS|650.127.1834|26-AUG-06|ST_CLERK|  2900|            - |       122|           50|
|        135|        Ki|        Gee|    KGEE|650.127.1734|12-DEC-07|ST_CLERK|  2400|            - |       122|           50|
|        136|     Hazel| Philtanker|HPHILTAN|650.127.1634|06-FEB-08|ST_CLERK|  2200|            - |       122|           50|
|        137|    Renske|     Ladwig| RLADWIG|650.121.1234|14-JUL-03|ST_CLERK|  3600|            - |       123|           50|
|        138|   Stephen|     Stiles| SSTILES|650.121.2034|26-OCT-05|ST_CLERK|  3200|            - |       123|           50|
|        139|      John|        Seo|    JSEO|650.121.2019|12-FEB-06|ST_CLERK|  2700|            - |       123|           50|
|        140|    Joshua|      Patel|  JPATEL|650.121.1834|06-APR-06|ST_CLERK|  2500|            - |       123|           50|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+


# Applying two filter/where conditions together and selecting only a few columns from the dataframe
>>> df2.filter((col("DEPARTMENT_ID") == 50) & (col("SALARY") < 5000)).select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show()
+-----------+----------+------+-------------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+----------+------+-------------+
|        198|    Donald|  2600|           50|
|        199|   Douglas|  2600|           50|
|        125|     Julia|  3200|           50|
|        126|     Irene|  2700|           50|
|        127|     James|  2400|           50|
|        128|    Steven|  2200|           50|
|        129|     Laura|  3300|           50|
|        130|     Mozhe|  2800|           50|
|        131|     James|  2500|           50|
|        132|        TJ|  2100|           50|
|        133|     Jason|  3300|           50|
|        134|   Michael|  2900|           50|
|        135|        Ki|  2400|           50|
|        136|     Hazel|  2200|           50|
|        137|    Renske|  3600|           50|
|        138|   Stephen|  3200|           50|
|        139|      John|  2700|           50|
|        140|    Joshua|  2500|           50|
+-----------+----------+------+-------------+


>>> # The same can also be written as
>>> df2.filter((col("DEPARTMENT_ID") == 50) & (col("SALARY") < 5000)).select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show()
+-----------+----------+------+-------------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+----------+------+-------------+
|        198|    Donald|  2600|           50|
|        199|   Douglas|  2600|           50|
|        125|     Julia|  3200|           50|
|        126|     Irene|  2700|           50|
|        127|     James|  2400|           50|
|        128|    Steven|  2200|           50|
|        129|     Laura|  3300|           50|
|        130|     Mozhe|  2800|           50|
|        131|     James|  2500|           50|
|        132|        TJ|  2100|           50|
|        133|     Jason|  3300|           50|
|        134|   Michael|  2900|           50|
|        135|        Ki|  2400|           50|
|        136|     Hazel|  2200|           50|
|        137|    Renske|  3600|           50|
|        138|   Stephen|  3200|           50|
|        139|      John|  2700|           50|
|        140|    Joshua|  2500|           50|
+-----------+----------+------+-------------+


>>> # to find employees not working in the department 50
>>> df2.filter(col("DEPARTMENT_ID") != 50).select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show()
+-----------+-----------+------+-------------+
|EMPLOYEE_ID| FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+-----------+------+-------------+
|        200|   Jennifer|  4400|           10|
|        201|    Michael| 13000|           20|
|        202|        Pat|  6000|           20|
|        203|      Susan|  6500|           40|
|        204|    Hermann| 10000|           70|
|        205|    Shelley| 12008|          110|
|        206|    William|  8300|          110|
|        100|     Steven| 24000|           90|
|        101|      Neena| 17000|           90|
|        102|        Lex| 17000|           90|
|        103|  Alexander|  9000|           60|
|        104|      Bruce|  6000|           60|
|        105|      David|  4800|           60|
|        106|      Valli|  4800|           60|
|        107|      Diana|  4200|           60|
|        108|      Nancy| 12008|          100|
|        109|     Daniel|  9000|          100|
|        110|       John|  8200|          100|
|        111|     Ismael|  7700|          100|
|        112|Jose Manuel|  7800|          100|
+-----------+-----------+------+-------------+
only showing top 20 rows


>>> # MYSQL kinf of syntax can also be used for the above operation
>>> df2.filter("DEPARTMENT_ID == 50 AND SALARY < 5000").select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show()
+-----------+----------+------+-------------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+----------+------+-------------+
|        198|    Donald|  2600|           50|
|        199|   Douglas|  2600|           50|
|        125|     Julia|  3200|           50|
|        126|     Irene|  2700|           50|
|        127|     James|  2400|           50|
|        128|    Steven|  2200|           50|
|        129|     Laura|  3300|           50|
|        130|     Mozhe|  2800|           50|
|        131|     James|  2500|           50|
|        132|        TJ|  2100|           50|
|        133|     Jason|  3300|           50|
|        134|   Michael|  2900|           50|
|        135|        Ki|  2400|           50|
|        136|     Hazel|  2200|           50|
|        137|    Renske|  3600|           50|
|        138|   Stephen|  3200|           50|
|        139|      John|  2700|           50|
|        140|    Joshua|  2500|           50|
+-----------+----------+------+-------------+



>>> # Working with duplicates
>>> # Method 1 : The distinct() function will return only the distinct records without any redundant data from the dataFrame
>>> 
>>> df2.distinct().show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        120|   Matthew|    Weiss|  MWEISS|650.123.1234|18-JUL-04|    ST_MAN|  8000|            - |       100|           50|
|        118|       Guy|   Himuro| GHIMURO|515.127.4565|15-NOV-06|  PU_CLERK|  2600|            - |       114|           30|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
|        123|    Shanta|  Vollman|SVOLLMAN|650.123.4234|10-OCT-05|    ST_MAN|  6500|            - |       100|           50|
|        124|     Kevin|  Mourgos|KMOURGOS|650.123.5234|16-NOV-07|    ST_MAN|  5800|            - |       100|           50|
|        137|    Renske|   Ladwig| RLADWIG|650.121.1234|14-JUL-03|  ST_CLERK|  3600|            - |       123|           50|
|        132|        TJ|    Olson| TJOLSON|650.124.8234|10-APR-07|  ST_CLERK|  2100|            - |       121|           50|
|        113|      Luis|     Popp|   LPOPP|515.124.4567|07-DEC-07|FI_ACCOUNT|  6900|            - |       108|          100|
|        139|      John|      Seo|    JSEO|650.121.2019|12-FEB-06|  ST_CLERK|  2700|            - |       123|           50|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        135|        Ki|      Gee|    KGEE|650.127.1734|12-DEC-07|  ST_CLERK|  2400|            - |       122|           50|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        121|      Adam|    Fripp|  AFRIPP|650.123.2234|10-APR-05|    ST_MAN|  8200|            - |       100|           50|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        117|     Sigal|   Tobias| STOBIAS|515.127.4564|24-JUL-05|  PU_CLERK|  2800|            - |       114|           30|
|        130|     Mozhe| Atkinson|MATKINSO|650.124.6234|30-OCT-05|  ST_CLERK|  2800|            - |       121|           50|
|        134|   Michael|   Rogers| MROGERS|650.127.1834|26-AUG-06|  ST_CLERK|  2900|            - |       122|           50|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
only showing top 20 rows

>>> # Method 2 using dropDuplicates() function which checks for ducplicates and drop it. This method becomes useful incase we have to check for duplicates will be checked and dropped in few columns even
>>> df2.dropDuplicates(["DEPARTMENT_ID","HIRE_DATE"]).show()
+-----------+----------+----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME| LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|  JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|        200|  Jennifer|    Whalen| JWHALEN|515.123.4444|17-SEP-03| AD_ASST|  4400|            - |       101|           10|
|        202|       Pat|       Fay|    PFAY|603.123.6666|17-AUG-05|  MK_REP|  6000|            - |       201|           20|
|        201|   Michael| Hartstein|MHARTSTE|515.123.5555|17-FEB-04|  MK_MAN| 13000|            - |       100|           20|
|        114|       Den|  Raphaely|DRAPHEAL|515.127.4561|07-DEC-02|  PU_MAN| 11000|            - |       100|           30|
|        119|     Karen|Colmenares|KCOLMENA|515.127.4566|10-AUG-07|PU_CLERK|  2500|            - |       114|           30|
|        118|       Guy|    Himuro| GHIMURO|515.127.4565|15-NOV-06|PU_CLERK|  2600|            - |       114|           30|
|        115| Alexander|      Khoo|   AKHOO|515.127.4562|18-MAY-03|PU_CLERK|  3100|            - |       114|           30|
|        116|    Shelli|     Baida|  SBAIDA|515.127.4563|24-DEC-05|PU_CLERK|  2900|            - |       114|           30|
|        117|     Sigal|    Tobias| STOBIAS|515.127.4564|24-JUL-05|PU_CLERK|  2800|            - |       114|           30|
|        203|     Susan|    Mavris| SMAVRIS|515.123.7777|07-JUN-02|  HR_REP|  6500|            - |       101|           40|
|        122|     Payam|  Kaufling|PKAUFLIN|650.123.3234|01-MAY-03|  ST_MAN|  7900|            - |       100|           50|
|        140|    Joshua|     Patel|  JPATEL|650.121.1834|06-APR-06|ST_CLERK|  2500|            - |       123|           50|
|        136|     Hazel|Philtanker|HPHILTAN|650.127.1634|06-FEB-08|ST_CLERK|  2200|            - |       122|           50|
|        128|    Steven|    Markle| SMARKLE|650.124.1434|08-MAR-08|ST_CLERK|  2200|            - |       120|           50|
|        121|      Adam|     Fripp|  AFRIPP|650.123.2234|10-APR-05|  ST_MAN|  8200|            - |       100|           50|
|        132|        TJ|     Olson| TJOLSON|650.124.8234|10-APR-07|ST_CLERK|  2100|            - |       121|           50|
|        123|    Shanta|   Vollman|SVOLLMAN|650.123.4234|10-OCT-05|  ST_MAN|  6500|            - |       100|           50|
|        135|        Ki|       Gee|    KGEE|650.127.1734|12-DEC-07|ST_CLERK|  2400|            - |       122|           50|
|        139|      John|       Seo|    JSEO|650.121.2019|12-FEB-06|ST_CLERK|  2700|            - |       123|           50|
|        199|   Douglas|     Grant|  DGRANT|650.507.9844|13-JAN-08|SH_CLERK|  2600|            - |       124|           50|
+-----------+----------+----------+--------+------------+---------+--------+------+--------------+----------+-------------+
only showing top 20 rows




>>> # To use aggregation function, we should import the below library
>>> 
>>> from pyspark.sql.functions import *
>>> 
>>> df2.count()
50

>>> # alias function
>>> df2.select(count("salary").alias("record count")).show()
+------------+
|record count|
+------------+
|          50|
+------------+


>>> # Find the maximum salary in df2
>>> df2.select(max("SALARY").alias("Maximum Salary")).show()
+--------------+
|Maximum Salary|
+--------------+
|         24000|
+--------------+

>>> # find the minimum salary in df2
>>> df2.select(min("SALARY").alias("Miniumum Salary")).show()
+---------------+
|Miniumum Salary|
+---------------+
|           2100|
+---------------+

>>> # average salary in df2
>>> df2.select(avg("SALARY").alias("Average Salary")).show()
+--------------+
|Average Salary|
+--------------+
|       6182.32|
+--------------+

>>> # orderBy() clause in Spark
>>> 
>>> df2.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy("SALARY").show()
+-----------+----------+-------------+------+
|EMPLOYEE_ID|FIRST_NAME|DEPARTMENT_ID|SALARY|
+-----------+----------+-------------+------+
|        132|        TJ|           50|  2100|
|        136|     Hazel|           50|  2200|
|        128|    Steven|           50|  2200|
|        127|     James|           50|  2400|
|        135|        Ki|           50|  2400|
|        131|     James|           50|  2500|
|        119|     Karen|           30|  2500|
|        140|    Joshua|           50|  2500|
|        198|    Donald|           50|  2600|
|        199|   Douglas|           50|  2600|
|        118|       Guy|           30|  2600|
|        126|     Irene|           50|  2700|
|        139|      John|           50|  2700|
|        130|     Mozhe|           50|  2800|
|        117|     Sigal|           30|  2800|
|        116|    Shelli|           30|  2900|
|        134|   Michael|           50|  2900|
|        115| Alexander|           30|  3100|
|        125|     Julia|           50|  3200|
|        138|   Stephen|           50|  3200|
+-----------+----------+-------------+------+
only showing top 20 rows



>>> # OrderBy() in descending order and that too on multiple rows
>>> df2.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy( col("DEPARTMENT_ID").asc(), col("SALARY").desc() ).show()
+-----------+----------+-------------+------+
|EMPLOYEE_ID|FIRST_NAME|DEPARTMENT_ID|SALARY|
+-----------+----------+-------------+------+
|        200|  Jennifer|           10|  4400|
|        201|   Michael|           20| 13000|
|        202|       Pat|           20|  6000|
|        114|       Den|           30| 11000|
|        115| Alexander|           30|  3100|
|        116|    Shelli|           30|  2900|
|        117|     Sigal|           30|  2800|
|        118|       Guy|           30|  2600|
|        119|     Karen|           30|  2500|
|        203|     Susan|           40|  6500|
|        121|      Adam|           50|  8200|
|        120|   Matthew|           50|  8000|
|        122|     Payam|           50|  7900|
|        123|    Shanta|           50|  6500|
|        124|     Kevin|           50|  5800|
|        137|    Renske|           50|  3600|
|        133|     Jason|           50|  3300|
|        129|     Laura|           50|  3300|
|        125|     Julia|           50|  3200|
|        138|   Stephen|           50|  3200|
+-----------+----------+-------------+------+
only showing top 20 rows


>>> # groupBy() in spark
>>> 
>>> df2.groupBy("DEPARTMENT_ID").sum("SALARY").show(100)
+-------------+-----------+
|DEPARTMENT_ID|sum(SALARY)|
+-------------+-----------+
|           20|      19000|
|           40|       6500|
|          100|      51608|
|           10|       4400|
|           50|      85600|
|           70|      10000|
|           90|      58000|
|           60|      28800|
|          110|      20308|
|           30|      24900|
+-------------+-----------+


# groupBy(), aggregation() and orderBy() in one query
>>> df2.groupBy("DEPARTMENT_ID").sum("SALARY").orderBy("DEPARTMENT_ID").show(100)
+-------------+-----------+
|DEPARTMENT_ID|sum(SALARY)|
+-------------+-----------+
|           10|       4400|
|           20|      19000|
|           30|      24900|
|           40|       6500|
|           50|      85600|
|           60|      28800|
|           70|      10000|
|           90|      58000|
|          100|      51608|
|          110|      20308|
+-------------+-----------+



# Another way of sorting using the sorted() function
# using .collect() 
# .collect() returns the output as a collection of rows
>>> sorted(df2.groupBy("DEPARTMENT_ID").sum("SALARY").collect())
[Row(DEPARTMENT_ID=10, sum(SALARY)=4400),
 Row(DEPARTMENT_ID=20, sum(SALARY)=19000), 
 Row(DEPARTMENT_ID=30, sum(SALARY)=24900), 
 Row(DEPARTMENT_ID=40, sum(SALARY)=6500), 
 Row(DEPARTMENT_ID=50, sum(SALARY)=85600), 
 Row(DEPARTMENT_ID=60, sum(SALARY)=28800), 
 Row(DEPARTMENT_ID=70, sum(SALARY)=10000), 
 Row(DEPARTMENT_ID=90, sum(SALARY)=58000), 
 Row(DEPARTMENT_ID=100, sum(SALARY)=51608), 
 Row(DEPARTMENT_ID=110, sum(SALARY)=20308)]



>>> # Using groupBy() on multiple columns
>>> 
>>> df2.groupBy("DEPARTMENT_ID","JOB_ID").sum("SALARY").show()
+-------------+----------+-----------+
|DEPARTMENT_ID|    JOB_ID|sum(SALARY)|
+-------------+----------+-----------+
|           90|   AD_PRES|      24000|
|           30|    PU_MAN|      11000|
|           70|    PR_REP|      10000|
|           50|    ST_MAN|      36400|
|           40|    HR_REP|       6500|
|           60|   IT_PROG|      28800|
|           10|   AD_ASST|       4400|
|           30|  PU_CLERK|      13900|
|           50|  ST_CLERK|      44000|
|           20|    MK_REP|       6000|
|           50|  SH_CLERK|       5200|
|           90|     AD_VP|      34000|
|          100|FI_ACCOUNT|      39600|
|          110|    AC_MGR|      12008|
|          110|AC_ACCOUNT|       8300|
|           20|    MK_MAN|      13000|
|          100|    FI_MGR|      12008|
+-------------+----------+-----------+

# also applying orderBy() on DEPARTMENT_ID column
>>> df2.groupBy("DEPARTMENT_ID","JOB_ID").sum("SALARY").orderBy("DEPARTMENT_ID").show()
+-------------+----------+-----------+
|DEPARTMENT_ID|    JOB_ID|sum(SALARY)|
+-------------+----------+-----------+
|           10|   AD_ASST|       4400|
|           20|    MK_REP|       6000|
|           20|    MK_MAN|      13000|
|           30|  PU_CLERK|      13900|
|           30|    PU_MAN|      11000|
|           40|    HR_REP|       6500|
|           50|    ST_MAN|      36400|
|           50|  ST_CLERK|      44000|
|           50|  SH_CLERK|       5200|
|           60|   IT_PROG|      28800|
|           70|    PR_REP|      10000|
|           90|   AD_PRES|      24000|
|           90|     AD_VP|      34000|
|          100|    FI_MGR|      12008|
|          100|FI_ACCOUNT|      39600|
|          110|    AC_MGR|      12008|
|          110|AC_ACCOUNT|       8300|
+-------------+----------+-----------+

>>> 
>>> # to use different aggregations after groupBy() 
>>> df2.groupBy("DEPARTMENT_ID","JOB_ID").agg(sum("SALARY").alias("SUM_OF_SALARY"), max("SALARY").alias("MAXIMUM_SALARY"), 
min("SALARY").alias("MIN_SALARY"), avg("SALARY").alias("AVERAGE_SALARY")).orderBy("DEPARTMENT_ID").show()
+-------------+----------+-------------+--------------+----------+--------------+
|DEPARTMENT_ID|    JOB_ID|SUM_OF_SALARY|MAXIMUM_SALARY|MIN_SALARY|AVERAGE_SALARY|
+-------------+----------+-------------+--------------+----------+--------------+
|           10|   AD_ASST|         4400|          4400|      4400|        4400.0|
|           20|    MK_REP|         6000|          6000|      6000|        6000.0|
|           20|    MK_MAN|        13000|         13000|     13000|       13000.0|
|           30|  PU_CLERK|        13900|          3100|      2500|        2780.0|
|           30|    PU_MAN|        11000|         11000|     11000|       11000.0|
|           40|    HR_REP|         6500|          6500|      6500|        6500.0|
|           50|    ST_MAN|        36400|          8200|      5800|        7280.0|
|           50|  ST_CLERK|        44000|          3600|      2100|        2750.0|
|           50|  SH_CLERK|         5200|          2600|      2600|        2600.0|
|           60|   IT_PROG|        28800|          9000|      4200|        5760.0|
|           70|    PR_REP|        10000|         10000|     10000|       10000.0|
|           90|   AD_PRES|        24000|         24000|     24000|       24000.0|
|           90|     AD_VP|        34000|         17000|     17000|       17000.0|
|          100|    FI_MGR|        12008|         12008|     12008|       12008.0|
|          100|FI_ACCOUNT|        39600|          9000|      6900|        7920.0|
|          110|    AC_MGR|        12008|         12008|     12008|       12008.0|
|          110|AC_ACCOUNT|         8300|          8300|      8300|        8300.0|
+-------------+----------+-------------+--------------+----------+--------------+


>>> # also using a filter condition on the query
>>> # filter() and where() is same in pyspark 

>>> df2.groupBy("DEPARTMENT_ID","JOB_ID").agg(sum("SALARY").alias("SUM_OF_SALARY"), max("SALARY").alias("MAXIMUM_SALARY"), 
min("SALARY").alias("MIN_SALARY"), avg("SALARY").alias("AVERAGE_SALARY")).orderBy("DEPARTMENT_ID")
.filter(col("MAXIMUM_SALARY") >= 10000).show()
+-------------+-------+-------------+--------------+----------+--------------+
|DEPARTMENT_ID| JOB_ID|SUM_OF_SALARY|MAXIMUM_SALARY|MIN_SALARY|AVERAGE_SALARY|
+-------------+-------+-------------+--------------+----------+--------------+
|           20| MK_MAN|        13000|         13000|     13000|       13000.0|
|           30| PU_MAN|        11000|         11000|     11000|       11000.0|
|           70| PR_REP|        10000|         10000|     10000|       10000.0|
|           90|  AD_VP|        34000|         17000|     17000|       17000.0|
|           90|AD_PRES|        24000|         24000|     24000|       24000.0|
|          100| FI_MGR|        12008|         12008|     12008|       12008.0|
|          110| AC_MGR|        12008|         12008|     12008|       12008.0|
+-------------+-------+-------------+--------------+----------+--------------+


>>> # Using a case when statement in pyspark
>>> 
>>> # Deriving a new DataFrame out of df2 along with the case when column included

>>> df3 = df2.withColumn("EMPLOYEE_GRADE", when( col("SALARY") >= 15000, "A GRADE").when( (col("SALARY") >= 10000) & (col("SALARY") < 15000), "B GRADE").otherwise("C GRADE"))
>>> df3.show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+--------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|EMPLOYEE_GRADE|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+--------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|       C GRADE|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|       C GRADE|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|       C GRADE|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|       B GRADE|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|       C GRADE|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|       C GRADE|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|       B GRADE|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|       B GRADE|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|       C GRADE|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|       A GRADE|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|       A GRADE|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|       A GRADE|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|       C GRADE|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|       C GRADE|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|       C GRADE|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|       C GRADE|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|       C GRADE|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|       B GRADE|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|       C GRADE|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|       C GRADE|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+--------------+
only showing top 20 rows

>>> df3.select("EMPLOYEE_ID","FIRST_NAME","EMPLOYEE_GRADE").show()
+-----------+----------+--------------+
|EMPLOYEE_ID|FIRST_NAME|EMPLOYEE_GRADE|
+-----------+----------+--------------+
|        198|    Donald|       C GRADE|
|        199|   Douglas|       C GRADE|
|        200|  Jennifer|       C GRADE|
|        201|   Michael|       B GRADE|
|        202|       Pat|       C GRADE|
|        203|     Susan|       C GRADE|
|        204|   Hermann|       B GRADE|
|        205|   Shelley|       B GRADE|
|        206|   William|       C GRADE|
|        100|    Steven|       A GRADE|
|        101|     Neena|       A GRADE|
|        102|       Lex|       A GRADE|
|        103| Alexander|       C GRADE|
|        104|     Bruce|       C GRADE|
|        105|     David|       C GRADE|
|        106|     Valli|       C GRADE|
|        107|     Diana|       C GRADE|
|        108|     Nancy|       B GRADE|
|        109|    Daniel|       C GRADE|
|        110|      John|       C GRADE|
+-----------+----------+--------------+
only showing top 20 rows


>>> # JOINS IN PYSPARK

>>> df2.join(df1, df2.DEPARTMENT_ID == df1.DEPARTMENT_ID, "INNER").show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+-------------+----------------+----------+-----------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|DEPARTMENT_ID| DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+-------------+----------------+----------+-----------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|           50|        Shipping|       121|       1500|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|           50|        Shipping|       121|       1500|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|           10|  Administration|       200|       1700|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|           20|       Marketing|       201|       1800|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|           20|       Marketing|       201|       1800|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|           40| Human Resources|       203|       2400|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|           70|Public Relations|       204|       2700|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|          110|      Accounting|       205|       1700|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|          110|      Accounting|       205|       1700|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|           90|       Executive|       100|       1700|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|           90|       Executive|       100|       1700|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|           90|       Executive|       100|       1700|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|           60|              IT|       103|       1400|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|           60|              IT|       103|       1400|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|           60|              IT|       103|       1400|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|           60|              IT|       103|       1400|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|           60|              IT|       103|       1400|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|          100|         Finance|       108|       1700|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|          100|         Finance|       108|       1700|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|          100|         Finance|       108|       1700|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+-------------+----------------+----------+-----------+
only showing top 20 rows


>>> df2.join(df1, df2.DEPARTMENT_ID == df1.DEPARTMENT_ID, "INNER").select(df2.EMPLOYEE_ID, 
df2.FIRST_NAME, df2.DEPARTMENT_ID, df1.DEPARTMENT_NAME).show()
+-----------+----------+-------------+----------------+
|EMPLOYEE_ID|FIRST_NAME|DEPARTMENT_ID| DEPARTMENT_NAME|
+-----------+----------+-------------+----------------+
|        198|    Donald|           50|        Shipping|
|        199|   Douglas|           50|        Shipping|
|        200|  Jennifer|           10|  Administration|
|        201|   Michael|           20|       Marketing|
|        202|       Pat|           20|       Marketing|
|        203|     Susan|           40| Human Resources|
|        204|   Hermann|           70|Public Relations|
|        205|   Shelley|          110|      Accounting|
|        206|   William|          110|      Accounting|
|        100|    Steven|           90|       Executive|
|        101|     Neena|           90|       Executive|
|        102|       Lex|           90|       Executive|
|        103| Alexander|           60|              IT|
|        104|     Bruce|           60|              IT|
|        105|     David|           60|              IT|
|        106|     Valli|           60|              IT|
|        107|     Diana|           60|              IT|
|        108|     Nancy|          100|         Finance|
|        109|    Daniel|          100|         Finance|
|        110|      John|          100|         Finance|
+-----------+----------+-------------+----------------+
only showing top 20 rows



>>> # FULL OUTER JOIN

>>> df2.join(df1, df2.DEPARTMENT_ID == df1.DEPARTMENT_ID, "fullouter").select(df2.EMPLOYEE_ID, df2.FIRST_NAME, df2.DEPARTMENT_ID, df1.DEPARTMENT_NAME).show()
+-----------+----------+-------------+---------------+
|EMPLOYEE_ID|FIRST_NAME|DEPARTMENT_ID|DEPARTMENT_NAME|
+-----------+----------+-------------+---------------+
|        200|  Jennifer|           10| Administration|
|        201|   Michael|           20|      Marketing|
|        202|       Pat|           20|      Marketing|
|        114|       Den|           30|     Purchasing|
|        115| Alexander|           30|     Purchasing|
|        116|    Shelli|           30|     Purchasing|
|        117|     Sigal|           30|     Purchasing|
|        118|       Guy|           30|     Purchasing|
|        119|     Karen|           30|     Purchasing|
|        203|     Susan|           40|Human Resources|
|        198|    Donald|           50|       Shipping|
|        199|   Douglas|           50|       Shipping|
|        120|   Matthew|           50|       Shipping|
|        121|      Adam|           50|       Shipping|
|        122|     Payam|           50|       Shipping|
|        123|    Shanta|           50|       Shipping|
|        124|     Kevin|           50|       Shipping|
|        125|     Julia|           50|       Shipping|
|        126|     Irene|           50|       Shipping|
|        127|     James|           50|       Shipping|
+-----------+----------+-------------+---------------+
only showing top 20 rows


>>> # Find the name of employees who are managers themselves
>>> # We do self inner join on the df2 dataframe to find the employees who are managers themselves.

>>> df2.alias("emp1").join(df2.alias("emp2"), col("emp1.MANAGER_ID") == col("emp2.EMPLOYEE_ID"),"INNER").select(col("emp1.MANAGER_ID"), col("emp2.FIRST_NAME"), col("emp2.LAST_NAME")).show(100)
+----------+----------+---------+
|MANAGER_ID|FIRST_NAME|LAST_NAME|
+----------+----------+---------+
|       201|   Michael|Hartstein|
|       205|   Shelley|  Higgins|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       102|       Lex|  De Haan|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       124|     Kevin|  Mourgos|
|       124|     Kevin|  Mourgos|
+----------+----------+---------+

>>> df2.alias("emp1").join(df2.alias("emp2"), col("emp1.MANAGER_ID") == col("emp2.EMPLOYEE_ID"),"INNER").select(col("emp1.MANAGER_ID"), col("emp2.FIRST_NAME"), col("emp2.LAST_NAME")).dropDuplicates().orderBy("emp1.MANAGER_ID").show(100)
+----------+----------+---------+
|MANAGER_ID|FIRST_NAME|LAST_NAME|
+----------+----------+---------+
|       100|    Steven|     King|
|       101|     Neena|  Kochhar|
|       102|       Lex|  De Haan|
|       103| Alexander|   Hunold|
|       108|     Nancy|Greenberg|
|       114|       Den| Raphaely|
|       120|   Matthew|    Weiss|
|       121|      Adam|    Fripp|
|       122|     Payam| Kaufling|
|       123|    Shanta|  Vollman|
|       124|     Kevin|  Mourgos|
|       201|   Michael|Hartstein|
|       205|   Shelley|  Higgins|
+----------+----------+---------+




# Join df2 with df1 and filter by condition that the location_id = 1700

>>> 
>>> df2.join(df1, (df2.DEPARTMENT_ID == df1.DEPARTMENT_ID) & (df1.LOCATION_ID == 1700), "inner").select(df2.EMPLOYEE_ID, df2.FIRST_NAME, df2.DEPARTMENT_ID, df1.DEPARTMENT_NAME, df1.LOCATION_ID).show(100)
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




>>> # MULTIPLE JOINS IN PYSPARK
>>> 
>>> # Creating a new DataFrame to do a multiple join
>>> 
>>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType
>>> location_data = [(1700, "INDIA"), (1800, "USA")]
>>> schema = StructType([StructField ("LOCATION_ID", IntegerType(),True), StructField("LOCATION_NAME", StringType(),True)])
>>> locDF = spark.createDataFrame(data=location_data, schema = schema)
>>> locDF.printSchema()
root
 |-- LOCATION_ID: integer (nullable = true)
 |-- LOCATION_NAME: string (nullable = true)

>>> locDF.show()
+-----------+-------------+                                                     
|LOCATION_ID|LOCATION_NAME|
+-----------+-------------+
|       1700|        INDIA|
|       1800|          USA|
+-----------+-------------+

>>> 