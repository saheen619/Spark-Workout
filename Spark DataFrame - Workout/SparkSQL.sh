abc@3a9cd9a16407:~/workspace$ pyspark
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

>>> df1 = spark.read.option("header",True).option("inferSchema",True).csv("/departments.csv")
>>> df2 = spark.read.option("header",True).option("inferSchema",True).csv("/employees.csv")
>>> 
>>> 
>>> df1.head()
Row(DEPARTMENT_ID=10, DEPARTMENT_NAME='Administration', MANAGER_ID='200', LOCATION_ID=1700)
>>> 
>>> df2.head()
Row(EMPLOYEE_ID=198, FIRST_NAME='Donald', LAST_NAME='OConnell', EMAIL='DOCONNEL', PHONE_NUMBER='650.507.9833', HIRE_DATE='21-JUN-07', JOB_ID='SH_CLERK', SALARY=2600, COMMISSION_PCT=' - ', MANAGER_ID='124', DEPARTMENT_ID=50)
>>> 
>>> 
>>> # To use spark.sql on a dataframe, first we need to conmvert that DataFrame into a Table and then query in sql on top of it.
>>> 
>>> df1.createOrReplaceTempView("departments")
>>> df2.createOrReplaceTempView("employees")
>>> 
>>> # Now we call the spark session and then use the .sql method to query in sql over the temporary view
>>> 
>>> spark.sql("SELECT * FROM employees").show()
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

>>> spark.sql("SELECT * FROM departments").show()
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


>>> # Using a case when statement in sql
>>> spark.sql("SELECT employee_id, first_name, last_name, department_id, CASE WHEN salary >= 15000 THEN 'GRADE A' 
WHEN salary >= 10000 AND salary < 15000 THEN 'GRADE B' ELSE 'GRADE C' END AS employee_grade FROM employees").show()
+-----------+----------+---------+-------------+--------------+
|employee_id|first_name|last_name|department_id|employee_grade|
+-----------+----------+---------+-------------+--------------+
|        198|    Donald| OConnell|           50|       GRADE C|
|        199|   Douglas|    Grant|           50|       GRADE C|
|        200|  Jennifer|   Whalen|           10|       GRADE C|
|        201|   Michael|Hartstein|           20|       GRADE B|
|        202|       Pat|      Fay|           20|       GRADE C|
|        203|     Susan|   Mavris|           40|       GRADE C|
|        204|   Hermann|     Baer|           70|       GRADE B|
|        205|   Shelley|  Higgins|          110|       GRADE B|
|        206|   William|    Gietz|          110|       GRADE C|
|        100|    Steven|     King|           90|       GRADE A|
|        101|     Neena|  Kochhar|           90|       GRADE A|
|        102|       Lex|  De Haan|           90|       GRADE A|
|        103| Alexander|   Hunold|           60|       GRADE C|
|        104|     Bruce|    Ernst|           60|       GRADE C|
|        105|     David|   Austin|           60|       GRADE C|
|        106|     Valli|Pataballa|           60|       GRADE C|
|        107|     Diana|  Lorentz|           60|       GRADE C|
|        108|     Nancy|Greenberg|          100|       GRADE B|
|        109|    Daniel|   Faviet|          100|       GRADE C|
|        110|      John|     Chen|          100|       GRADE C|
+-----------+----------+---------+-------------+--------------+
only showing top 20 rows




# USing window functions

>>> spark.sql("SELECT employee_id, department_id, RANK() OVER(PARTITION BY department_id ORDER BY salary DESC) AS salary_rank 
FROM employees").show(100)
+-----------+-------------+-----------+
|employee_id|department_id|salary_rank|
+-----------+-------------+-----------+
|        200|           10|          1|
|        201|           20|          1|
|        202|           20|          2|
|        114|           30|          1|
|        115|           30|          2|
|        116|           30|          3|
|        117|           30|          4|
|        118|           30|          5|
|        119|           30|          6|
|        203|           40|          1|
|        121|           50|          1|
|        120|           50|          2|
|        122|           50|          3|
|        123|           50|          4|
|        124|           50|          5|
|        137|           50|          6|
|        129|           50|          7|
|        133|           50|          7|
|        125|           50|          9|
|        138|           50|          9|
|        134|           50|         11|
|        130|           50|         12|
|        126|           50|         13|
|        139|           50|         13|
|        198|           50|         15|
|        199|           50|         15|
|        131|           50|         17|
|        140|           50|         17|
|        127|           50|         19|
|        135|           50|         19|
|        128|           50|         21|
|        136|           50|         21|
|        132|           50|         23|
|        103|           60|          1|
|        104|           60|          2|
|        105|           60|          3|
|        106|           60|          3|
|        107|           60|          5|
|        204|           70|          1|
|        100|           90|          1|
|        101|           90|          2|
|        102|           90|          2|
|        108|          100|          1|
|        109|          100|          2|
|        110|          100|          3|
|        112|          100|          4|
|        111|          100|          5|
|        113|          100|          6|
|        205|          110|          1|
|        206|          110|          2|
+-----------+-------------+-----------+



>>> # Using JOINS

>>> spark.sql("SELECT e.employee_id, e.first_name, e.job_id, e.salary, d.* FROM employees e JOIN departments d 
ON e.department_id = d.department_id").show(100)
+-----------+-----------+----------+------+-------------+----------------+----------+-----------+
|employee_id| first_name|    job_id|salary|DEPARTMENT_ID| DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-----------+-----------+----------+------+-------------+----------------+----------+-----------+
|        198|     Donald|  SH_CLERK|  2600|           50|        Shipping|       121|       1500|
|        199|    Douglas|  SH_CLERK|  2600|           50|        Shipping|       121|       1500|
|        200|   Jennifer|   AD_ASST|  4400|           10|  Administration|       200|       1700|
|        201|    Michael|    MK_MAN| 13000|           20|       Marketing|       201|       1800|
|        202|        Pat|    MK_REP|  6000|           20|       Marketing|       201|       1800|
|        203|      Susan|    HR_REP|  6500|           40| Human Resources|       203|       2400|
|        204|    Hermann|    PR_REP| 10000|           70|Public Relations|       204|       2700|
|        205|    Shelley|    AC_MGR| 12008|          110|      Accounting|       205|       1700|
|        206|    William|AC_ACCOUNT|  8300|          110|      Accounting|       205|       1700|
|        100|     Steven|   AD_PRES| 24000|           90|       Executive|       100|       1700|
|        101|      Neena|     AD_VP| 17000|           90|       Executive|       100|       1700|
|        102|        Lex|     AD_VP| 17000|           90|       Executive|       100|       1700|
|        103|  Alexander|   IT_PROG|  9000|           60|              IT|       103|       1400|
|        104|      Bruce|   IT_PROG|  6000|           60|              IT|       103|       1400|
|        105|      David|   IT_PROG|  4800|           60|              IT|       103|       1400|
|        106|      Valli|   IT_PROG|  4800|           60|              IT|       103|       1400|
|        107|      Diana|   IT_PROG|  4200|           60|              IT|       103|       1400|
|        108|      Nancy|    FI_MGR| 12008|          100|         Finance|       108|       1700|
|        109|     Daniel|FI_ACCOUNT|  9000|          100|         Finance|       108|       1700|
|        110|       John|FI_ACCOUNT|  8200|          100|         Finance|       108|       1700|
|        111|     Ismael|FI_ACCOUNT|  7700|          100|         Finance|       108|       1700|
|        112|Jose Manuel|FI_ACCOUNT|  7800|          100|         Finance|       108|       1700|
|        113|       Luis|FI_ACCOUNT|  6900|          100|         Finance|       108|       1700|
|        114|        Den|    PU_MAN| 11000|           30|      Purchasing|       114|       1700|
|        115|  Alexander|  PU_CLERK|  3100|           30|      Purchasing|       114|       1700|
|        116|     Shelli|  PU_CLERK|  2900|           30|      Purchasing|       114|       1700|
|        117|      Sigal|  PU_CLERK|  2800|           30|      Purchasing|       114|       1700|
|        118|        Guy|  PU_CLERK|  2600|           30|      Purchasing|       114|       1700|
|        119|      Karen|  PU_CLERK|  2500|           30|      Purchasing|       114|       1700|
|        120|    Matthew|    ST_MAN|  8000|           50|        Shipping|       121|       1500|
|        121|       Adam|    ST_MAN|  8200|           50|        Shipping|       121|       1500|
|        122|      Payam|    ST_MAN|  7900|           50|        Shipping|       121|       1500|
|        123|     Shanta|    ST_MAN|  6500|           50|        Shipping|       121|       1500|
|        124|      Kevin|    ST_MAN|  5800|           50|        Shipping|       121|       1500|
|        125|      Julia|  ST_CLERK|  3200|           50|        Shipping|       121|       1500|
|        126|      Irene|  ST_CLERK|  2700|           50|        Shipping|       121|       1500|
|        127|      James|  ST_CLERK|  2400|           50|        Shipping|       121|       1500|
|        128|     Steven|  ST_CLERK|  2200|           50|        Shipping|       121|       1500|
|        129|      Laura|  ST_CLERK|  3300|           50|        Shipping|       121|       1500|
|        130|      Mozhe|  ST_CLERK|  2800|           50|        Shipping|       121|       1500|
|        131|      James|  ST_CLERK|  2500|           50|        Shipping|       121|       1500|
|        132|         TJ|  ST_CLERK|  2100|           50|        Shipping|       121|       1500|
|        133|      Jason|  ST_CLERK|  3300|           50|        Shipping|       121|       1500|
|        134|    Michael|  ST_CLERK|  2900|           50|        Shipping|       121|       1500|
|        135|         Ki|  ST_CLERK|  2400|           50|        Shipping|       121|       1500|
|        136|      Hazel|  ST_CLERK|  2200|           50|        Shipping|       121|       1500|
|        137|     Renske|  ST_CLERK|  3600|           50|        Shipping|       121|       1500|
|        138|    Stephen|  ST_CLERK|  3200|           50|        Shipping|       121|       1500|
|        139|       John|  ST_CLERK|  2700|           50|        Shipping|       121|       1500|
|        140|     Joshua|  ST_CLERK|  2500|           50|        Shipping|       121|       1500|
+-----------+-----------+----------+------+-------------+----------------+----------+-----------+


