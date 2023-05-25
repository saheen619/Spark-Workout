abc@3a9cd9a16407:~/workspace$ pyspark

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
>>> # Now we call the spark session and then query in sql over the temporary view
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



