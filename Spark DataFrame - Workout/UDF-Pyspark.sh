# A regular user defined function in python :

>>> def upperCase(in_str):
...     out_str = in_str.upper()
...     return out_str
... 
>>> print(upperCase("hello this is saheen ahzan"))
HELLO THIS IS SAHEEN AHZAN
>>> 


# To have a UDF in pyspark,

>>> from pyspark.sql.functions import *
>>> upperCaseUDF = udf(lambda z : upperCase(z) , StringType())
>>> 
>>> df2.select(col("EMPLOYEE_ID"), col("FIRST_NAME"), col("LAST_NAME"), upperCaseUDF(col("FIRST_NAME")), upperCaseUDF(col("LAST_NAME"))).show()
+-----------+----------+---------+--------------------+-------------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|<lambda>(FIRST_NAME)|<lambda>(LAST_NAME)|
+-----------+----------+---------+--------------------+-------------------+
|        198|    Donald| OConnell|              DONALD|           OCONNELL|
|        199|   Douglas|    Grant|             DOUGLAS|              GRANT|
|        200|  Jennifer|   Whalen|            JENNIFER|             WHALEN|
|        201|   Michael|Hartstein|             MICHAEL|          HARTSTEIN|
|        202|       Pat|      Fay|                 PAT|                FAY|
|        203|     Susan|   Mavris|               SUSAN|             MAVRIS|
|        204|   Hermann|     Baer|             HERMANN|               BAER|
|        205|   Shelley|  Higgins|             SHELLEY|            HIGGINS|
|        206|   William|    Gietz|             WILLIAM|              GIETZ|
|        100|    Steven|     King|              STEVEN|               KING|
|        101|     Neena|  Kochhar|               NEENA|            KOCHHAR|
|        102|       Lex|  De Haan|                 LEX|            DE HAAN|
|        103| Alexander|   Hunold|           ALEXANDER|             HUNOLD|
|        104|     Bruce|    Ernst|               BRUCE|              ERNST|
|        105|     David|   Austin|               DAVID|             AUSTIN|
|        106|     Valli|Pataballa|               VALLI|          PATABALLA|
|        107|     Diana|  Lorentz|               DIANA|            LORENTZ|
|        108|     Nancy|Greenberg|               NANCY|          GREENBERG|
|        109|    Daniel|   Faviet|              DANIEL|             FAVIET|
|        110|      John|     Chen|                JOHN|               CHEN|
+-----------+----------+---------+--------------------+-------------------+
only showing top 20 rows

>>> 

