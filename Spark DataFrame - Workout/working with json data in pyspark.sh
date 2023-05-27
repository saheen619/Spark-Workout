>>> 
>>> jsonDf = spark.read.json("/jsonexample.json")
>>> jsonDf.show()
+------------+----+-----+-------+
|      Array1|Num1|Text1|  Text2|
+------------+----+-----+-------+
|   [7, 8, 9]| 5.0|Hello|GoodBye|
|[70, 88, 91]| 6.5| This|   That|
|   [1, 2, 3]| 2.0|  Yes|     No|
+------------+----+-----+-------+

>>> jsonDf.printSchema()
root
 |-- Array1: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- Num1: double (nullable = true)
 |-- Text1: string (nullable = true)
 |-- Text2: string (nullable = true)


>>> jsonDf.select(jsonDf.Text1, jsonDf.Array1).show()
+-----+------------+
|Text1|      Array1|
+-----+------------+
|Hello|   [7, 8, 9]|
| This|[70, 88, 91]|
|  Yes|   [1, 2, 3]|
+-----+------------+

>>> jsonDf.select(jsonDf.Text1, jsonDf.Array1, jsonDf.Array1[0], jsonDf.Array1[1], jsonDf.Array1[2]).show()
+-----+------------+---------+---------+---------+
|Text1|      Array1|Array1[0]|Array1[1]|Array1[2]|
+-----+------------+---------+---------+---------+
|Hello|   [7, 8, 9]|        7|        8|        9|
| This|[70, 88, 91]|       70|       88|       91|
|  Yes|   [1, 2, 3]|        1|        2|        3|
+-----+------------+---------+---------+---------+

>>> 
>>> 
>>> # To have this Array1 json content as different records in different rows, we can use the explode method
>>> 
>>> jsonDf.select(jsonDf.Text1, explode(jsonDf.Array1)).show()
+-----+---+
|Text1|col|
+-----+---+
|Hello|  7|
|Hello|  8|
|Hello|  9|
| This| 70|
| This| 88|
| This| 91|
|  Yes|  1|
|  Yes|  2|
|  Yes|  3|
+-----+---+

>>> # The above DF can be used for further analysis using individual records from the json object
>>> # The exploade method helps us to flatten the data out of the json object and then work over it.
>>> 
>>> 