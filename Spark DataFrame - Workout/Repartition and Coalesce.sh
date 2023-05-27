>>> # To know the partition info on any particular dataframe, We have to convert the dataFrame to RDD
>>> # This is because RDD is the default data structure in spark and we have to use the RDD's class to get partitiion info
>>> 
>>> resultDf.rdd.getNumPartitions()
1
>>> df2.rdd.getNumPartitions()
1
>>> df1.rdd.getNumPartitions()
1
>>> # Here, all the data we used were of very small size and we didnt do any partitoning on it.
>>> # Now to partition,
>>> 
>>> resultDf.repartition(10)
DataFrame[EMPLOYEE_ID: int, FIRST_NAME: string, DEPARTMENT_ID: int, DEPARTMENT_NAME: string, LOCATION_ID: int]
>>> 
>>> resultDf.rdd.getNumPartitions()
1
>>> # The number of partitioning didnt change, because you know that these changes are not persistant in spark.
>>> # Thus we have to assign this to a new DataFrame
>>> 
>>> newDf = resultDf.repartition(10)
>>> newDf.rdd.getNumPartitions()
10
>>> 
>>> 
>>> # Now, to reduce the number of partitions, we can either use repartition() or coalesce()
>>> 
>>> newDf1 = newDf.repartition(6)
>>> newDf1.rdd.getNumPartitions()
6
>>> # Here, in the repartition() the data is getting full shuffled, thus is expensive. so its suggested to use coalesce() instead 
>>> # which is less expensive
>>> 
>>> newDf2 = newDf.coalesce(5)
>>> newDf2.rdd.getNumPartitions()
5
>>> 