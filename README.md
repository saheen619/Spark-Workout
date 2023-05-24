# Spark Workout

### 1. Validate if hadoop is working

Open terminal and execute below command

1. Create a sample.txt file with content "This is hadoop and spark lab."
```
echo "This is hadoop and spark lab">sample.txt
```

2. Upload your sample.txt file to Hadoop file system
```
hdfs dfs -put sample.txt /
```

3. List uploaded file in hadoop 
```
hdfs dfs -ls /
```

### 2. Validate if pyspark is working

Execute below command
```
pyspark
```
Above command will launch pyspark terminal and you can execute pyspark command in interactive shell.

Close your terminal

### 3. Now let's run a script file "demo.py"

Execute the spark script. 
```
python demo.py
```

## URL to view data in UI:

Spark Master - <your_app_name>.ineuron.app:8080

History Server - <your_app_name>.ineuron.app:18080

Spark Worker Node - <your_app_name>.ineuron.app:8081