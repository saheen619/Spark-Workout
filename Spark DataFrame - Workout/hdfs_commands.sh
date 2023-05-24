abc@b21941d8ee62:~/workspace$ ls
demo.py  departments.csv  employees.csv  README.md  requirements.txt

abc@b21941d8ee62:~/workspace$ hdfs dfs -ls /
abc@b21941d8ee62:~/workspace$ hadoop fs -ls /
abc@b21941d8ee62:~/workspace$ hdfs dfs -mkdir /input_data
abc@b21941d8ee62:~/workspace$ hdfs dfs -ls /
Found 1 items
drwxr-xr-x   - abc supergroup          0 2023-05-24 22:42 /input_data

abc@b21941d8ee62:~/workspace$ hdfs dfs -put departments.csv /input_data/
2023-05-24 22:44:01,717 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
abc@b21941d8ee62:~/workspace$ hdfs dfs -ls /
Found 1 items
drwxr-xr-x   - abc supergroup          0 2023-05-24 22:44 /input_data
abc@b21941d8ee62:~/workspace$ hdfs dfs -ls /input_data
Found 1 items
-rw-r--r--   1 abc supergroup        709 2023-05-24 22:44 /input_data/departments.csv
abc@b21941d8ee62:~/workspace$ hdfs dfs -put employees.csv /input_data/
2023-05-24 22:44:53,287 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
abc@b21941d8ee62:~/workspace$ hdfs dfs -ls /input_data
Found 2 items
-rw-r--r--   1 abc supergroup        709 2023-05-24 22:44 /input_data/departments.csv
-rw-r--r--   1 abc supergroup       3778 2023-05-24 22:44 /input_data/employees.csv
abc@b21941d8ee62:~/workspace$ 