# EDVai_test
Probando GitHub

Actualizacion: vamos a probar el primer Commit

##Vamos a cargar las instrucciones vistas en la clase 3
plchiapero@LAPTOP-HJNIQGOK:~$ docker run --name edvai_hadoop -p 8081:8081 -p 8080:8080 -p 8088:8088 -p 8889:8889 -p 9870:9870 -p 9868:9868 -p 9864:9864 -p 1527:1527 -p 10000:10000 -p 10002:10002 -p 8010:8010 -p 9093:9093 -p 2181:2182 -it --restart unless-stopped fedepineyro/edvai_ubuntu:v6 /bin/bash -c "/home/hadoop/scripts/start-services.sh"
 * Starting OpenBSD Secure Shell server sshd                                                                     [ OK ]
 * Starting PostgreSQL 12 database server                                                                        [ OK ]
Starting namenodes on [7b946e8f971d]
7b946e8f971d: Warning: Permanently added '7b946e8f971d' (ECDSA) to the list of known hosts.
Starting datanodes
Starting secondary namenodes [7b946e8f971d]
Starting resourcemanager
Starting nodemanagers
starting org.apache.spark.deploy.master.Master, logging to /home/hadoop/spark/logs/spark-hadoop-org.apache.spark.deploy.master.Master-1-7b946e8f971d.out
starting org.apache.spark.deploy.worker.Worker, logging to /home/hadoop/spark/logs/spark-hadoop-org.apache.spark.deploy.worker.Worker-1-7b946e8f971d.out
nohup: appending output to 'nohup.out'
nohup: appending output to 'nohup.out'
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
Running the Gunicorn Server with:
Workers: 4 sync
Host: 0.0.0.0:8010
Timeout: 120
Logfiles: - -
Access Logformat:
=================================================================
[2024-08-12 18:08:20,856] {dagbag.py:507} INFO - Filling up the DagBag from /dev/null
[2024-08-12 18:08:21,220] {manager.py:585} INFO - Removed Permission menu access on Permissions to role Admin
[2024-08-12 18:08:21,262] {manager.py:543} INFO - Removed Permission View: menu_access on Permissions
[2024-08-12 18:08:21,409] {manager.py:508} INFO - Created Permission View: menu access on Permissions
[2024-08-12 18:08:21,428] {manager.py:568} INFO - Added Permission menu access on Permissions to role Admin
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
hadoop@7b946e8f971d:/$ docker exec -it edvai_hadoop bash
bash: docker: command not found
hadoop@7b946e8f971d:/$ pwd
/
hadoop@7b946e8f971d:/$ docker exec -it edvai_hadoop bash
bash: docker: command not found
hadoop@7b946e8f971d:/$ docker exec -it edvai_hadoop bash
bash: docker: command not found
hadoop@7b946e8f971d:/$ docker ps
bash: docker: command not found
hadoop@7b946e8f971d:/$ docker
bash: docker: command not found
hadoop@7b946e8f971d:/$ ls
bin   dev  home  lib32  libx32  mnt  proc  run   srv  tmp  var
boot  etc  lib   lib64  media   opt  root  sbin  sys  usr
hadoop@7b946e8f971d:/$ docker exec -it edvai_hadoop bash
bash: docker: command not found
hadoop@7b946e8f971d:/$ hdfs dfs -ls /
Found 6 items
drwxr-xr-x   - hadoop supergroup          0 2022-05-09 17:58 /ingest
drwxr-xr-x   - hadoop supergroup          0 2022-04-26 19:51 /inputs
drwxr-xr-x   - hadoop supergroup          0 2022-01-22 21:35 /logs
drwxr-xr-x   - hadoop supergroup          0 2023-03-06 11:05 /sqoop
drwxrwxr-x   - hadoop supergroup          0 2022-05-02 20:46 /tmp
drwxr-xr-x   - hadoop supergroup          0 2022-01-23 13:15 /user
hadoop@7b946e8f971d:/$ cd /home/hadoop/landing
hadoop@7b946e8f971d:~/landing$ ls
yellow_tripdata_2021-01.csv
hadoop@7b946e8f971d:~/landing$ ls
yellow_tripdata_2021-01.csv
hadoop@7b946e8f971d:~/landing$ ls
yellow_tripdata_2021-01.csv
hadoop@7b946e8f971d:~/landing$ hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest
put: `/ingest/yellow_tripdata_2021-01.csv': File exists
hadoop@7b946e8f971d:~/landing$ hdfs dfs ls
ls: Unknown command
Did you mean -ls?  This command begins with a dash.
Usage: hadoop fs [generic options]
        [-appendToFile <localsrc> ... <dst>]
        [-cat [-ignoreCrc] <src> ...]
        [-checksum [-v] <src> ...]
        [-chgrp [-R] GROUP PATH...]
        [-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...]
        [-chown [-R] [OWNER][:[GROUP]] PATH...]
        [-copyFromLocal [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>]
        [-copyToLocal [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
        [-count [-q] [-h] [-v] [-t [<storage type>]] [-u] [-x] [-e] <path> ...]
        [-cp [-f] [-p | -p[topax]] [-d] <src> ... <dst>]
        [-createSnapshot <snapshotDir> [<snapshotName>]]
        [-deleteSnapshot <snapshotDir> <snapshotName>]
        [-df [-h] [<path> ...]]
        [-du [-s] [-h] [-v] [-x] <path> ...]
        [-expunge [-immediate] [-fs <path>]]
        [-find <path> ... <expression> ...]
        [-get [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
        [-getfacl [-R] <path>]
        [-getfattr [-R] {-n name | -d} [-e en] <path>]
        [-getmerge [-nl] [-skip-empty-file] <src> <localdst>]
        [-head <file>]
        [-help [cmd ...]]
        [-ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [-e] [<path> ...]]
        [-mkdir [-p] <path> ...]
        [-moveFromLocal <localsrc> ... <dst>]
        [-moveToLocal <src> <localdst>]
        [-mv <src> ... <dst>]
        [-put [-f] [-p] [-l] [-d] <localsrc> ... <dst>]
        [-renameSnapshot <snapshotDir> <oldName> <newName>]
        [-rm [-f] [-r|-R] [-skipTrash] [-safely] <src> ...]
        [-rmdir [--ignore-fail-on-non-empty] <dir> ...]
        [-setfacl [-R] [{-b|-k} {-m|-x <acl_spec>} <path>]|[--set <acl_spec> <path>]]
        [-setfattr {-n name [-v value] | -x name} <path>]
        [-setrep [-R] [-w] <rep> <path> ...]
        [-stat [format] <path> ...]
        [-tail [-f] [-s <sleep interval>] <file>]
        [-test -[defswrz] <path>]
        [-text [-ignoreCrc] <src> ...]
        [-touch [-a] [-m] [-t TIMESTAMP ] [-c] <path> ...]
        [-touchz <path> ...]
        [-truncate [-w] <length> <path> ...]
        [-usage [cmd ...]]

Generic options supported are:
-conf <configuration file>        specify an application configuration file
-D <property=value>               define a value for a given property
-fs <file:///|hdfs://namenode:port> specify default filesystem URL to use, overrides 'fs.defaultFS' property from configurations.
-jt <local|resourcemanager:port>  specify a ResourceManager
-files <file1,...>                specify a comma-separated list of files to be copied to the map reduce cluster
-libjars <jar1,...>               specify a comma-separated list of jar files to be included in the classpath
-archives <archive1,...>          specify a comma-separated list of archives to be unarchived on the compute machines

The general command line syntax is:
command [genericOptions] [commandOptions]

hadoop@7b946e8f971d:~/landing$ hdfs dfs -ls /ingest
Found 1 items
-rw-r--r--   1 hadoop supergroup  125981363 2022-05-09 17:58 /ingest/yellow_tripdata_2021-01.csv
hadoop@7b946e8f971d:~/landing$ pyspark
Python 3.8.10 (default, Mar 15 2022, 12:22:08)
[GCC 9.4.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/home/hadoop/spark/jars/spark-unsafe_2.12-3.2.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
2024-08-13 12:49:26,081 WARN conf.HiveConf: HiveConf of name hive.metastore.local does not exist
2024-08-13 12:49:26,221 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2024-08-13 12:49:28,937 WARN yarn.Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/

Using Python version 3.8.10 (default, Mar 15 2022 12:22:08)
Spark context Web UI available at http://7b946e8f971d:4040
Spark context available as 'sc' (master = yarn, app id = application_1723496883076_0001).
SparkSession available as 'spark'.
>>> df = spark.read.csv ("/ingest/yellow_tripdata_2021-01.csv")")
  File "<stdin>", line 1
    df = spark.read.csv ("/ingest/yellow_tripdata_2021-01.csv")")
                                                                ^
SyntaxError: EOL while scanning string literal
>>> df = spark.read.csv("/ingest/yellow_tripdata_2021-01.csv")
>>> df.printSchema()
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
 |-- _c7: string (nullable = true)
 |-- _c8: string (nullable = true)
 |-- _c9: string (nullable = true)
 |-- _c10: string (nullable = true)
 |-- _c11: string (nullable = true)
 |-- _c12: string (nullable = true)
 |-- _c13: string (nullable = true)
 |-- _c14: string (nullable = true)
 |-- _c15: string (nullable = true)
 |-- _c16: string (nullable = true)
 |-- _c17: string (nullable = true)

>>> df = spark.read.option("header", "true").csv("/ingest/yellow_tripdata_2021-01.csv")
>>> df.printSchema()
root
 |-- VendorID: string (nullable = true)
 |-- tpep_pickup_datetime: string (nullable = true)
 |-- tpep_dropoff_datetime: string (nullable = true)
 |-- passenger_count: string (nullable = true)
 |-- trip_distance: string (nullable = true)
 |-- RatecodeID: string (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: string (nullable = true)
 |-- DOLocationID: string (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- fare_amount: string (nullable = true)
 |-- extra: string (nullable = true)
 |-- mta_tax: string (nullable = true)
 |-- tip_amount: string (nullable = true)
 |-- tolls_amount: string (nullable = true)
 |-- improvement_surcharge: string (nullable = true)
 |-- total_amount: string (nullable = true)
 |-- congestion_surcharge: string (nullable = true)

>>> df.show(5)
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
|       1| 2021-01-01 00:30:10|  2021-01-01 00:36:12|              1|         2.10|         1|                 N|         142|          43|           2|          8|    3|    0.5|         0|           0|                  0.3|        11.8|                 2.5|
|       1| 2021-01-01 00:51:20|  2021-01-01 00:52:19|              1|          .20|         1|                 N|         238|         151|           2|          3|  0.5|    0.5|         0|           0|                  0.3|         4.3|                   0|
|       1| 2021-01-01 00:43:30|  2021-01-01 01:11:06|              1|        14.70|         1|                 N|         132|         165|           1|         42|  0.5|    0.5|      8.65|           0|                  0.3|       51.95|                   0|
|       1| 2021-01-01 00:15:48|  2021-01-01 00:31:01|              0|        10.60|         1|                 N|         138|         132|           1|         29|  0.5|    0.5|      6.05|           0|                  0.3|       36.35|                   0|
|       2| 2021-01-01 00:31:49|  2021-01-01 00:48:21|              1|         4.94|         1|                 N|          68|          33|           1|       16.5|  0.5|    0.5|      4.06|           0|                  0.3|       24.36|                 2.5|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
only showing top 5 rows
