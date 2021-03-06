# Docker hadoop spawningpool
This is local docker hadoop cluster builder for robust Hadoop apps test.
You can construct fully distributed Hadoop cluster on local by this script.
This project itself doesn't contain any docker image, but it will generate/download required files, and set up hadoop ecosystems.  
Basically this is for Hadoop or Hadoop ecosystem apps test on your local with fully distributed hadoop system.  

# Prerequisite
- Python3
- Docker
- 16GB+ RAM laptop/desktop as it requires many resource to run.

# How to start
```bash
$ pip install -r requirements
$ python main.py {options}
$ cd target
$ sh ./bin/builder.sh all  # Build Hadoop and starter image
$ docker-compose up -d
```
After run above command, Python script will download Hadoop, Hive, Spark binaries and then generate Dockerfile and required bash script files under `target` directory.
It will take time as binaries are quite Huge.  
Once it's done, you can run `builder.sh` and run `docker-compose up -d`.
Finally you can see the all cluster up after 3~5min once you run the command(you may have to wait for more)  
You can check following addresses.

|  Component  |  Address  | Misc   |
|-----------|---------|------------|
|  Yarn resource manager  |  localhost:8088  |  |
|  Namenode(active)  |  localhost:9870  |  |
|  Namenode(standby)  |  localhost:9871  |  |
|  Datanode  |  localhost:9864, 9865, ...  |  |
|  HiveServer  |  localhost:10002  |  |
|  Hue  |  localhost:8888 |  |
|  Presto  | localhost:8081 |  |


# Options
1. `--num-datanode` 
Number of datanode that you want to use. default 3
   
2. `--hive`
Enable Hive. Hive docker instance will be included in an instance.
   
3. `--hue`
Enable Hue. Hue docker instance will be included in an instance.
   
4. `--spark-history`
Enable spark history server. Spark history instance will be included in an instance.
   
5. `--spark-thrift`
Enable spark thrift server. Spark thrift server will be included in an instance.
   
6. `--presto`, `--num-presto-worker`
Enable standalone presto. Presto server will run on primary-namenode instance, also presto workers will run on datanodes
   

# Example
```bash
$ python main.py --num-datanode 3 --hive --hue --spark-history --spark-thrift
```

# Supported tech stack and version
|  name | version |        | 
|---------|---------|-------|
|  Hadoop | 3.3.0 |     |
|  Hive   | 3.1.2 |      |
|  Spark  | 3.1.2 | Compiled by scala 2.13    |
|  Hue    | 4.9.0 |     |
|  PostgresQL  |  13.1  |  |
|  Presto  | 0.252 |  |

# Predefined User and password
Service user is added as proxy user in `core-site.xml`

|  name | password  | is Proxy/Service User | Proxy scope |
|---------|---------|-------------|-------------|
| hdfs    | hdfs    | Y           | All |
| webhdfs | webhdfs | Y           | All |
| hive  | hive | Y | All hadoop user except admin user(hdfs, webhdfs) |
| hue  | hue | Y | All hadoop user except admin user(hdfs, webhdfs) |
| spark  | spark | Y | All hadoop user except admin user(hdfs, webhdfs) |
| bi_svc | bi_svc | Y | bi_user_group |
| bi_user | bi_user | N | N/A |
| ml_svc | ml_svc | Y | ml_user_group |
| ml_user | ml_user | N | N/A |
| de_svc | de_svc | Y | de_user_group |
| de_user | de_user | N | N/A |

# Local docker cluster overview
## Instances
![docker-instances](./images/docker-instances.png)  
Basically instance overview consists of above image
- Instance `primary-namenode` contains active namenode, yarn history server, journalnode, zookeeper and optionally Hive server, metastore and presto server
- Instance `secondary-namenode` contains standby namenode, resource manager, journal node, zookeeper and optionally spark thrift and history server.
- Instance `Datanode1` contains journalnode, zookeeper, datanode and node manager.
- If you specify `--num-datanode` more than 1, additional datanode will be instantiated.
- If user specify `--hue`, Hue instance will be added
- Cluster starter is temporary instantiated, it loads all hadoop components and prepare all initial state in order. After All cluster are ready to serve, Cluster starter is terminated.
- If user specify `--presto`, Presto workers run on data node


# Road map 
- Support Kafka
- Support Airflow or Oozie
- Support Sqoop
- Support various version of each framework

# Any suggestion or need support?
Ask me through writing an issue
