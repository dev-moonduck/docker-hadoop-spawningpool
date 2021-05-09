from copy import deepcopy

PREDEF_GROUPS = {
  "admin": 150, "hadoop": 151, "hadoopsvc": 152, "usersvc": 154, "dataplatform_user": 155
}

PREDEF_USERS = {
  "hdfs": { "uid": 180, "groups": ["admin"], "isSvc": True },
  "webhdfs": { "uid": 181, "groups": ["admin"], "isSvc": True },
  "hive": { "uid": 182, "groups": ["hadoopsvc"], "isSvc": True },
  "hue": { "uid": 183, "groups": ["hadoopsvc"], "isSvc": True },
  "spark": { "uid": 184, "groups": ["hadoopsvc"], "isSvc": True },
  "bi_user": { "uid": 185, "groups": ["dataplatform_user"], "isSvc": False },
  "bi_svc": { "uid": 186, "groups": ["usersvc"], "isSvc": True },
  "ml_user": { "uid": 187, "groups": ["dataplatform_user"], "isSvc": False },
  "ml_svc": { "uid": 188, "groups": ["usersvc"], "isSvc": True },
  "de_user": { "uid": 189, "groups": ["dataplatform_user"], "isSvc": False },
  "de_svc": { "uid": 190, "groups": ["usersvc"], "isSvc": True }
}

MINIMUM_INSTANCES = {
    "cluster-starter": {
      "image": "cluster-starter"
    },
    "primary-namenode": {
        "hosts": ["primary-namenode", "namenode1", "journalnode1", "resource-manager", "yarn-history", "zookeeper1"],
        "components": ["primary-namenode", "journalnode", "resource-manager", "yarn-history"],
        "image": "hadoop",
        "ports": ["9870:9870", "8088:8088", "8188:8188"],
        "volumes": [],
        "environment": {"MY_NODE_NUM=1"}
    },
    "secondary-namenode": {
        "hosts": ["secondary-namenode", "namenode2", "journalnode2", "zookeeper2"],
        "components": ["secondary-namenode", "journalnode"],
        "image": "hadoop",
        "ports": ["9871:9870"],
        "volumes": [],
        "environment": {"MY_NODE_NUM=2"}
    },
    "datanode1": {
        "hosts": ["datanode1", "journalnode3", "zookeeper3"],
        "components": ["datanode", "journalnode"],
        "image": "hadoop",
        "ports": ["9864:9864"],
        "volumes": [],
        "environment": {"MY_NODE_NUM=3"}
    }
}

INSTANCE_MAPPING = {
    "hive": "primary-namenode",
    "spark-history": "secondary-namenode",
    "spark-thrift": "secondary-namenode",
    "hue": "secondary-namenode"
}


def build_config_from_args(args):
    return {
        "clusterName": "nameservice",
        "dependencyVersions": _component_versions(args),
        "instances": _instances(args),
        "hosts": _component_hosts(args),
        "groups": PREDEF_GROUPS,
        "users": PREDEF_USERS,
        "components": _component_to_build(args),
        "additional": _additional_config(args)
    }


def _component_to_build(args):
    component_name = ["cluster-starter", "hadoop"]
    if args.hive or args.all:
        component_name.append("hive")
    if args.hue or args.all:
        component_name.append("hue")
    if args.spark_thrift or args.all:
        component_name.append("spark-thrift")
    if args.spark_history or args.all:
        component_name.append("spark-history")
    return component_name

def _additional_config(args):
    config = {
        "image-name": {
            "hadoop": args.image_name_hadoop,
            "hive": args.image_name_hive,
            "hue": args.image_name_hue,
            "cluster-starter": "cluster-starter"
        },
        "agent": {
            "port": "3333"
        }
    }
    if args.hive or args.all:
        config["hive-metastore"] = {
            "metastore-db-name": "metastore"
        }
    if args.hue or args.all:
        config["hue"] = {
            "hue-db-name": "hue",
            "db-username": "hue",
            "db-password": "hue"
        }
    return config


def _component_hosts(args):
    hosts = {
        "primary-namenode": {
            "host": "primary-namenode", "rpc-port": "9000", "http-port": "9870"
        },
        "secondary-namenode": {
            "host": "secondary-namenode", "rpc-port": "9000", "http-port": "9870"
        },
        "datanode": {
            "host": ["datanode1"], "rpc-port": "9864", "nodemanager-port": "8042"
        },
        "journalnode": {"host": ["journalnode1", "journalnode2", "journalnode3"], "port": "8485"},
        "zookeeper": {"host": ["zookeeper1", "zookeeper2", "zookeeper3"], "port": "2181"},
        "yarn-history": {"host": "yarn-history", "port": "8188"},
        "resource-manager": {
            "host": "resource-manager", "port": "8032", "web-port": "8088", "resource-tracker-port": "8031", "scheduler-port": "8030"
        }
    }
    for i in range(2, args.num_datanode + 1):
        hosts["datanode"]["host"].append("datanode" + str(i))
    if args.hive or args.all:
        hosts["hive-server"] = {"host": "hive-server", "thrift-port": "10000", "http-port": "10001"}
        hosts["hive-metastore"] = {"host": "hive-metastore", "thrift-port": "9083", "metastore-db-host": "cluster-db",
                                   "metastore-db-port": "5432"}
    if args.spark_history or args.all:
        hosts["spark-history"] = {"host": "spark-history", "port": "18080"}

    if args.spark_thrift or args.all:
        hosts["spark-thrift"] = {"host": "spark-thrift", "port": "4040"}

    if args.hue or args.all:
        hosts["hue"] = {"host": "hue", "port": "8888", "db-host": "cluster-db", "db-port": "5432"}

    return hosts


def _component_versions(args):
    version = {
        "java": args.java_version,
        "hadoop": args.hadoop_version,
        "zookeeper": args.zookeeper_version,
    }
    if args.hive or args.all:
        version["hive"] = args.hive_version
    if args.hue or args.all:
        version["hue"] = args.hue_version
    if args.spark_history or args.spark_thrift or args.all:
        version["spark"] = args.spark_version
    return version


def _instances(args):
    # Required instances
    all_instances = deepcopy(MINIMUM_INSTANCES)

    # Set optional instances
    # Add more datanode
    num_datanode = args.num_datanode
    for i in range(2, num_datanode + 1):
        external_port = 9864 + i - 1
        all_instances["datanode" + str(i)] = {
            "hosts": ["datanode" + str(i)],
            "components": ["datanode"],
            "image": "hadoop",
            "ports": [str(external_port) + ":9864"]  # 9865, 9866, ...
        }

    # Local resource is limited... hence we run multiple components in an instance
    # Namenode instances should not be busy at all, hence places all additional instances at namenodes
    if args.hive or args.all:
        instance_to_run = INSTANCE_MAPPING["hive"]
        all_instances[instance_to_run]["hosts"] += ["hive-server", "hive-metastore"]
        all_instances[instance_to_run]["components"] += ["hive-server", "hive-metastore"]
        all_instances[instance_to_run]["ports"] += ["10000:10000", "10001:10001", "10002:10002", "9083:9083"]
        all_instances[instance_to_run]["image"] = "hadoop"
        all_instances[instance_to_run]["environment"].add("HIVE_HOME=/opt/hive")
        all_instances[instance_to_run]["environment"].add("HIVE_CONF_DIR=/opt/hive/conf")
        all_instances[instance_to_run]["environment"].add("PATH=$PATH:/opt/hive/bin")
        all_instances[instance_to_run]["volumes"] += [
            "./hive/scripts/run_hive_metastore.sh:/scripts/run_hive_metastore.sh",
            "./hive/scripts/run_hive_server.sh:/scripts/run_hive_server.sh",
            "./hive/hive-bin:/opt/hive"
        ]

    if args.spark_history or args.all:
        instance_to_run = INSTANCE_MAPPING["spark-history"]
        all_instances[instance_to_run]["hosts"] += ["spark-history"]
        all_instances[instance_to_run]["components"] += ["spark-history"]
        all_instances[instance_to_run]["ports"] += ["18080:18080"]
        all_instances[instance_to_run]["volumes"] += [
            "./spark/spark-bin:/opt/spark",
            "./spark-history/scripts/run_history_server.sh:/scripts/run_history_server.sh",
            "./spark-thrift/scripts/run_thrift_server.sh:/scripts/run_thrift_server.sh",
            "./spark-history/conf/history_server.conf:/spark_history_server.conf"
        ]
        all_instances[instance_to_run]["environment"].add("SPARK_HOME=/opt/spark")

    if args.spark_thrift or args.all:
        instance_to_run = INSTANCE_MAPPING["spark-thrift"]
        all_instances[instance_to_run]["hosts"] += ["spark-thrift"]
        all_instances[instance_to_run]["components"] += ["spark-thrift"]
        all_instances[instance_to_run]["ports"] += ["10010:10000", "10011:10001", "10012:10002", "4040:4040"]
        all_instances[instance_to_run]["environment"].add("SPARK_HOME=/opt/spark")

    if args.hue or args.all:
        instance_to_run = INSTANCE_MAPPING["hue"]
        all_instances[instance_to_run]["hosts"] += ["hue"]
        all_instances[instance_to_run]["image"] = "hue"
        all_instances[instance_to_run]["ports"] += ["8888:8888"]

    return all_instances


def _provided_bins(args):
    paths = {}
    hadoop_bin_path = args.provided_hadoop
    if hadoop_bin_path:
        paths["hadoop"] = hadoop_bin_path
    spark_bin_path = args.provided_spark
    if spark_bin_path:
        paths["spark"] = spark_bin_path
    hive_bin_path = args.provided_hive
    if hive_bin_path:
        paths["hive"] = hive_bin_path
    return paths
