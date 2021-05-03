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


def build_config_from_args(args):
    return {
        "dependencyVersions": _component_versions(args),
        "instances": _instances(args),
        "binary": _provided_bins(args),
        "hosts": _component_hosts(args),
        "groups": PREDEF_GROUPS,
        "users": PREDEF_USERS
    }


def _component_hosts(args):
    hosts = {
        "primary-namenode": "primary-namenode1",
        "secondary-namenode": "secondary-namenode1",
        "datanode": ["datanode1"],
        "journalnode": ["journalnode1", "journalnode2", "journalnode3"],
        "zookeeper": ["zookeeper1", "zookeeper2", "zookeeper3"]
    }
    for i in range(2, args.num_datanode):
        hosts["datanode"] += ("datanode" + i)
    if args.hive or args.all:
        hosts["hive-server"] = "hive-server"
        hosts["hive-metastore"] = "hive-metastore"
    if args.spark_thrift or args.all:
        hosts["spark-thrift"] = "spark-thrift"
    return hosts


def _component_versions(args):
    version = {
        "java": args.java_version,
        "hadoop": args.hadoop_version,
        "zookeeper": args.zookeeper_version
    }
    if args.hive or args.all:
        version["hive"] = args.hive
    if args.spark_thrift or args.all:
        version["spark"] = args.spark_version
    if args.hue or args.all:
        version["hue"] = args.hue_version
    return version


def _instances(args):
    # Required instances
    all_instances = {
        "cluster-starter": {
          "image": "cluster-starter"
        },
        "primary-namenode": {
            "hosts": ["primary-namenode1", "namenode1", "nameservice1", "journalnode1", "resource-manager",
                      "yarn-history", "zookeeper1"],
            "components": ["primary-namenode", "journalnode", "resource-manager", "yarn-history"],
            "image": "hadoop",
            "ports": ["9870:9870", "8088:8088"]
        },
        "secondary-namenode": {
            "hosts": ["secondary-namenode1", "namenode2", "journalnode2", "zookeeper2"],
            "components": ["secondary-namenode", "journalnode"],
            "image": "hadoop",
            "ports": ["9871:9870"]
        },
        "datanode1": {
            "hosts": ["datanode1", "journalnode3", "zookeeper3"],
            "components": ["datanode", "journalnode"],
            "image": "hadoop",
            "ports": ["9864:9864"]
        }
    }

    # Set optional instances
    # Add more datanode
    num_datanode = args.num_datanode
    for i in range(2, num_datanode + 1):
        all_instances["datanode" + i] = {
            "hosts": ["datanode1"],
            "components": ["datanode"],
            "ports": [(9864 + i - 1) + ":9864"]  # 9865, 9866, ...
        }

    if args.hive or args.all:
        all_instances["primary-namenode"]["hosts"] += ["hive-server1", "hive-metastore1"]
        all_instances["primary-namenode"]["components"] += ["hive-server", "hive-metastore"]
        all_instances["primary-namenode"]["ports"] += ["10000:10000", "10001:10001", "10002:10002", "9083:9083"]
        all_instances["primary-namenode"]["image"] = "hive"

    if args.spark_thrift or args.all:
        all_instances["secondary-namenode"]["hosts"] += ["spark-thrift1", "spark-history"]
        all_instances["secondary-namenode"]["components"] += ["spark-thrift"]
        all_instances["secondary-namenode"]["ports"] += ["10010:10000", "10011:10001", "10012:10002", "18080:18080"]
        all_instances["secondary-namenode"]["image"] = "hive"

    if args.hue or args.all:
        all_instances["secondary-namenode"]["hosts"] += ["hue"]
        all_instances["secondary-namenode"]["image"] = "hue"
        all_instances["secondary-namenode"]["ports"] += ["8888:8888"]

    return all_instances


def _provided_bins(args):
    paths = {}
    hadoop_bin_path = args.provided_hadoop
    if hadoop_bin_path:
        paths["hadoop"] = hadoop_bin_path
    spark_bin_path = args.provided_spark
    if spark_bin_path:
        paths["spark"] = spark_bin_path
    return paths
