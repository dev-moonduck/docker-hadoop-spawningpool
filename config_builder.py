def build_config_from_args(args):
    return {
        "dependencyVersions": _component_versions(args),
        "instances": _instances(args),
        "component-bin": _provided_bins(args)
    }


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
        "primary-namenode": {
            "hosts": ["primary-namenode1", "namenode1", "nameservice1", "journalnode1", "resource-manager",
                      "yarn-history"],
            "components": ["primary-namenode", "journalnode", "resource-manager", "yarn-history"],
            "image": "hadoop",
            "ports": ["9870:9870", "8088:8088"]
        },
        "secondary-namenode": {
            "hosts": ["secondary-namenode1", "namenode2", "journalnode2", "spark-history"],
            "components": ["secondary-namenode", "journalnode"],
            "image": "hadoop",
            "ports": ["9871:9870", "18080:18080"]
        },
        "datanode1": {
            "hosts": ["datanode1", "journalnode2"],
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
            "ports": [(9864 + i - 1) + ":9864"] # 9865, 9866, ...
        }

    if args.hive or args.all:
        all_instances["primary-namenode"]["hosts"] += ["hive-server1", "hive-metastore1"]
        all_instances["primary-namenode"]["components"] += ["hive-server", "hive-metastore"]
        all_instances["primary-namenode"]["ports"] += ["10000:10000", "10001:10001", "10002:10002", "9083:9083"]
        all_instances["primary-namenode"]["image"] = "hive"

    if args.spark_thrift or args.all:
        all_instances["secondary-namenode"]["hosts"] += ["spark-thrift1"]
        all_instances["secondary-namenode"]["components"] += ["spark-thrift"]
        all_instances["secondary-namenode"]["ports"] += ["10010:10000", "10011:10001", "10012:10002"]
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