import argparse
import pathlib


BASE_PATH = pathlib.Path(__file__).parent.absolute()
IMAGE_LAYER = {
    "hadoop": 1,
    "hive": 2,
    "hue": 3
}
SUPPORTED_COMPONENTS = {
    "primary-namenode": {
        "image": "hadoop"
    },
    "secondary-namenode": {
        "image": "hadoop"
    },
    "datanode": {
        "image": "hadoop"
    },
    "journalnode": {
        "image": "hadoop"
    },
    "hive-server": {
        "image": "hive"
    },
    "hive-metastore": {
        "image": "hive"
    },
    "spark-thrift": {
        "image": "hive"
    },
    "hue": {
        "image": "hue"
    }
}


def parse_arg():
    parser = argparse.ArgumentParser(description="Docker hadoop compose yaml generator")
    parser.add_argument("--num-datanode", default=1, type=int, help="number of datanode. Default 3")
    parser.add_argument("--hive", action='store_true', help="build hive server, metastore")
    parser.add_argument("--spark-thrift", action='store_true', help="build spark thrift server for adhoc query")
    parser.add_argument("--hue", action='store_true', help="build hue")
    parser.add_argument("--all", action='store_true', help="Equivalent to --hive --spark --spark-thrift --hue")
    parser.add_argument("--provided-hadoop", help="If you already have hadoop tar, provide local path with this option")
    parser.add_argument("--hadoop-version", default="3.3.0",
                        help="Hadoop version, if you specified --provided-hadoop option, it should match "
                             + "with provided hadoop version")
    parser.add_argument("--hive-version", default="3.1.2", help="Hive version")
    parser.add_argument("--provided-spark", help="If you already have spark tar, provide local path with this option")
    parser.add_argument("--spark-version", default="3.1.1", help="Spark version, if you specified --provided-spark" +
                        "option, it should match with provided spark version")
    parser.add_argument("--java-version", default="8", help="Java version, Only 8 or 11 are supported")
    parser.add_argument("--zookeeper-version", default="3.6.2", help="Zookeeper version")
    parser.add_argument("--hue-version", default="4.9.0", help="Docker hue version")
    return parser.parse_args()


def componentVersions(args):
    version = {
        "java": args.jave_version,
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


def get_instances(args):
    # Required instances
    instances = {
        "primary-namenode": {
            "hosts": ["primary-namenode1", "namenode1", "nameservice1", "journalnode1"],
            "components": ["primary-namenode", "journalnode"]
        },
        "secondary-namenode": {
            "hosts": ["secondary-namenode1", "namenode2", "journalnode2"],
            "components": ["secondary-namenode", "journalnode"]
        },
        "datanode1": {
            "hosts": ["datanode1", "journalnode2"],
            "components": ["datanode", "journalnode"]
        }
    }

    # Set optional instances
    # Add more datanode
    num_datanode = args.num_datanode
    for i in range(2, num_datanode + 1):
        instances["datanode" + i] = {
            "hosts": ["datanode1"],
            "components": ["datanode"]
        }

    if args.hive or args.all:
        instances["primary-namenode"]["hosts"] += ["hive-server1", "hive-metastore1"]
        instances["primary-namenode"]["components"] += ["hive-server", "hive-metastore"]

    if args.spark_thrift or args.all:
        instances["secondary-namenode"]["hosts"] += ["spark-thrift1"]
        instances["secondary-namenode"]["components"] += ["spark-thrift"]

    if args.hue or args.all:
        instances["secondary-namenode"]["hosts"] += ["hue"]

    return instances


def run():
    args = parse_arg()
    print(get_instances(args))


if __name__ == '__main__':
    run()
