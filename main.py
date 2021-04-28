import argparse

def parse_arg():
    parser = argparse.ArgumentParser(description="Docker hadoop compose yaml generator")
    parser.add_argument("--num-datanode", default=3, type=int, help="number of datanode")
    parser.add_argument("--hive", default=False, help="build hive server, metastore")
    parser.add_argument("--spark", default=False, help="build spark jar and put in hdfs")
    parser.add_argument("--spark-thrift", default=False, help="build spark thrift server for adhoc query")
    parser.add_argument("--hue", default=False, help="build hue")

    return parser.parse_args()


def run():
    

if __name__ == '__main__':
    run()
