import argparse
from pathlib import Path
import config_builder
import file_handler
import traceback
import utils
import template
import docker_compose

BASE_PATH = Path(__file__).parent.absolute()


def parse_arg():
    # Todo: Target clean up option
    parser = argparse.ArgumentParser(description="Docker hadoop compose yaml generator")
    parser.add_argument("--num-datanode", default=1, type=int, help="number of datanode. Default 3")
    parser.add_argument("--hive", action='store_true', help="build hive server, metastore")
    parser.add_argument("--spark-thrift", action='store_true', help="build spark thrift server for adhoc query")
    parser.add_argument("--hue", action='store_true', help="build hue")
    parser.add_argument("--all", action='store_true', help="Equivalent to --hive --spark --spark-thrift --hue")
    parser.add_argument("--provided-hadoop", help="If you already have hadoop tar, provide local path(must be relative "
                                                  + "path) with this option")
    parser.add_argument("--hadoop-version", default="3.3.0",
                        help="Hadoop version, if you specified --provided-hadoop option, it should match "
                             + "with provided hadoop version")
    parser.add_argument("--hive-version", default="3.1.2", help="Hive version")
    parser.add_argument("--provided-spark", help="If you already have spark tar, provide local path(must be relative "
                                                 + "path with this option")
    parser.add_argument("--spark-version", default="3.1.1", help="Spark version, if you specified --provided-spark" +
                        "option, it should match with provided spark version")
    parser.add_argument("--java-version", default="8", help="Java version, Only 8 or 11 are supported")
    parser.add_argument("--zookeeper-version", default="3.6.2", help="Zookeeper version")
    parser.add_argument("--hue-version", default="4.9.0", help="Docker hue version")
    return parser.parse_args()


def run():
    args = parse_arg()
    try:
        template_data = config_builder.build_config_from_args(args)
        images_to_build = utils.get_images_to_build(template_data)
        file_handler.copy_all_non_templates(images_to_build)
        file_handler.write_all_templates(images_to_build, template, template_data)
        file_handler.write_docker_compose(docker_compose.generate_yaml(template_data))
    except Exception as e:
        traceback.print_exception()
        print("Template data: {}".format(template_data))
        exit(-1)


if __name__ == '__main__':
    run()
