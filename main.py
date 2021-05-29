from argparse import ArgumentParser, Namespace
import traceback
from component import ComponentFactory, DownloadRequired, FilesCopyRequired, TemplateRequired, DecompressRequired
from utils import DownloadUtil, TemplateUtil, CopyUtil, DecompressUtil, FileUtil
from docker_compose import build_components, generate_yaml


def parse_arg() -> Namespace:
    # Todo: Target clean up option
    parser = ArgumentParser(description="Docker hadoop compose yaml generator")
    parser.add_argument("--num-datanode", default=1, type=int, help="number of datanode. Default 3")

    # Enable/disable components
    parser.add_argument("--hive", action='store_true', help="build hive server, metastore")
    parser.add_argument("--spark", action='store_true', help="download spark")
    parser.add_argument("--spark-history", action='store_true', help="download spark and run spark history server")
    parser.add_argument("--spark-thrift", action='store_true', help="download spark and run spark thrift server")
    parser.add_argument("--hue", action='store_true', help="build hue")
    parser.add_argument("--presto", action='store_true', help="build presto server")
    parser.add_argument("--presto-spark", action='store_true', help="build presto on spark")
    parser.add_argument("--all", action='store_true', help="Equivalent to --hive --spark --spark-thrift --hue --presto")

    parser.add_argument("--force-download-hadoop", action='store_true', help="Always download hadoop")
    parser.add_argument("--force-download-hive", action='store_true', help="Always download hive")
    parser.add_argument("--force-download-spark", action='store_true', help="Always download spark")
    parser.add_argument("--force-download-presto", action='store_true', help="Always download presto")
    parser.add_argument("--force-download-presto_spark", action='store_true', help="Always download presto on spark")

    # Dependency version configs
    parser.add_argument("--hadoop-version", default="3.3.0",
                        help="Hadoop version, if you specified --provided-hadoop option, it should match "
                             + "with provided hadoop version")
    parser.add_argument("--hive-version", default="3.1.2", help="Hive version")
    parser.add_argument("--spark-version", default="3.1.1", help="Spark version, if you specified --provided-spark" +
                        "option, it should match with provided spark version")
    parser.add_argument("--java-version", default="8", help="Java version, Only 8 or 11 are supported")
    parser.add_argument("--zookeeper-version", default="3.6.2", help="Zookeeper version")
    parser.add_argument("--hue-version", default="4.9.0", help="Docker hue version")
    parser.add_argument("--scala-version", default="2.13", help="Scala version to download spark")
    parser.add_argument("--presto-version", default="0.252", help="Presto on spark version")

    # Docker image name
    parser.add_argument("--image-name-hadoop", default="local-hadoop", help="hadoop docker image name")
    return parser.parse_args()


def run():
    args = parse_arg()
    try:
        components = ComponentFactory.get_components(args)
        to_download = list(filter(lambda c: isinstance(c, DownloadRequired), components))
        DownloadUtil().download_all(to_download)
        to_decompress = list(filter(lambda c: isinstance(c, DecompressRequired), components))
        DecompressUtil().decompress_all(to_decompress)
        to_copy = list(filter(lambda c: isinstance(c, FilesCopyRequired), components))
        CopyUtil().copy_all(to_copy)

        to_template = list(filter(lambda c: isinstance(c, TemplateRequired), components))
        TemplateUtil().do_template(to_template)
        FileUtil.write_to_target("docker-compose.yml", generate_yaml(build_components(args)))

        # template_data = config_builder.build_config_from_args(args)
        # downloader.download(args)
        # file_handler.decompress_tarball(args)
        # images_to_build = template_data["components"]
        # file_handler.copy_all_non_templates(images_to_build)
        # file_handler.write_all_templates(images_to_build, template, template_data)
        # file_handler.write_docker_compose(docker_compose.generate_yaml(template_data))
    except Exception as e:
        traceback.print_exception()
        # print("Template data: {}".format(template_data))
        exit(-1)


if __name__ == '__main__':
    run()
