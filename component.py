from __future__ import annotations
import os
from pathlib import Path
import shutil
from urllib.request import urlretrieve
import tarfile
from typing import Tuple
from argparse import Namespace
import threading
from abc import ABC
import re
from constants import HasConstants


class HasComponentBaseDirectory:
    @property
    def component_base_dir(self) -> str:
        raise NotImplementedError("Base class not implement base_dir")


class HasData:
    @property
    def data(self) -> dict:
        raise NotImplementedError("Base class not implement data")


class FileDiscoverable:
    @staticmethod
    def discover(dir_path: str, regex_pattern: str) -> list[Path]:
        paths = []
        pattern = re.compile(regex_pattern)
        for file_or_dir in Path(dir_path).rglob("*"):
            if file_or_dir.is_file() and pattern.match(str(file_or_dir.name)):
                paths.append(file_or_dir)
        return paths


class DestinationFigurable(HasConstants):
    def get_dest(self, src: str) -> Path:
        relative_path = src[len(self.BASE_PATH):]
        return Path(self.TARGET_BASE_PATH + relative_path)


class TemplateRequired(HasComponentBaseDirectory, FileDiscoverable, DestinationFigurable):

    @property
    def template_files(self) -> list[Path]:
        dir_to_traverse = os.path.join(self.BASE_PATH, Path(self.component_base_dir).name)
        pattern = ".*\\.{EXTENSION}$".format(EXTENSION=self.TEMPLATE_EXTENSION)
        return self.discover(dir_to_traverse, pattern)

    def do_template(self, engine, data) -> None:
        for to_template in self.template_files:
            content = engine.render(to_template, data)
            dest = Path(os.path.splitext(self.get_dest(str(to_template)))[0])
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(str(dest), "w") as f:
                f.write(content)
            if str(dest.suffix) in [".sh", ".py"]:
                os.chmod(dest, 0o755)


class FilesCopyRequired(ABC, HasComponentBaseDirectory, FileDiscoverable, DestinationFigurable):
    @property
    def files_to_copy(self) -> list[Path]:
        dir_to_traverse = os.path.join(self.BASE_PATH, Path(self.component_base_dir).name)
        pattern = "(?!.*\\.{EXTENSION}$)".format(EXTENSION=self.TEMPLATE_EXTENSION)
        return self.discover(dir_to_traverse, pattern)

    def copy(self) -> None:
        for to_copy in self.files_to_copy:
            dest = self.get_dest(str(to_copy))
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(to_copy, dest)
            if str(dest.suffix) in [".sh", ".py"]:
                os.chmod(dest, 0o755)


class DownloadRequired(HasComponentBaseDirectory, HasConstants):
    def __init__(self, force_download: bool):
        self.force_download = force_download

    def download_async(self) -> list[threading.Thread]:
        Path(self.component_base_dir).mkdir(parents=True, exist_ok=True)
        links = self.links_to_download
        awaitables = []
        for i in range(0, len(links)):
            link, output_file = links[i]
            download_func = self._download
            if not self.force_download and Path(output_file).exists():
                download_func = self._dummy_download

            awaitables.append(threading.Thread(target=download_func,
                                               args=(link, output_file)))
        return awaitables

    @staticmethod
    def _dummy_download(url: str, output_file: Path) -> None:
        print("Download from {URL} is ignored as {PATH} already exists".format(URL=url, PATH=str(output_file)))
        return

    @staticmethod
    def _download(url: str, output_file: Path) -> None:
        print("Downloading from {SOURCE} to {DESTINATION}".format(SOURCE=url, DESTINATION=output_file))
        urlretrieve(url, filename=output_file)

    @property
    def links_to_download(self) -> list[Tuple[str, Path]]:
        raise NotImplementedError("Base class not implement links_to_download")


class DecompressRequired:
    def decompress_async(self) -> list[threading.Thread]:
        awaitables = []
        for compressed, dest in self.files_to_decompress:
            decompress_func = self._decompress
            if dest.exists():
                decompress_func = self._dummy_decompress

            awaitables.append(threading.Thread(target=decompress_func, args=(compressed, dest)))
        return awaitables

    @staticmethod
    def _dummy_decompress(compressed: Path, dest_path: Path) -> None:
        print("Decompressing {COMPRESSED} is ignored as {PATH} already exists".format(
            COMPRESSED=str(compressed), PATH=str(dest_path)))
        return

    @staticmethod
    def _decompress(compressed: Path, dest_path: Path) -> None:
        dest_path.mkdir(parents=True, exist_ok=True)
        with tarfile.open(Path(compressed)) as f:
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(f, dest_path)

    @property
    def files_to_decompress(self) -> list[Tuple[Path, Path]]:
        raise NotImplementedError("Base class not implement decompress")


class Component(ABC):
    pass


class Scripts(Component, TemplateRequired):
    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "bin")

    @property
    def data(self) -> dict:
        return {}


class ClusterStarter(Component, FilesCopyRequired, TemplateRequired, HasData, HasConstants):

    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "cluster-starter")

    @property
    def data(self) -> dict:
        return {
            "additional": {
                "image": {
                    "cluster-starter": self.CLUSTER_STARTER_IMAGE_NAME
                }
            }
        }


class Hue(Component, FilesCopyRequired, TemplateRequired, HasData):
    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "hue")

    @property
    def data(self) -> dict:
        return {
            "hue": {
                "db-user": "hue", "db-password": "hue", "db-name": "hue", "db-host": "cluster-db", "db-port": "5432"
            }
        }


class Hadoop(Component, FilesCopyRequired, TemplateRequired, DownloadRequired, DecompressRequired, HasData, HasConstants):
    TAR_FILE_NAME = "hadoop.tar.gz"
    PREDEF_GROUPS = {
        "admin": 150, "hadoop": 151, "hadoopsvc": 152, "usersvc": 154, "dataplatform_user": 155, "hadoopUser":156,
        "bi_user_group": 157, "ml_user_group": 158, "de_user_group": 159
    }

    PREDEF_USERS = {
        "hdfs": {"uid": 180, "groups": ["admin"], "isSvc": True, "proxyGroup": "*"},
        "webhdfs": {"uid": 181, "groups": ["admin"], "isSvc": True, "proxyGroup": "*"},
        "hive": {"uid": 182, "groups": ["hadoopsvc", "hadoopUser"], "isSvc": True, "proxyGroup": "hadoopUser"},
        "hue": {"uid": 183, "groups": ["hadoopsvc", "hadoopUser"], "isSvc": True, "proxyGroup": "hadoopUser"},
        "spark": {"uid": 184, "groups": ["hadoopsvc", "hadoopUser"], "isSvc": True, "proxyGroup": "hadoopUser"},
        "bi_user": {"uid": 185, "groups": ["dataplatform_user", "hadoopUser", "bi_user_group"], "isSvc": False},
        "bi_svc": {"uid": 186, "groups": ["usersvc", "hadoopUser"], "isSvc": True, "proxyGroup": "bi_user_group"},
        "ml_user": {"uid": 187, "groups": ["dataplatform_user", "hadoopUser", "ml_user_group"], "isSvc": False},
        "ml_svc": {"uid": 188, "groups": ["usersvc", "hadoopUser"], "isSvc": True, "proxyGroup": "ml_user_group"},
        "de_user": {"uid": 189, "groups": ["dataplatform_user", "hadoopUser", "de_user_group"], "isSvc": False},
        "de_svc": {"uid": 190, "groups": ["usersvc", "hadoopUser"], "isSvc": True, "proxyGroup": "de_user_group"}
    }

    def __init__(self, args: Namespace):
        DownloadRequired.__init__(self, force_download=args.force_download_hadoop)
        self.hadoop_version = args.hadoop_version
        self.java_version = args.java_version
        self.num_datanode = args.num_datanode

    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "hadoop")

    @property
    def links_to_download(self) -> list[Tuple[str, Path]]:
        return [
            ("https://github.com/dev-moonduck/hadoop/releases/download/v{HADOOP_VERSION}/hadoop-{HADOOP_VERSION}.tar.gz"
             .format(HADOOP_VERSION=self.hadoop_version),
             Path(os.path.join(self.component_base_dir, self.TAR_FILE_NAME)))
        ]

    @property
    def files_to_decompress(self) -> list[Tuple[Path, Path]]:
        return [
            (Path(os.path.join(self.component_base_dir, self.TAR_FILE_NAME)),
             Path(os.path.join(self.component_base_dir, "hadoop-bin")))
        ]

    @property
    def data(self) -> dict:
        return {
            "primary_namenode": {
                "host": "primary-namenode", "rpc-port": "9000", "http-port": "9870"
            },
            "secondary_namenode": {
                "host": "secondary-namenode", "rpc-port": "9000", "http-port": "9870"
            },
            "journalnode": {"host": ["journalnode1", "journalnode2", "journalnode3"], "port": "8485"},
            "zookeeper": {"host": ["zookeeper1", "zookeeper2", "zookeeper3"], "port": "2181"},
            "yarn_history": {"host": "yarn-history", "port": "8188"},
            "resource_manager": {
                "host": "resource-manager", "port": "8032", "web-port": "8088", "resource-tracker-port": "8031",
                "scheduler-port": "8030"
            },
            "datanode": {
                "host": list(map(lambda i: "datanode" + str(i), range(1, self.num_datanode + 1))),
                "rpc-port": "9864", "nodemanager-port": "8042"
            },
            "additional": {
                "users": self.PREDEF_USERS, "groups": self.PREDEF_GROUPS,
                "dependency-versions": {
                    "hadoop": self.hadoop_version, "java": self.java_version
                },
                "agent": {
                    "port": "3333"
                },
                "image": {
                    "hadoop": self.HADOOP_IMAGE_NAME
                }
            }
        }


class Hive(Component, FilesCopyRequired, TemplateRequired, DownloadRequired, DecompressRequired, HasData):
    TAR_FILE_NAME = "hive.tar.gz"

    def __init__(self, args: Namespace):
        DownloadRequired.__init__(self, force_download=args.force_download_hive)
        self.hive_version = args.hive_version

    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "hive")

    @property
    def links_to_download(self) -> list[Tuple[str, Path]]:
        return [
            (("https://github.com/dev-moonduck/hive/releases/download/v{HIVE_VERSION}"
             + "/apache-hive-{HIVE_VERSION}.tar.gz").format(HIVE_VERSION=self.hive_version),
             Path(os.path.join(self.component_base_dir, self.TAR_FILE_NAME)))
        ]

    @property
    def files_to_decompress(self) -> list[Tuple[Path, Path]]:
        return [
            (Path(os.path.join(self.component_base_dir, self.TAR_FILE_NAME)),
             Path(os.path.join(self.component_base_dir, "hive-bin")))
        ]

    @property
    def data(self) -> dict:
        return {
            "hive_server": {"host": "hive-server", "thrift-port": "10000", "http-port": "10001"},
            "hive_metastore": {"host": "hive-metastore", "thrift-port": "9083", "metastore-db-host": "cluster-db",
                               "metastore-db-port": "5432", "metastore-db-name": "metastore",
                               "metastore-db-user": "hive", "metastore-db-password": "hive"},
            "additional": {
                "dependency-versions": {
                    "hive": self.hive_version
                }
            }
        }


class Spark(Component, FilesCopyRequired, TemplateRequired, DownloadRequired, DecompressRequired, HasData):
    TAR_FILE_NAME = "spark.tar.gz"

    def __init__(self, args: Namespace):
        DownloadRequired.__init__(self, force_download=args.force_download_spark)
        self.spark_version = args.spark_version
        self.scala_version = args.scala_version
        self.hadoop_version = args.hadoop_version

    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "spark")

    @property
    def links_to_download(self) -> list[Tuple[str, Path]]:
        return [(
            ("https://github.com/dev-moonduck/spark/releases/download/v{SPARK_VERSION}-{SCALA_VERSION}-{HADOOP_VERSION}"
             + "/spark-{SPARK_VERSION}-{SCALA_VERSION}-{HADOOP_VERSION}.tar.gz").format(
                SPARK_VERSION=self.spark_version, SCALA_VERSION=self.scala_version, HADOOP_VERSION=self.hadoop_version),
            Path(os.path.join(self.component_base_dir, self.TAR_FILE_NAME)))
        ]

    @property
    def files_to_decompress(self) -> list[Tuple[Path, Path]]:
        return [
            (Path(os.path.join(self.component_base_dir, self.TAR_FILE_NAME)),
             Path(os.path.join(self.component_base_dir, "spark-bin")))
        ]

    @property
    def data(self) -> dict:
        return {}


class SparkHistory(Component, TemplateRequired, FilesCopyRequired, HasData):
    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "spark-history")

    @property
    def data(self) -> dict:
        return {
            "spark_history": {"host": "spark-history", "port": "18080"}
        }


class SparkThrift(Component, TemplateRequired, FilesCopyRequired, HasData):
    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "spark-thrift")

    @property
    def data(self) -> dict:
        return {
            "spark_thrift": {"host": "spark-thrift", "thrift-port": "10010", "http-port": "10011"}
        }


class Presto(Component, FilesCopyRequired, TemplateRequired, DownloadRequired, DecompressRequired, HasData):
    TAR_FILE_NAME = "presto.tar.gz"

    def __init__(self, args: Namespace):
        DownloadRequired.__init__(self, force_download=args.force_download_presto)
        self.presto_version = args.presto_version
        self.num_worker = args.num_presto_worker

    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "presto")

    @property
    def links_to_download(self) -> list[Tuple[str, Path]]:
        return [
            (("https://github.com/dev-moonduck/presto/releases/download/v{PRESTO_VERSION}"
             + "/presto-server-{PRESTO_VERSION}.tar.gz").format(PRESTO_VERSION=self.presto_version),
             Path(os.path.join(self.component_base_dir, self.TAR_FILE_NAME)))
        ]

    @property
    def files_to_decompress(self) -> list[Tuple[Path, Path]]:
        return [
            (Path(os.path.join(self.component_base_dir, self.TAR_FILE_NAME)),
             Path(os.path.join(self.component_base_dir, "presto-bin")))
        ]

    @property
    def data(self) -> dict:
        workers = [{"host": "presto-worker" + str(i)} for i in range(1, self.num_worker + 1)]
        return {
            "presto_server": {"host": "presto-server", "port": "8081"},
            "presto_worker": workers
        }


class ComponentFactory:
    @staticmethod
    def get_components(args: Namespace) -> list[Component]:
        components = [Scripts(), ClusterStarter(), Hadoop(args)]
        if args.hive or args.all:
            components.append(Hive(args))
        if args.spark_thrift or args.spark_history or args.all:
            components.append(Spark(args))
        if args.spark_history or args.all:
            components.append(SparkHistory())
        if args.spark_thrift or args.all:
            components.append(SparkThrift())
        if args.presto or args.all:
            components.append(Presto(args))
        if args.hue or args.all:
            components.append(Hue())
        return components
