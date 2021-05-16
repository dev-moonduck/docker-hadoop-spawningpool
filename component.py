from __future__ import annotations
import os
import sys
from pathlib import Path
import shutil
import template
from rich.progress import *
from urllib.request import urlretrieve
import tarfile
from typing import Awaitable, Tuple
from argparse import Namespace
from main import ROOT_PATH
import threading


class HasConstants:
    BASE_PATH = os.path.join(str(ROOT_PATH), "templates")
    TARGET_BASE_PATH = os.path.join(str(ROOT_PATH), "target")
    TEMPLATE_EXTENSION = ".template"
    CLUSTER_NAME = "nameservice"


class PrepareRequired(HasConstants):
    def prepare_required(self) -> None:
        raise NotImplementedError("Base class not implement prepare_required")


class HasComponentBaseDirectory:
    @property
    def component_base_dir(self) -> str:
        raise NotImplementedError("Base class not implement base_dir")


class FileDiscoverable:
    @staticmethod
    def discover(dir_path: str, glob_pattern: str) -> list[Path]:
        paths = []
        for file_or_dir in Path(dir_path).rglob(glob_pattern):
            if file_or_dir.is_file():
                paths.append(file_or_dir)
        return paths


class DestinationFigurable:
    def get_dest(self, src: str) -> Path:
        relative_path = src[len(self.BASE_PATH):]
        return Path(self.TARGET_BASE_PATH + relative_path)


class TemplateRequired(HasComponentBaseDirectory, FileDiscoverable, DestinationFigurable):

    @property
    def template_files(self) -> list[Path]:
        dir_to_traverse = os.path.join(self.BASE_PATH, Path(self.component_base_dir).name)
        pattern = "*{EXTENSION}".format(EXTENSION=self.TEMPLATE_EXTENSION)
        return self.discover(dir_to_traverse, pattern)

    def do_template(self, data: dict) -> None:
        for to_template in self.template_files:
            content = template.render(to_template, data)
            dest = self.get_dest(str(to_template))
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(str(dest), "w") as f:
                f.write(content)


class FilesCopyRequired(HasComponentBaseDirectory, HasConstants, FileDiscoverable, DestinationFigurable):
    @property
    def files_to_copy(self) -> list[Path]:
        dir_to_traverse = os.path.join(self.BASE_PATH, Path(self.component_base_dir).name)
        pattern = "*[!{EXTENSION}]".format(EXTENSION=self.TEMPLATE_EXTENSION)
        return self.discover(dir_to_traverse, pattern)

    def copy(self) -> None:
        for to_copy in self.files_to_copy:
            dest = self.get_dest(str(to_copy))
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(to_copy, dest)

class CopyUtil:
    @staticmethod
    def copy_all(copiables: list[FilesCopyRequired]):
        for copiable in copiables:
            copiable.copy()

class DownloadProgressBar:
    def __init__(self, desc: str):
        pass

    def update_to(self, b=1, bsize=1, tsize=None):
        pass


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
        bar = DownloadProgressBar("")
        urlretrieve(url, filename=output_file, reporthook=bar.update_to)

    @property
    def links_to_download(self) -> list[Tuple[str, Path]]:
        raise NotImplementedError("Base class not implement links_to_download")


class DownloadUtil:
    @staticmethod
    def download_all(downloadables: list[DownloadRequired]):
        awaitables = []
        for downloadable in downloadables:
            new_awaitables = downloadable.download_async()
            for awaitable in new_awaitables:
                awaitable.start()
            awaitables += new_awaitables
        for awaitable in awaitables:
            awaitable.join()

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
            f.extractall(dest_path)

    @property
    def files_to_decompress(self) -> list[Tuple[Path, Path]]:
        raise NotImplementedError("Base class not implement decompress")


class DecompressUtil:
    def decompress_all(self, decompressables: list[DecompressRequired]) -> None:
        awaitables = []
        for decompressable in decompressables:
            new_awaitables = decompressable.decompress_async()
            for awaitable in new_awaitables:
                awaitable.start()
            awaitables += new_awaitables

        for awaitable in awaitables:
            awaitable.join()


class Component:
    pass

class HasData:
    @property
    def data(self) -> dict:
        raise NotImplementedError("HasData not implement data")


import collections

def dict_merge(dct, merge_dct):
    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], collections.Mapping)):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]

class HasTemplate(TemplateRequired, HasData):
    pass

class TemplateUtil:
    def do_template(self, hasTemplate: list[HasTemplate]) -> None:
        agg_data = {
            "clusterName": "local_hadoop"
        }
        for c in hasTemplate:
            dict_merge(agg_data, c.data)

        for c in hasTemplate:
            c.do_template(agg_data)


class Hadoop(Component, FilesCopyRequired, HasTemplate, DownloadRequired, DecompressRequired):
    TAR_FILE_NAME = "hadoop.tar.gz"
    PREDEF_GROUPS = {
        "admin": 150, "hadoop": 151, "hadoopsvc": 152, "usersvc": 154, "dataplatform_user": 155
    }

    PREDEF_USERS = {
        "hdfs": {"uid": 180, "groups": ["admin"], "isSvc": True},
        "webhdfs": {"uid": 181, "groups": ["admin"], "isSvc": True},
        "hive": {"uid": 182, "groups": ["hadoopsvc"], "isSvc": True},
        "hue": {"uid": 183, "groups": ["hadoopsvc"], "isSvc": True},
        "spark": {"uid": 184, "groups": ["hadoopsvc"], "isSvc": True},
        "bi_user": {"uid": 185, "groups": ["dataplatform_user"], "isSvc": False},
        "bi_svc": {"uid": 186, "groups": ["usersvc"], "isSvc": True},
        "ml_user": {"uid": 187, "groups": ["dataplatform_user"], "isSvc": False},
        "ml_svc": {"uid": 188, "groups": ["usersvc"], "isSvc": True},
        "de_user": {"uid": 189, "groups": ["dataplatform_user"], "isSvc": False},
        "de_svc": {"uid": 190, "groups": ["usersvc"], "isSvc": True}
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
                }
            }
        }


class Hive(Component, FilesCopyRequired, HasTemplate, DownloadRequired):
    TAR_FILE_NAME = "hive.tar.gz"

    def __init__(self, args: Namespace):
        super().__init__(name="hive")
        self.hive_version = args.hive_version

    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "hive")

    @property
    def links_to_download(self) -> list[Tuple[str, Path]]:
        return [
            (("https://github.com/dev-moonduck/hive/releases/download/v{HIVE_VERSION}"
             + "/apache-hive-{HIVE_VERSION}-bin.tar.gz").format(HIVE_VERSION=self.hive_version),
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
            "hive-server": {"host": "hive-server", "thrift-port": "10000", "http-port": "10001"},
            "hive-metastore": {"host": "hive-metastore", "thrift-port": "9083", "metastore-db-host": "cluster-db",
                               "metastore-db-port": "5432", "metastore-db-name": "metastore",
                               "metastore-db-user": "hive", "metastore-db-password": "hive"},
            "additional-data": {
                "users": self.PREDEF_USERS, "groups": self.PREDEF_GROUPS,
                "dependencyVersions": {
                    "hadoop": self.hadoop_version
                }
            }
        }


class Spark(Component, FilesCopyRequired, HasTemplate, DownloadRequired):
    TAR_FILE_NAME = "spark.tar.gz"

    def __init__(self, args: Namespace):
        super().__init__(name="spark")
        self.spark_version = args.spark_version
        self.scala_version = args.scala_version
        self.hadoop_version = args.hadoop_version

    @property
    def component_base_dir(self) -> str:
        return os.path.join(self.TARGET_BASE_PATH, "spark")

    @property
    def links_to_download(self) -> list[Tuple[str, Path]]:
        return [
            (("https://github.com/dev-moonduck/spark/releases/download/v{SPARK_VERSION}-{SCALA_VERSION}-{HADOOP_VERSION}"
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
        return {
            "spark-history": {"host": "spark-history", "port": "18080"},
            "spark-thrift": {"host": "spark-thrift", "port": "4040"}
        }


class Presto(Component, FilesCopyRequired, HasTemplate, DownloadRequired):
    TAR_FILE_NAME = "presto.tar.gz"

    def __init__(self, args: Namespace):
        super().__init__(name="presto")
        self.presto_version = args.presto_version

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
        return {
            "presto-coordinator": {"host": "presto-coordinator", "port": "8080"},
            "presto-worker": {
                "host": ["presto-worker1", "presto-worker2", "presto-worker1"]
            }
        }


class DockerComponent(PrepareRequired):
    @property
    def volumes(self) -> list[str]:
        return self._volumes

    @property
    def environment(self) -> list[str]:
        return self._environment

    @property
    def ports(self) -> list[str]:
        return self._ports

    @property
    def hosts(self) -> list[str]:
        return self._hosts

    @property
    def name(self) -> str:
        return self._name

    @property
    def more_options(self) -> dict:
        return self._more_options



class ComponentFactory:
    @staticmethod
    def get_components(args: Namespace) -> list[Component]:
        components = [Hadoop(args)]
        # if args.hive or args.all:
        #     components.append(Hive(args))
        # if args.spark_thrift or args.all:
        #     components.append(Spark(args))
        # if args.presto or args.all:
        #     components.append(Presto(args))
        return components
