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
            paths.append(file_or_dir)
        return paths


class DestinationFigurable:
    def get_dest(self, src: str) -> Path:
        relative_path = src[0: len(self.BASE_PATH)]
        return Path(os.path.join(self.TARGET_BASE_PATH, relative_path))


class TemplateRequired(HasComponentBaseDirectory, FileDiscoverable, DestinationFigurable):

    @property
    def template_files(self) -> list[Path]:
        dir_to_traverse = os.join(self.BASE_PATH, self.component_base_dir)
        pattern = "*{EXTENSION}".format(EXTENSION=self.TEMPLATE_EXTENSION)
        return self.discover(dir_to_traverse, pattern)

    def do_template(self, data: dict) -> None:
        for to_template in self.template_files:
            content = template.render(to_template, data)
            dest = self.get_dest(to_template)
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(str(dest), "w") as f:
                f.write(content)


class FilesCopyRequired(HasComponentBaseDirectory, HasConstants, FileDiscoverable, DestinationFigurable):
    @property
    def files_to_copy(self) -> list[Path]:
        dir_to_traverse = os.join(self.BASE_PATH, self.component_base_dir)
        pattern = "*[!{EXTENSION}]".format(EXTENSION=self.TEMPLATE_EXTENSION)
        return self.discover(dir_to_traverse, pattern)

    def copy(self) -> None:
        for to_copy in self.files_to_copy:
            dest = self.get_dest(str(to_copy))
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(to_copy, dest)


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
            awaitables.append(threading.Thread(target=self._decompress, args=(compressed, dest)))
        return awaitables

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

class Hadoop(Component, FilesCopyRequired, TemplateRequired, DownloadRequired, DecompressRequired):
    TAR_FILE_NAME = "hadoop.tar.gz"

    def __init__(self, args: Namespace):
        DownloadRequired.__init__(self, force_download=args.force_download_hadoop)
        self.hadoop_version = args.hadoop_version

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


class Hive(Component, FilesCopyRequired, TemplateRequired, DownloadRequired):
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

class Spark(Component, FilesCopyRequired, TemplateRequired, DownloadRequired):
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

class Presto(Component, FilesCopyRequired, TemplateRequired, DownloadRequired):
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
