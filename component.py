from __future__ import annotations
import os
import sys
from pathlib import Path
import shutil
import template
from tqdm import tqdm
import urllib
import tarfile
from typing import Awaitable, Tuple
from argparse import Namespace
import asyncio
from main import ROOT_PATH


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


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


class DownloadRequired(HasComponentBaseDirectory, HasConstants):
    def download_async(self, position_start) -> list[Awaitable]:
        links = self.links_to_download
        awaitables = []
        for i in range(0, len(links)):
            awaitables.append(self._download(links[i], self.component_base_dir, position_start + i))

        return awaitables

    @staticmethod
    async def _download(url: str, output: str, position: int) -> None:
        with DownloadProgressBar(unit='B', unit_scale=True, miniters=1, desc=url.split('/')[-1],
                                 position=position) as bar:
            urllib.request.urlretrieve(url, filename=output, reporthook=bar.update_to)

    @property
    def links_to_download(self) -> list[str]:
        raise NotImplementedError("Base class not implement links_to_download")


class DownloadUtil:
    @staticmethod
    def download_all(downloadables: list[DownloadRequired]) -> None:
        position_start = 0
        awaitables = []
        for downloadable in downloadables:
            new_awaitables = downloadable.download_async(position_start)
            awaitables += new_awaitables
            position_start += len(new_awaitables)

        loop = asyncio.get_event_loop()
        for awaitable in awaitables:
            loop.run_until_complete(awaitable)
        loop.close()


class DecompressRequired:
    def decompress(self):
        awaitables = []
        for compressed, dest in self.files_to_decompress:
            awaitables.append(self._decompress(compressed, dest))
        return awaitables

    @staticmethod
    async def _decompress(compressed: Path, dest_path: Path) -> None:
        with tarfile.open(Path(compressed)) as f:
            f.extractall(dest_path)

    @property
    def files_to_decompress(self) -> list[Tuple[Path, Path]]:
        raise NotImplementedError("Base class not implement decompress")


class DecompressUtil:
    def download_all(self, decompressables: list[DecompressRequired]) -> None:
        awaitables = []
        for decompressable in decompressables:
            new_awaitables = decompressable.decompress()
            awaitables += new_awaitables

        loop = asyncio.get_event_loop()
        for awaitable in awaitables:
            loop.run_until_complete(awaitable)
        loop.close()


class Hadoop(FilesCopyRequired, TemplateRequired, DownloadRequired):
    def __init__(self, args: Namespace):
        self.hadoop_version = args.hadoop_version
        # self.data = {
        #     "clusterName": self.CLUSTER_NAME,
        #     "dependencyVersions": {
        #         "java": args.java_version,
        #         "hadoop": args.hadoop_version
        #     }
        # }

    @property
    def component_base_dir(self) -> str:
        return self.TARGET_BASE_PATH + "/hadoop"

    @property
    def links_to_download(self) -> list[str]:
        return [
            "https://github.com/dev-moonduck/hadoop/releases/download/v{HADOOP_VERSION}/hadoop-{HADOOP_VERSION}.tar.gz"
            .format(HADOOP_VERSION=self.hadoop_version)
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
