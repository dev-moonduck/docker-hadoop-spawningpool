from __future__ import annotations
import time
import hashlib
import uuid
import random
import collections
from component import DownloadRequired, DecompressRequired, FilesCopyRequired, TemplateRequired
from pathlib import Path
from jinja2 import Environment, StrictUndefined


class RandomUtil:
    @staticmethod
    def random_string():
        return hashlib.md5("{}{}{}".format(str(time.time_ns()), str(uuid.uuid4()), str(random.random()))
                           .encode("UTF-8")).hexdigest()[0:10]


class DictUtil:
    @classmethod
    def dict_merge(cls, dct, merge_dct):
        for k, v in merge_dct.items():
            if (k in dct and isinstance(dct[k], dict)
                    and isinstance(merge_dct[k], collections.Mapping)):
                cls.dict_merge(dct[k], merge_dct[k])
            else:
                dct[k] = merge_dct[k]


class CopyUtil:
    @staticmethod
    def copy_all(copiables: list[FilesCopyRequired]):
        for copiable in copiables:
            copiable.copy()


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


class DecompressUtil:
    @staticmethod
    def decompress_all(decompressables: list[DecompressRequired]) -> None:
        awaitables = []
        for decompressable in decompressables:
            new_awaitables = decompressable.decompress_async()
            for awaitable in new_awaitables:
                awaitable.start()
            awaitables += new_awaitables

        for awaitable in awaitables:
            awaitable.join()


class TemplateUtil:
    @classmethod
    def do_template(cls, hasTemplate: list[TemplateRequired]) -> None:
        agg_data = {
            "clusterName": "local_hadoop"
        }

        for c in hasTemplate:
            DictUtil.dict_merge(agg_data, c.data)

        engine = cls
        for c in hasTemplate:
            c.do_template(engine, agg_data)

    @classmethod
    def render(cls, template_path: Path, data: dict) -> str:
        print("Rendering {}".format(template_path))
        env = Environment(autoescape=False)
        env.undefined = StrictUndefined
        env.filters["keys"] = cls._keys
        env.filters["values"] = cls._values
        template = env.from_string(template_path.read_text())
        return template.render(data)

    @staticmethod
    def _keys(obj):
        return obj.keys()

    @staticmethod
    def _values(obj):
        return obj.values()

