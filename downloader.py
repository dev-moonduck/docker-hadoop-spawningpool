import urllib.request
from tqdm import tqdm
import threading
from pathlib import Path
from file_handler import BASE_PATH


# Apparently Apache official mirror for download restrict download if it is not browser to protect against DOS attack
# But github allows script to download with good performance.
# So I'd forked repository and uploaded binaries in my repository
TO_DOWNLOAD = {
    "spark": "https://github.com/dev-moonduck/spark/releases/download/v{SPARK_VERSION}-{SCALA_VERSION}-{HADOOP_VERSION}"
             + "/spark-{SPARK_VERSION}-{SCALA_VERSION}-{HADOOP_VERSION}.tar.gz",
    "hadoop": "https://github.com/dev-moonduck/hadoop/releases/download"
              + "/v{HADOOP_VERSION}/hadoop-{HADOOP_VERSION}.tar.gz",
    "hive": "https://github.com/dev-moonduck/hive/releases/download/v{HIVE_VERSION}"
            + "/apache-hive-{HIVE_VERSION}-bin.tar.gz",
    "presto": "https://repo1.maven.org/maven2/com/facebook/presto/presto-spark-package"
              "/{PRESTO_VERSION}/presto-spark-package-{PRESTO_VERSION}.tar.gz"
}

TARGET_BASE_PATH = str(BASE_PATH) + "/target"

DOWNLOAD_LOCATION = {
    "spark": TARGET_BASE_PATH + "/spark",
    "hadoop": TARGET_BASE_PATH + "/hadoop",
    "hive": TARGET_BASE_PATH + "/hive",
    "presto": TARGET_BASE_PATH + "/presto"
}


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def _download_file(url, output_path):
    with DownloadProgressBar(unit='B', unit_scale=True, miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)


def launch_downloader(source_url, destination):
    print("Downloading from {} and saving to {}".format(source_url, destination))
    Path(destination).parent.mkdir(parents=True, exist_ok=True)
    thread = threading.Thread(target=_download_file, args=(source_url, destination))
    thread.start()
    return thread


def download(args):
    downloader_threads = []
    dest = DOWNLOAD_LOCATION["hadoop"] + "/hadoop-{HADOOP_VERSION}.tar.gz".format(
        HADOOP_VERSION=args.hadoop_version,
        SCALA_VERSION=args.scala_version)

    if not Path(dest).exists() or args.force_download_hadoop:
        downloader_threads.append(launch_downloader(TO_DOWNLOAD["hadoop"].format(HADOOP_VERSION=args.hadoop_version,
                                                                                 SCALA_VERSION=args.scala_version),
                                                    dest))

    dest = DOWNLOAD_LOCATION["hive"] + "/apache-hive-{HIVE_VERSION}-bin.tar.gz".format(HIVE_VERSION=args.hive_version)
    if (args.hive or args.all) and \
            (not Path(dest).exists() or args.force_download_hive):
        downloader_threads.append(launch_downloader(TO_DOWNLOAD["hive"].format(HIVE_VERSION=args.hive_version), dest))

    dest = DOWNLOAD_LOCATION["spark"] + "/spark-{SPARK_VERSION}-{SCALA_VERSION}-{HADOOP_VERSION}.tar.gz".format(
        HADOOP_VERSION=args.hadoop_version,
        SCALA_VERSION=args.scala_version,
        SPARK_VERSION=args.spark_version
    )

    if (args.spark_history or args.spark_thrift or args.all) and \
            (not Path(dest).exists() or args.force_download_spark):
        downloader_threads.append(launch_downloader(
            TO_DOWNLOAD["spark"].format(HADOOP_VERSION=args.hadoop_version, SCALA_VERSION=args.scala_version,
                                        SPARK_VERSION=args.spark_version), dest))

    dest = DOWNLOAD_LOCATION["presto"] + "/presto-spark-package-{PRESTO_VERSION}.tar.gz".format(
        PRESTO_VERSION=args.presto_version
    )

    if (args.presto or args.all) and (not Path(dest).exists() or args.force_download_spark):
        downloader_threads.append(launch_downloader(TO_DOWNLOAD["presto"].format(PRESTO_VERSION=args.presto_version),
                                                    dest))
    for t in downloader_threads:
        t.join()
