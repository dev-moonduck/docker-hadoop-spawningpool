from pathlib import Path
import shutil
import os
import tarfile

BASE_PATH = Path(__file__).parent.absolute()
TEMPLATE_PATH = os.path.join(BASE_PATH, "templates")
TEMPLATE_SUFFIX = ".template"
TARGET_DIR = "target"


def _get_file_list(dir_path):
    files = []
    for file_or_dir in Path(dir_path).rglob("*"):
        if file_or_dir.is_file():
            files.append(file_or_dir.absolute())
    return files


def _copy_files_to_target(relative_path):
    src = os.path.join(TEMPLATE_PATH, relative_path)
    target = os.path.join(BASE_PATH, TARGET_DIR, relative_path)
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, target)
    return target


def copy_all_non_templates(base_components):
    for base_component in base_components:
        src_dir = os.path.join(TEMPLATE_PATH, base_component)
        all_files = _get_file_list(src_dir)
        without_templates = filter(lambda f: f.suffix != TEMPLATE_SUFFIX, all_files)
        src_base_path = Path(TEMPLATE_PATH)
        for non_template in without_templates:
            relative_path = Path(non_template).relative_to(src_base_path)
            copied = Path(_copy_files_to_target(relative_path))
            if copied.suffix == ".sh" or copied.suffix == ".py":
                os.chmod(copied, 0o744)


def _write_file(dest, content):
    print("writing to {}...".format(dest))
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(str(dest), "w") as f:
        f.write(content)
    if dest.suffix == ".sh" or dest.suffix == ".py":
        os.chmod(dest, 0o744)


def write_all_templates(base_components, template_engine, data):
    for base_component in base_components:
        src_dir = os.path.join(TEMPLATE_PATH, base_component)
        all_files = _get_file_list(src_dir)
        templates = filter(lambda f: f.suffix == TEMPLATE_SUFFIX, all_files)
        src_base_path = Path(TEMPLATE_PATH)
        for template in templates:
            relative_path = Path(template).relative_to(src_base_path)
            rendered = template_engine.render(template, data)
            _write_file(Path(os.path.join(BASE_PATH, TARGET_DIR, os.path.splitext(relative_path)[0])), rendered)

    build_script = "builder.sh.template"

    rendered = template_engine.render(os.path.join(TEMPLATE_PATH, build_script), data)
    _write_file(Path(os.path.join(BASE_PATH, TARGET_DIR, os.path.splitext(build_script)[0])), rendered)


def write_docker_compose(yaml_str):
    print("writing docker-compose.yml")
    _write_file(Path(os.path.join(BASE_PATH, TARGET_DIR, "docker-compose.yml")), yaml_str)


def decompress_tarball(args):
    if args.hive or args.all:
        tar_dest = Path(os.path.join(BASE_PATH, TARGET_DIR, "hive", "hive-bin"))
        if not tar_dest.exists():
            tarball = "apache-hive-{HIVE_VERSION}-bin.tar.gz".format(HIVE_VERSION=args.hive_version)
            tar_path = Path(os.path.join(BASE_PATH, TARGET_DIR, "hive", tarball))
            with tarfile.open(tar_path) as f:
                f.extractall(tar_dest.parent)
            shutil.move(os.path.join(tar_dest.parent, "apache-hive-{HIVE_VERSION}-bin".format(
                HIVE_VERSION=args.hive_version)), tar_dest)

    if args.spark_history or args.spark_thrift or args.all:
        tar_dest = Path(os.path.join(BASE_PATH, TARGET_DIR, "spark", "spark-bin"))
        if not tar_dest.exists():
            tarball = "spark-{SPARK_VERSION}-{SCALA_VERSION}-{HADOOP_VERSION}.tar.gz".format(
                HADOOP_VERSION=args.hadoop_version, SCALA_VERSION=args.scala_version, SPARK_VERSION=args.spark_version)
            tar_path = Path(os.path.join(BASE_PATH, TARGET_DIR, "spark", tarball))
            with tarfile.open(tar_path) as f:
                f.extractall(tar_dest.parent)
            shutil.move(os.path.join(tar_dest.parent, "spark-{SPARK_VERSION}-bin-spark".format(
                SPARK_VERSION=args.spark_version)), tar_dest)
