from pathlib import Path
import shutil, os
from main import BASE_PATH

TEMPLATE_PATH = os.path.join(BASE_PATH, "templates")
TEMPLATE_SUFFIX = ".template"
TARGET_DIR = "target"


def _create_target_if_not_exists(target_dir):
    Path(target_dir).mkdir(parents=True, exist_ok=True)


def _get_file_list(dir_path):
    files = []
    for file_or_dir in Path(dir_path).rglob("*"):
        if file_or_dir.is_file():
            files.append(file_or_dir.relative_to())
    return files


def _copy_template_to_target(relative_path):
    src = os.path.join(TEMPLATE_PATH, relative_path)
    target = os.path.join(BASE_PATH, TARGET_DIR, relative_path)
    print("copy {} to {}".format(src, target))
    shutil.copy2(src, target)


def copy_all_non_templates(base_components):

    for base_component in base_components:
        src_dir = os.path.join(TEMPLATE_PATH, base_component)
        all_files = _get_file_list(src_dir)
        without_templates = filter(lambda f: f.suffix is not TEMPLATE_SUFFIX, all_files)

        src_base_path = Path(TEMPLATE_PATH)
        for non_template in without_templates:
            relative_path = Path(non_template).relative_to(src_base_path)
            _copy_template_to_target(relative_path)



