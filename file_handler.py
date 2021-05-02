from pathlib import Path
import shutil, os
from main import BASE_PATH

TEMPLATE_PATH = os.path.join(BASE_PATH, "templates")
TEMPLATE_SUFFIX = ".template"
TARGET_DIR = "target"


def _get_file_list(dir_path):
    files = []
    for file_or_dir in Path(dir_path).rglob("*"):
        if file_or_dir.is_file():
            files.append(file_or_dir.absolute())
    return files


def _copy_template_to_target(relative_path):
    src = os.path.join(TEMPLATE_PATH, relative_path)
    target = os.path.join(BASE_PATH, TARGET_DIR, relative_path)
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, target)


def copy_all_non_templates(base_components):
    for base_component in base_components:
        src_dir = os.path.join(TEMPLATE_PATH, base_component)
        all_files = _get_file_list(src_dir)
        without_templates = filter(lambda f: f.suffix != TEMPLATE_SUFFIX, all_files)
        src_base_path = Path(TEMPLATE_PATH)
        for non_template in without_templates:
            relative_path = Path(non_template).relative_to(src_base_path)
            _copy_template_to_target(relative_path)


def _write_file(dest, content):
    print("writing to {}...".format(dest))
    print(content)


def write_all_templates(base_components, template_engine, data):
    for base_component in base_components:
        src_dir = os.path.join(TEMPLATE_PATH, base_component)
        all_files = _get_file_list(src_dir)
        templates = filter(lambda f: f.suffix == TEMPLATE_SUFFIX, all_files)
        src_base_path = Path(TEMPLATE_PATH)
        for template in templates:
            relative_path = Path(template).relative_to(src_base_path)
            rendered = template_engine.render(template, data)
            _write_file(Path(os.path.join(BASE_PATH, TARGET_DIR, relative_path.stem)), rendered)
