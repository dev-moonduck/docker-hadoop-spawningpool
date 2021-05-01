from pathlib import Path
from jinja2 import Template


def render(template_path, data):
    template = Template(Path(template_path).read_text())
    return template.render(data)

