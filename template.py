from pathlib import Path
from jinja2 import Environment
from jinja2 import StrictUndefined


def render(template_path: Path, data: dict) -> str:
    print("Rendering {}".format(template_path))
    env = Environment(autoescape=False)
    env.undefined = StrictUndefined
    env.filters["keys"] = _keys
    env.filters["values"] = _values
    template = env.from_string(template_path.read_text())
    return template.render(data)


def _keys(obj):
    return obj.keys()


def _values(obj):
    return obj.values()
