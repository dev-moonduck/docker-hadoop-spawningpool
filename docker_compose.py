import yaml
from yaml import dump
try:
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Dumper
import copy
from collections import OrderedDict


DOCKER_COMPOSE_YAML = OrderedDict({
    "version": "3",
    "services": {},
    "networks": {
        "hadoop.net": None
    }
})

yaml.add_representer(type(None), lambda dumper, value: dumper.represent_scalar(u'tag:yaml.org,2002:null', ''),
                     Dumper=Dumper)
yaml.add_representer(OrderedDict, lambda self, data:  self.represent_mapping('tag:yaml.org,2002:map', data.items()),
                     Dumper=Dumper)


def generate_yaml(data):
    compose_yaml = copy.deepcopy(DOCKER_COMPOSE_YAML)
    for name, instance in data["instances"].items():
        instance_conf = {
            "image": instance["image"],
            "container_name": name,
            "networks": {
                "hadoop.net": None
            }
        }
        if instance["ports"]:
            instance_conf["ports"] = instance["ports"]
        if instance["hosts"]:
            instance_conf["networks"]["hadoop.net"] = {"aliases": instance["hosts"]}
        compose_yaml["services"][name] = instance_conf
    return dump(compose_yaml, Dumper=Dumper)
