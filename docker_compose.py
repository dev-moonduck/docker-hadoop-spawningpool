import yaml
from yaml import dump
try:
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Dumper
import copy
from collections import OrderedDict
from instance import DockerComponent, MultipleComponent, PrimaryNamenode, SecondaryNamenode, JournalNode, DataNode, \
    ResourceManager, YarnHistoryServer, ClusterStarter, ClusterDb, ZookeeperNode
from argparse import Namespace
from typing import List



DOCKER_COMPOSE_YAML = OrderedDict({
    "version": "3",
    "services": {},
    "networks": {
        "hadoop.net": {
            "external": True
        }
    }
})

yaml.add_representer(type(None), lambda dumper, value: dumper.represent_scalar(u'tag:yaml.org,2002:null', ''),
                     Dumper=Dumper)
yaml.add_representer(OrderedDict, lambda self, data:  self.represent_mapping('tag:yaml.org,2002:map', data.items()),
                     Dumper=Dumper)


def generate_yaml(instances: List[DockerComponent]):
    compose_yaml = copy.deepcopy(DOCKER_COMPOSE_YAML)
    for instance in instances:
        instance_conf = {
            "image": instance.image,
            "container_name": instance.name,
            "networks": {
                "hadoop.net": None
            },
            "tty": True
        }
        if getattr(instance, "ports") and instance.ports:
            instance_conf["ports"] = list(instance.ports)

        if getattr(instance, "hosts") and instance.hosts:
            instance_conf["networks"]["hadoop.net"] = {"aliases": list(instance.hosts)}

        if getattr(instance, "volumes") and instance.volumes:
            instance_conf["volumes"] = list(instance.volumes)

        if getattr(instance, "environment") and instance.environment:
            instance_conf["environment"] = instance.environment

        if getattr(instance, "more_options") and instance.more_options:
            for k, v in instance.more_options.items():
                instance_conf[k] = v

        compose_yaml["services"][instance.name] = instance_conf
    # if "hive-metastore" in data["hosts"] or "hue" in data["hosts"]:
    #     compose_yaml["services"]["cluster-db"] = {
    #         "image": "postgres:13.1",
    #         "container_name": "cluster-db",
    #         "restart": "always",
    #         "environment": {
    #             "POSTGRES_PASSWORD": "postgres",
    #             "POSTGRES_HOST_AUTH_METHOD": "trust"
    #         },
    #         "networks": ["hadoop.net"],
    #         "ports": ["5432:5432"],
    #         "volumes": []
    #     }
    #     if "hive-metastore" in data["hosts"]:
    #         compose_yaml["services"]["cluster-db"]["volumes"].append(
    #             "./hive/sql/create_db.sql:/docker-entrypoint-initdb.d/create_hive_db.sql"
    #         )
    #     if "hue" in data["hosts"]:
    #         compose_yaml["services"]["cluster-db"]["volumes"].append(
    #             "./hue/sql/create_db.sql:/docker-entrypoint-initdb.d/create_hue_db.sql"
    #         )

    return dump(compose_yaml, Dumper=Dumper)


def build_components(args: Namespace) -> List[DockerComponent]:
    components = [ClusterStarter()]

    if args.all or args.hive or args.hue:
        components.append(ClusterDb(args))
    primary_nn = [PrimaryNamenode(), JournalNode(1), ZookeeperNode(1), YarnHistoryServer()]
    secondary_nn = [SecondaryNamenode(), JournalNode(2), ZookeeperNode(2), ResourceManager()]
    datanode1 = [DataNode(1), JournalNode(3), ZookeeperNode(3)]

    components.append(MultipleComponent("primary-namenode", primary_nn))
    components.append(MultipleComponent("secondary-namenode", secondary_nn))
    components.append(MultipleComponent("datanode1", datanode1))

    return components
