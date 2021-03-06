import yaml
from yaml import dump
try:
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Dumper
import copy
from collections import OrderedDict
from instance import DockerComponent, MultipleComponent, PrimaryNamenode, SecondaryNamenode, JournalNode, DataNode, \
    ResourceManager, YarnHistoryServer, ClusterStarter, ClusterDb, ZookeeperNode, HiveServer, HiveMetastore, \
    SparkHistory, SparkThrift, Hue, PrestoServer, PrestoWorker
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
    return dump(compose_yaml, Dumper=Dumper)


def build_components(args: Namespace) -> List[DockerComponent]:
    components = [ClusterStarter()]

    if args.all or args.hive or args.hue:
        components.append(ClusterDb(args))
    primary_nn = [PrimaryNamenode(), JournalNode(1), ZookeeperNode(1), YarnHistoryServer()]
    if args.all or args.hive:
        primary_nn.append(HiveServer())
        primary_nn.append(HiveMetastore())

    if args.all or args.presto:
        primary_nn.append(PrestoServer())

    components.append(MultipleComponent("primary-namenode", primary_nn))

    secondary_nn = [SecondaryNamenode(), JournalNode(2), ZookeeperNode(2), ResourceManager()]

    if args.all or args.spark or args.spark_history or args.spark_thrift:
        secondary_nn.append(SparkHistory())

    if args.all or args.spark_thrift:
        secondary_nn.append(SparkThrift())

    components.append(MultipleComponent("secondary-namenode", secondary_nn))

    datanode1 = [DataNode(1), JournalNode(3), ZookeeperNode(3)]
    if args.all or args.presto:
        datanode1.append(PrestoWorker(1))
    components.append(MultipleComponent("datanode1", datanode1))

    additional_datanodes = []
    for i in range(2, args.num_datanode + 1):
        additional_datanodes.append(DataNode(i))

    # Add presto worker in data node, num of presto worker does not exceed num of datanode
    if (args.all or args.presto) and args.num_presto_worker > 1:
        worker_cnt = 1
        while worker_cnt < args.num_presto_worker and worker_cnt <= len(additional_datanodes):
            datanode = additional_datanodes[worker_cnt - 1]
            additional_datanodes[worker_cnt - 1] = MultipleComponent(datanode.name, [datanode,
                                                                                     PrestoWorker(worker_cnt + 1)])
            worker_cnt += 1

    components += additional_datanodes

    if args.hue or args.all:
        components.append(Hue(args))

    return components
