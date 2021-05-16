from component import Component
from abc import ABC


class Instance:
    DEFAULT_NETWORK = "hadoop.net"

    def __init__(self, image: str, name: str, components: list[Component]):
        self._image = image
        self._container_name = name
        self._network = {
            self.DEFAULT_NETWORK: None
        }
        self._environment = []
        self._volumes = []
        self._ports = []
        self._more_options = {
            "tty": True
        }
        for c in components:
            if c.environment:
                self._environment += c.environment
            if c.volumes:
                self._volumes += c.volumes
            if c.ports:
                self._ports += c.ports
            if c.more_options:
                self._more_options.update(c.more_options)
            if c.hosts:
                if not self._network[self.DEFAULT_NETWORK]:
                    self._network[self.DEFAULT_NETWORK] = { "aliases": [] }
                cur_hosts = self._network[self.DEFAULT_NETWORK]["aliases"]
                cur_hosts += c.hosts

    @property
    def image(self):
        return self._image

    @property
    def environment(self):
        return self._environment

    @property
    def volumes(self):
        return self._volumes

    @property
    def container_name(self):
        return self._container_name

    @property
    def networks(self):
        return self._networks

    @property
    def ports(self):
        return self._ports

    @property
    def more_options(self):
        return self._more_options



class DockerInstance:
    @property
    def volumes(self) -> list[str]:
        raise NotImplementedError()

    @property
    def environment(self) -> dict:
        raise NotImplementedError()

    @property
    def ports(self) -> list[str]:
        raise NotImplementedError()

    @property
    def hosts(self) -> list[str]:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def more_options(self) -> dict:
        raise NotImplementedError()


class HadoopNode(ABC, DockerInstance):
    @property
    def volumes(self) -> list[str]:
        return [
            "hadoop/hadoop-bin:/opt/hadoop",
            "scripts/agent.py:/scripts/agent.py",
            "scripts/entrypoint.sh:/scripts/entrypoint.sh",
            "scripts/initialize.sh:/scripts/initialize.sh"
        ]

    @property
    def environment(self) -> dict:
        raise NotImplementedError()

    @property
    def ports(self) -> list[str]:
        raise NotImplementedError()

    @property
    def hosts(self) -> list[str]:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def more_options(self) -> dict:
        raise NotImplementedError()


class PrimaryNamenode(HadoopNode):
    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
            "scripts/run_active_nn.sh:/scripts/run_active_nn.sh"
        ]

    @property
    def environment(self) -> dict:
        return {}

    @property
    def ports(self) -> list[str]:
        return ["9870:9870"]

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "primary-namenode"

    @property
    def more_options(self) -> dict:
        return {}


class SecondaryNamenode(HadoopNode):
    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
            "scripts/run_standby_nn.sh:/scripts/run_standby_nn.sh"
        ]

    @property
    def environment(self) -> dict:
        return {}

    @property
    def ports(self) -> list[str]:
        return ["9871:9870"]

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "secondary-namenode"

    @property
    def more_options(self) -> dict:
        return {}


class JournalNode(HadoopNode):
    def __init__(self, id):
        self.id = id

    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
            "scripts/run_journal.sh:/scripts/run_journal.sh",
            "scripts/run_zookeeper.sh:/scripts/run_zookeeper.sh",
            "conf/zoo.cfg:/opt/zookeeper/zoo.cfg"
        ]

    @property
    def environment(self) -> dict:
        return {"MY_NODE_NUM": self.id}

    @property
    def ports(self) -> list[str]:
        return []

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "journalnode" + str(self.id)

    @property
    def more_options(self) -> dict:
        return {}


class DataNode(HadoopNode):
    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
            "scripts/run_datanode.sh:/scripts/run_datanode.sh",
            "scripts/run_nodemanager.sh:/scripts/run_nodemanager.sh"
        ]

    @property
    def environment(self) -> dict:
        return {}

    @property
    def ports(self) -> list[str]:
        return []

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "journalnode" + str(self.id)

    @property
    def more_options(self) -> dict:
        return {}

class ResourceManager(HadoopNode):
    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
           "scripts/run_rm.sh:/scripts/run_rm.sh"
        ]

class YarnHistoryServer(HadoopNode):
    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
            "scripts/run_yarn_hs.sh:/scripts/run_yarn_hs.sh"
        ]
