from abc import ABC


class DockerComponent:
    @property
    def image(self) -> str:
        raise NotImplementedError()

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


class MultipleComponent(DockerComponent):
    def __init__(self, name: str, components: list[DockerComponent]):
        self._image = None
        self._volumes = []
        self._environment = {}
        self._ports = []
        self._hosts = []
        self._name = name
        self._more_options = {}

        for component in components:
            if not self._image:
                self._image = component.image
            if component.volumes:
                self._volumes += component.volumes
            if component.environment:
                self._environment.update(component.environment)
            if component.ports:
                self._ports += component.ports
            if component.hosts:
                self._hosts += component.hosts
            if component.more_options:
                self._more_options.update(component.more_options)

    @property
    def image(self) -> str:
        return self._image

    @property
    def volumes(self) -> list[str]:
        return self._volumes

    @property
    def environment(self) -> dict[str, str]:
        return self._environment

    @property
    def ports(self) -> list[str]:
        return self._ports

    @property
    def hosts(self) -> list[str]:
        return self._hosts

    @property
    def name(self) -> str:
        return self._name

    @property
    def more_options(self) -> dict:
        return self._more_options


class ClusterStater(DockerComponent):
    @property
    def image(self) -> str:
        return "cluster-starter"

    @property
    def volumes(self) -> list[str]:
        return []

    @property
    def environment(self) -> dict[str, str]:
        return {}

    @property
    def ports(self) -> list[str]:
        return []

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "cluster-starter"

    @property
    def more_options(self) -> dict:
        return {}


class ClusterDb(DockerComponent):
    @property
    def image(self) -> str:
        return "postgres:13.1"

    @property
    def volumes(self) -> list[str]:
        return [
            "hive/sql/create_db.sql:/docker-entrypoint-initdb.d/create_hive_db.sql",
            "hue/sql/create_db.sql:/docker-entrypoint-initdb.d/create_hue_db.sql"
        ]

    @property
    def environment(self) -> dict[str, str]:
        return {
            "POSTGRES_HOST_AUTH_METHOD": "trust",
            "POSTGRES_PASSWORD": "postgres"
        }

    @property
    def ports(self) -> list[str]:
        return ["5432:5432"]

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "cluster-db"

    @property
    def more_options(self) -> dict:
        return {}


class Hue(DockerComponent):
    @property
    def image(self) -> str:
        return "gethue/hue:4.9.0"

    @property
    def volumes(self) -> list[str]:
        return [
            "hue/conf/hue.ini:/usr/share/hue/desktop/conf/hue.ini",
            "hue/conf/log.conf:/usr/share/hue/desktop/conf/log.conf"
        ]

    @property
    def environment(self) -> dict[str, str]:
        return {
            "HUE_HOME": "/usr/share/hue"
        }

    @property
    def ports(self) -> list[str]:
        return ["8888:8888"]

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "hue"

    @property
    def more_options(self) -> dict:
        return {
            "mem_limit": "2g"
        }


class HadoopNode(ABC, DockerComponent):
    @property
    def image(self) -> str:
        return "local-hadoop"

    @property
    def volumes(self) -> list[str]:
        return [
            "hadoop/hadoop-bin:/opt/hadoop",
            "scripts/agent.py:/scripts/agent.py",
            "scripts/entrypoint.sh:/scripts/entrypoint.sh",
            "scripts/initialize.sh:/scripts/initialize.sh"
        ]

    @property
    def environment(self) -> dict[str, str]:
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
    def environment(self) -> dict[str, str]:
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
    def environment(self) -> dict[str, str]:
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
    def environment(self) -> dict[str, str]:
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
    def __init__(self, id):
        self.id = id

    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
            "scripts/run_datanode.sh:/scripts/run_datanode.sh",
            "scripts/run_nodemanager.sh:/scripts/run_nodemanager.sh"
        ]

    @property
    def environment(self) -> dict[str, str]:
        return {}

    @property
    def ports(self) -> list[str]:
        return [str(9864 + self.id - 1) + ":9864"]

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "datanode" + str(self.id)

    @property
    def more_options(self) -> dict:
        return {}


class ResourceManager(HadoopNode):
    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
           "scripts/run_rm.sh:/scripts/run_rm.sh"
        ]

    @property
    def environment(self) -> dict[str, str]:
        return {}

    @property
    def ports(self) -> list[str]:
        return ["8088:8088"]

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "resource-manager"

    @property
    def more_options(self) -> dict:
        return {}


class YarnHistoryServer(HadoopNode):
    @property
    def volumes(self) -> list[str]:
        return super().volumes + [
            "scripts/run_yarn_hs.sh:/scripts/run_yarn_hs.sh"
        ]

    @property
    def environment(self) -> dict[str, str]:
        return {}

    @property
    def ports(self) -> list[str]:
        return ["8188:8188"]

    @property
    def hosts(self) -> list[str]:
        return [self.name]

    @property
    def name(self) -> str:
        return "yarn-history"

    @property
    def more_options(self) -> dict:
        return {}
