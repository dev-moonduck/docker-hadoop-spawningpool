from abc import ABC
from typing import List, Dict, Set


class DockerComponent:
    @property
    def image(self) -> str:
        raise NotImplementedError()

    @property
    def volumes(self) -> Set[str]:
        raise NotImplementedError()

    @property
    def environment(self) -> Dict[str, str]:
        raise NotImplementedError()

    @property
    def ports(self) -> Set[str]:
        raise NotImplementedError()

    @property
    def hosts(self) -> Set[str]:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def more_options(self) -> dict:
        raise NotImplementedError()


class MultipleComponent(DockerComponent):
    def __init__(self, name: str, components: List[DockerComponent]):
        image = None
        volumes = set()
        environment = {}
        ports = set()
        hosts = set()
        more_options = {}

        for component in components:
            if not image:
                image = component.image
            if component.volumes:
                volumes = volumes.union(component.volumes)
            if component.environment:
                environment.update(component.environment)
            if component.ports:
                ports = ports.union(component.ports)
            if component.hosts:
                hosts = hosts.union(component.hosts)
            if component.more_options:
                more_options.update(component.more_options)
        self._image = image
        self._volumes = list(volumes)
        self._environment = environment
        self._ports = list(ports)
        self._hosts = list(hosts)
        self._more_options = more_options
        self._name = name

    @property
    def image(self) -> str:
        return self._image

    @property
    def volumes(self) -> Set[str]:
        return self._volumes

    @property
    def environment(self) -> Dict[str, str]:
        return self._environment

    @property
    def ports(self) -> Set[str]:
        return self._ports

    @property
    def hosts(self) -> Set[str]:
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
    def volumes(self) -> Set[str]:
        return set()

    @property
    def environment(self) -> Dict[str, str]:
        return {}

    @property
    def ports(self) -> Set[str]:
        return set()

    @property
    def hosts(self) -> Set[str]:
        return set([self.name])

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
    def volumes(self) -> Set[str]:
        return {
            "hive/sql/create_db.sql:/docker-entrypoint-initdb.d/create_hive_db.sql",
            "hue/sql/create_db.sql:/docker-entrypoint-initdb.d/create_hue_db.sql"
        }

    @property
    def environment(self) -> Dict[str, str]:
        return {
            "POSTGRES_HOST_AUTH_METHOD": "trust",
            "POSTGRES_PASSWORD": "postgres"
        }

    @property
    def ports(self) -> Set[str]:
        return set(["5432:5432"])

    @property
    def hosts(self) -> Set[str]:
        return set([self.name])

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
    def volumes(self) -> Set[str]:
        return {
            "hue/conf/hue.ini:/usr/share/hue/desktop/conf/hue.ini",
            "hue/conf/log.conf:/usr/share/hue/desktop/conf/log.conf"
        }

    @property
    def environment(self) -> Dict[str, str]:
        return {
            "HUE_HOME": "/usr/share/hue"
        }

    @property
    def ports(self) -> Set[str]:
        return set(["8888:8888"])

    @property
    def hosts(self) -> Set[str]:
        return set([self.name])

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
    def volumes(self) -> Set[str]:
        return {
            "hadoop/hadoop-bin:/opt/hadoop",
            "scripts/agent.py:/scripts/agent.py",
            "scripts/entrypoint.sh:/scripts/entrypoint.sh",
            "scripts/initialize.sh:/scripts/initialize.sh"
        }

    @property
    def environment(self) -> Dict[str, str]:
        raise NotImplementedError()

    @property
    def ports(self) -> Set[str]:
        raise NotImplementedError()

    @property
    def hosts(self) -> Set[str]:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def more_options(self) -> dict:
        raise NotImplementedError()


class PrimaryNamenode(HadoopNode):
    @property
    def volumes(self) -> Set[str]:
        super().volumes.union({
            "scripts/run_active_nn.sh:/scripts/run_active_nn.sh"
        })
        return super().volumes

    @property
    def environment(self) -> Dict[str, str]:
        return {}

    @property
    def ports(self) -> Set[str]:
        return {"9870:9870"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "primary-namenode"

    @property
    def more_options(self) -> dict:
        return {}


class SecondaryNamenode(HadoopNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "scripts/run_standby_nn.sh:/scripts/run_standby_nn.sh"
        })

    @property
    def environment(self) -> Dict[str, str]:
        return {}

    @property
    def ports(self) -> Set[str]:
        return {"9871:9870"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

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
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "scripts/run_journal.sh:/scripts/run_journal.sh",
            "scripts/run_zookeeper.sh:/scripts/run_zookeeper.sh",
            "conf/zoo.cfg:/opt/zookeeper/zoo.cfg"
        })

    @property
    def environment(self) -> Dict[str, str]:
        return {"MY_NODE_NUM": self.id}

    @property
    def ports(self) -> Set[str]:
        return set()

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

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
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "scripts/run_datanode.sh:/scripts/run_datanode.sh",
            "scripts/run_nodemanager.sh:/scripts/run_nodemanager.sh"
        })

    @property
    def environment(self) -> Dict[str, str]:
        return {}

    @property
    def ports(self) -> Set[str]:
        return {str(9864 + self.id - 1) + ":9864"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "datanode" + str(self.id)

    @property
    def more_options(self) -> dict:
        return {}


class ResourceManager(HadoopNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
           "scripts/run_rm.sh:/scripts/run_rm.sh"
        })

    @property
    def environment(self) -> Dict[str, str]:
        return {}

    @property
    def ports(self) -> Set[str]:
        return {"8088:8088"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "resource-manager"

    @property
    def more_options(self) -> dict:
        return {}


class YarnHistoryServer(HadoopNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "scripts/run_yarn_hs.sh:/scripts/run_yarn_hs.sh"
        })

    @property
    def environment(self) -> Dict[str, str]:
        return {}

    @property
    def ports(self) -> Set[str]:
        return {"8188:8188"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "yarn-history"

    @property
    def more_options(self) -> dict:
        return {}
