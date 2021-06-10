from abc import ABC
from typing import List, Dict, Set
from constants import HasConstants


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
        self._volumes = volumes
        self._environment = environment
        self._ports = ports
        self._hosts = hosts
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


class ClusterStarter(DockerComponent, HasConstants):
    @property
    def image(self) -> str:
        return self.CLUSTER_STARTER_IMAGE_NAME

    @property
    def volumes(self) -> Set[str]:
        return {"./cluster-starter/run.sh:/scripts/run.sh"}

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
    def __init__(self, args):
        self._volumes = set()
        if args.hive:
            self._volumes.add("./hive/sql/create_db.sql:/docker-entrypoint-initdb.d/create_hive_db.sql")
        if args.hue:
            self._volumes.add("./hue/sql/create_db.sql:/docker-entrypoint-initdb.d/create_hue_db.sql")

    @property
    def image(self) -> str:
        return "postgres:13.1"

    @property
    def volumes(self) -> Set[str]:
        return self._volumes

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
    def __init__(self, args):
        _volume = {
            "./hue/conf/hue.ini:/usr/share/hue/desktop/conf/hue.ini",
            "./hue/conf/log.conf:/usr/share/hue/desktop/conf/log.conf"
        }
        _env = {
            "HUE_HOME": "/usr/share/hue"
        }
        if args.hive or args.all:
            hive = HiveNode()
            _volume = _volume.union(hive.volumes)
            _env.update(hive.environment)
        self._volume = _volume
        self._env = _env

    @property
    def image(self) -> str:
        return "gethue/hue:4.9.0"

    @property
    def volumes(self) -> Set[str]:
        return self._volume

    @property
    def environment(self) -> Dict[str, str]:
        return self._env

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
            "mem_limit": "4g"
        }


class HadoopNode(ABC, DockerComponent, HasConstants):
    @property
    def image(self) -> str:
        return self.HADOOP_IMAGE_NAME

    @property
    def volumes(self) -> Set[str]:
        return {
            "./cluster-starter/agent.py:/scripts/agent.py",
            "./hadoop/hadoop-bin:/opt/hadoop",
            "./hadoop/scripts/entrypoint.sh:/scripts/entrypoint.sh",
            "./hadoop/scripts/initialize.sh:/scripts/initialize.sh"
        }

    @property
    def environment(self) -> Dict[str, str]:
        return {}

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
        return super().volumes.union({
            "./hadoop/scripts/run_active_nn.sh:/scripts/run_active_nn.sh"
        })

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
            "./hadoop/scripts/run_standby_nn.sh:/scripts/run_standby_nn.sh"
        })

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


class ZookeeperNode(HadoopNode):
    def __init__(self, _id):
        self._id = _id

    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./hadoop/scripts/run_zookeeper.sh:/scripts/run_zookeeper.sh",
            "./hadoop/conf/zoo.cfg:/opt/zookeeper/conf/zoo.cfg"
        })

    @property
    def environment(self) -> Dict[str, str]:
        env = super().environment
        env.update({"MY_NODE_NUM": self._id})
        return env

    @property
    def ports(self) -> Set[str]:
        return set()

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "zookeeper" + str(self._id)

    @property
    def more_options(self) -> dict:
        return {}


class JournalNode(HadoopNode):
    def __init__(self, _id):
        self._id = _id

    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./hadoop/scripts/run_journal.sh:/scripts/run_journal.sh"
        })

    @property
    def ports(self) -> Set[str]:
        return set()

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "journalnode" + str(self._id)

    @property
    def more_options(self) -> dict:
        return {}


class DataNode(HadoopNode):
    def __init__(self, _id):
        self._id = _id

    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./hadoop/scripts/run_datanode.sh:/scripts/run_datanode.sh",
            "./hadoop/scripts/run_nodemanager.sh:/scripts/run_nodemanager.sh"
        })

    @property
    def ports(self) -> Set[str]:
        return {str(9864 + self._id - 1) + ":9864"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "datanode" + str(self._id)

    @property
    def more_options(self) -> dict:
        return {}


class ResourceManager(HadoopNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
           "./hadoop/scripts/run_rm.sh:/scripts/run_rm.sh"
        })

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
            "./hadoop/scripts/run_yarn_hs.sh:/scripts/run_yarn_hs.sh"
        })

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


class HiveNode(HadoopNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./hive/hive-bin:/opt/hive"
        })

    @property
    def environment(self) -> Dict[str, str]:
        hive_home = "/opt/hive"
        env = super().environment
        env.update({
            "HIVE_CONF_DIR": f"{hive_home}/conf",
            "HIVE_HOME": hive_home
        })
        return env

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
        return {}


class HiveMetastore(HiveNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./hive/scripts/run_hive_metastore.sh:/scripts/run_hive_metastore.sh"
        })

    @property
    def ports(self) -> Set[str]:
        return {"9083:9083"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "hive-metastore"


class HiveServer(HiveNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./hive/scripts/run_hive_server.sh:/scripts/run_hive_server.sh"
        })

    @property
    def ports(self) -> Set[str]:
        return {"10000:10000", "10001:10001", "10002:10002"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "hive-server"


class SparkNode(HadoopNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({"./spark/spark-bin:/opt/spark"})

    @property
    def environment(self) -> Dict[str, str]:
        env = super().environment
        env.update({"SPARK_HOME": "/opt/spark"})
        return env

    @property
    def more_options(self) -> dict:
        return {}


class SparkHistory(SparkNode):
    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./spark-history/scripts/run_history_server.sh:/scripts/run_history_server.sh"
        })

    @property
    def ports(self) -> Set[str]:
        return {"18080:18080"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "spark-history"


class SparkThrift(SparkNode, HiveNode):
    @property
    def volumes(self) -> Set[str]:
        return super(SparkNode, self).volumes.union(super(HiveNode, self).volumes).union({
            "./spark-thrift/scripts/run_thrift_server.sh:/scripts/run_thrift_server.sh"
        })

    @property
    def ports(self) -> Set[str]:
        return {"10010:10000", "10011:10001"}

    @property
    def hosts(self) -> Set[str]:
        return {self.name}

    @property
    def name(self) -> str:
        return "spark-thrift"


class PrestoNode(DockerComponent):
    @property
    def image(self) -> str:
        return "openjdk:8-jre-slim"

    @property
    def volumes(self) -> Set[str]:
        return {
            "./presto/presto-bin/bin:/opt/presto/bin",
            "./presto/presto-bin/lib:/opt/presto/lib",
            "./presto/presto-bin/plugin:/opt/presto/plugin",
            "./presto/scripts/run.sh:/scripts/run_presto.sh"
        }

    @property
    def environment(self) -> Dict[str, str]:
        return {
            "PRESTO_HOME": "/opt/presto"
        }

    @property
    def more_options(self) -> dict:
        return {}


class PrestoServer(PrestoNode):
    @property
    def ports(self) -> Set[str]:
        return set(["8080:8080"])

    @property
    def hosts(self) -> Set[str]:
        return set([self.name])

    @property
    def name(self) -> str:
        return "presto-server"

    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./presto/conf/server/config.properties:/opt/presto/etc/config.properties",
            "./presto/conf/server/jvm.config:/opt/presto/etc/jvm.config"
        })

    @property
    def environment(self) -> Dict[str, str]:
        inherited = super().environment
        inherited.update({
            "PRESTO_NODE_ID": f"server1"
        })
        return inherited


class PrestoWorker(PrestoNode):
    def __init__(self, _id):
        self._id = _id

    @property
    def ports(self) -> Set[str]:
        return set()

    @property
    def hosts(self) -> Set[str]:
        return set([self.name])

    @property
    def name(self) -> str:
        return f"presto-worker{self._id}"

    @property
    def volumes(self) -> Set[str]:
        return super().volumes.union({
            "./presto/conf/worker/config.properties:/opt/presto/etc/config.properties",
            "./presto/conf/worker/jvm.config:/opt/presto/etc/jvm.config"
        })

    @property
    def environment(self) -> Dict[str, str]:
        inherited = super().environment
        inherited.update({
            "PRESTO_NODE_ID": f"worker{self._id}"
        })
        return inherited
