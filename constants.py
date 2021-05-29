from pathlib import Path
import os


class HasConstants:
    ROOT_PATH = Path(os.path.abspath(__file__)).parent
    BASE_PATH = os.path.join(str(ROOT_PATH), "templates")
    TARGET_BASE_PATH = os.path.join(str(ROOT_PATH), "target")
    TEMPLATE_EXTENSION = "template"
    CLUSTER_NAME = "nameservice"
    HADOOP_IMAGE_NAME = "local-hadoop"
    CLUSTER_STARTER_IMAGE_NAME = "cluster-starter"
