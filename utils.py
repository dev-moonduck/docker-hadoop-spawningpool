import time
import hashlib
import uuid
import random


def get_images_to_build(config):
    image_set = set()
    for _, instance in config["instances"].items():
        image_set.add(instance["image"])
    return image_set


def random_string():
    return hashlib.md5("{}{}{}".format(str(time.time_ns()), str(uuid.uuid4()), str(random.random()))
                       .encode("UTF-8")).hexdigest()[0:10]
