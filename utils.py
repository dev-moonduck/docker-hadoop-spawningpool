def get_images_to_build(config):
    image_set = set()
    for _, instance in config["instances"].items():
        image_set.add(instance["image"])
    return image_set
