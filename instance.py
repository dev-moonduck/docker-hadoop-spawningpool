from component import Component


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
