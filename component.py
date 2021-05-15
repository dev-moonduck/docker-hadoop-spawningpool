from abc import ABCMeta
from abc import abstractmethod


class Component(metaclass=ABCMeta):
    def __init__(self):
        pass

    @property
    @abstractmethod
    def to_copy(self, args) -> list[str]:
        raise NotImplementedError("Base component must not be instantiated")

    @property
    @abstractmethod
    def to_template(self, args) -> list[str]:
        raise NotImplementedError("Base component must not be instantiated")

    @property
    @abstractmethod
    def to_download(self, args) -> list[str]:
        raise NotImplementedError("Base component must not be instantiated")

    @property
    @abstractmethod
    def volumes(self, args) -> list[str]:
        raise NotImplementedError("Base component must not be instantiated")

    @property
    @abstractmethod
    def environment(self, args) -> list[str]:
        raise NotImplementedError("Base component must not be instantiated")

    @property
    @abstractmethod
    def ports(self, args) -> list[str]:
        raise NotImplementedError("Base component must not be instantiated")

    @property
    @abstractmethod
    def hosts(self, args) -> list[str]:
        raise NotImplementedError("Base component must not be instantiated")

    @property
    @abstractmethod
    def name(self, args) -> str:
        raise NotImplementedError("Base component must not be instantiated")

    @property
    @abstractmethod
    def more_options(self, args) -> dict:
        raise NotImplementedError("Base component must not be instantiated")
