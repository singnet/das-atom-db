from abc import ABC, abstractmethod
from typing import Any, List

WILDCARD = '*'
UNORDERED_LINK_TYPES = ['Similarity', 'Set']


class IAtomDB(ABC):
    def __repr__(self) -> str:
        return "<Atom database interface>"

    @abstractmethod
    def node_exists(self, node_type: str, node_name: str) -> bool:
        pass

    @abstractmethod
    def link_exists(self, link_type: str, targets: List[str]) -> bool:
        pass

    @abstractmethod
    def get_node_handle(self, node_type: str, node_name: str) -> str:
        pass

    @abstractmethod
    def get_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        pass

    @abstractmethod
    def get_link_targets(self, link_handle: str) -> List[str]:
        pass

    @abstractmethod
    def is_ordered(self, handle: str) -> bool:
        pass

    @abstractmethod
    def get_matched_links(self, link_type: str, target_handles: List[str]):
        pass

    @abstractmethod
    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        pass

    @abstractmethod
    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        pass

    @abstractmethod
    def get_matched_type(self, link_named_type: str):
        pass

    @abstractmethod
    def get_node_name(self, node_handle: str) -> str:
        pass

    @abstractmethod
    def get_matched_node_name(self, node_type: str, substring: str) -> str:
        pass

    @abstractmethod
    def add_node(self, node_type: str, node_name: str) -> None:
        pass

    @abstractmethod
    def add_atom(self, atom_type: str) -> None:
        pass

    @abstractmethod
    def add_link(self, link_type: str, targets: List[str]) -> None:
        pass

    def get_atom_as_dict(self, handle: str, arity: int):
        pass

    def get_atom_as_deep_representation(self, handle: str, arity: int):
        pass

    def count_atoms(self):
        pass

    def clear_database(self) -> None:
        pass
