from abc import ABC, abstractmethod
from typing import Any, List

from hyperon_das_atomdb.exceptions import (
    LinkDoesNotExistException,
    NodeDoesNotExistException,
)
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

WILDCARD = '*'
UNORDERED_LINK_TYPES = ['Similarity', 'Set']


class IAtomDB(ABC):
    def __repr__(self) -> str:
        return "<Atom database interface>"  # pragma no cover

    def _create_node_handle(self, node_type: str, node_name: str) -> str:
        return ExpressionHasher.terminal_hash(node_type, node_name)

    def _create_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        named_type_hash = ExpressionHasher.named_type_hash(link_type)
        return ExpressionHasher.expression_hash(
            named_type_hash, target_handles
        )

    def node_exists(self, node_type: str, node_name: str) -> bool:
        try:
            self.get_node_handle(node_type, node_name)
            return True
        except NodeDoesNotExistException:
            return False

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        try:
            self.get_link_handle(link_type, target_handles)
            return True
        except LinkDoesNotExistException:
            return False

    @abstractmethod
    def get_node_handle(self, node_type: str, node_name: str) -> str:
        ...  # pragma no cover

    @abstractmethod
    def get_node_name(self, node_handle: str) -> str:
        ...  # pragma no cover

    @abstractmethod
    def get_node_type(self, node_handle: str) -> str:
        ...  # pragma no cover

    @abstractmethod
    def get_matched_node_name(self, node_type: str, substring: str) -> str:
        ...  # pragma no cover

    @abstractmethod
    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        ...  # pragma no cover

    @abstractmethod
    def get_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        ...  # pragma no cover

    @abstractmethod
    def get_link_targets(self, link_handle: str) -> List[str]:
        ...  # pragma no cover

    @abstractmethod
    def is_ordered(self, link_handle: str) -> bool:
        ...  # pragma no cover

    @abstractmethod
    def get_matched_links(self, link_type: str, target_handles: List[str]):
        ...  # pragma no cover

    @abstractmethod
    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        ...  # pragma no cover

    @abstractmethod
    def get_matched_type(self, link_type: str):
        ...  # pragma no cover

    @abstractmethod
    def get_atom_as_dict(self, handle: str, arity: int):
        ...  # pragma no cover

    @abstractmethod
    def get_atom_as_deep_representation(self, handle: str, arity: int):
        ...  # pragma no cover

    @abstractmethod
    def count_atoms(self):
        ...  # pragma no cover

    @abstractmethod
    def clear_database(self) -> None:
        ...  # pragma no cover

    # def add_node(self, node_type: str, node_name: str) -> None:
    #     ...  # pragma no cover

    # def add_link(self, link_type: str, targets: List[str]) -> None:
    #     ...  # pragma no cover
