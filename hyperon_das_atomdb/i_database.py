from abc import ABC, abstractmethod
from typing import Any, List

WILDCARD = '*'
UNORDERED_LINK_TYPES = ['Similarity', 'Set']


class IAtomDB(ABC):
    def __repr__(self) -> str:
        """
        Magic method for string representation of the class.
        Returns a string representation of the IAtomDB class.
        """
        return "<Atom database interface>"  # pragma no cover

    @abstractmethod
    def node_exists(self, node_type: str, node_name: str) -> bool:
        """
        Check if a node with the specified type and name exists in the database.

        Args:
            node_type (str): The node type.
            node_name (str): The node name.

        Returns:
            bool: True if the node exists, False otherwise.
        """
        ...  # pragma no cover

    @abstractmethod
    def link_exists(self, link_type: str, targets: List[str]) -> bool:
        """
        Check if a link with the specified type and targets exists in the database.

        Args:
            link_type (str): The link type.
            targets (List[str]): A list of link target identifiers.

        Returns:
            bool: True if the link exists, False otherwise.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_node_handle(self, node_type: str, node_name: str) -> str:
        """
        Get the handle of the node with the specified type and name.

        Args:
            node_type (str): The node type.
            node_name (str): The node name.

        Returns:
            str: The node handle.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        """
        Get the handle of the link with the specified type and targets.

        Args:
            link_type (str): The link type.
            target_handles (List[str]): A list of link target identifiers.

        Returns:
            str: The link handle.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_link_targets(self, link_handle: str) -> List[str]:
        """
        Get the target handles of a link specified by its handle.

        Args:
            link_handle (str): The link handle.

        Returns:
            List[str]: A list of target identifiers of the link.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_link_type(self, link_handle: str) -> str:
        """
        Get the type of the link with the specified handle.

        Args:
            link_handle (str): The link handle.

        Returns:
            str: The link type.
        """
        ...  # pragma no cover

    @abstractmethod
    def is_ordered(self, link_handle: str) -> bool:
        """
        Check if a link specified by its handle is ordered.

        Args:
            link_handle (str): The link handle.

        Returns:
            bool: True if the link is ordered, False otherwise.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_matched_links(self, link_type: str, target_handles: List[str]):
        """
        Get links that match the specified type and targets.

        Args:
            link_type (str): The link type.
            target_handles (List[str]): A list of link target identifiers.

        Returns:
            [return-type]: The return type description (not specified in the code).
        """
        ...  # pragma no cover

    @abstractmethod
    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        """
        Get all nodes of a specific type.

        Args:
            node_type (str): The node type.
            names (bool, optional): If True, return node names instead of handles. Default is False.

        Returns:
            List[str]: A list of node handles or names, depending on the value of 'names'.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        """
        Get nodes that match a specified template.

        Args:
            template (List[Any]): A list of template parameters (parameter type not specified in the code).

        Returns:
            List[str]: A list of identifiers of nodes matching the template.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_matched_type(self, link_type: str):
        """
        Get links that match a specified link type.

        Args:
            link_type (str): The link type.

        Returns:
            [return-type]: The return type description (not specified in the code).
        """
        ...  # pragma no cover

    @abstractmethod
    def get_node_name(self, node_handle: str) -> str:
        """
        Get the name of the node with the specified handle.

        Args:
            node_handle (str): The node handle.

        Returns:
            str: The node name.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_node_type(self, node_handle: str) -> str:
        """
        Get the type of the node with the specified handle.

        Args:
            node_handle (str): The node handle.

        Returns:
            str: The node type.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_matched_node_name(self, node_type: str, substring: str) -> str:
        """
        Get the name of a node of the specified type containing the given substring.

        Args:
            node_type (str): The node type.
            substring (str): The substring to search for in node names.

        Returns:
            str: The name of the matching node.
        """
        ...  # pragma no cover

    def add_node(self, node_type: str, node_name: str) -> None:
        """
        Add a node with the specified type and name to the database.

        Args:
            node_type (str): The node type.
            node_name (str): The node name.
        """
        ...  # pragma no cover

    def add_link(self, link_type: str, targets: List[str]) -> None:
        """
        Add a link with the specified type and targets to the database.

        Args:
            link_type (str): The link type.
            targets (List[str]): A list of link target identifiers.
        """
        ...  # pragma no cover

    def get_atom_as_dict(self, handle: str, arity: int):
        """
        Get an atom as a dictionary representation.

        Args:
            handle (str): The atom handle.
            arity (int): The arity of the atom.

        Returns:
            [return-type]: The return type description (not specified in the code).
        """
        ...  # pragma no cover

    def get_atom_as_deep_representation(self, handle: str, arity: int):
        """
        Get an atom as a deep representation.

        Args:
            handle (str): The atom handle.
            arity (int): The arity of the atom.

        Returns:
            [return-type]: The return type description (not specified in the code).
        """
        ...  # pragma no cover

    def count_atoms(self):
        """
        Count the total number of atoms in the database.

        Returns:
            [return-type]: The return type description (not specified in the code).
        """
        ...  # pragma no cover

    def clear_database(self) -> None:
        """
        Clear the entire database, removing all data.

        Returns:
            None
        """
        ...  # pragma no cover
