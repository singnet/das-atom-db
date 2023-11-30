from abc import ABC, abstractmethod
from typing import Any, Dict, List

from hyperon_das_atomdb.exceptions import (
    AddNodeException,
    AddLinkException,
    LinkDoesNotExistException,
    NodeDoesNotExistException,
)
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

WILDCARD = '*'
UNORDERED_LINK_TYPES = []


class AtomDB(ABC):
    def __repr__(self) -> str:
        """
        Magic method for string representation of the class.
        Returns a string representation of the AtomDB class.
        """
        return "<Atom database abstract class>"  # pragma no cover

    def _node_handle(self, node_type: str, node_name: str) -> str:
        return ExpressionHasher.terminal_hash(node_type, node_name)

    def _link_handle(self, link_type: str, target_handles: List[str]) -> str:
        named_type_hash = ExpressionHasher.named_type_hash(link_type)
        return ExpressionHasher.expression_hash(
            named_type_hash, target_handles
        )

    def _recursive_link_split(self, params: Dict[str, Any]) -> (str, Any):
        name = params.get('name')
        atom_type = params['type']
        if name:
            return (_node_handle(atom_type, name), atom_type)
        targets, composite_type = [_recursive_link_handle(target) for target in params['target']]
        composite_type.insert(0, atom_type)
        return (_link_handle(atom_type, targets), composite_type)

    def _add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        node_type = node_params.get('type')
        node_name = node_params.get('name')
        if node_type is None or node_name is None:
            raise AddNodeException(
                message='The "name" and "type" fields must be sent',
                details=node_params,
            )

        handle = self._node_handle(node_type, node_name)
        node = {
            '_id': handle,
            'composite_type_hash': ExpressionHasher.named_type_hash(node_type),
            'name': node_name,
            'named_type': node_type,
        }
        node.update(node_params)
        node.pop('type')
        return (handle, node)

    def _add_link(self, link_params: Dict[str, Any], toplevel: bool = True) -> Dict[str, Any]:

        link_type = link_params.get('type')
        targets = link_params.get('targets')
        if link_type is None or targets is None:
            raise AddLinkException(
                message='The "type" and "targets" fields must be sent',
                details=link_params,
            )

        link_type_hash = ExpressionHasher.named_type_hash(link_type)

        targets_hash = []
        composite_type = [link_type_hash]
        composite_type_hash = [link_type_hash]
        for target in targets:
            if 'targets' not in target.keys():
                atom = self.add_node(target)
                atom_hash = ExpressionHasher.named_type_hash(
                    atom['named_type']
                )
                composite_type.append(atom_hash)
            else:
                atom = self.add_link(target, toplevel=False)
                composite_type.append(atom['composite_type'])
                atom_hash = atom['composite_type_hash']
            composite_type_hash.append(atom_hash)
            targets_hash.append(atom['_id'])
        handle = ExpressionHasher.expression_hash(link_type_hash, targets_hash)

        arity = len(targets)
        link = {
            '_id': handle,
            'composite_type_hash': ExpressionHasher.composite_hash(
                composite_type_hash
            ),
            'is_toplevel': toplevel,
            'composite_type': composite_type,
            'named_type': link_type,
            'named_type_hash': link_type_hash,
        }
        for item in range(arity):
            link[f'key_{item}'] = targets_hash[item]

        link.update(link_params)
        link.pop('type')
        link.pop('targets')

        return (handle, link, targets_hash)

    def node_exists(self, node_type: str, node_name: str) -> bool:
        """
        Check if a node with the specified type and name exists in the database.

        Args:
            node_type (str): The node type.
            node_name (str): The node name.

        Returns:
            bool: True if the node exists, False otherwise.
        """
        try:
            self.get_node_handle(node_type, node_name)
            return True
        except NodeDoesNotExistException:
            return False

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        """
        Check if a link with the specified type and targets exists in the database.

        Args:
            link_type (str): The link type.
            targets (List[str]): A list of link target identifiers.

        Returns:
            bool: True if the link exists, False otherwise.
        """
        try:
            self.get_link_handle(link_type, target_handles)
            return True
        except LinkDoesNotExistException:
            return False

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

    @abstractmethod
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

    @abstractmethod
    def count_atoms(self):
        """
        Count the total number of atoms in the database.

        Returns:
            [return-type]: The return type description (not specified in the code).
        """
        ...  # pragma no cover

    @abstractmethod
    def clear_database(self) -> None:
        """
        Clear the entire database, removing all data.

        Returns:
            None
        """
        ...  # pragma no cover

    @abstractmethod
    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a node to the database.

        This method allows you to add a node to the database
        with the specified node parameters. A node must have 'type' and
        'name' fields in the node_params dictionary.

        Args:
            node_params (Dict[str, Any]): A dictionary containing
                node parameters. It should have the following keys:
                - 'type': The type of the node.
                - 'name': The name of the node.

        Returns:
            Dict[str, Any]: The information about the added node,
            including its unique key and other details.

        Raises:
            AddNodeException: If the 'type' or 'name' fields are missing
                in node_params.

        Note:
            This method creates a unique key for the node based on its type
            and name. If a node with the same key already exists,
            it just returns the node.

        Example:
            To add a node, use this method like this:
            >>> node_params = {
                    'type': 'Reactome',
                    'name': 'Reactome:R-HSA-164843',
                }
            >>> db.add_node(node_params)
        """
        ...  # pragma no cover

    @abstractmethod
    def add_link(self, link_params: Dict[str, Any], toplevel: bool = True) -> Dict[str, Any]:
        """
        Adds a link to the database.

        This method allows to add a link to the database with the specified
            link parameters.
        A link must have a 'type' and 'targets' field
            in the link_params dictionary.

        Args:
            link_params (Dict[str, Any]): A dictionary containing
                link parameters.
                It should have the following keys:
                - 'type': The type of the link.
                - 'targets': A list of target elements.
            toplevel: boolean flag to indicate toplevel links
                i.e. links which are not nested inside other links.

        Returns:
            Dict[str, Any]: The information about the added link,
                including its unique key and other details.

        Raises:
            AddLinkException: If the 'type' or 'targets' fields
                are missing in link_params.

        Note:
            This method supports recursion when a target element
                itself contains links.
            It calculates a unique key for the link based on
                its type and targets.
            If a link with the same key already exists,
                it just returns the link.

        Example:
            To add a link, use this method like this:
            >>> link_params = {
                    'type': 'Evaluation',
                    'targets': [
                        {
                            'type': 'Predicate',
                            'name': 'Predicate:has_name'
                        },
                        {
                            'type': 'Set',
                            'targets': [
                                {
                                    'type': 'Reactome',
                                    'name': 'Reactome:R-HSA-164843',
                                },
                                {
                                    'type': 'Concept',
                                    'name': 'Concept:2-LTR circle formation',
                                },
                            ],
                        },
                    ],
                }
            >>> db.add_link(link_params)
        """
        ...  # pragma no cover
