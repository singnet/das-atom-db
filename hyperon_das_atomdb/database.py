"""
This module defines the abstract base class for Atom databases and provides various
utility methods for managing nodes and links.

The AtomDB class includes methods for adding, deleting, and retrieving nodes and links,
as well as methods for querying the database by different criteria. It also supports
indexing and pattern matching for efficient querying.

Classes:
    AtomDB: An abstract base class for Atom databases, providing a common interface
        for different implementations.
    FieldNames: An enumeration of field names used in the database.
    FieldIndexType: An enumeration of index types used in the database.

Constants:
    WILDCARD: A constant representing a wildcard character.
    UNORDERED_LINK_TYPES: A list of unordered link types.

Type Aliases:
    IncomingLinksT: A type alias for incoming links.
"""

import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from enum import Enum
from typing import Any, TypeAlias

from hyperon_das_atomdb.exceptions import AddLinkException, AddNodeException, AtomDoesNotExist
from hyperon_das_atomdb.logger import logger
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

WILDCARD = "*"
UNORDERED_LINK_TYPES: list[Any] = []

# pylint: disable=invalid-name

HandleT: TypeAlias = str

AtomT: TypeAlias = dict[str, Any]

NodeT: TypeAlias = AtomT

NodeParamsT: TypeAlias = NodeT

LinkT: TypeAlias = AtomT

LinkParamsT: TypeAlias = LinkT

HandleListT: TypeAlias = list[HandleT]

IncomingLinksT: TypeAlias = HandleListT | list[AtomT]

MatchedTargetsListT: TypeAlias = list[tuple[HandleT, tuple[HandleT, ...]]]

MatchedLinksResultT: TypeAlias = HandleListT | MatchedTargetsListT

MatchedTypesResultT: TypeAlias = MatchedTargetsListT

# pylint: enable=invalid-name


class FieldNames(str, Enum):
    """Enumeration of field names used in the AtomDB."""

    ID_HASH = "_id"
    COMPOSITE_TYPE = "composite_type"
    COMPOSITE_TYPE_HASH = "composite_type_hash"
    NODE_NAME = "name"
    TYPE_NAME = "named_type"
    TYPE_NAME_HASH = "named_type_hash"
    KEY_PREFIX = "key"
    KEYS = "keys"
    IS_TOPLEVEL = "is_toplevel"


class FieldIndexType(str, Enum):
    """Enumeration of index types used in the AtomDB."""

    BINARY_TREE = "binary_tree"
    TOKEN_INVERTED_LIST = "token_inverted_list"


class AtomDB(ABC):
    """
    Abstract class for Atom databases.
    """

    key_pattern = re.compile(r"key_\d+")

    def __repr__(self) -> str:
        """
        Magic method for string representation of the class.
        Returns a string representation of the AtomDB class.
        """
        return "<Atom database abstract class>"  # pragma no cover

    @staticmethod
    def node_handle(node_type: str, node_name: str) -> str:
        """
        Generate a unique handle for a node based on its type and name.

        Args:
            node_type (str): The type of the node.
            node_name (str): The name of the node.

        Returns:
            str: A unique handle for the node.
        """
        return ExpressionHasher.terminal_hash(node_type, node_name)

    @staticmethod
    def link_handle(link_type: str, target_handles: list[str]) -> str:
        """
        Generate a unique handle for a link based on its type and target handles.

        Args:
            link_type (str): The type of the link.
            target_handles (list[str]): A list of target handles for the link.

        Returns:
            str: A unique handle for the link.
        """
        named_type_hash = ExpressionHasher.named_type_hash(link_type)
        return ExpressionHasher.expression_hash(named_type_hash, target_handles)

    def _reformat_document(self, document: AtomT, **kwargs) -> AtomT:
        """
        Transform a document to the target format.

        Args:
            document (AtomT): The document to transform.
            **kwargs: Additional keyword arguments that may be used for transformation.
                - targets_document (bool, optional): If True, include the `targets_document` in the
                    response. Defaults to False.
                - deep_representation (bool, optional): If True, include a deep representation of
                    the targets. Defaults to False.

        Returns:
            AtomT: The transformed document in the target format.
        """
        answer: AtomT = document
        if kwargs.get("targets_document", False):
            targets_document = [self.get_atom(target) for target in answer["targets"]]
            answer["targets_document"] = targets_document

        if kwargs.get("deep_representation", False):

            def _recursive_targets(targets, **_kwargs):
                return [self.get_atom(target, **_kwargs) for target in targets]

            if "targets" in answer:
                deep_targets = _recursive_targets(answer["targets"], **kwargs)
                answer["targets"] = deep_targets

        return answer

    def _build_node(self, node_params: NodeParamsT) -> tuple[str, NodeT]:
        """
        Build a node with the specified parameters.

        Args:
            node_params (NodeParamsT): A mapping containing node parameters.
                It should have the following keys:
                - 'type': The type of the node.
                - 'name': The name of the node.

        Returns:
            tuple[str, NodeT]: A tuple containing the handle of the node and the node dictionary.

        Raises:
            AddNodeException: If the 'type' or 'name' fields are missing in node_params.
        """
        reserved_parameters = ["handle", "_id", "composite_type_hash", "named_type"]

        valid_params = {
            key: value for key, value in node_params.items() if key not in reserved_parameters
        }

        node_type = valid_params.get("type")
        node_name = valid_params.get("name")

        if node_type is None or node_name is None:
            raise AddNodeException(
                message='The "name" and "type" fields must be sent',
                details=f"{valid_params=}",
            )

        handle = self.node_handle(node_type, node_name)

        node: NodeT = {
            FieldNames.ID_HASH: handle,
            "handle": handle,
            FieldNames.COMPOSITE_TYPE_HASH: ExpressionHasher.named_type_hash(node_type),
            FieldNames.NODE_NAME: node_name,
            FieldNames.TYPE_NAME: node_type,
        }

        node.update(valid_params)

        return handle, node

    def _build_link(
        self, link_params: LinkParamsT, toplevel: bool = True
    ) -> tuple[str, LinkT, list[str]] | None:
        """
        Build a link the specified parameters.

        Args:
            link_params (LinkParamsT): A mapping containing link parameters.
                It should have the following keys:
                - 'type': The type of the link.
                - 'targets': A list of target elements.
            toplevel (bool): A boolean flag to indicate toplevel links, i.e., links which are not
                nested inside other links. Defaults to True.

        Returns:
            tuple[str, LinkT, list[str]] | None: A tuple containing the handle of the link, the
            link dictionary, and a list of target hashes. Or None if something went wrong.

        Raises:
            AddLinkException: If the 'type' or 'targets' fields are missing in
            link_params.
        """
        reserved_parameters = [
            "handle",
            "targets",
            "_id",
            "composite_type_hash",
            "composite_type",
            "is_toplevel",
            "named_type",
            "named_type_hash",
            "key_n",
        ]

        valid_params = {
            key: value
            for key, value in link_params.items()
            if key not in reserved_parameters and not re.search(AtomDB.key_pattern, key)
        }

        targets = link_params.get("targets")
        link_type = link_params.get("type")

        if link_type is None or targets is None:
            raise AddLinkException(
                message='The "type" and "targets" fields must be sent',
                details=f"{valid_params=}",
            )

        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        target_handles = []
        composite_type = [link_type_hash]
        composite_type_hash = [link_type_hash]

        for target in targets:
            if not isinstance(target, dict):
                raise ValueError("The target must be a dictionary")
            if "targets" not in target:
                atom = self.add_node(target)
                atom_hash = atom["composite_type_hash"]
                composite_type.append(atom_hash)
            else:
                atom = self.add_link(target, toplevel=False)
                atom_hash = atom["composite_type_hash"]
                composite_type.append(atom["composite_type"])
            composite_type_hash.append(atom_hash)
            target_handles.append(atom["_id"])

        handle = ExpressionHasher.expression_hash(link_type_hash, target_handles)

        link: LinkT = {
            FieldNames.ID_HASH: handle,
            "handle": handle,
            "targets": target_handles,
            FieldNames.COMPOSITE_TYPE_HASH: ExpressionHasher.composite_hash(composite_type_hash),
            FieldNames.IS_TOPLEVEL: toplevel,
            FieldNames.COMPOSITE_TYPE: composite_type,
            FieldNames.TYPE_NAME: link_type,
            FieldNames.TYPE_NAME_HASH: link_type_hash,
        }

        for item in range(len(targets)):
            link[f"key_{item}"] = target_handles[item]

        link.update(valid_params)

        return handle, link, target_handles

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
        except AtomDoesNotExist:
            return False

    def link_exists(self, link_type: str, target_handles: list[str]) -> bool:
        """
        Check if a link with the specified type and targets exists in the database.

        Args:
            link_type (str): The link type.
            target_handles (list[str]): A list of link target identifiers.

        Returns:
            bool: True if the link exists, False otherwise.
        """
        try:
            self.get_link_handle(link_type, target_handles)
            return True
        except AtomDoesNotExist:
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

    @abstractmethod
    def get_node_name(self, node_handle: str) -> str:
        """
        Get the name of the node with the specified handle.

        Args:
            node_handle (str): The node handle.

        Returns:
            str: The node name.
        """

    @abstractmethod
    def get_node_type(self, node_handle: str) -> str | None:
        """
        Get the type of the node with the specified handle.

        Args:
            node_handle (str): The node handle.

        Returns:
            str | None: The node type. Or None if the node does not exist.
        """

    @abstractmethod
    def get_node_by_name(self, node_type: str, substring: str) -> list[str]:
        """
        Get the name of a node of the specified type containing the given substring.

        Args:
            node_type (str): The node type.
            substring (str): The substring to search for in node names.

        Returns:
            list[str]: list of handles of nodes whose names matched the criteria.
        """

    @abstractmethod
    def get_atoms_by_field(self, query: list[OrderedDict[str, str]]) -> list[str]:
        """
        Query the database by field and value, the performance is improved if the database already
        have indexes created for the fields, check 'create_field_index' to create indexes.
        Ordering the fields as the index previously created can improve performance.

        Args:
            query (list[dict[str, str]]): list of dicts containing 'field' and 'value' keys

        Returns:
            list[str]: list of node IDs
        """

    @abstractmethod
    def get_atoms_by_index(
        self,
        index_id: str,
        query: list[OrderedDict[str, str]],
        cursor: int = 0,
        chunk_size: int = 500,
    ) -> tuple[int, list[AtomT]]:
        """
        Queries the database to return all atoms matching a specific index ID, filtering the
        results based on the provided query dictionary. This method is useful for efficiently
        retrieving atoms that match certain criteria, especially when the database has been
        indexed using the `create_field_index` function.

        Args:
            index_id (str): The ID of the index to query against. This index should have been
                created previously using the `create_field_index` method.
            query (list[OrderedDict[str, str]]): A list of ordered dictionaries, each containing
                a "field" and "value" key, representing the criteria for filtering atoms.
            cursor (int): An optional cursor indicating the starting point within the result set
                from which to return atoms. This can be used for pagination or to resume a
                previous query. If not provided, the query starts from the beginning.
            chunk_size (int): An optional size indicating the maximum number of atom IDs to
                return in one response. Useful for controlling response size and managing large
                datasets. If not provided, a default value is used.

        Returns:
            tuple[int, list[AtomT]]: A tuple containing the cursor position and a list of
            retrieved atoms.

        Note:
            The `cursor` and `chunk_size` parameters are particularly useful for handling large
            datasets by allowing the retrieval of results in manageable chunks rather than all
            at once.
        """

    @abstractmethod
    def get_atoms_by_text_field(
        self,
        text_value: str,
        field: str | None = None,
        text_index_id: str | None = None,
    ) -> list[str]:
        """
        Query the database by a text field, use the text_value arg to query using an existing text
        index (text_index_id is optional), if a TOKEN_INVERTED_LIST type of index wasn't previously
        created the field arg must be provided, or it will raise an Exception.
        When 'text_value' and 'field' value are provided, it will default to a regex search,
        creating an index to the field can improve the performance.

        Args:
            text_value (str): Value to search for, if only this argument is provided it will use
                a TOKEN_INVERTED_LIST index in the search
            field (str | None): Field to be used to search, if this argument is provided
                it will not use TOKEN_INVERTED_LIST in the search
            text_index_id (str | None): TOKEN_INVERTED_LIST index id to search for


        Returns:
            list[str]: list of node IDs ordered by the closest match
        """

    @abstractmethod
    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> list[str]:
        """
        Query the database by node name starting with 'startswith' value, this query is indexed
        and the performance is improved by searching only the index that starts with the
        requested value.

        Args:
            node_type (str): _description_
            startswith (str): _description_

        Returns:
            list[str]: list of node IDs
        """

    @abstractmethod
    def get_all_nodes(self, node_type: str, names: bool = False) -> list[str]:
        """
        Get all nodes of a specific type.

        Args:
            node_type (str): The node type.
            names (bool): If True, return node names instead of handles. Default is False.

        Returns:
            list[str]: A list of node handles or names, depending on the value of 'names'.
        """

    @abstractmethod
    def get_all_links(self, link_type: str, **kwargs) -> tuple[int | None, list[str]]:
        """
        Get all links of a specific type.

        Args:
            link_type (str): The type of the link.
            **kwargs: Additional arguments that may be used for filtering or other purposes.

        Returns:
            tuple[int | None, list[str]]: tuple containing a cursor (which can be None if cursor is
                not applicable) and a list of link handles.
        """

    @abstractmethod
    def get_link_handle(self, link_type: str, target_handles: list[str]) -> str:
        """
        Get the handle of the link with the specified type and targets.

        Args:
            link_type (str): The link type.
            target_handles (list[str]): A list of link target identifiers.

        Returns:
            str: The link handle.
        """

    @abstractmethod
    def get_link_type(self, link_handle: str) -> str | None:
        """
        Get the type of the link with the specified handle.

        Args:
            link_handle (str): The link handle.

        Returns:
            str | None: The link type. Or None if the link does not exist.
        """

    @abstractmethod
    def get_link_targets(self, link_handle: str) -> list[str]:
        """
        Get the target handles of a link specified by its handle.

        Args:
            link_handle (str): The link handle.

        Returns:
            list[str]: A list of target identifiers of the link.
        """

    @abstractmethod
    def is_ordered(self, link_handle: str) -> bool:
        """
        Check if a link specified by its handle is ordered.

        Args:
            link_handle (str): The link handle.

        Returns:
            bool: True if the link is ordered, False otherwise.
        """

    @abstractmethod
    def get_incoming_links(self, atom_handle: str, **kwargs) -> IncomingLinksT:
        """
        Retrieve incoming links for a specified atom handle.

        Args:
            atom_handle (str): The handle of the atom for which to retrieve incoming links.
            **kwargs: Additional arguments that may be used for filtering or other purposes.

        Returns:
            IncomingLinksT: List of incoming links.
        """

    @abstractmethod
    def get_matched_links(
        self, link_type: str, target_handles: list[str], **kwargs
    ) -> MatchedLinksResultT:
        """
        Retrieve links that match a specified link type and target handles.

        Args:
            link_type (str): The type of the link to match.
            target_handles (list[str]): A list of target handles to match.
            **kwargs: Additional arguments that may be used for filtering or other
                purposes.

        Returns:
            MatchedLinksResultT: List of matching link handles.
        """

    @abstractmethod
    def get_matched_type_template(self, template: list[Any], **kwargs) -> HandleListT:
        """
        Retrieve links that match a specified type template.

        Args:
            template (list[Any]): A list representing the type template to match.
            **kwargs: Additional arguments that may be used for filtering or other
                purposes.

        Returns:
            HandleListT: List of matching link handles.
        """

    @abstractmethod
    def get_matched_type(self, link_type: str, **kwargs) -> HandleListT:
        """
        Retrieve links that match a specified link type.

        Args:
            link_type (str): The type of the link to match.
            **kwargs: Additional arguments that may be used for filtering or other
                purposes.

        Returns:
            HandleListT: List of matching link handles.
        """

    def get_atom(self, handle: str, **kwargs) -> AtomT:
        """
        Retrieve an atom by its handle.

        Args:
            handle (str): The handle of the atom to retrieve.
            **kwargs: Additional arguments that may be used for filtering or other purposes.
                - no_target_format (bool, optional): If True, return the document without
                    transforming it to the target format. Defaults to False.
                - targets_document (bool, optional): If True, include the `targets_document` in the
                    response. Defaults to False.
                - deep_representation (bool, optional): If True, include a deep representation of
                    the targets. Defaults to False.

        Returns:
            AtomT: A dictionary representation of the atom, if found.

        Raises:
            AtomDoesNotExist: If the atom with the specified handle does not exist.
        """
        document = self._get_atom(handle)
        if document:
            if not kwargs.get("no_target_format", False):
                document = self._reformat_document(document, **kwargs)
            return document
        else:
            logger().error(
                f"Failed to retrieve atom for handle: {handle}. "
                f"This atom does not exist. - Details: {kwargs}"
            )
            raise AtomDoesNotExist(
                message="Nonexistent atom",
                details=f"handle: {handle}",
            )

    @abstractmethod
    def _get_atom(self, handle: str) -> AtomT | None:
        """
        Retrieve an atom by its handle.

        Args:
            handle (str): The handle of the atom to retrieve.

        Returns:
            AtomT | None: A dictionary representation of the atom if found, None otherwise.

        Note:
            This method is intended for internal use and should not be called directly.
            It must be implemented by subclasses to provide a concrete way to retrieve atoms by
            their handles.
        """

    @abstractmethod
    def get_atom_type(self, handle: str) -> str | None:
        """
        Retrieve the atom's type by its handle.

        Args:
            handle (str): The handle of the atom to retrieve the type for.

        Returns:
            str | None: The type of the atom. Or None if the atom does not exist.
        """

    @abstractmethod
    def get_atom_as_dict(self, handle: str, arity: int | None = 0) -> dict[str, Any]:
        """
        Get an atom as a dictionary representation.

        Args:
            handle (str): The atom handle.
            arity (int | None): The arity of the atom. Defaults to 0.

        Returns:
            dict[str, Any]: A dictionary representation of the atom.
        """

    @abstractmethod
    def count_atoms(self, parameters: dict[str, Any] | None = None) -> dict[str, int]:
        """
        Count the total number of atoms in the database.
        If the optional parameter 'precise' is set to True returns the node count and link count
        (slow), otherwise return the atom_count (fast).

        Args:
            parameters (dict[str, Any] | None): An optional dictionary containing the
                following key:
                    'precise' (bool)  If set to True, the count provides an accurate count
                    but may be slower. If set to False, the count will be an estimate, which is
                    faster but less precise. Defaults to None.

        Returns:
            dict[str, int]: A dictionary containing the following keys:
                'node_count' (int): The count of node atoms
                'link_count' (int): The count of link atoms
                'atom_count' (int): The total count of all atoms
        """

    @abstractmethod
    def clear_database(self) -> None:
        """Clear the entire database, removing all data."""

    @abstractmethod
    def add_node(self, node_params: NodeParamsT) -> NodeT | None:
        """
        Adds a node to the database.

        This method allows you to add a node to the database with the specified node parameters.
        A node must have 'type' and 'name' fields in the node_params dictionary.

        Args:
            node_params (NodeParamsT): A mapping containing node parameters. It should have the
            following keys:
                - 'type': The type of the node.
                - 'name': The name of the node.

        Returns:
            NodeT | None: The information about the added node, including its unique key and
            other details. None if for some reason the node was not added.

        Raises:
            AddNodeException: If the 'type' or 'name' fields are missing in node_params.

        Note:
            This method creates a unique key for the node based on its type and name. If a node
            with the same key already exists, it just returns the node.

        Example:
            To add a node, use this method like this:
            >>> node_params = {
                    'type': 'Reactome',
                    'name': 'Reactome:R-HSA-164843',
                }
            >>> db.add_node(node_params)
        """

    @abstractmethod
    def add_link(self, link_params: LinkParamsT, toplevel: bool = True) -> LinkT | None:
        """
        Adds a link to the database.

        This method allows to add a link to the database with the specified link parameters.
        A link must have a 'type' and 'targets' field in the link_params dictionary.

        Args:
            link_params (LinkParamsT): A dictionary containing link parameters.
                It should have the following keys:
                - 'type': The type of the link.
                - 'targets': A list of target elements.
            toplevel: boolean flag to indicate toplevel links i.e. links which are not nested
            inside other links.

        Returns:
            LinkT | None: The information about the added link, including its unique key and
            other details. Or None if for some reason the link was not added.

        Raises:
            AddLinkException: If the 'type' or 'targets' fields are missing in link_params.

        Note:
            This method supports recursion when a target element itself contains links. It
            calculates a unique key for the link based on its type and targets. If a link with
            the same key already exists, it just returns the link.

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

    @abstractmethod
    def reindex(
        self, pattern_index_templates: dict[str, list[dict[str, Any]]] | None = None
    ) -> None:
        """
        Reindex inverted pattern index according to passed templates.

        Args:
            pattern_index_templates: indexes are specified by atom type in a dict mapping from atom
                types to a pattern template:

                {
                    <atom type>: <pattern template>
                }

                <pattern template> is a list of dicts, each dict specifies a pattern template for:

                {
                    "named_type": True/False,
                    "selected_positions": [n1, n2, ...],
                }

                Pattern templates are applied to each link entered in the atom space in order to
                determine which entries should be created in the inverted pattern index. Entries
                in the inverted pattern index are like patterns where the link type and each of
                its targets may be replaced by wildcards. For instance, given a similarity link
                Similarity(handle1, handle2) it could be used to create any of the following
                entries in the inverted pattern index:

                    *(handle1, handle2)
                    Similarity(*, handle2)
                    Similarity(handle1, *)
                    Similarity(*, *)

                If we create all possibilities of index entries to all links, the pattern index size
                will grow exponentially, so we limit the entries we want to create by each type of
                link. This is what a pattern template for a given link type is. For instance if
                we apply this pattern template:

                {
                    "named_type": False
                    "selected_positions": [0, 1]
                }

                to Similarity(handle1, handle2) we'll create only the following entries:

                    Similarity(*, handle2)
                    Similarity(handle1, *)
                    Similarity(*, *)

                If we apply this pattern template instead:

                {
                    "named_type": True
                    "selected_positions": [1]
                }

                We'll have:

                    *(handle1, handle2)
                    Similarity(handle1, *)
        """

    @abstractmethod
    def delete_atom(self, handle: str, **kwargs) -> None:
        """Delete an atom from the database

        Args:
            handle (str): Atom handle

        Raises:
            AtomDoesNotExist: If the atom does not exist
        """

    @abstractmethod
    def create_field_index(
        self,
        atom_type: str,
        fields: list[str],
        named_type: str | None = None,
        composite_type: list[Any] | None = None,
        index_type: FieldIndexType | None = None,
    ) -> str:
        """
        Create an index for the specified fields in the database.

        Args:
            atom_type (str): The type of the atom for which the index is created.
            fields (list[str]): A list of fields to be indexed.
            named_type (str | None): The named type of the atom. Defaults to None.
            composite_type (list[Any] | None): A list representing the composite type of
                the atom. Defaults to None.
            index_type (FieldIndexType | None): The type of the index to create. Defaults to None.

        Returns:
            str: The ID of the created index.
        """

    @abstractmethod
    def bulk_insert(self, documents: list[AtomT]) -> None:
        """
        Insert multiple documents into the database.

        Args:
            documents (list[AtomT]): A list of dictionaries, each representing a document to be
            inserted into the database.
        """

    @abstractmethod
    def retrieve_all_atoms(self) -> list[AtomT]:
        """
        Retrieve all atoms from the database.

        Returns:
            list[AtomT]: A list of dictionaries representing the atoms, or a list of tuples
            containing atom handles and their associated data.
        """

    @abstractmethod
    def commit(self, **kwargs) -> None:
        """Commit the current state of the database.

        This method is intended to be implemented by subclasses to handle the commit operation,
        which may involve persisting changes to a storage backend or performing other necessary
        actions to finalize the current state of the database.

        Args:
            **kwargs: Additional keyword arguments that may be used by the implementation of the
            commit operation.
        """
