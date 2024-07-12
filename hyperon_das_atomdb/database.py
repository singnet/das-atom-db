import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, OrderedDict, Tuple, Union

from hyperon_das_atomdb.exceptions import (
    AddLinkException,
    AddNodeException,
    LinkDoesNotExist,
    NodeDoesNotExist,
)
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

WILDCARD = "*"
UNORDERED_LINK_TYPES = []

IncomingLinksT = Union[str, dict, Tuple[dict, list]]


class FieldNames(str, Enum):
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
    BINARY_TREE = "binary_tree"
    TOKEN_INVERTED_LIST = "token_inverted_list"


class AtomDB(ABC):
    key_pattern = re.compile(r"key_\d+")

    def __repr__(self) -> str:
        """
        Magic method for string representation of the class.
        Returns a string representation of the AtomDB class.
        """
        return "<Atom database abstract class>"  # pragma no cover

    @staticmethod
    def node_handle(node_type: str, node_name: str) -> str:
        return ExpressionHasher.terminal_hash(node_type, node_name)

    @staticmethod
    def link_handle(link_type: str, target_handles: List[str]) -> str:
        named_type_hash = ExpressionHasher.named_type_hash(link_type)
        return ExpressionHasher.expression_hash(named_type_hash, target_handles)

    def _transform_to_target_format(
        self, document: Dict[str, Any], **kwargs
    ) -> Union[Tuple[Dict[str, Any], List[Dict[str, Any]]], Dict[str, Any]]:
        answer = {"handle": document["_id"], "type": document["named_type"]}

        for key, value in document.items():
            if key == "_id":
                continue
            if re.search(AtomDB.key_pattern, key):
                answer.setdefault("targets", []).append(value)
            else:
                answer[key] = value

        if kwargs.get("targets_document", False):
            targets_document = [self.get_atom(target) for target in answer["targets"]]
            return answer, targets_document
        elif kwargs.get("deep_representation", False):

            def _recursive_targets(targets, **_kwargs):
                return [self.get_atom(target, **_kwargs) for target in targets]

            if "targets" in answer:
                deep_targets = _recursive_targets(answer["targets"], **kwargs)
                answer["targets"] = deep_targets

            return answer

        return answer

    # TODO: check with Edgar if this method is still needed
    # def _recursive_link_split(self, params: Dict[str, Any]) -> Tuple[str, Any]:
    #     name = params.get("name")
    #     atom_type = params["type"]
    #     if name:
    #         return self.node_handle(atom_type, name), atom_type
    #     targets, composite_type = [
    #         self._recursive_link_handle(target) for target in params["target"]
    #     ]
    #     composite_type.insert(0, atom_type)
    #     return self.link_handle(atom_type, targets), composite_type

    def _add_node(self, node_params: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        reserved_parameters = ["handle", "_id", "composite_type_hash", "named_type"]

        valid_params = {
            key: value for key, value in node_params.items() if key not in reserved_parameters
        }

        node_type = valid_params.get("type")
        node_name = valid_params.get("name")

        if node_type is None or node_name is None:
            raise AddNodeException(
                message="The 'name' and 'type' fields must be sent",
                details=f"{valid_params=}",
            )

        handle = self.node_handle(node_type, node_name)

        node = {
            FieldNames.ID_HASH: handle,
            FieldNames.COMPOSITE_TYPE_HASH: ExpressionHasher.named_type_hash(node_type),
            FieldNames.NODE_NAME: node_name,
            FieldNames.TYPE_NAME: node_type,
        }

        node.update(valid_params)
        node.pop("type")

        return handle, node

    def _add_link(
        self, link_params: Dict[str, Any], toplevel: bool = True
    ) -> Tuple[str, Dict[str, Any], List[str]]:
        reserved_parameters = [
            "handle",
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

        link_type = valid_params.get("type")
        targets = valid_params.get("targets")

        if link_type is None or targets is None:
            raise AddLinkException(
                message="The 'type' and 'targets' fields must be sent",
                details=f"{valid_params=}",
            )

        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        targets_hash = []
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
            targets_hash.append(atom["_id"])

        handle = ExpressionHasher.expression_hash(link_type_hash, targets_hash)

        link = {
            FieldNames.ID_HASH: handle,
            FieldNames.COMPOSITE_TYPE_HASH: ExpressionHasher.composite_hash(composite_type_hash),
            FieldNames.IS_TOPLEVEL: toplevel,
            FieldNames.COMPOSITE_TYPE: composite_type,
            FieldNames.TYPE_NAME: link_type,
            FieldNames.TYPE_NAME_HASH: link_type_hash,
        }

        for item in range(len(targets)):
            link[f"key_{item}"] = targets_hash[item]

        link.update(valid_params)
        link.pop("type")
        link.pop("targets")

        return handle, link, targets_hash

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
        except NodeDoesNotExist:
            return False

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        """
        Check if a link with the specified type and targets exists in the database.

        Args:
            link_type (str): The link type.
            target_handles (List[str]): A list of link target identifiers.

        Returns:
            bool: True if the link exists, False otherwise.
        """
        try:
            self.get_link_handle(link_type, target_handles)
            return True
        except LinkDoesNotExist:
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
    def get_node_by_name(self, node_type: str, substring: str) -> str:
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
    def get_atoms_by_field(self, query: List[OrderedDict[str, str]]) -> List[str]:
        """
        Query the database by field and value, the performance is improved if the database already
        have indexes created for the fields, check "create_field_index" to create indexes.
        Ordering the fields as the index previously created can improve performance.

        Args:
            query (List[Dict[str, str]]): List of dicts containing "field" and "value" keys

        Returns:
            List[str]: List of node IDs
        """
        ...  # pragma no cover

    @abstractmethod
    def get_atoms_by_index(
        self,
        index_id: str,
        query: List[OrderedDict[str, str]],
        cursor: Optional[int] = 0,
        chunk_size: Optional[int] = 500,
    ) -> List[str]:
        """
        Queries the database to return all atoms matching a specific index ID, filtering the results
        based on the provided query dictionary. This method is useful for efficiently retrieving
        atoms that match certain criteria, especially when the database has been indexed using the
        `create_field_index` function.

        Args:
            index_id (str): The ID of the index to query against. This index should have been
                created previously using the `create_field_index` method.
            query (List[Dict[str, str]]): A list of dictionaries, each containing a "field" and
                "value" key, representing the criteria for filtering atoms.
            cursor (Optional[int]): An optional cursor indicating the starting point within the
                result set from which to return atoms. This can be used for pagination or to resume
                a previous query. If not provided, the query starts from the beginning.
            chunk_size (Optional[int]): An optional size indicating the maximum number of atom IDs
                to return in one response. Useful for controlling response size and managing large
                datasets. If not provided, a default value is used.

        Returns:
            List[str]: A list of atom IDs that match the query criteria, filtered by the specified
                index. The atoms are returned as a list of their unique identifiers (IDs).

        Note:
            The `cursor` and `chunk_size` parameters are particularly useful for handling large
            datasets by allowing the retrieval of results in manageable chunks rather than all at
            once.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_atoms_by_text_field(
        self,
        text_value: str,
        field: Optional[str] = None,
        text_index_id: Optional[str] = None,
    ) -> List[str]:
        """
        Queries the database by a text field. Use `text_value` to search with an existing text
        index (`text_index_id` is optional). If a TOKEN_INVERTED_LIST index hasn't been created,
        `field` is required to avoid an Exception. When both `text_value` and `field` are provided,
        it defaults to a regex search, enhancing search performance with an index on the field.

        Args:
            text_value (str): The value to search for. Utilizes a TOKEN_INVERTED_LIST index if
                this is the sole argument.
            field (Optional[str]): The field for the search. If provided, TOKEN_INVERTED_LIST
                indexing is bypassed.
            text_index_id (Optional[str]): The TOKEN_INVERTED_LIST index ID for the search, if
                available.

        Returns:
            List[str]: A list of node IDs, sorted by the closest match.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> List[str]:
        """
        Query the database by node name starting with "startswith" value, this query is indexed
        and the performance is improved by searching only the index that starts with the
        requested value

        Args:
            node_type (str): _description_
            startswith (str): _description_

        Returns:
            List[str]: List of node IDs
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
            List[str]: A list of node handles or names, depending on the value of "names".
        """
        ...  # pragma no cover

    @abstractmethod
    def get_all_links(self, link_type: str, **kwargs) -> Union[List[str], Tuple[int, List[str]]]:
        """
        Get all link of a specific type.

        Args:
            link_type (str): The link type.

        Returns:
            Union[List[str], Tuple[int, List[str]]]: A list of link handles or a tuple containing
                the cursor number and the list of link handles.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
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
    def get_matched_links(
        self, link_type: str, target_handles: List[str], **kwargs
    ) -> Union[tuple, list]:
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
    def get_incoming_links(self, atom_handle: str, **kwargs) -> List[Any]:
        """Get all links pointing to Atom

        Args:
            atom_handle (str): The atom handle

        Returns:
            List[Any]: A list of handles or documents or tuple
        """
        ...  # pragma no cover

    @abstractmethod
    def get_matched_type_template(self, template: List[Any], **kwargs) -> Union[tuple, list]:
        """
        Get nodes that match a specified template.

        Args:
            template (List[Any]): A list of template parameters (parameter type not specified in the code).

        Returns:
            List[str]: A list of identifiers of nodes matching the template.
        """
        ...  # pragma no cover

    @abstractmethod
    def get_matched_type(self, link_type: str, **kwargs) -> Union[tuple, list]:
        """
        Get links that match a specified link type.

        Args:
            link_type (str): The link type.

        Returns:
            [return-type]: The return type description (not specified in the code).
        """
        ...  # pragma no cover

    @abstractmethod
    def get_atom(
        self, handle: str, **kwargs
    ) -> Union[Tuple[Dict[str, Any], List[Dict[str, Any]]], Dict[str, Any]]:
        ...  # pragma no cover

    @abstractmethod
    def get_atom_type(self, handle: str) -> str:
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
        Adds a node to the database with specified parameters.

        This method creates a new node in the database based on the provided parameters.
        It generates a unique handle for the node using its type and name, ensuring that
        each node can be uniquely identified. If a node with the same type and name already
        exists, it will not create a duplicate but instead return the existing node's details.

        Args:
            node_params (Dict[str, Any]): A dictionary containing the parameters for the node.
                Must include 'type' and 'name' keys among potentially others.

        Returns:
            Dict[str, Any]: A dictionary containing details of the added or existing node,
                including its unique handle and any other relevant information.

        Raises:
            AddNodeException: If either 'type' or 'name' keys are missing in the `node_params`
                dictionary, indicating that essential information for node creation is absent.

        Example:
            >>> node_params = {"type": "Person", "name": "John Doe"}
            >>> db.add_node(node_params)
            {"_id": "unique_node_handle", "type": "Person", "name": "John Doe"}

        Note:
            The method ensures that each node is uniquely identifiable in the database by its handle,
            which is generated based on the node's type and name. This approach prevents the creation
            of duplicate nodes with the same type and name.
        """
        ...  # pragma no cover

    @abstractmethod
    def add_link(self, link_params: Dict[str, Any], toplevel: bool = True) -> Dict[str, Any]:
        """
        Adds a link to the database based on the specified parameters.

        This method creates a new link in the database using the provided parameters,
        generating a unique handle for the link based on its type and target handles.
        If a link with the same type and target handles already exists, it will not
        create a duplicate but instead return the existing link's details. The method
        also supports the addition of links at the top level or nested within other links.

        Args:
            link_params (Dict[str, Any]): A dictionary containing the parameters for the link.
                Must include 'type' and 'targets' keys among potentially others.
            toplevel (bool, optional): A flag indicating whether the link is a top-level link.
                Defaults to True.

        Returns:
            Dict[str, Any]: A dictionary containing details of the added or existing link,
                including its unique handle and any other relevant information.

        Raises:
            AddLinkException: If either 'type' or 'targets' keys are missing in the `link_params`
                dictionary, indicating that essential information for link creation is absent.

        Example:
            >>> link_params = {
                    "type": "Friendship",
                    "targets": ["JohnDoeHandle", "JaneDoeHandle"]
                }
            >>> db.add_link(link_params)
            {"_id": "unique_link_handle", "type": "Friendship", "targets": ["JohnDoeHandle", "JaneDoeHandle"]}

        Note:
            The method ensures that each link is uniquely identifiable in the database by its handle,
            which is generated based on the link's type and target handles. This approach prevents the
            creation of duplicate links with the same type and target handles.
        """
        ...  # pragma no cover

    @abstractmethod
    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        """
        Reindex inverted pattern index according to passed templates.

        Args:
            pattern_index_templates: Indexes specified by atom type in a dict mapping from atom
                types to a pattern template. Defaults to None.

        Pattern template structure:
            >>> {
            >>>     <atom type>: {
            >>>         "named_type": <bool>,
            >>>         "selected_positions": <List[int]>
            >>>     }
            >>> }
            >>>

        Description:
            Pattern templates are applied to each link entered in the atom space to determine
            which entries should be created in the inverted pattern index. Entries in the
            inverted pattern index are like patterns where the link type and each of its
            targets may be replaced by wildcards. For instance, given a similarity link
            Similarity(handle1, handle2), it could be used to create any of the following
            entries in the inverted pattern index:
                `*(handle1, handle2)`\n
                `Similarity(*, handle2)`\n
                `Similarity(handle1, *)`\n
                `Similarity(*, *)`\n

            If we create all possibilities of index entries to all links, the pattern index
            size will grow exponentially. So, we limit the entries we want to create by each
            type of link. This is what a pattern template for a given link type is. For
            instance, if we apply this pattern template:
                >>> {
                >>>     "named_type": False,
                >>>     "selected_positions": [0, 1]
                >>> }
                >>>

            To Similarity(handle1, handle2), we'll create only the following entries:
                `Similarity(*, handle2)`\n
                `Similarity(handle1, *)`\n
                `Similarity(*, *)`\n

            If we apply this pattern template instead:
                >>> {
                >>>     "named_type": True,
                >>>     "selected_positions": [1]
                >>> }
                >>>

            We'll have:
                `*(handle1, handle2)`\n
                `Similarity(handle1, *)`\n
        """
        ...  # pragma no cover

    @abstractmethod
    def delete_atom(self, handle: str, **kwargs) -> None:
        """Delete an atom from the database

        Args:
            handle (str): Atom handle
        """
        ...  # pragma no cover

    @abstractmethod
    def create_field_index(
        self,
        atom_type: str,
        fields: List[str],
        named_type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
        index_type: Optional[FieldIndexType] = None,
    ) -> str:
        """
        Creates a field index for efficient querying of atoms based on specified fields.

        This method allows for the creation of indexes on atom fields to optimize query performance.
        Indexes can be created for specific atom types and can be further customized by specifying
        whether the index should consider named types, composite types, and the type of index to be
        created (e.g., binary tree, token inverted list).

        Args:
            atom_type (str): The type of atom for which the index is being created.
            fields (List[str]): A list of field names to be included in the index.
            named_type (Optional[str], optional): Specifies if the index should be created for a
                specific named type. Defaults to None, indicating no named type specification.
            composite_type (Optional[List[Any]], optional): Specifies if the index should consider
                composite types. Defaults to None, indicating composite types are not considered.
            index_type (Optional[FieldIndexType], optional): The type of index to create. Supported
                values are defined in the `FieldIndexType` enum. Defaults to None, indicating the
                default index type should be used.

        Returns:
            str: The identifier of the created index, which can be used for querying.

        Example:
            >>> db.create_field_index("Person", ["name", "age"], index_type=FieldIndexType.BINARY_TREE)
            'index_id_123'

        Note:
            The actual implementation of index creation and the support for different index types
            may vary depending on the database backend.
        """
        ...  # pragma no cover

    @abstractmethod
    def bulk_insert(self, documents: List[Dict[str, Any]]) -> None:
        ...  # pragma no cover

    @abstractmethod
    def retrieve_all_atoms(self) -> List[Dict[str, Any]]:
        ...  # pragma no cover
