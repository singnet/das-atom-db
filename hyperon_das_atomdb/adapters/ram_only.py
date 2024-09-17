"""
This module provides an in-memory implementation of the AtomDB interface using hashtables (dict).

The InMemoryDB class offers methods for managing nodes and links, including adding, deleting,
and retrieving them. It also supports indexing and pattern matching for efficient querying.

Classes:
    Database: A dataclass representing the structure of the in-memory database.
    InMemoryDB: A concrete implementation of the AtomDB interface using hashtables.
"""

from collections import OrderedDict
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any, Iterable

from hyperon_das_atomdb.database import (
    UNORDERED_LINK_TYPES,
    WILDCARD,
    AtomDB,
    AtomT,
    FieldIndexType,
    FieldNames,
    IncomingLinksT,
    LinkParamsT,
    LinkT,
    MatchedLinksResultT,
    MatchedTargetsListT,
    MatchedTypesResultT,
    NodeParamsT,
    NodeT,
)
from hyperon_das_atomdb.exceptions import AtomDoesNotExist, InvalidOperationException
from hyperon_das_atomdb.logger import logger
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher
from hyperon_das_atomdb.utils.patterns import build_pattern_keys


@dataclass
class Database:
    """Dataclass representing the structure of the in-memory database"""

    atom_type: dict[str, Any] = dc_field(default_factory=dict)
    node: dict[str, AtomT] = dc_field(default_factory=dict)
    link: dict[str, AtomT] = dc_field(default_factory=dict)
    outgoing_set: dict[str, Any] = dc_field(default_factory=dict)
    incoming_set: dict[str, set[str]] = dc_field(default_factory=dict)
    patterns: dict[str, set[tuple[str, tuple[str, ...]]]] = dc_field(default_factory=dict)
    templates: dict[str, set[tuple[str, tuple[str, ...]]]] = dc_field(default_factory=dict)


class InMemoryDB(AtomDB):
    """A concrete implementation using hashtable (dict)"""

    def __repr__(self) -> str:
        """
        Return a string representation of the InMemoryDB instance.

        This method is intended to provide a human-readable representation of the
        InMemoryDB instance, which can be useful for debugging and logging purposes.

        Returns:
            str: A string representing the InMemoryDB instance.
        """
        return "<Atom database InMemory>"  # pragma no cover

    def __init__(self, database_name: str = "das"):
        """
        Initialize an InMemoryDB instance.

        Args:
            database_name (str): The name of the database. Defaults to "das".
        """
        self.database_name: str = database_name
        self.named_type_table: dict[str, str] = {}  # keyed by named type hash
        self.all_named_types: set[str] = set()
        self.db: Database = Database()

    def _get_link(self, handle: str) -> dict[str, Any] | None:
        """
        Retrieve a link from the database by its handle.

        Args:
            handle (str): The handle of the link to retrieve.

        Returns:
            dict[str, Any] | None: The link document if found, otherwise None.
        """
        return self.db.link.get(handle, None)

    def _get_and_delete_link(self, link_handle: str) -> dict[str, Any] | None:
        """
        Retrieve and delete a link from the database by its handle.

        Args:
            link_handle (str): The handle of the link to retrieve and delete.

        Returns:
            dict[str, Any] | None: The link document if found and deleted, otherwise None.
        """
        return self.db.link.pop(link_handle, None)

    def _build_named_type_hash_template(self, template: str | list[Any]) -> str | list[Any]:
        """
        Build a named type hash template from the given template.

        Args:
            template (str | list[Any]): The template to build the named type hash from. It can be
                either a string or a list of elements.

        Returns:
            str | list[Any]: The named type hash if the template is a string, or a list of named
                type hashes if the template is a list.
        """
        if isinstance(template, str):
            return ExpressionHasher.named_type_hash(template)
        return [self._build_named_type_hash_template(element) for element in template]

    @staticmethod
    def _build_atom_type_key_hash(_name: str) -> str:
        """
        Build a hash key for the given atom type name.

        Args:
            _name (str): The name of the atom type.

        Returns:
            str: The hash key for the atom type.
        """
        name_hash = ExpressionHasher.named_type_hash(_name)
        type_hash = ExpressionHasher.named_type_hash("Type")
        typedef_mark_hash = ExpressionHasher.named_type_hash(":")
        return ExpressionHasher.expression_hash(typedef_mark_hash, [name_hash, type_hash])

    def _add_atom_type(self, atom_type_name: str, atom_type: str = "Type") -> None:
        """
        Add a type atom to the database.

        Args:
            atom_type_name (str): The name of the atom to add.
            atom_type (str): The type of the atom. Defaults to "Type".
        """
        if atom_type_name in self.all_named_types:
            return

        self.all_named_types.add(atom_type_name)
        name_hash = ExpressionHasher.named_type_hash(atom_type_name)
        type_hash = ExpressionHasher.named_type_hash(atom_type)
        typedef_mark_hash = ExpressionHasher.named_type_hash(":")

        key = ExpressionHasher.expression_hash(typedef_mark_hash, [name_hash, type_hash])

        _atom_type = self.db.atom_type.get(key)
        if _atom_type is None:
            base_type_hash = ExpressionHasher.named_type_hash("Type")
            composite_type = [typedef_mark_hash, type_hash, base_type_hash]
            composite_type_hash = ExpressionHasher.composite_hash(composite_type)
            _atom_type = {
                FieldNames.ID_HASH: key,
                FieldNames.COMPOSITE_TYPE_HASH: composite_type_hash,
                FieldNames.TYPE_NAME: atom_type_name,
                FieldNames.TYPE_NAME_HASH: name_hash,
            }
            self.db.atom_type[key] = _atom_type
            self.named_type_table[name_hash] = atom_type_name

    def _delete_atom_type(self, _name: str) -> None:
        """
        Delete an atom type from the database.

        Args:
            _name (str): The name of the atom type to delete.
        """
        key = self._build_atom_type_key_hash(_name)
        self.db.atom_type.pop(key, None)
        self.all_named_types.remove(_name)

    def _add_outgoing_set(self, key: str, targets_hash: list[str]) -> None:
        """
        Add an outgoing set to the database.

        Args:
            key (str): The key for the outgoing set.
            targets_hash (list[str]): A list of target hashes to be added to the outgoing set.
        """
        self.db.outgoing_set[key] = targets_hash

    def _get_and_delete_outgoing_set(self, handle: str) -> list[str] | None:
        """
        Retrieve and delete an outgoing set from the database by its handle.

        Args:
            handle (str): The handle of the outgoing set to retrieve and delete.

        Returns:
            list[str] | None: The outgoing set if found and deleted, otherwise None.
        """
        return self.db.outgoing_set.pop(handle, None)

    def _add_incoming_set(self, key: str, targets_hash: list[str]) -> None:
        """
        Add an incoming set to the database.

        Args:
            key (str): The key for the incoming set.
            targets_hash (list[str]): A list of target hashes to be added to the incoming set.
        """
        for target_hash in targets_hash:
            self.db.incoming_set.setdefault(target_hash, set()).add(key)

    def _delete_incoming_set(self, link_handle: str, atoms_handle: list[str]) -> None:
        """
        Delete an incoming set from the database.

        Args:
            link_handle (str): The handle of the link to delete.
            atoms_handle (list[str]): A list of atom handles associated with the link.
        """
        for atom_handle in atoms_handle:
            if handles := self.db.incoming_set.get(atom_handle):
                handles.remove(link_handle)

    def _add_templates(
        self,
        composite_type_hash: str,
        named_type_hash: str,
        key: str,
        targets_hash: list[str],
    ) -> None:
        """
        Add templates to the database.

        Args:
            composite_type_hash (str): The hash of the composite type.
            named_type_hash (str): The hash of the named type.
            key (str): The key for the template.
            targets_hash (list[str]): A list of target hashes to be added to the template.
        """
        template_composite_type_hash = self.db.templates.get(composite_type_hash)
        template_named_type_hash = self.db.templates.get(named_type_hash)

        if template_composite_type_hash is not None:
            template_composite_type_hash.add((key, tuple(targets_hash)))
        else:
            self.db.templates[composite_type_hash] = {(key, tuple(targets_hash))}

        if template_named_type_hash is not None:
            template_named_type_hash.add((key, tuple(targets_hash)))
        else:
            self.db.templates[named_type_hash] = {(key, tuple(targets_hash))}

    def _delete_templates(self, link_document: dict, targets_hash: list[str]) -> None:
        """
        Delete templates from the database.

        Args:
            link_document (dict): The document of the link whose templates are to be deleted.
            targets_hash (list[str]): A list of target hashes associated with the link.
        """
        template_composite_type = self.db.templates.get(
            link_document[FieldNames.COMPOSITE_TYPE_HASH], set()
        )
        if len(template_composite_type) > 0:
            template_composite_type.remove((link_document[FieldNames.ID_HASH], tuple(targets_hash)))

        template_named_type = self.db.templates.get(link_document[FieldNames.TYPE_NAME_HASH], set())
        if len(template_named_type) > 0:
            template_named_type.remove((link_document[FieldNames.ID_HASH], tuple(targets_hash)))

    def _add_patterns(self, named_type_hash: str, key: str, targets_hash: list[str]) -> None:
        """
        Add patterns to the database.

        Args:
            named_type_hash (str): The hash of the named type.
            key (str): The key for the pattern.
            targets_hash (list[str]): A list of target hashes to be added to the pattern.
        """
        pattern_keys = build_pattern_keys([named_type_hash, *targets_hash])

        for pattern_key in pattern_keys:
            self.db.patterns.setdefault(
                pattern_key,
                set(),
            ).add((key, tuple(targets_hash)))

    def _delete_patterns(self, link_document: dict, targets_hash: list[str]) -> None:
        """
        Delete patterns from the database.

        Args:
            link_document (dict): The document of the link whose patterns are to be deleted.
            targets_hash (list[str]): A list of target hashes associated with the link.
        """
        pattern_keys = build_pattern_keys([link_document[FieldNames.TYPE_NAME_HASH], *targets_hash])
        for pattern_key in pattern_keys:
            if pattern := self.db.patterns.get(pattern_key):
                pattern.remove((link_document[FieldNames.ID_HASH], tuple(targets_hash)))

    def _delete_link_and_update_index(self, link_handle: str) -> None:
        """
        Delete a link from the database and update the index.

        Args:
            link_handle (str): The handle of the link to delete.
        """
        if link_document := self._get_and_delete_link(link_handle):
            self._update_index(atom=link_document, delete_atom=True)

    def _filter_non_toplevel(self, matches: MatchedTargetsListT) -> MatchedTargetsListT:
        """
        Filter out non-toplevel matches from the provided list.

        Args:
            matches (MatchedTargetsListT): A list of matches, where each match is a tuple
            containing a link handle and a tuple of target handles.

        Returns:
            MatchedTargetsListT: A list of matches that are toplevel only.
        """
        if not self.db.link:
            return matches
        return [
            (link_handle, matched_targets)
            for link_handle, matched_targets in matches
            if (link := self.db.link.get(link_handle)) and link.get(FieldNames.IS_TOPLEVEL)
        ]

    @staticmethod
    def _build_targets_list(link: dict[str, Any]) -> list[Any]:
        """
        Build a list of target handles from the given link document.

        Args:
            link (dict[str, Any]): The link document from which to extract target handles.

        Returns:
            list[Any]: A list of target handles extracted from the link document.
        """
        return [
            handle
            for count in range(len(link))
            if (handle := link.get(f"key_{count}", None)) is not None
        ]

    def _update_atom_indexes(self, documents: Iterable[dict[str, Any]], **kwargs) -> None:
        """
        Update the indexes for the provided documents.

        Args:
            documents (Iterable[dict[str, any]]): An iterable of documents to update the indexes for.
            **kwargs: Additional keyword arguments that may be used for updating the indexes.
        """
        for document in documents:
            self._update_index(document, **kwargs)

    def _delete_atom_index(self, atom: AtomT) -> None:
        """
        Delete an atom from the index.

        Args:
            atom (AtomT): The atom to delete from the index.

        Raises:
            AtomDoesNotExist: If the atom does not exist in the database.
        """
        link_handle = atom[FieldNames.ID_HASH]

        handles = self.db.incoming_set.pop(link_handle, None)

        if handles:
            for handle in handles:
                self._delete_link_and_update_index(handle)

        outgoing_atoms = self._get_and_delete_outgoing_set(link_handle)

        if outgoing_atoms:
            self._delete_incoming_set(link_handle, outgoing_atoms)

        targets_hash = self._build_targets_list(atom)

        self._delete_templates(atom, targets_hash)

        self._delete_patterns(atom, targets_hash)

    def _add_atom_index(self, atom: AtomT) -> None:
        """
        Add an atom to the index.

        Args:
            atom (AtomT): The atom to add to the index.

        Raises:
            AtomDoesNotExist: If the atom does not exist in the database.
        """
        atom_type_name = atom[FieldNames.TYPE_NAME]
        self._add_atom_type(atom_type_name=atom_type_name)
        if FieldNames.NODE_NAME not in atom:
            handle = atom[FieldNames.ID_HASH]
            targets_hash = self._build_targets_list(atom)
            # self._add_atom_type(atom_type_name=atom_type_name)  # see 4 ln above - duplicate?
            self._add_outgoing_set(handle, targets_hash)
            self._add_incoming_set(handle, targets_hash)
            self._add_templates(
                atom[FieldNames.COMPOSITE_TYPE_HASH],
                atom[FieldNames.TYPE_NAME_HASH],
                handle,
                targets_hash,
            )
            self._add_patterns(
                atom[FieldNames.TYPE_NAME_HASH],
                handle,
                targets_hash,
            )

    def _update_index(self, atom: AtomT, **kwargs) -> None:
        """
        Update the index for the provided atom.

        Args:
            atom (AtomT): The atom document to update the index for.
            **kwargs: Additional keyword arguments that may be used for updating the index.
                - delete_atom (bool): If True, the atom will be deleted from the index.

        Raises:
            AtomDoesNotExist: If the atom does not exist when attempting to delete it.
        """
        if kwargs.get("delete_atom", False):
            self._delete_atom_index(atom)
        else:
            self._add_atom_index(atom)

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = self.node_handle(node_type, node_name)
        if node_handle in self.db.node:
            return node_handle
        logger().error(
            f"Failed to retrieve node handle for {node_type}:{node_name}. "
            "This node may not exist."
        )
        raise AtomDoesNotExist(
            message="Nonexistent atom",
            details=f"{node_type}:{node_name}",
        )

    def get_node_name(self, node_handle: str) -> str:
        node = self.db.node.get(node_handle)
        if node is None:
            logger().error(
                f"Failed to retrieve node name for handle: {node_handle}. This node may not exist."
            )
            raise AtomDoesNotExist(
                message="Nonexistent atom",
                details=f"node_handle: {node_handle}",
            )
        return node[FieldNames.NODE_NAME]

    def get_node_type(self, node_handle: str) -> str | None:
        node = self.db.node.get(node_handle)
        # TODO(angelo): here should we return None if `node` is `None` like redis_mongo_db does?
        if node is not None:
            return node[FieldNames.TYPE_NAME]
        logger().error(
            f"Failed to retrieve node type for handle: {node_handle}. This node may not exist."
        )
        raise AtomDoesNotExist(
            message="Nonexistent atom",
            details=f"node_handle: {node_handle}",
        )

    def get_node_by_name(self, node_type: str, substring: str) -> list[str]:
        node_type_hash = ExpressionHasher.named_type_hash(node_type)
        return [
            key
            for key, value in self.db.node.items()
            if substring in value[FieldNames.NODE_NAME]
            and node_type_hash == value[FieldNames.COMPOSITE_TYPE_HASH]
        ]

    def get_all_nodes(self, node_type: str, names: bool = False) -> list[str]:
        node_type_hash = ExpressionHasher.named_type_hash(node_type)

        if names:
            return [
                node[FieldNames.NODE_NAME]
                for node in self.db.node.values()
                if node[FieldNames.COMPOSITE_TYPE_HASH] == node_type_hash
            ]

        return [
            handle
            for handle, node in self.db.node.items()
            if node[FieldNames.COMPOSITE_TYPE_HASH] == node_type_hash
        ]

    def get_all_links(self, link_type: str, **kwargs) -> tuple[int | None, list[str]]:
        answer = [
            link[FieldNames.ID_HASH]
            for _, link in self.db.link.items()
            if link[FieldNames.TYPE_NAME] == link_type
        ]
        return kwargs.get("cursor"), answer

    def get_link_handle(self, link_type: str, target_handles: list[str]) -> str:
        link_handle = self.link_handle(link_type, target_handles)
        if link_handle in self.db.link:
            return link_handle
        logger().error(
            f"Failed to retrieve link handle for {link_type}:{target_handles}. "
            f"This link may not exist."
        )
        raise AtomDoesNotExist(
            message="Nonexistent atom",
            details=f"{link_type}:{target_handles}",
        )

    def get_link_type(self, link_handle: str) -> str | None:
        link = self._get_link(link_handle)
        if link is not None:
            return link[FieldNames.TYPE_NAME]
        logger().error(f"Failed to retrieve link type for {link_handle}. This link may not exist.")
        raise AtomDoesNotExist(
            message="Nonexistent atom",
            details=f"link_handle: {link_handle}",
        )

    def get_link_targets(self, link_handle: str) -> list[str]:
        answer = self.db.outgoing_set.get(link_handle)
        if answer is not None:
            return answer
        logger().error(
            f"Failed to retrieve link targets for {link_handle}. This link may not exist."
        )
        raise AtomDoesNotExist(
            message="Nonexistent atom",
            details=f"link_handle: {link_handle}",
        )

    def is_ordered(self, link_handle: str) -> bool:
        link = self._get_link(link_handle)
        if link is not None:
            return True
        logger().error(
            f"Failed to retrieve document for link handle: {link_handle}. "
            f"The link may not exist."
        )
        raise AtomDoesNotExist(
            message="Nonexistent atom",
            details=f"link_handle: {link_handle}",
        )

    def get_matched_links(
        self, link_type: str, target_handles: list[str], **kwargs
    ) -> MatchedLinksResultT:
        if link_type != WILDCARD and WILDCARD not in target_handles:
            try:
                answer = [self.get_link_handle(link_type, target_handles)]
            except AtomDoesNotExist:
                answer = []
            return answer

        link_type_hash = (
            WILDCARD if link_type == WILDCARD else ExpressionHasher.named_type_hash(link_type)
        )
        # NOTE unreachable
        if link_type in UNORDERED_LINK_TYPES:  # pragma: no cover
            logger().error(
                "Failed to get matched links: Queries with unordered links are not implemented. "
                f"link_type: {link_type}"
            )
            raise InvalidOperationException(
                message="Queries with unordered links are not implemented",
                details=f"link_type: {link_type}",
            )

        pattern_hash = ExpressionHasher.composite_hash([link_type_hash, *target_handles])

        patterns_matched = list(pattern) if (pattern := self.db.patterns.get(pattern_hash)) else []

        if kwargs.get("toplevel_only", False):
            return self._filter_non_toplevel(patterns_matched)

        return patterns_matched

    def get_incoming_links(self, atom_handle: str, **kwargs) -> IncomingLinksT:
        links = self.db.incoming_set.get(atom_handle, set())
        if kwargs.get("handles_only", False):
            return list(links)
        return [self.get_atom(handle, **kwargs) for handle in links]

    def get_matched_type_template(self, template: list[Any], **kwargs) -> MatchedTypesResultT:
        hash_base = self._build_named_type_hash_template(template)
        template_hash = ExpressionHasher.composite_hash(hash_base)
        templates_matched = list(self.db.templates.get(template_hash, set()))
        if kwargs.get("toplevel_only", False):
            return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_matched_type(self, link_type: str, **kwargs) -> MatchedTypesResultT:
        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        templates_matched = list(self.db.templates.get(link_type_hash, set()))
        if kwargs.get("toplevel_only", False):
            return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_atoms_by_field(
        self, query: list[OrderedDict[str, str]]
    ) -> list[str]:  # pragma: no cover
        raise NotImplementedError()

    def get_atoms_by_index(
        self,
        index_id: str,
        query: list[OrderedDict[str, str]],
        cursor: int = 0,
        chunk_size: int = 500,
    ) -> tuple[int, list[AtomT]]:  # pragma: no cover
        raise NotImplementedError()

    def get_atoms_by_text_field(
        self,
        text_value: str,
        field: str | None = None,
        text_index_id: str | None = None,
    ) -> list[str]:  # pragma: no cover
        raise NotImplementedError()

    def get_node_by_name_starting_with(
        self, node_type: str, startswith: str
    ) -> list[str]:  # pragma: no cover
        raise NotImplementedError()

    def _get_atom(self, handle: str) -> AtomT | None:
        return self.db.node.get(handle) or self._get_link(handle)

    def get_atom_type(self, handle: str) -> str | None:
        atom = node if (node := self.db.node.get(handle)) else self._get_link(handle)
        return atom.get(FieldNames.TYPE_NAME) if atom else None

    def get_atom_as_dict(self, handle: str, arity: int | None = 0) -> dict[str, Any]:
        atom = self.db.node.get(handle)
        if atom is not None:
            return {
                "handle": atom[FieldNames.ID_HASH],
                "type": atom[FieldNames.TYPE_NAME],
                "name": atom[FieldNames.NODE_NAME],
            }
        atom = self._get_link(handle)
        if atom is not None:
            return {
                "handle": atom[FieldNames.ID_HASH],
                "type": atom[FieldNames.TYPE_NAME],
                "targets": self._build_targets_list(atom),
            }
        logger().error(f"Failed to retrieve atom for handle: {handle}. This link may not exist.")
        raise AtomDoesNotExist(
            message="Nonexistent atom",
            details=f"handle: {handle}",
        )

    def count_atoms(self, parameters: dict[str, Any] | None = None) -> dict[str, int]:
        node_count = len(self.db.node)
        link_count = len(self.db.link)
        atom_count = node_count + link_count
        return {
            "atom_count": atom_count,
            "node_count": node_count,
            "link_count": link_count,
        }

    def clear_database(self) -> None:
        self.named_type_table = {}
        self.all_named_types = set()
        self.db = Database()

    def add_node(self, node_params: NodeParamsT) -> NodeT | None:
        handle, node = self._build_node(node_params)
        self.db.node[handle] = node
        self._update_index(node)
        return node

    def add_link(self, link_params: LinkParamsT, toplevel: bool = True) -> LinkT:
        handle, link, _ = self._build_link(link_params, toplevel)
        self.db.link[handle] = link
        self._update_index(link)
        return link

    def reindex(
        self, pattern_index_templates: dict[str, list[dict[str, Any]]] | None = None
    ) -> None:  # pragma: no cover
        raise NotImplementedError()

    def delete_atom(self, handle: str, **kwargs) -> None:
        node = self.db.node.pop(handle, None)

        if node:
            handles = self.db.incoming_set.pop(handle)
            if handles:
                for h in handles:
                    self._delete_link_and_update_index(h)
        else:
            try:
                self._delete_link_and_update_index(handle)
            except AtomDoesNotExist as ex:
                logger().error(
                    f"Failed to delete atom for handle: {handle}. "
                    f"This atom may not exist. - Details: {kwargs}"
                )
                ex.details = f"handle: {handle}"
                raise ex

    def create_field_index(
        self,
        atom_type: str,
        fields: list[str],
        named_type: str | None = None,
        composite_type: list[Any] | None = None,
        index_type: FieldIndexType | None = None,
    ) -> str:  # pragma: no cover
        raise NotImplementedError()

    def bulk_insert(self, documents: list[AtomT]) -> None:
        try:
            for document in documents:
                handle = document[FieldNames.ID_HASH]
                if FieldNames.NODE_NAME in document:
                    self.db.node[handle] = document
                else:
                    self.db.link[handle] = document
                self._update_index(document)
        except Exception as e:  # pylint: disable=broad-except
            logger().error(f"Error bulk inserting documents: {str(e)}")

    def retrieve_all_atoms(self) -> list[AtomT]:
        try:
            return list(self.db.node.values()) + list(self.db.link.values())
        except Exception as e:
            logger().error(f"Error retrieving all atoms: {str(e)}")
            raise e

    def commit(self, **kwargs) -> None:  # pragma: no cover
        raise NotImplementedError()
