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
from typing import Any, Iterable

from hyperon_das_atomdb.database import (
    UNORDERED_LINK_TYPES,
    WILDCARD,
    AtomDB,
    FieldIndexType,
    FieldNames,
    IncomingLinksT,
)
from hyperon_das_atomdb.exceptions import (
    AtomDoesNotExist,
    InvalidOperationException,
    LinkDoesNotExist,
    NodeDoesNotExist,
)
from hyperon_das_atomdb.logger import logger
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher
from hyperon_das_atomdb.utils.patterns import build_patern_keys


@dataclass
class Database:
    """Dataclass representing the structure of the in-memory database"""

    atom_type: dict[str, Any]
    node: dict[str, Any]
    link: dict[str, Any]
    outgoing_set: dict[str, Any]
    incoming_set: dict[str, set[str]]
    patterns: dict[str, set[tuple[str, tuple[str, ...]]]]
    templates: dict[str, set[tuple[str, tuple[str, ...]]]]


class InMemoryDB(AtomDB):
    """A concrete implementation using hashtable (dict)"""

    def __repr__(self) -> str:
        return "<Atom database InMemory>"  # pragma no cover

    def __init__(self, database_name: str = "das") -> None:
        self.database_name = database_name
        self.named_type_table = {}  # keyed by named type hash
        self.all_named_types = set()
        self.db: Database = Database(
            atom_type={},
            node={},
            link={},
            outgoing_set={},
            incoming_set={},
            patterns={},
            templates={},
        )

    def _get_link(self, handle: str) -> dict[str, Any] | None:
        link = self.db.link.get(handle)
        if link is not None:
            return link
        return None

    def _get_and_delete_link(self, link_handle: str) -> dict[str, Any] | None:
        return self.db.link.pop(link_handle, None)

    def _build_named_type_hash_template(self, template: str | list[Any]) -> str | list[Any]:
        if isinstance(template, str):
            return ExpressionHasher.named_type_hash(template)
        return [self._build_named_type_hash_template(element) for element in template]

    @staticmethod
    def _build_atom_type_key_hash(_name: str) -> str:
        name_hash = ExpressionHasher.named_type_hash(_name)
        type_hash = ExpressionHasher.named_type_hash("Type")
        typedef_mark_hash = ExpressionHasher.named_type_hash(":")
        return ExpressionHasher.expression_hash(typedef_mark_hash, [name_hash, type_hash])

    def _add_atom_type(self, _name: str, _type: str | None = "Type") -> None:
        if _name in self.all_named_types:
            return

        self.all_named_types.add(_name)
        name_hash = ExpressionHasher.named_type_hash(_name)
        type_hash = ExpressionHasher.named_type_hash(_type)
        typedef_mark_hash = ExpressionHasher.named_type_hash(":")

        key = ExpressionHasher.expression_hash(typedef_mark_hash, [name_hash, type_hash])

        atom_type = self.db.atom_type.get(key)
        if atom_type is None:
            base_type_hash = ExpressionHasher.named_type_hash("Type")
            composite_type = [typedef_mark_hash, type_hash, base_type_hash]
            composite_type_hash = ExpressionHasher.composite_hash(composite_type)
            atom_type = {
                FieldNames.ID_HASH: key,
                FieldNames.COMPOSITE_TYPE_HASH: composite_type_hash,
                FieldNames.TYPE_NAME: _name,
                FieldNames.TYPE_NAME_HASH: name_hash,
            }
            self.db.atom_type[key] = atom_type
            self.named_type_table[name_hash] = _name

    def _delete_atom_type(self, _name: str) -> None:
        key = self._build_atom_type_key_hash(_name)
        self.db.atom_type.pop(key, None)
        self.all_named_types.remove(_name)

    def _add_outgoing_set(self, key: str, targets_hash: list[str]) -> None:
        self.db.outgoing_set[key] = targets_hash

    def _get_and_delete_outgoing_set(self, handle: str) -> list[str] | None:
        return self.db.outgoing_set.pop(handle, None)

    def _add_incoming_set(self, key: str, targets_hash: list[str]) -> None:
        for target_hash in targets_hash:
            incoming_set = self.db.incoming_set.get(target_hash)
            if incoming_set is None:
                self.db.incoming_set[target_hash] = {key}
            else:
                self.db.incoming_set[target_hash].add(key)

    def _delete_incoming_set(self, link_handle: str, atoms_handle: list[str]) -> None:
        for atom_handle in atoms_handle:
            handles = self.db.incoming_set.get(atom_handle, set())
            if len(handles) > 0:
                handles.remove(link_handle)

    def _add_templates(
        self, composite_type_hash: str, named_type_hash: str, key: str, targets_hash: list[str]
    ) -> None:
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
        template_composite_type = self.db.templates.get(
            link_document[FieldNames.COMPOSITE_TYPE_HASH], set()
        )
        if len(template_composite_type) > 0:
            template_composite_type.remove((link_document[FieldNames.ID_HASH], tuple(targets_hash)))

        template_named_type = self.db.templates.get(link_document[FieldNames.TYPE_NAME_HASH], set())
        if len(template_named_type) > 0:
            template_named_type.remove((link_document[FieldNames.ID_HASH], tuple(targets_hash)))

    def _add_patterns(self, named_type_hash: str, key: str, targets_hash: list[str]) -> None:
        pattern_keys = build_patern_keys([named_type_hash, *targets_hash])

        for pattern_key in pattern_keys:
            pattern_key_hash = self.db.patterns.get(pattern_key)
            if pattern_key_hash is not None:
                pattern_key_hash.add((key, tuple(targets_hash)))
            else:
                self.db.patterns[pattern_key] = {(key, tuple(targets_hash))}

    def _delete_patterns(self, link_document: dict, targets_hash: list[str]) -> None:
        pattern_keys = build_patern_keys([link_document[FieldNames.TYPE_NAME_HASH], *targets_hash])
        for pattern_key in pattern_keys:
            pattern = self.db.patterns.get(pattern_key, set())
            if len(pattern) > 0:
                pattern.remove((link_document[FieldNames.ID_HASH], tuple(targets_hash)))

    def _delete_link_and_update_index(self, link_handle: str) -> None:
        link_document = self._get_and_delete_link(link_handle)
        self._update_index(atom=link_document, delete_atom=True)

    def _filter_non_toplevel(
        self, matches: list[tuple[str, tuple[str, ...]]]
    ) -> list[tuple[str, tuple[str, ...]]]:
        matches_toplevel_only: list[tuple[str, tuple[str, ...]]] = []
        if len(matches) > 0:
            for match in matches:
                link_handle = match[0]
                links = self.db.link
                if links[link_handle][FieldNames.IS_TOPLEVEL]:
                    matches_toplevel_only.append(match)
        return matches_toplevel_only

    @staticmethod
    def _build_targets_list(link: dict[str, Any]) -> list[Any]:
        targets = []
        count = 0
        while True:
            handle = link.get(f"key_{count}")
            if handle is None:
                break
            targets.append(handle)
            count += 1
        return targets

    def _update_atom_indexes(self, documents: Iterable[dict[str, any]], **kwargs) -> None:
        for document in documents:
            self._update_index(document, **kwargs)

    def _update_index(self, atom: dict[str, Any] | None, **kwargs) -> None:
        if kwargs.get("delete_atom", False):
            if atom is None:
                raise LinkDoesNotExist("Nonexistent link")

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
        else:
            atom_type = atom[FieldNames.TYPE_NAME]
            self._add_atom_type(_name=atom_type)
            if FieldNames.NODE_NAME not in atom:
                handle = atom[FieldNames.ID_HASH]
                targets_hash = self._build_targets_list(atom)
                self._add_atom_type(_name=atom_type)
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

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = self.node_handle(node_type, node_name)
        if node_handle in self.db.node:
            return node_handle
        logger().error(
            f"Failed to retrieve node handle for {node_type}:{node_name}. "
            "This node may not exist."
        )
        raise NodeDoesNotExist(
            message="Nonexistent node",
            details=f"{node_type}:{node_name}",
        )

    def get_node_name(self, node_handle: str) -> str:
        node = self.db.node.get(node_handle)
        if node is None:
            logger().error(
                f"Failed to retrieve node name for handle: {node_handle}. This node may not exist."
            )
            raise NodeDoesNotExist(
                message="Nonexistent node",
                details=f"node_handle: {node_handle}",
            )
        return node[FieldNames.NODE_NAME]

    def get_node_type(self, node_handle: str) -> str:
        node = self.db.node.get(node_handle)
        if node is None:
            logger().error(
                f"Failed to retrieve node type for handle: {node_handle}. This node may not exist."
            )
            raise NodeDoesNotExist(
                message="Nonexistent node",
                details=f"node_handle: {node_handle}",
            )
        return node[FieldNames.TYPE_NAME]

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
                value[FieldNames.NODE_NAME]
                for value in self.db.node.values()
                if value[FieldNames.COMPOSITE_TYPE_HASH] == node_type_hash
            ]

        return [
            key
            for key, value in self.db.node.items()
            if value[FieldNames.COMPOSITE_TYPE_HASH] == node_type_hash
        ]

    def get_all_links(self, link_type: str, **kwargs) -> list[str]:
        answer = []
        for _, link in self.db.link.items():
            if link[FieldNames.TYPE_NAME] == link_type:
                answer.append(link[FieldNames.ID_HASH])
        return answer

    def get_link_handle(self, link_type: str, target_handles: list[str]) -> str:
        link_handle = self.link_handle(link_type, target_handles)
        if link_handle in self.db.link:
            return link_handle

        logger().error(
            f"Failed to retrieve link handle for {link_type}:{target_handles}. "
            f"This link may not exist."
        )
        raise LinkDoesNotExist(
            message="Nonexistent link",
            details=f"{link_type}:{target_handles}",
        )

    def get_link_type(self, link_handle: str) -> str:
        link = self._get_link(link_handle)
        if link is not None:
            return link[FieldNames.TYPE_NAME]

        logger().error(f"Failed to retrieve link type for {link_handle}. This link may not exist.")
        raise LinkDoesNotExist(
            message="Nonexistent link",
            details=f"link_handle: {link_handle}",
        )

    def get_link_targets(self, link_handle: str) -> list[str]:
        answer = self.db.outgoing_set.get(link_handle)
        if answer is None:
            logger().error(
                f"Failed to retrieve link targets for {link_handle}. This link may not exist."
            )
            raise LinkDoesNotExist(
                message="Nonexistent link",
                details=f"link_handle: {link_handle}",
            )
        return answer

    def is_ordered(self, link_handle: str) -> bool:
        link = self._get_link(link_handle)
        if link is not None:
            return True

        logger().error(
            f"Failed to retrieve document for link handle: {link_handle}. "
            f"The link may not exist."
        )
        raise LinkDoesNotExist(
            message="Nonexistent link",
            details=f"link_handle: {link_handle}",
        )

    def get_matched_links(
        self, link_type: str, target_handles: list[str], **kwargs
    ) -> (
        list[str]
        | list[list[str]]
        | list[tuple[str, tuple[str, ...]]]
        | tuple[int, list[str]]
        | tuple[int, list[list[str]]]  # TODO(angelo): simplify this return type
    ):
        if link_type != WILDCARD and WILDCARD not in target_handles:
            link_handle = self.get_link_handle(link_type, target_handles)
            return [link_handle]

        if link_type == WILDCARD:
            link_type_hash = WILDCARD
        else:
            link_type_hash = ExpressionHasher.named_type_hash(link_type)

        if link_type in UNORDERED_LINK_TYPES:
            logger().error(
                "Failed to get matched links: Queries with unordered links are not implemented. "
                f"link_type: {link_type}"
            )
            raise InvalidOperationException(
                message="Queries with unordered links are not implemented",
                details=f"link_type: {link_type}",
            )

        pattern_hash = ExpressionHasher.composite_hash([link_type_hash, *target_handles])

        patterns_matched = list(self.db.patterns.get(pattern_hash, set()))

        if kwargs.get("toplevel_only"):
            return self._filter_non_toplevel(patterns_matched)

        return patterns_matched

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> tuple[int, list[IncomingLinksT]] | list[IncomingLinksT]:
        links = self.db.incoming_set.get(atom_handle, set())
        if kwargs.get("handles_only", False):
            return list(links)
        return [self.get_atom(handle, **kwargs) for handle in links]

    def get_matched_type_template(
        self, template: list[Any], **kwargs
    ) -> (
        list[tuple[str, tuple[str, ...]]]
        | tuple[int, list[str] | list[str]]
        | list[str]
        | list[str]  # TODO(angelo): simplify this return type
    ):
        template = self._build_named_type_hash_template(template)
        template_hash = ExpressionHasher.composite_hash(template)
        templates_matched = list(self.db.templates.get(template_hash, set()))
        if kwargs.get("toplevel_only"):
            return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_matched_type(
        self, link_type: str, **kwargs
    ) -> (
        list[tuple[str, tuple[str, ...]]]
        | tuple[int, list[str] | list[str]]
        | list[str]
        | list[str]  # TODO(angelo): simplify this return type
    ):
        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        templates_matched = list(self.db.templates.get(link_type_hash, set()))
        if kwargs.get("toplevel_only"):
            return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_atoms_by_field(self, query: list[OrderedDict[str, str]]) -> list[str]:
        raise NotImplementedError()

    def get_atoms_by_index(
        self,
        index_id: str,
        query: list[OrderedDict[str, str]],
        cursor: int | None = 0,
        chunk_size: int | None = 500,
    ) -> list[str]:
        raise NotImplementedError()

    def get_atoms_by_text_field(
        self, text_value: str, field: str | None = None, text_index_id: str | None = None
    ) -> list[str]:
        raise NotImplementedError()

    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> list[str]:
        raise NotImplementedError()

    def get_atom(
        self, handle: str, **kwargs
    ) -> (  # TODO(angelo): simplify this return type
        dict[str, Any]
        | tuple[dict[str, Any], list[dict[str, Any]]]
        | tuple[dict[str, Any], list[tuple[dict, list]]]
    ):
        document = self.db.node.get(handle)
        if document is None:
            document = self._get_link(handle)
        if document:
            if not kwargs.get("no_target_format", False):
                return self._transform_to_target_format(document, **kwargs)
            return document

        logger().error(
            f"Failed to retrieve atom for handle: {handle}. "
            f"This link may not exist. - Details: {kwargs}"
        )
        raise AtomDoesNotExist(
            message="Nonexistent atom",
            details=f"handle: {handle}",
        )

    def get_atom_type(self, handle: str) -> str | None:
        atom = self.db.node.get(handle)

        if atom is None:
            atom = self._get_link(handle)

        if atom is not None:
            return atom[FieldNames.TYPE_NAME]

        return None

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

    def count_atoms(self) -> tuple[int, int]:
        return len(self.db.node), len(self.db.link)

    def clear_database(self) -> None:
        self.named_type_table = {}
        self.all_named_types = set()
        self.db = Database(
            atom_type={},
            node={},
            link={},
            outgoing_set={},
            incoming_set={},
            patterns={},
            templates={},
        )

    def add_node(self, node_params: dict[str, Any]) -> dict[str, Any]:
        handle, node = self._add_node(node_params)
        self.db.node[handle] = node
        self._update_index(node)
        return node

    def add_link(self, link_params: dict[str, Any], toplevel: bool = True) -> dict[str, Any]:
        handle, link, _ = self._add_link(link_params, toplevel)
        self.db.link[handle] = link
        self._update_index(link)
        return link

    def reindex(self, pattern_index_templates: dict[str, dict[str, Any]] | None = None) -> None:
        raise NotImplementedError()

    def delete_atom(self, handle: str, **kwargs) -> None:
        node = self.db.node.pop(handle, None)

        if node:
            handles = self.db.incoming_set.pop(handle, set())

            if handles:
                for h in handles:
                    self._delete_link_and_update_index(h)
        else:
            try:
                self._delete_link_and_update_index(handle)
            except LinkDoesNotExist:
                # pylint: disable=raise-missing-from
                logger().error(
                    f"Failed to delete atom for handle: {handle}. "
                    f"This atom may not exist. - Details: {kwargs}"
                )
                raise AtomDoesNotExist(
                    message="Nonexistent atom",
                    details=f"handle: {handle}",
                )

    def create_field_index(
        self,
        atom_type: str,
        fields: list[str],
        named_type: str | None = None,
        composite_type: list[Any] | None = None,
        index_type: FieldIndexType | None = None,
    ) -> str:
        pass

    def bulk_insert(self, documents: list[dict[str, Any]]) -> None:
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

    def retrieve_all_atoms(self) -> list[dict[str, Any]] | list[tuple[str, Any]]:
        try:
            answer = list(self.db.node.items())
            answer.extend(list(self.db.link.items()))
            return answer
        except Exception as e:
            logger().error(f"Error retrieving all atoms: {str(e)}")
            raise e

    def commit(self, **kwargs) -> None:
        raise NotImplementedError()
