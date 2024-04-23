from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from hyperon_das_atomdb.database import UNORDERED_LINK_TYPES, WILDCARD, AtomDB, IncomingLinksT
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
    atom_type: Dict[str, Any]
    node: Dict[str, Any]
    link: Dict[str, Any]
    outgoing_set: Dict[str, Any]
    incoming_set: Dict[str, Any]
    patterns: Dict[str, List[Tuple]]
    templates: Dict[str, List[Tuple]]


class InMemoryDB(AtomDB):
    """A concrete implementation using hashtable (dict)"""

    def __repr__(self) -> str:
        return "<Atom database InMemory>"  # pragma no cover

    def __init__(self, database_name: str = 'das') -> None:
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

    def _get_link(self, handle: str) -> Optional[Dict[str, Any]]:
        link = self.db.link.get(handle)
        if link is not None:
            return link
        return None

    def _get_and_delete_link(self, link_handle: str) -> Dict[str, Any]:
        link_document = self.db.link.pop(link_handle, None)
        if link_document:
            return link_document

    def _build_named_type_hash_template(self, template: Union[str, List[Any]]) -> List[Any]:
        if isinstance(template, str):
            return ExpressionHasher.named_type_hash(template)
        else:
            return [self._build_named_type_hash_template(element) for element in template]

    def _build_atom_type_key_hash(self, _name: str) -> str:
        name_hash = ExpressionHasher.named_type_hash(_name)
        type_hash = ExpressionHasher.named_type_hash('Type')
        typedef_mark_hash = ExpressionHasher.named_type_hash(":")
        return ExpressionHasher.expression_hash(typedef_mark_hash, [name_hash, type_hash])

    def _add_atom_type(self, _name: str, _type: Optional[str] = 'Type'):
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
                '_id': key,
                'composite_type_hash': composite_type_hash,
                'named_type': _name,
                'named_type_hash': name_hash,
            }
            self.db.atom_type[key] = atom_type
            self.named_type_table[name_hash] = _name

    def _delete_atom_type(self, _name: str) -> None:
        key = self._build_atom_type_key_hash(_name)
        self.db.atom_type.pop(key, None)
        self.all_named_types.remove(_name)

    def _add_outgoing_set(self, key: str, targets_hash: List[str]) -> None:
        self.db.outgoing_set[key] = targets_hash

    def _get_and_delete_outgoing_set(self, handle: str) -> List[str]:
        return self.db.outgoing_set.pop(handle, None)

    def _add_incoming_set(self, key: str, targets_hash: List[str]) -> None:
        for target_hash in targets_hash:
            incoming_set = self.db.incoming_set.get(target_hash)
            if incoming_set is None:
                self.db.incoming_set[target_hash] = set([key])
            else:
                self.db.incoming_set[target_hash].add(key)

    def _delete_incoming_set(self, link_handle: str, atoms_handle: List[str]) -> None:
        for atom_handle in atoms_handle:
            handles = self.db.incoming_set.get(atom_handle, set())
            if len(handles) > 0:
                handles.remove(link_handle)

    def _add_templates(
        self, composite_type_hash: str, named_type_hash: str, key: str, targets_hash: List[str]
    ) -> None:
        template_composite_type_hash = self.db.templates.get(composite_type_hash)
        template_named_type_hash = self.db.templates.get(named_type_hash)

        if template_composite_type_hash is not None:
            template_composite_type_hash.add((key, tuple(targets_hash)))
        else:
            self.db.templates[composite_type_hash] = set([(key, tuple(targets_hash))])

        if template_named_type_hash is not None:
            template_named_type_hash.add((key, tuple(targets_hash)))
        else:
            self.db.templates[named_type_hash] = set([(key, tuple(targets_hash))])

    def _delete_templates(self, link_document: dict, targets_hash: List[str]) -> None:
        template_composite_type = self.db.templates.get(link_document['composite_type_hash'], set())
        if len(template_composite_type) > 0:
            template_composite_type.remove((link_document['_id'], tuple(targets_hash)))

        template_named_type = self.db.templates.get(link_document['named_type_hash'], set())
        if len(template_named_type) > 0:
            template_named_type.remove((link_document['_id'], tuple(targets_hash)))

    def _add_patterns(self, named_type_hash: str, key: str, targets_hash: List[str]):
        pattern_keys = build_patern_keys([named_type_hash, *targets_hash])

        for pattern_key in pattern_keys:
            pattern_key_hash = self.db.patterns.get(pattern_key)
            if pattern_key_hash is not None:
                pattern_key_hash.add((key, tuple(targets_hash)))
            else:
                self.db.patterns[pattern_key] = set([(key, tuple(targets_hash))])

    def _delete_patterns(self, link_document: dict, targets_hash: List[str]) -> None:
        pattern_keys = build_patern_keys([link_document['named_type_hash'], *targets_hash])
        for pattern_key in pattern_keys:
            pattern = self.db.patterns.get(pattern_key, set())
            if len(pattern) > 0:
                pattern.remove((link_document['_id'], tuple(targets_hash)))

    def _delete_link_and_update_index(self, link_handle: str) -> None:
        link_document = self._get_and_delete_link(link_handle)
        self._update_index(atom=link_document, delete_atom=True)

    def _filter_non_toplevel(self, matches: list) -> list:
        matches_toplevel_only = []
        if len(matches) > 0:
            for match in matches:
                link_handle = match[0]
                links = self.db.link
                if links[link_handle]['is_toplevel']:
                    matches_toplevel_only.append(match)
        return matches_toplevel_only

    def _build_targets_list(self, link: Dict[str, Any]):
        targets = []
        count = 0
        while True:
            handle = link.get(f'key_{count}')
            if handle is None:
                break
            targets.append(handle)
            count += 1
        return targets

    def _update_atom_indexes(self, documents: Iterable[Dict[str, any]], **kwargs) -> None:
        for document in documents:
            self._update_index(document, **kwargs)

    def _update_index(self, atom: Dict[str, Any], **kwargs):
        if kwargs.get('delete_atom', False):
            if atom is None:
                raise LinkDoesNotExist('Nonexistent link')

            link_handle = atom['_id']

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
            atom_type = atom['named_type']
            self._add_atom_type(_name=atom_type)
            if 'name' not in atom:
                handle = atom['_id']
                targets_hash = self._build_targets_list(atom)
                self._add_atom_type(_name=atom_type)
                self._add_outgoing_set(handle, targets_hash)
                self._add_incoming_set(handle, targets_hash)
                self._add_templates(
                    atom['composite_type_hash'],
                    atom['named_type_hash'],
                    handle,
                    targets_hash,
                )
                self._add_patterns(
                    atom['named_type_hash'],
                    handle,
                    targets_hash,
                )

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = self.node_handle(node_type, node_name)
        if node_handle in self.db.node:
            return node_handle
        else:
            logger().error(
                f'Failed to retrieve node handle for {node_type}:{node_name}. This node may not exist.'
            )
            raise NodeDoesNotExist(
                message='Nonexistent node',
                details=f'{node_type}:{node_name}',
            )

    def get_node_name(self, node_handle: str) -> str:
        node = self.db.node.get(node_handle)
        if node is None:
            logger().error(
                f'Failed to retrieve node name for handle: {node_handle}. This node may not exist.'
            )
            raise NodeDoesNotExist(
                message='Nonexistent node',
                details=f'node_handle: {node_handle}',
            )
        return node['name']

    def get_node_type(self, node_handle: str) -> str:
        node = self.db.node.get(node_handle)
        if node is None:
            logger().error(
                f'Failed to retrieve node type for handle: {node_handle}. This node may not exist.'
            )
            raise NodeDoesNotExist(
                message='Nonexistent node',
                details=f'node_handle: {node_handle}',
            )
        return node['named_type']

    def get_matched_node_name(self, node_type: str, substring: Optional[str] = '') -> str:
        node_type_hash = ExpressionHasher.named_type_hash(node_type)

        return [
            key
            for key, value in self.db.node.items()
            if substring in value['name'] and node_type_hash == value['composite_type_hash']
        ]

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        node_type_hash = ExpressionHasher.named_type_hash(node_type)

        if names:
            return [
                value['name']
                for value in self.db.node.values()
                if value['composite_type_hash'] == node_type_hash
            ]
        else:
            return [
                key
                for key, value in self.db.node.items()
                if value['composite_type_hash'] == node_type_hash
            ]

    def get_all_links(self, link_type: str, **kwargs) -> List[str]:
        answer = []
        for _, link in self.db.link.items():
            if link['named_type'] == link_type:
                answer.append(link['_id'])
        return answer

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = self.link_handle(link_type, target_handles)
        if link_handle in self.db.link:
            return link_handle
        else:
            logger().error(
                f'Failed to retrieve link handle for {link_type}:{target_handles}. This link may not exist.'
            )
            raise LinkDoesNotExist(
                message='Nonexistent link',
                details=f'{link_type}:{target_handles}',
            )

    def get_link_type(self, link_handle: str) -> str:
        link = self._get_link(link_handle)
        if link is not None:
            return link['named_type']
        else:
            logger().error(
                f'Failed to retrieve link type for {link_handle}. This link may not exist.'
            )
            raise LinkDoesNotExist(
                message='Nonexistent link',
                details=f'link_handle: {link_handle}',
            )

    def get_link_targets(self, link_handle: str) -> List[str]:
        answer = self.db.outgoing_set.get(link_handle)
        if answer is None:
            logger().error(
                f'Failed to retrieve link targets for {link_handle}. This link may not exist.'
            )
            raise LinkDoesNotExist(
                message='Nonexistent link',
                details=f'link_handle: {link_handle}',
            )
        return answer

    def is_ordered(self, link_handle: str) -> bool:
        link = self._get_link(link_handle)
        if link is not None:
            return True
        else:
            logger().error(
                'Failed to retrieve document for link handle: {link_handle}. The link may not exist.'
            )
            raise LinkDoesNotExist(
                message='Nonexistent link',
                details=f'link_handle: {link_handle}',
            )

    def get_matched_links(self, link_type: str, target_handles: List[str], **kwargs) -> list:
        if link_type != WILDCARD and WILDCARD not in target_handles:
            link_handle = self.get_link_handle(link_type, target_handles)
            return [link_handle]

        if link_type == WILDCARD:
            link_type_hash = WILDCARD
        else:
            link_type_hash = ExpressionHasher.named_type_hash(link_type)

        if link_type in UNORDERED_LINK_TYPES:
            target_handles = sorted(target_handles)
            logger().error(
                'Failed to get matched links: Queries with unordered links are not implemented. link_type: {link_type}'
            )
            raise InvalidOperationException(
                message='Queries with unordered links are not implemented',
                details=f'link_type: {link_type}',
            )

        pattern_hash = ExpressionHasher.composite_hash([link_type_hash, *target_handles])

        patterns_matched = list(self.db.patterns.get(pattern_hash, set()))

        if kwargs.get('toplevel_only'):
            return self._filter_non_toplevel(patterns_matched)

        return patterns_matched

    def get_incoming_links(self, atom_handle: str, **kwargs) -> List[IncomingLinksT]:
        links = self.db.incoming_set.get(atom_handle, set())
        if kwargs.get('handles_only', False):
            return list(links)
        else:
            return [self.get_atom(handle, **kwargs) for handle in links]

    def get_matched_type_template(self, template: List[Any], **kwargs) -> list:
        template = self._build_named_type_hash_template(template)
        template_hash = ExpressionHasher.composite_hash(template)
        templates_matched = list(self.db.templates.get(template_hash, set()))
        if kwargs.get('toplevel_only'):
            return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_matched_type(self, link_type: str, **kwargs) -> list:
        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        templates_matched = list(self.db.templates.get(link_type_hash, set()))
        if kwargs.get('toplevel_only'):
            return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_atom(
        self, handle: str, **kwargs
    ) -> Union[Tuple[Dict[str, Any], List[Dict[str, Any]]], Dict[str, Any]]:
        document = self.db.node.get(handle)
        if document is None:
            document = self._get_link(handle)
        if document:
            if not kwargs.get('no_target_format', False):
                return self._transform_to_target_format(document, **kwargs)
            else:
                return document
        else:
            logger().error(
                f'Failed to retrieve atom for handle: {handle}. This link may not exist. - Details: {kwargs}'
            )
            raise AtomDoesNotExist(
                message='Nonexistent atom',
                details=f'handle: {handle}',
            )

    def get_atom_type(self, handle: str) -> str:
        atom = self.db.node.get(handle)

        if atom is None:
            atom = self._get_link(handle)

        if atom is not None:
            return atom['named_type']

    def get_atom_as_dict(self, handle: str, arity: Optional[int] = 0) -> Dict[str, Any]:
        atom = self.db.node.get(handle)
        if atom is not None:
            return {
                'handle': atom['_id'],
                'type': atom['named_type'],
                'name': atom['name'],
            }
        atom = self._get_link(handle)
        if atom is not None:
            return {
                'handle': atom['_id'],
                'type': atom['named_type'],
                'targets': self._build_targets_list(atom),
            }
        logger().error(f'Failed to retrieve atom for handle: {handle}. This link may not exist.')
        raise AtomDoesNotExist(
            message='Nonexistent atom',
            details=f'handle: {handle}',
        )

    def count_atoms(self) -> Tuple[int, int]:
        return (len(self.db.node), len(self.db.link))

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

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        handle, node = self._add_node(node_params)
        self.db.node[handle] = node
        self._update_index(node)
        return node

    def add_link(self, link_params: Dict[str, Any], toplevel: bool = True) -> Dict[str, Any]:
        handle, link, targets = self._add_link(link_params, toplevel)
        self.db.link[handle] = link
        self._update_index(link)
        return link

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]]):
        raise NotImplementedError()

    def delete_atom(self, handle: str, **kwargs) -> None:
        node = self.db.node.pop(handle, None)

        if node:
            handles = self.db.incoming_set.pop(handle, set())

            if handles:
                for handle in handles:
                    self._delete_link_and_update_index(handle)
        else:
            try:
                self._delete_link_and_update_index(handle)
            except LinkDoesNotExist:
                logger().error(
                    f'Failed to delete atom for handle: {handle}. This atom may not exist. - Details: {kwargs}'
                )
                raise AtomDoesNotExist(
                    message='Nonexistent atom',
                    details=f'handle: {handle}',
                )

    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        pass

    def bulk_insert(self, documents: List[Dict[str, Any]]) -> None:
        try:
            for document in documents:
                handle = document['_id']
                if 'name' in document:
                    self.db.node[handle] = document
                else:
                    self.db.link[handle] = document
                self._update_index(document)
        except Exception:
            logger().error("Error bulk inserting documents")
            return None

    def retrieve_all_atoms(self) -> List[Dict[str, Any]]:
        try:
            answer = list(self.db.node.items())
            answer.extend(list(self.db.link.items()))
            return answer
        except Exception as e:
            logger().error(f"Error retrieving all atoms: {str(e)}")
            raise e

    def commit(self, **kwargs) -> None:
        raise NotImplementedError()
