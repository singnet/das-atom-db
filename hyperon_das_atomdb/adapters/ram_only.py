from typing import Any, Dict, List, Optional, Tuple, Union

from hyperon_das_atomdb.entity import Database, Link
from hyperon_das_atomdb.exceptions import (
    AddLinkException,
    AddNodeException,
    AtomDoesNotExistException,
    LinkDoesNotExistException,
    NodeDoesNotExistException,
)
from hyperon_das_atomdb.i_database import (
    UNORDERED_LINK_TYPES,
    WILDCARD,
    IAtomDB,
)
from hyperon_das_atomdb.utils.decorators import set_is_toplevel
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher
from hyperon_das_atomdb.utils.patterns import build_patern_keys


class InMemoryDB(IAtomDB):
    """A concrete implementation using hashtable (dict)"""

    def __repr__(self) -> str:
        return "<Atom database InMemory>"  # pragma no cover

    def __init__(self, database_name: str = 'das') -> None:
        self.database_name = database_name
        self.db: Database = Database(
            atom_type={},
            node={},
            link=Link(arity_1={}, arity_2={}, arity_n={}),
            outgoing_set={},
            incomming_set={},
            patterns={},
            templates={},
            names={},
        )

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = self._create_node_handle(node_type, node_name)
        try:
            self.db.node[node_handle]
            return node_handle
        except KeyError:
            raise NodeDoesNotExistException(
                message='This node does not exist',
                details=f'{node_type}:{node_name}',
            )

    def node_exists(self, node_type: str, node_name: str) -> bool:
        try:
            self.get_node_handle(node_type, node_name)
            return True
        except NodeDoesNotExistException:
            return False

    def get_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        link_handle = self._create_link_handle(link_type, target_handles)
        arity = len(target_handles)
        try:
            table = self.db.link.get_table(arity)
            table[link_handle]
            return link_handle
        except KeyError:
            raise LinkDoesNotExistException(
                message='This link does not exist',
                details=f'{link_type}:{target_handles}',
            )

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        try:
            self.get_link_handle(link_type, target_handles)
            return True
        except LinkDoesNotExistException:
            return False

    def get_link_targets(self, link_handle: str) -> List[str]:
        try:
            answer = self.db.outgoing_set[link_handle]
            return answer
        except KeyError:
            raise LinkDoesNotExistException(
                message='This link does not exist',
                details=f'link_handle: {link_handle}',
            )

    def is_ordered(self, link_handle: str) -> bool:
        tables = self.db.link.all_tables()
        data = tables.get(link_handle)
        if data is not None:
            return True
        return False

    def get_matched_links(
        self,
        link_type: str,
        target_handles: List[str],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> list:
        if link_type != WILDCARD and WILDCARD not in target_handles:
            try:
                link_handle = self.get_link_handle(link_type, target_handles)
                return [link_handle]
            except LinkDoesNotExistException as e:
                raise e

        if link_type == WILDCARD:
            link_type_hash = WILDCARD
        else:
            link_type_hash = ExpressionHasher.named_type_hash(link_type)

        if link_type in UNORDERED_LINK_TYPES:
            target_handles = sorted(target_handles)

        pattern_hash = ExpressionHasher.composite_hash(
            [link_type_hash, *target_handles]
        )

        patterns_matched = self.db.patterns.get(pattern_hash, [])

        if len(patterns_matched) > 0:
            if extra_parameters and extra_parameters.get('toplevel_only'):
                return self._filter_non_toplevel(patterns_matched)

        return patterns_matched

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

    def get_matched_type_template(
        self,
        template: List[Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        template = self._build_named_type_hash_template(template)
        template_hash = ExpressionHasher.composite_hash(template)
        templates_matched = self.db.templates.get(template_hash, [])
        if len(templates_matched) > 0:
            if extra_parameters and extra_parameters.get('toplevel_only'):
                return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_matched_type(
        self, link_type: str, extra_parameters: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        templates_matched = self.db.templates.get(link_type_hash, [])
        if len(templates_matched) > 0:
            if extra_parameters and extra_parameters.get('toplevel_only'):
                return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_node_name(self, node_handle: str) -> str:
        try:
            node = self.db.node[node_handle]
            return node['name']
        except KeyError:
            raise NodeDoesNotExistException(
                message='This node does not exist',
                details=f'node_handle: {node_handle}',
            )

    def get_node_type(self, node_handle: str) -> str:
        try:
            node = self.db.node[node_handle]
            return node['named_type']
        except KeyError:
            raise NodeDoesNotExistException(
                message='This node does not exist',
                details=f'node_handle: {node_handle}',
            )

    def get_matched_node_name(
        self, node_type: str, substring: Optional[str] = ''
    ) -> str:
        node_type_hash = ExpressionHasher.named_type_hash(node_type)

        return [
            key
            for key, value in self.db.node.items()
            if substring in value['name']
            and node_type_hash == value['composite_type_hash']
        ]

    def count_atoms(self) -> Tuple[int, int]:
        nodes = self.db.node
        links = self.db.link.all_tables()
        return (len(nodes), len(links))

    def get_atom_as_dict(
        self, handle: str, arity: Optional[int] = 0
    ) -> Dict[str, Any]:
        try:
            node = self.db.node[handle]
            return {
                'handle': node['_id'],
                'type': node['named_type'],
                'name': node['name'],
            }
        except KeyError:
            try:
                if arity > 0:
                    table = self.db.link.get_table(arity)
                else:
                    table = self.db.link.all_tables()

                link = table[handle]
                return {
                    'handle': link['_id'],
                    'type': link['named_type'],
                    'template': self._build_named_type_template(
                        link['composite_type']
                    ),
                    'targets': self.get_link_targets(link['_id']),
                }
            except KeyError:
                raise AtomDoesNotExistException(
                    message='This atom does not exist',
                    details=f'handle: {handle}',
                )

    def get_atom_as_deep_representation(
        self, handle: str, arity: Optional[int] = 0
    ) -> Dict[str, Any]:
        try:
            node = self.db.node[handle]
            return {'type': node['named_type'], 'name': node['name']}
        except KeyError:
            try:
                if arity > 0:
                    table = self.db.link.get_table(arity)
                else:
                    table = self.db.link.all_tables()

                link = table[handle]
                return {
                    'type': link['named_type'],
                    'targets': [
                        self.get_atom_as_deep_representation(target)
                        for target in self.get_link_targets(link['_id'])
                    ],
                }
            except KeyError:
                raise AtomDoesNotExistException(
                    message='This atom does not exist',
                    details=f'handle: {handle}',
                )

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a node to the in-memory database.

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
        if 'type' not in node_params or 'name' not in node_params:
            raise AddNodeException(
                message='The "name" and "type" fields must be sent',
                details=node_params,
            )
        node_params_copy = node_params.copy()
        node_type = node_params_copy.pop('type')
        node_name = node_params_copy.pop('name')

        key = self._create_node_handle(node_type, node_name)

        try:
            self.db.node[key]
        except KeyError:
            self.db.node[key] = {
                '_id': key,
                'composite_type_hash': ExpressionHasher.named_type_hash(
                    node_type
                ),
                'name': node_name,
                'named_type': node_type,
            }
            self.db.node[key].update(node_params_copy)

        # self._add_atom_type(_name=node_name, _type=node_type)
        self._add_atom_type(_name=node_type)
        self._add_names(_name=node_name, _type=node_type)

        return self.db.node[key]

    @set_is_toplevel
    def add_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a link to the in-memory database.

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

        if 'type' not in link_params or 'targets' not in link_params:
            raise AddLinkException(
                message='The "type" and "targets" fields must be sent',
                details=link_params,
            )

        link_params_copy = link_params.copy()

        link_type = link_params_copy.pop('type')
        targets = link_params_copy.pop('targets')

        data = {'type': link_type, 'targets': targets}

        for target in targets:
            if 'targets' not in target.keys():
                self.add_node(target.copy())
            else:
                # recursion without decorator
                self.add_link.__wrapped__(self, target.copy())

        targets_hash = self._calculate_targets_hash(data)
        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        key = ExpressionHasher.expression_hash(link_type_hash, targets_hash)

        composite_type = self._calculate_composite_type(data)
        composite_type_copy = composite_type[:]

        arity_number = len(targets)
        link_db = self.db.link.get_table(arity_number)

        try:
            link_db[key]
        except KeyError:
            link_db[key] = {
                '_id': key,
                'composite_type_hash': self._calculate_composite_type_hash(
                    composite_type_copy
                ),
                'is_toplevel': False,
                'composite_type': composite_type,
                'named_type': link_type,
                'named_type_hash': ExpressionHasher.named_type_hash(link_type),
            }

            for item in range(arity_number):
                link_db[key][f'key_{item}'] = targets_hash[item]

            link_db[key].update(link_params_copy)

            self._add_atom_type(_name=link_type)

            self._add_outgoing_set(key, targets_hash)

            self._add_incomming_set(key, targets_hash)

            self._add_templates(
                link_db[key]['composite_type_hash'],
                link_db[key]['named_type_hash'],
                link_db[key]['_id'],
                targets_hash,
            )

            self._add_patterns(
                link_db[key]['named_type_hash'],
                link_db[key]['_id'],
                targets_hash,
            )

        return link_db[key]

    def _create_node_handle(self, node_type: str, node_name: str) -> str:
        return ExpressionHasher.terminal_hash(node_type, node_name)

    def _create_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        named_type_hash = ExpressionHasher.named_type_hash(link_type)
        return ExpressionHasher.expression_hash(
            named_type_hash, target_handles
        )

    def _build_named_type_hash_template(
        self, template: Union[str, List[Any]]
    ) -> List[Any]:
        if isinstance(template, str):
            return ExpressionHasher.named_type_hash(template)
        else:
            return [
                self._build_named_type_hash_template(element)
                for element in template
            ]

    def _build_named_type_template(
        self, composite_type: Union[str, List[Any]]
    ) -> List[Any]:
        if isinstance(composite_type, str):
            for key, values in self.db.atom_type.items():
                if values['named_type_hash'] == composite_type:
                    return values['named_type']
            return ''
        else:
            return [
                self._build_named_type_template(element)
                for element in composite_type
            ]

    def _add_atom_type(
        self, _name: str, _type: Optional[str] = 'Type'
    ) -> Dict[str, Any]:
        name_hash = ExpressionHasher.named_type_hash(_name)
        type_hash = ExpressionHasher.named_type_hash(_type)
        typedef_mark_hash = ExpressionHasher.named_type_hash(":")

        key = ExpressionHasher.expression_hash(
            typedef_mark_hash, [name_hash, type_hash]
        )

        try:
            self.db.atom_type[key]
        except KeyError:
            base_type_hash = ExpressionHasher.named_type_hash("Type")
            composite_type = [typedef_mark_hash, type_hash, base_type_hash]
            composite_type_hash = ExpressionHasher.composite_hash(
                composite_type
            )

            self.db.atom_type[key] = {
                '_id': key,
                'composite_type_hash': composite_type_hash,
                'named_type': _name,
                'named_type_hash': name_hash,
            }

        return self.db.atom_type[key]

    def _add_names(self, _name: str, _type: str) -> Dict[str, str]:
        key = self._create_node_handle(_type, _name)
        try:
            self.db.names[key]
        except KeyError:
            self.db.names[key] = _name
        return self.db.names[key]

    def _add_outgoing_set(
        self, key: str, targets_hash: Dict[str, Any]
    ) -> None:
        outgoing_set = self.db.outgoing_set.get(key)
        if outgoing_set is None:
            self.db.outgoing_set[key] = targets_hash
        else:
            self.db.outgoing_set[key] + targets_hash

    def _add_incomming_set(
        self, key: str, targets_hash: Dict[str, Any]
    ) -> None:
        for target_hash in targets_hash:
            incomming_set = self.db.incomming_set.get(target_hash)
            if incomming_set is None:
                self.db.incomming_set[target_hash] = [key]
            else:
                self.db.incomming_set[target_hash].append(key)

    def _add_templates(
        self,
        composite_type_hash: str,
        named_type_hash: str,
        key: str,
        targets_hash: List[str],
    ) -> None:
        template_composite_type_hash = self.db.templates.get(
            composite_type_hash
        )
        template_named_type_hash = self.db.templates.get(named_type_hash)

        if template_composite_type_hash is not None:
            # template_composite_type_hash.append([key, targets_hash])
            template_composite_type_hash.append((key, tuple(targets_hash)))
        else:
            # self.db.templates[composite_type_hash] = [[key, targets_hash]]
            self.db.templates[composite_type_hash] = [
                (key, tuple(targets_hash))
            ]

        if template_named_type_hash is not None:
            # template_named_type_hash.append([key, targets_hash])
            template_named_type_hash.append((key, tuple(targets_hash)))
        else:
            # self.db.templates[named_type_hash] = [[key, targets_hash]]
            self.db.templates[named_type_hash] = [(key, tuple(targets_hash))]

    def _add_patterns(
        self, named_type_hash: str, key: str, targets_hash: List[str]
    ):
        pattern_keys = build_patern_keys([named_type_hash, *targets_hash])

        for pattern_key in pattern_keys:
            pattern_key_hash = self.db.patterns.get(pattern_key)
            if pattern_key_hash is not None:
                # pattern_key_hash.append([key, targets_hash])
                pattern_key_hash.append((key, tuple(targets_hash)))
            else:
                # self.db.patterns[pattern_key] = [[key, targets_hash]]
                self.db.patterns[pattern_key] = [(key, tuple(targets_hash))]

    def _calculate_composite_type(self, data) -> list:
        composite_type = []
        if 'targets' in data:
            for target in data['targets']:
                if 'targets' in target:
                    composite_type.append(
                        self._calculate_composite_type(target.copy())
                    )
                else:
                    composite_type.append(
                        ExpressionHasher.named_type_hash(target.get('type'))
                    )
        composite_type.insert(
            0, ExpressionHasher.named_type_hash(data.get('type'))
        )
        return composite_type

    def _calculate_targets_hash(self, data) -> List[str]:
        target_type = data['type']
        target_name = data.get('name')

        if 'targets' not in data:
            return ExpressionHasher.terminal_hash(target_type, target_name)

        if 'targets' in data:
            sub_targets = data['targets']
            result = []
            for sub_target in sub_targets:
                ret = self._calculate_targets_hash(sub_target.copy())
                result.append(ret)

            for item in result:
                if isinstance(item, list):
                    index = result.index(item)
                    result[index] = ExpressionHasher.expression_hash(
                        ExpressionHasher.named_type_hash(sub_target['type']),
                        item,
                    )

            return result

    def _calculate_composite_type_hash(self, composite_type: list) -> str:
        for _hash in composite_type:
            if isinstance(_hash, list):
                _hash_copy = list(_hash)
                self._calculate_composite_type_hash(_hash_copy)
                index = composite_type.index(_hash)
                composite_type[index] = ExpressionHasher.composite_hash(
                    _hash_copy
                )
        return ExpressionHasher.composite_hash(composite_type)

    def _filter_non_toplevel(self, matches: list) -> list:
        matches_toplevel_only = []
        for match in matches:
            link_handle = match[0]
            links = self.db.link.get_table(len(match[-1]))
            if links[link_handle]['is_toplevel']:
                matches_toplevel_only.append(match)
        return matches_toplevel_only
