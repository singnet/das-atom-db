from typing import Any, Dict, List, Optional, Union

from hyperon_das_atomdb.constants.redis_mongo_db import MongoFieldNames
from hyperon_das_atomdb.entity import Database, Link
from hyperon_das_atomdb.exceptions import (
    AddLinkException,
    AddNodeException,
    LinkDoesNotExistException,
    NodeDoesNotExistException,
)
from hyperon_das_atomdb.i_database import (
    UNORDERED_LINK_TYPES,
    WILDCARD,
    IAtomDB,
)
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


class InMemoryDB(IAtomDB):
    """A concrete implementation using hashtable (dict)"""

    def __init__(self, database_name: str = 'das') -> None:
        self.database_name = database_name
        self.db: Database = Database(
            atom_type={},
            node={},
            link=Link(arity_1={}, arity_2={}, arity_n={}),
            outgoing_set={},
            ingoing_set={},
            patterns={},
            templates={},
            names={},
        )

    def _create_node_handle(self, node_type: str, node_name: str) -> str:
        return ExpressionHasher.terminal_hash(node_type, node_name)

    def _create_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        named_type_hash = ExpressionHasher.named_type_hash(link_type)
        return ExpressionHasher.expression_hash(
            named_type_hash, target_handles
        )

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = self._create_node_handle(node_type, node_name)
        try:
            self.db.node[node_handle]
            return node_handle
        except KeyError:
            raise NodeDoesNotExistException(
                message=f'This node does not exist',
                details=f'{node_type}:{node_name}',
            )

    def node_exists(self, node_type: str, node_name: str) -> bool:
        try:
            self.get_node_handle(node_type, node_name)
            return True
        except LinkDoesNotExistException:
            return False

    def get_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        link_handle = self._create_link_handle(link_type, target_handles)
        arity = len(target_handles)
        try:
            arity = self.db.link.get_arity(arity)
            arity[link_handle]
            return link_handle
        except KeyError:
            raise LinkDoesNotExistException(
                message=f'This link does not exist',
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
                message=f'This link does not exist',
                details=f'link_handle: {link_handle}',
            )

    def is_ordered(self, link_handle: str) -> bool:
        all_arityes = self.db.link.all_arities()
        data = all_arityes.get(link_handle)
        if data is not None:
            return True
        return False

    def get_matched_links(
        self, link_type: str, target_handles: List[str]
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

        return self.db.patterns.get(pattern_hash, [])

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

    def _build_named_type_hash_template(
        self, template: Union[str, List[Any]]
    ) -> List[Any]:
        if isinstance(template, str):
            return self._get_atom_type_hash(template)
        else:
            answer = [
                self._build_named_type_hash_template(element)
                for element in template
            ]
            return answer

    def get_matched_type_template(self, template: List[Any]) -> List[str]:
        template = self._build_named_type_hash_template(template)
        template_hash = ExpressionHasher.composite_hash(template)
        return self.db.templates.get(template_hash, [])

    def get_matched_type(self, link_type: str) -> List[str]:
        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        return self.db.templates.get(link_type_hash, [])

    def get_node_name(self, node_handle: str) -> str:
        try:
            node = self.db.node[node_handle]
            return node['name']
        except KeyError:
            raise NodeDoesNotExistException(
                message=f'This node does not exist',
                details=f'node_handle: {node_handle}',
            )

    def get_matched_node_name(self, node_type: str, substring: str) -> str:
        node_type_hash = ExpressionHasher.named_type_hash(node_type)

        return [
            key
            for key, value in self.db.node.items()
            if substring in value['name']
            and node_type_hash == value['composite_type_hash']
        ]

    def add_node(self, node_params: Dict[str, Any]) -> None:
        keys = node_params.keys()

        if 'type' not in keys or 'name' not in keys:
            raise AddNodeException(
                message='The "name" and "type" fields must be sent',
                details=node_params,
            )

        node_type = node_params.pop('type')
        node_name = node_params.pop('name')

        key = self._create_node_handle(node_type, node_name)

        try:
            node = self.db.node[key]
        except KeyError:
            self.db.node[key] = {
                '_id': key,
                'composite_type_hash': ExpressionHasher.named_type_hash(
                    node_type
                ),
                'name': node_name,
                'named_type': node_type,
            }
            self.db.node[key].update(node_params)
            node = self.db.node[key]

        # self._add_atom_type(_name=node_name, _type=node_type)
        self._add_atom_type(_name=node_type)
        self._add_names(_name=node_name, _type=node_type)

        return node

    def _add_atom_type(self, _name: str, _type: Optional[str] = 'Type'):
        name_hash = ExpressionHasher.named_type_hash(_name)
        type_hash = ExpressionHasher.named_type_hash(_type)
        typedef_mark_hash = ExpressionHasher.named_type_hash(":")

        key = ExpressionHasher.expression_hash(
            typedef_mark_hash, [name_hash, type_hash]
        )

        try:
            atom = self.db.atom_type[key]
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

            atom = self.db.atom_type[key]

        return atom

    def _add_names(self, _name: str, _type: str):
        key = self._create_node_handle(_type, _name)
        try:
            name = self.db.names[key]
        except KeyError:
            self.db.names[key] = _name
            name = self.db.names[key]
        return name

    def add_link(self, link_params: Dict[str, Any]) -> None:
        if 'type' not in link_params or 'targets' not in link_params:
            raise AddLinkException(
                message='The "type" and "targets" fields must be sent',
                details=link_params,
            )

        link_type = link_params.pop('type')
        targets = link_params.pop('targets')

        data = {'type': link_type, 'targets': targets}

        for target in targets:
            if 'targets' not in target.keys():
                self.add_node(target.copy())
            else:
                self.add_link(target.copy())

        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        targets_hash = self._calculate_targets_hash(data)
        key = ExpressionHasher.expression_hash(link_type_hash, targets_hash)

        composite_type = self._calculate_composite_type(data)
        composite_type_copy = composite_type[:]

        arity_number = len(targets)
        link_db = self.db.link.get_arity(arity_number)

        link_db[key] = {
            '_id': key,
            'composite_type_hash': self._calculate_composite_type_hash(
                composite_type_copy
            ),
            'is_toplevel': True,
            'composite_type': composite_type,
            'named_type': link_type,
            'named_type_hash': ExpressionHasher.named_type_hash(link_type),
        }

        for item in range(arity_number):
            link_db[key][f'key_{item}'] = targets_hash[item]

        link = link_db[key]

        return link

    def _calculate_composite_type(self, data) -> list:
        # Fix bug during calculate the composite_type to nested link with more than 1 level
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

        if target_name is not None:
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
                self._calculate_composite_type_hash(_hash)
                index = composite_type.index(_hash)
                composite_type[index] = ExpressionHasher.composite_hash(_hash)
        return ExpressionHasher.composite_hash(composite_type)


if __name__ == '__main__':
    db = InMemoryDB()

    data1 = {
        'type': 'Evaluation',
        'targets': [
            {'type': 'Predicate', 'name': 'Predicate:has_name'},
            {
                'type': 'Evaluation',
                'targets': [
                    {'type': 'Predicate', 'name': 'Predicate:has_name'},
                    {
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
                        'type': 'Set',
                    },
                ],
            },
        ],
    }

    data2 = {
        'type': 'Similarity',
        'targets': [
            {'type': 'Concept', 'name': 'human'},
            {'type': 'Concept', 'name': 'monkey'},
        ],
    }

    resp = db.add_link(data1)

    print(resp)
