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
from hyperon_das_atomdb.utils.decorators import set_is_toplevel
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

"""
 
        {
            'atom_type': {
                {
                    '_id': '0cc896470995bd4187a844163b6f1012',
                    'composite_type_hash': '26a3ccbc2d2c83e0e39a01e0eccc4fd1',
                    'named_type': 'Similarity',
                    'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc'
                },
                {
                    '_id': '01959fba970d810fdc38bf0c6b4884db',
                    'composite_type_hash': '26a3ccbc2d2c83e0e39a01e0eccc4fd1',
                    'named_type': 'Concept',
                    'named_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3'
                },
                {
                    '_id': '3e419e4d468bdac682103ea2615d0902',
                    'composite_type_hash': '26a3ccbc2d2c83e0e39a01e0eccc4fd1',
                    'named_type': 'Inheritance',
                    'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba'
                }
            },
            'node': [
                {
                    '_id': 'af12f10f9ae2002a1607ba0b47ba8407',
                    'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
                    'name': 'human',
                    'named_type': 'Concept'
                },
                {
                    '_id': '1cdffc6b0b89ff41d68bec237481d1e1',
                    'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
                    'name': 'monkey',
                    'named_type': 'Concept'
                }
            ],
            'link': [
                {
                    '_id': '2e9c59947cae7dd133af21bbfcf79902',
                    'composite_type_hash': 'db6163e5526ce17d293f16fe88a9948c',
                    'is_toplevel': True,
                    'composite_type': [
                        'a9dea78180588431ec64d6bc4872fdbc',
                        '99e9bae675b12967251c175696f00a70',
                        'd0763edaa9d9bd2a9516280e9044d885'
                    ],
                    'named_type': 'Similarity',
                    'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
                    'key_0': 'bd497eb24420dd50fed5f3d2e6cdd7c1',
                    'key_1': '305e7d502a0ce80b94374ff0d79a6464'
                },
                {
                    '_id': 'a0b576b9ce2d3b4d0a1528c8db300d5a',
                    'composite_type_hash': 'aed5c623856552ad33861b7bb7e39d47',
                    'is_toplevel': True,
                    'composite_type': [
                        'a9dea78180588431ec64d6bc4872fdbc',
                        '99e9bae675b12967251c175696f00a70',
                        '46bdd2ce40657500dcb21ca51d22b29f'
                    ],
                    'named_type': 'Similarity',
                    'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
                    'key_0': 'bd497eb24420dd50fed5f3d2e6cdd7c1',
                    'key_1': 'a1fb3a4de5c459bfa4bd87dc423019c3'
                },
            ],}
        
        o = {    
            'outgoing_set': [
                {
                    'outgoing_set:062558af0dc7bd964acec001e694c984': [
                        [   
                           'a1fb3a4de5c459bfa4bd87dc423019c3',
                           'bd497eb24420dd50fed5f3d2e6cdd7c1'
                        ]
                    ],
                    'outgoing_set:3afd51603c2d1b64c5b9455ec5a8c166': [
                        [   
                           'd1ec11ec366a1deb24a079dc39863c68',
                           'c90242e2dbece101813762cc2a83d726'
                        ]
                    ]
                }
            ],
        #     'incomming_set': [
        #         {
        #             'incomming_set:e2d9b15ab3461228d75502e754137caa': [
        #                 [   
        #                    'b40e1aa0ffb4b30df8b08e1b70554cfa',
        #                    '2b8838a60dcc3be433c258d61f62a215',
        #                    'be02e317942f1c218dfaeb1ca22c5722',
        #                    '51266232a5ebdf68da3f237bc9462277',
        #                    '062558af0dc7bd964acec001e694c984',
        #                 ]
        #             ],
        #             'incomming_set:d1ec11ec366a1deb24a079dc39863c68': [
        #                 [   
        #                    '3afd51603c2d1b64c5b9455ec5a8c166',
        #                    '85366aa321bd5a01774da82563460bc1'
        #                 ]
        #             ]
        #         }
        #     ],
        #     'patterns': [
        #         {
        #             'patterns:7d51188687cfbc9dbbf0470f9a4e87d2': [
        #                 '��n� bcd9571e92e117d511052270c94ffb17�� a1fb3a4de5c459bfa4bd87dc423019c3�� bd497eb24420dd50fed5f3d2e6cdd7c1�����.'
        #             ]
        #         }
        #     ],
        #     'templates': [
        #         {
        #            'templates:23884923c039ec963b4b9939eb6fa660': [
        #                '0cc896470995bd4187a844163b6f1012',
        #                 [   
        #                    '26a3ccbc2d2c83e0e39a01e0eccc4fd1',
        #                    'e40489cd1e7102e35469c937e05c8bba'
        #                 ]
        #             ]
        #         }
        #     ],
        #     'names': {
        #         'names:bdfe4e7a431f73386f37c6448afe5840': 'mammal',
        #         'names:af12f10f9ae2002a1607ba0b47ba8407': 'human',
        #         'names:80aff30094874e75028033a38ce677bb': 'plant',
        #     }
        # }
        



"""


class InMemoryDB(IAtomDB):
    """A concrete implementation using hashtable (dict)"""

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
        except NodeDoesNotExistException:
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

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a node to the in-memory database.

        This method allows you to add a node to the database
        with the specified node parameters. A node must have 'type' and
        'name' fields in the node_params dictionary.

        Args:
            node_params (Dict[str, Any]): A dictionary containing node parameters.
                It should have the following keys:
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

    def add_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        # temporary_outgoing_set = {}
        link = self._process_link(link_params)
        link['is_toplevel'] = True
        # self._add_incomming_set(temporary_outgoing_set)
        return link

    def _process_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a link to the in-memory database.

        This method allows to add a link to the database with the specified link parameters.
        A link must have a 'type' and 'targets' field in the link_params dictionary.

        Args:
            link_params (Dict[str, Any]): A dictionary containing link parameters.
                It should have the following keys:
                - 'type': The type of the link.
                - 'targets': A list of target elements.

        Returns:
            Dict[str, Any]: The information about the added link,
                including its unique key and other details.

        Raises:
            AddLinkException: If the 'type' or 'targets' fields are missing in link_params.

        Note:
            This method supports recursion when a target element itself contains links.
            It calculates a unique key for the link based on its type and targets.
            If a link with the same key already exists, it just returns the link.

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
                # self.add_link.__wrapped__(self, target.copy())
                self._process_link(target.copy())

        targets_hash = self._calculate_targets_hash(data)
        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        key = ExpressionHasher.expression_hash(link_type_hash, targets_hash)

        composite_type = self._calculate_composite_type(data)
        composite_type_copy = composite_type[:]

        arity_number = len(targets)
        link_db = self.db.link.get_arity(arity_number)

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

            # Add outgoing_set
            outgoing_set = self.db.outgoing_set.get(key)
            if outgoing_set is None:
                self.db.outgoing_set[key] = targets_hash
            else:
                self.db.outgoing_set[key] + targets_hash

            # Add incoming_set
            for target_hash in targets_hash:
                incomming_set = self.db.incomming_set.get(target_hash)
                if incomming_set is None:
                    self.db.incomming_set[target_hash] = [key]
                else:
                    self.db.incomming_set[target_hash].append(key)

            # outgoing_set.update(self._add_outgoing_set(link_db[key]))

        return link_db[key]

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

    def _add_outgoing_set(self, link: Dict[str, Any]) -> Dict[str, list]:
        outgoing_set_key = link['_id']
        result = {outgoing_set_key: []}
        try:
            self.db.outgoing_set[outgoing_set_key]
        except KeyError:
            outsets = []
            for key, value in link.items():
                if 'key' in key:
                    outsets.append(value)
                    result[outgoing_set_key].append(value)

            self.db.outgoing_set[outgoing_set_key] = outsets

        return result

    def _add_incomming_set(self, outgoing_set: Dict[str, Any]) -> None:
        for key, values in outgoing_set.items():
            for value in values:
                if value not in self.db.incomming_set:
                    self.db.incomming_set[value] = [key]
                else:
                    self.db.incomming_set[value].append(key)

    def _add_templates(self):
        pass

    def _add_patterns(self):
        pass

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

        if not 'targets' in data:
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

    node_1 = {'type': 'Concept', 'name': 'human'}
    node_2 = {'type': 'Concept', 'name': 'monkey'}
    node_3 = {'type': 'Concept', 'name': 'chimp'}
    node_4 = {'type': 'Concept', 'name': 'snake'}
    node_5 = {'type': 'Concept', 'name': 'earthworm'}
    node_6 = {'type': 'Concept', 'name': 'rhino'}
    node_7 = {'type': 'Concept', 'name': 'triceratops'}
    node_8 = {'type': 'Concept', 'name': 'vine'}
    node_9 = {'type': 'Concept', 'name': 'ent'}
    node_10 = {'type': 'Concept', 'name': 'mammal'}
    node_11 = {'type': 'Concept', 'name': 'animal'}
    node_12 = {'type': 'Concept', 'name': 'reptile'}
    node_13 = {'type': 'Concept', 'name': 'dinosaur'}
    node_14 = {'type': 'Concept', 'name': 'plant'}

    link_1 = {'type': 'Similarity', 'targets': [node_1, node_2]}
    link_2 = {'type': 'Similarity', 'targets': [node_1, node_3]}
    link_3 = {'type': 'Similarity', 'targets': [node_3, node_2]}
    link_4 = {'type': 'Similarity', 'targets': [node_4, node_5]}
    link_5 = {'type': 'Similarity', 'targets': [node_6, node_7]}
    link_6 = {'type': 'Similarity', 'targets': [node_4, node_8]}
    link_7 = {'type': 'Similarity', 'targets': [node_1, node_9]}
    link_8 = {'type': 'Inheritance', 'targets': [node_1, node_10]}
    link_9 = {'type': 'Inheritance', 'targets': [node_2, node_10]}
    link_10 = {'type': 'Inheritance', 'targets': [node_3, node_10]}
    link_11 = {'type': 'Inheritance', 'targets': [node_10, node_11]}
    link_12 = {'type': 'Inheritance', 'targets': [node_12, node_11]}
    link_13 = {'type': 'Inheritance', 'targets': [node_4, node_12]}
    link_14 = {'type': 'Inheritance', 'targets': [node_13, node_12]}
    link_15 = {'type': 'Inheritance', 'targets': [node_7, node_13]}
    link_16 = {'type': 'Inheritance', 'targets': [node_5, node_11]}
    link_17 = {'type': 'Inheritance', 'targets': [node_6, node_10]}
    link_18 = {'type': 'Inheritance', 'targets': [node_8, node_14]}
    link_19 = {'type': 'Inheritance', 'targets': [node_9, node_14]}
    link_20 = {'type': 'Similarity', 'targets': [node_2, node_1]}
    link_21 = {'type': 'Similarity', 'targets': [node_3, node_1]}
    link_22 = {'type': 'Similarity', 'targets': [node_2, node_3]}
    link_23 = {'type': 'Similarity', 'targets': [node_5, node_4]}
    link_24 = {'type': 'Similarity', 'targets': [node_7, node_6]}
    link_25 = {'type': 'Similarity', 'targets': [node_8, node_4]}
    link_26 = {'type': 'Similarity', 'targets': [node_9, node_1]}

    all_nodes = [
        node_1,
        node_2,
        node_3,
        node_4,
        node_5,
        node_6,
        node_7,
        node_8,
        node_9,
        node_10,
        node_11,
        node_12,
        node_13,
        node_14,
    ]
    all_links = [
        link_1,
        link_2,
        link_3,
        link_4,
        link_5,
        link_6,
        link_7,
        link_8,
        link_9,
        link_10,
        link_11,
        link_12,
        link_13,
        link_14,
        link_15,
        link_16,
        link_17,
        link_18,
        link_19,
        link_20,
        link_21,
        link_22,
        link_23,
        link_24,
        link_25,
        link_26,
    ]

    for node in all_nodes:
        db.add_node(node)

    for link in all_links:
        db.add_link(link)
    # db.add_link(data1)

    incoming_set = db.db.incomming_set

    print('Acabou')
