import copy
from typing import Any, Dict, List, Optional, Tuple, Union

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
            ingoing_set={},
            patterns={},
            templates={},
            names={}
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
                details=node_params
            )       

        node_type = node_params.pop('type')
        node_name = node_params.pop('name')

        key = self._create_node_handle(node_type, node_name)
        
        try:
            node = self.db.node[key]
        except KeyError:    
            self.db.node[key] = {
                '_id': key,
                'composite_type_hash': ExpressionHasher.named_type_hash(node_type),
                'name': node_name,
                'named_type': node_type
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
        
        key = ExpressionHasher.expression_hash(typedef_mark_hash, [name_hash, type_hash])

        try:
            atom = self.db.atom_type[key]
        except KeyError:    
            base_type_hash = ExpressionHasher.named_type_hash("Type")
            composite_type = [typedef_mark_hash, type_hash, base_type_hash]    
            composite_type_hash = ExpressionHasher.composite_hash(composite_type)
        
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

        keys = link_params.keys()
        
        if 'type' not in keys or 'targets' not in keys:
            raise AddLinkException(
                message='The "type" and "targets" fields must be sent',
                details=link_params
            )        
        
        link_type = link_params.pop('type')
        targets = link_params.pop('targets')
        
        for target in targets:
            if 'targets' not in target.keys():
                self.add_node(target.copy())
            else:
                self.add_link(target.copy())        
        
        arity_number = len(targets)
        link_db = self.db.link.get_arity(arity_number)
        
        link_type_hash = ExpressionHasher.named_type_hash(link_type)
        
        composite_type = []
        composite_type.append(link_type_hash)
        for target in targets:
            composite_type.append(ExpressionHasher.named_type_hash(target['type']))

        targets_hash = [ExpressionHasher.terminal_hash(target['type'], target['name']) for target in targets]
        key = ExpressionHasher.expression_hash(link_type_hash, targets_hash)

        link_db[key] = {
            '_id': key,
            'composite_type_hash': ExpressionHasher.composite_hash(composite_type),
            'is_toplevel': True,
            'composite_type': composite_type,
            'named_type': link_type,
            'named_type_hash': ExpressionHasher.named_type_hash(link_type)
        }
    
        for item in range(arity_number):
            link_db[key][f'key_{item}'] = targets_hash[item]
        
        # self._add_atom_type(_name=link_type)
        
        link = link_db[key]
        
        return link
    
    def _calculate_composite_type(self, data):
        composite_type = []
        if 'targets' in data:   
            for target in data['targets']:
                if 'targets' in target:
                    composite_type.append(self._calculate_composite_type(target.copy()))
                else:
                    composite_type.append(ExpressionHasher.named_type_hash(target.get('type')))
        composite_type.insert(0, ExpressionHasher.named_type_hash(data.get('type')))
        return composite_type

if __name__ == '__main__':
    db = InMemoryDB()
    
    data1 = {
            'type': 'Evaluation',
            'targets': [
                {
                    'type': 'Predicate',
                    'name': 'Predicate:has_name'
                },
                {
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
                                    'name': 'Reactome:R-HSA-164843'   
                                },
                                {
                                    'type': 'Concept',
                                    'name': 'Concept:2-LTR circle formation'            
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    data2 = {
        'type': 'Similarity',
        'targets': [
            {
                'type': 'Concept',
                'name': 'human'
            },
            {
                'type': 'Concept',
                'name': 'monkey'
            },      
        ]
    }
    
    # resp = db.add_link({
    #                         'type': 'Set',
    #                         'targets': [
    #                             {
    #                                 'type': 'Reactome',
    #                                 'name': 'Reactome:R-HSA-164843'   
    #                             },
    #                             {
    #                                 'type': 'Concept',
    #                                 'name': 'Concept:2-LTR circle formation'            
    #                             }
    #                         ]
    #                     })
    
    # resp = db.add_node({'name':'human','type':'Concept'})
    
    data = 	{
        'type': 'Evaluation',
	    'targets': [
		    {'type': 'Predicate'},
            {
                'type': 'Evaluation',
                'targets': [
                    {'type': 'Predicate'},
                    {
                        'type': 'Set',
                        'targets': [
                            {'type': 'Reactome'},
                            {'type': 'Concept'}
                        ]
                    }
                ]
            }
        ]
    }
    
    def link(data):
        result = []
        if 'targets' in data:   
            for target in data['targets']:
                if 'targets' in target:
                    result.append(link(target.copy()))
                else:
                    result.append(ExpressionHasher.named_type_hash(target.get('type')))
        
        result.insert(0, ExpressionHasher.named_type_hash(data.get('type')))      

        return result
    
    resp = link(data)    
    
    print(resp)