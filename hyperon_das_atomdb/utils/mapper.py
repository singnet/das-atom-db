from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List

from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


class FieldNames(str, Enum):
    ID_HASH = '_id'
    NODE_NAME = 'name'
    TYPE_NAME = 'named_type'
    TYPE_NAME_HASH = 'named_type_hash'
    TYPE = 'composite_type_hash'
    COMPOSITE_TYPE = 'composite_type'
    KEY_PREFIX = 'key'
    KEYS = 'keys'


@dataclass
class Table:
    table_name: str = ""
    columns: List[Any] = field(default_factory=list)
    rows: List[Any] = field(default_factory=list)

    def add_column(self, name: str, data_type: str, constraint_type: str = '') -> None:
        self.columns.append({"name": name, "type": data_type, "constraint_type": constraint_type})

    def add_row(self, row_data: Dict[str, Any]) -> None:
        if len(row_data) != len(self.columns):
            raise ValueError("error")  # Improve the message
        self.rows.append(row_data)

    def get_column_names(self) -> list:
        return [column['name'] for column in self.columns]

    def to_dict(self) -> dict:
        return {self.table_name: self.rows}


class SQLMapper(ABC):
    @abstractmethod
    def map_table(self, table: Table) -> None:
        ...  # pragma: no cover


class SQL2AtomeseMapper(SQLMapper):
    def map_table(self, table: Table) -> None:
        return self._to_atoms_type_atomese(table.to_dict())

    def _to_atoms_type_atomese(self, table: Table) -> Dict[str, Any]:
        """WIP"""
        ...  # pragma no cover


class SQL2MettaMapper(SQLMapper):
    def map_table(self, table: Table) -> None:
        return self._to_atoms_type_metta(table)
    
    def node(self, name: str, type: str = "Symbol", **kwargs) -> Dict[str, Any]:
        return self._create_node(name, type, **kwargs)

    def _to_atoms_type_metta(self, table: Table) -> List[Dict[str, Any]]:
        atoms = [self._create_node(name=table.table_name, is_literal=False)]

        # for column in table.columns:
        #     if column['constraint_type'] == 'PK':
        #         continue

        #     atoms.append(self._create_node(name=f"{table.table_name}.{column['name']}"))

        for row in table.rows:
            _, pk_value = list(row.items())[0]

            key_0 = {'type': 'Symbol', 'name': table.table_name, 'is_literal': False}
            key_1 = {'type': 'Symbol', 'name': pk_value, 'is_literal': True}

            # if self._check_numerical_type(pk_value) == 'int':
            #     key_1['value_as_int'] = pk_value
            # elif self._check_numerical_type(pk_value) == 'float':
            #     key_1['value_as_float'] = pk_value

            pk_link = {'type': 'Expression', 'targets': [key_0, key_1]}

            for key, value in list(row.items())[1:]:
                key_0 = {'type': 'Symbol', 'name': f'{table.table_name}.{key}', 'is_literal': False}
                key_1 = pk_link
                key_2 = {'type': 'Symbol', 'name': value, 'is_literal': True}

                # if self._check_numerical_type(value) == 'int':
                #     key_2['value_as_int'] = value
                # elif self._check_numerical_type(value) == 'float':
                #     key_2['value_as_float'] = value

                answers = self._create_link(targets=[key_0, key_1, key_2])

                for answer in answers:
                    if answer not in atoms:
                        atoms.append(answer)

        return atoms

    def _is_literal(self, name: str) -> bool:
        if (
            name
            in [
                'Type',
                'MettaType',
                'Concept',
                'Symbol',
                'Expression',
                'Similarity',
                'Inheritance',
                ':',
            ]
        ) or ('.' in name):
            return False
        return True

    def _check_numerical_type(self, name) -> str:
        try:
            int(name)
            return 'int'
        except ValueError:
            try:
                float(name)
                return 'float'
            except ValueError:
                return None

    def _create_node(self, name: str, type: str = "Symbol", **kwargs) -> Dict[str, Any]:
        if (literal := kwargs.get('is_literal')) is None:
            literal = True if self._is_literal(name) else False

        node = {'is_literal': literal}

        if self._check_numerical_type(name) == 'int':
            node['value_as_int'] = name
        elif self._check_numerical_type(name) == 'float':
            node['value_as_float'] = name

        if literal:
            name = f'"{name}"'

        node.update(
            {
                '_id': ExpressionHasher.terminal_hash(type, name),
                'composite_type_hash': ExpressionHasher.named_type_hash(type),
                'named_type': type,
                'name': name,
            }
        )

        return node

    def _create_link(
        self,
        targets: List[Dict[str, Any]],
        type: str = "Expression",
        toplevel: bool = True,
        **kwargs,
    ) -> Dict[str, Any]:
        link_type_hash = ExpressionHasher.named_type_hash(type)

        targets_hash = []
        composite_type = [link_type_hash]
        composite_type_hash = [link_type_hash]

        targets_atom = []

        for target in targets:
            if 'targets' not in target.keys():
                atom = self._create_node(**target)
                atom_hash = ExpressionHasher.named_type_hash(atom['named_type'])
                composite_type.append(atom_hash)
            else:
                atom = self._create_link(**target, toplevel=False)[0]
                composite_type.append(atom['composite_type'])
                atom_hash = atom['composite_type_hash']

            composite_type_hash.append(atom_hash)

            targets_hash.append(atom['_id'])

            targets_atom.append(atom)

        link = {
            '_id': ExpressionHasher.expression_hash(link_type_hash, targets_hash),
            'composite_type_hash': ExpressionHasher.composite_hash(composite_type_hash),
            'is_toplevel': toplevel,
            'composite_type': composite_type,
            'named_type': type,
            'named_type_hash': link_type_hash,
        }

        for item in range(len(targets)):
            link[f'key_{item}'] = targets_hash[item]

        ret = [link]
        ret.extend(targets_atom)

        return ret


def create_mapper(mapper: str) -> SQLMapper:
    if mapper == "sql2atomese":
        return SQL2AtomeseMapper()
    elif mapper == "sql2metta":
        return SQL2MettaMapper()
    else:
        raise ValueError("Unknown mapper")
