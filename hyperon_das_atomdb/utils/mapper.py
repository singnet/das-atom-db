from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List

from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


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
    def map_table(self, table: Table) -> List[Dict[str, Any]]:
        ...  # pragma: no cover


class SQL2AtomeseMapper(SQLMapper):
    def map_table(self, table: Table) -> List[Dict[str, Any]]:
        return self._to_atoms_type_atomese(table)

    def _to_atoms_type_atomese(self, table: Table) -> List[Dict[str, Any]]:
        """WIP"""
        ...  # pragma no cover


class SQL2MettaMapper(SQLMapper):
    def __init__(self):
        self.unique_id = set()

    def map_table(self, table: Table) -> List[Dict[str, Any]]:
        return self._to_atoms_type_metta(table, [])

    def _to_atoms_type_metta(self, table: Table, buffer: list = []) -> List[Dict[str, Any]]:
        self._add_node(name=table.table_name, is_literal=False, buffer=buffer)

        for row in table.rows:
            _, pk_value = list(row.items())[0]

            pk_link = {
                'type': 'Expression',
                'targets': [
                    {'type': 'Symbol', 'name': table.table_name, 'is_literal': False},
                    {'type': 'Symbol', 'name': pk_value, 'is_literal': True},
                ],
            }

            self._add_node(
                **{'type': 'Symbol', 'name': pk_value, 'is_literal': True}, buffer=buffer
            )

            for key, value in list(row.items())[1:]:
                self._add_link(
                    targets=[
                        {
                            'type': 'Symbol',
                            'name': f'{table.table_name}.{key}',
                            'is_literal': False,
                        },
                        pk_link,
                        {'type': 'Symbol', 'name': value, 'is_literal': True},
                    ],
                    buffer=buffer,
                )

        return buffer

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
            try:
                int(name)
                return 'int'
            except ValueError:
                try:
                    float(name)
                    return 'float'
                except ValueError:
                    return None
        except Exception:
            return None

    def _add_node(self, name: str, type: str = "Symbol", **kwargs) -> Dict[str, Any]:
        if (literal := kwargs.get('is_literal')) is None:
            literal = True if self._is_literal(name) else False

        node = {'is_literal': literal}

        if self._check_numerical_type(name) == 'int':
            node['value_as_int'] = name
        elif self._check_numerical_type(name) == 'float':
            node['value_as_float'] = name

        if literal:
            name = f'"{name}"' if name is not None else ''

        node.update(
            {
                '_id': ExpressionHasher.terminal_hash(type, name),
                'composite_type_hash': ExpressionHasher.named_type_hash(type),
                'named_type': type,
                'name': name,
            }
        )

        if node['_id'] not in self.unique_id:
            kwargs['buffer'].append(node)

        self.unique_id.add(node['_id'])

        return node

    def _add_link(
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
                atom = self._add_node(**target, buffer=kwargs['buffer'])
                atom_hash = ExpressionHasher.named_type_hash(atom['named_type'])
                composite_type.append(atom_hash)
            else:
                atom = self._add_link(**target, toplevel=False, buffer=kwargs['buffer'])
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

        if link['_id'] not in self.unique_id:
            kwargs['buffer'].append(link)

        self.unique_id.add(link['_id'])

        return link


def create_mapper(mapper: str) -> SQLMapper:
    if mapper == "sql2atomese":
        return SQL2AtomeseMapper()
    elif mapper == "sql2metta":
        return SQL2MettaMapper()
    else:
        raise ValueError("Unknown mapper")
