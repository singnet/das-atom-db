from abc import ABC, abstractmethod
from hashlib import md5
from typing import Any, Dict, List, Optional, Tuple


class Index(ABC):
    @staticmethod
    def generate_index_id(
        atom_type: str,
        fields: List[str],
        conditionals: Dict[str, Any],
        extras: Optional[List[str]] = None,
    ) -> str:
        """Generates an index ID based on the field name.

        Args:
            atom_type (str): Atom's type.
            fields (List[str]): Field names that will be joined with commas at the beginning of the index ID.
            conditionals (Dict[str, Any]): A dictionary containing the conditionals for the index;
                this will be MD5 hashed.
            extras (Optional[List[str]]): A list of optional values that will be joined with underscores at the end
                of the index ID.

        Returns:
            str: The index ID.
        """
        index_fields = ','.join(fields)
        index_conditionals = md5(
            str(conditionals).encode()
        ).hexdigest()  # Hashing to MD5 to avoid large objects
        index_extras = '_'.join(extras or [])
        return f'{atom_type}_{index_fields}_{index_conditionals}_{index_extras}'

    @abstractmethod
    def create(
        self,
        atom_type: str,
        fields: List[str],
        **kwargs,
    ) -> Tuple[str, Any]:
        """Creates an index on the given field.

        Args:
            atom_type (str): Atom's type
            fields (List[str]): The fields to create the index on.


        Returns:
            Tuple[str, Any]: Returns the index id and the index properties dict
        """
        ...  # pragma: no cover

    @abstractmethod
    def index_exists(self, index_id: str) -> bool:
        """Checks if an index exists

        Args:
            index_id (str): The index ID.

        Returns:
            bool: True if the index exists, False otherwise.
        """
        ...  # pragma: no cover
