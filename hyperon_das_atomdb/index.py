from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


class Index(ABC):
    @staticmethod
    def generate_index_id(field: str, conditionals: Dict[str, Any]) -> str:
        """Generates an index ID based on the field name.

        Args:
            field (str): The field name.

        Returns:
            str: The index ID.
        """
        return ExpressionHasher._compute_hash(f'{field}{conditionals}')

    @abstractmethod
    def create(self, atom_type: str, field: str, **kwargs) -> Tuple[str, Any]:
        """Creates an index on the given field.

        Args:
            field (str): The field to create the index on.

        Returns:
            Tuple[str, Any]: The index ID.
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
