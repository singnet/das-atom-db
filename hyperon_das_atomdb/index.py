from abc import ABC, abstractmethod
from typing import Any, Tuple

from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


class Index(ABC):
    @staticmethod
    def generate_index_id(field: str) -> str:
        """Generates an index ID based on the field name.

        Args:
            field (str): The field name.

        Returns:
            str: The index ID.
        """
        return f"index_{ExpressionHasher._compute_hash(field)}"

    @abstractmethod
    def create(self, field: str, **kwargs) -> Tuple[str, Any]:
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
