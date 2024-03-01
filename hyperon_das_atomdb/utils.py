from hashlib import md5
from typing import Any, List


class ExpressionHasher:
    compound_separator = " "

    @staticmethod
    def _compute_hash(text: str) -> str:
        return md5(text.encode("utf-8")).digest().hex()

    @staticmethod
    def named_type_hash(name: str) -> str:
        return ExpressionHasher._compute_hash(name)

    @staticmethod
    def terminal_hash(named_type: str, terminal_name: str) -> str:
        return ExpressionHasher._compute_hash(
            ExpressionHasher.compound_separator.join([named_type, terminal_name])
        )

    @staticmethod
    def expression_hash(named_type_hash: str, elements: List[str]) -> str:
        return ExpressionHasher.composite_hash([named_type_hash, *elements])

    @staticmethod
    def composite_hash(hash_base: Any) -> str:
        if isinstance(hash_base, str):
            return hash_base
        elif isinstance(hash_base, list):
            if len(hash_base) == 1:
                return hash_base[0]
            else:
                return ExpressionHasher._compute_hash(
                    ExpressionHasher.compound_separator.join(hash_base)
                )
        else:
            raise ValueError(
                "Invalid base to compute composite hash: " f"{type(hash_base)}: {hash_base}"
            )


class StringExpressionHasher:
    @staticmethod
    def _compute_hash(text: str) -> str:
        return str

    @staticmethod
    def named_type_hash(name: str) -> str:
        return f"<Type: {name}>"

    @staticmethod
    def terminal_hash(named_type: str, terminal_name: str) -> str:
        return f"<{named_type}: {terminal_name}>"

    @staticmethod
    def expression_hash(named_type_hash: str, elements: List[str]) -> str:
        return f"<{named_type_hash}: {elements}>"

    @staticmethod
    def composite_hash(hash_list: List[str]) -> str:
        if len(hash_list) == 1:
            return hash_list[0]
        return f"{hash_list}"


# def generate_binary_matrix(numbers: int) -> list:
#     """This function is more efficient if numbers are greater than 5"""
#     return list(itertools.product([0, 1], repeat=numbers))


def generate_binary_matrix(numbers: int) -> list:
    if numbers <= 0:
        return [[]]
    smaller_matrix = generate_binary_matrix(numbers - 1)
    new_matrix = []
    for matrix in smaller_matrix:
        new_matrix.append(matrix + [0])
        new_matrix.append(matrix + [1])
    return new_matrix


def multiply_binary_matrix_by_string_matrix(
    binary_matrix: List[List[int]], string_matrix: List[str]
) -> List[List[str]]:
    from hyperon_das_atomdb.database import WILDCARD

    result_matrix = []
    for binary_row in binary_matrix:
        result_row = [
            string if bit == 1 else WILDCARD for bit, string in zip(binary_row, string_matrix)
        ]
        result_matrix.append(result_row)
    return result_matrix[:-1]


def build_patern_keys(hash_list: List[str]) -> List[str]:
    binary_matrix = generate_binary_matrix(len(hash_list))
    result_matrix = multiply_binary_matrix_by_string_matrix(binary_matrix, hash_list)
    keys = [
        ExpressionHasher.expression_hash(matrix_item[:1][0], matrix_item[1:])
        for matrix_item in result_matrix
    ]
    return keys
