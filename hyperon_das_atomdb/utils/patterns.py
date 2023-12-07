from typing import List

from hyperon_das_atomdb.database import WILDCARD
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

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
