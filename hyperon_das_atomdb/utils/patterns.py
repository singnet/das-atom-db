"""
This module provides utility functions for generating binary matrices and manipulating them.

It includes functions to generate binary matrices of a given size, multiply binary matrices
by string matrices, and build pattern keys using a list of hashes. These utilities are useful
for various operations involving binary and string data manipulation.
"""

from hyperon_das_atomdb.database import WILDCARD
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

# TODO(angelo,andre): delete this commented function?
# def generate_binary_matrix(numbers: int) -> list:
#     """This function is more efficient if numbers are greater than 5"""
#     return list(itertools.product([0, 1], repeat=numbers))


def generate_binary_matrix(numbers: int) -> list[list[int]]:
    """
    Generate a binary matrix of the given size.

    Args:
        numbers (int): The size of the binary matrix to generate. If numbers
            is less than or equal to 0, returns a matrix with an empty list.

    Returns:
        list[list[int]]: A binary matrix represented as a list of lists, where
        each sublist is a row in the matrix.
    """
    if numbers <= 0:
        return [[]]
    return [matrix + [bit] for matrix in generate_binary_matrix(numbers - 1) for bit in (0, 1)]


def multiply_binary_matrix_by_string_matrix(
    binary_matrix: list[list[int]], string_matrix: list[str]
) -> list[list[str]]:
    """
    Multiply a binary matrix by a string matrix.

    Args:
        binary_matrix (list[list[int]]): A binary matrix represented as a list
            of lists, where each sublist is a row in the matrix.
        string_matrix (list[str]): A list of strings to multiply with the
            binary matrix.

    Returns:
        list[list[str]]: A matrix represented as a list of lists, where each
        sublist is a row in the resulting matrix.
    """
    return [
        [string if bit == 1 else WILDCARD for bit, string in zip(binary_row, string_matrix)]
        for binary_row in binary_matrix
    ][:-1]


def build_pattern_keys(hash_list: list[str]) -> list[str]:
    """
    Build pattern keys using a list of hashes.

    Args:
        hash_list (list[str]): A list of hash strings to build pattern keys from.

    Returns:
        list[str]: A list of pattern keys generated from the hash list.
    """
    return [
        ExpressionHasher.expression_hash(matrix_item[:1][0], matrix_item[1:])
        for matrix_item in multiply_binary_matrix_by_string_matrix(
            generate_binary_matrix(len(hash_list)), hash_list
        )
    ]
