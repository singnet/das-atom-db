#pragma once

#include <string>
#include <vector>

#include "constants.hpp"
#include "expression_hasher.hpp"

using namespace std;

namespace atomdb {

using IntMatrix = vector<vector<int>>;
using StringMatrix = vector<vector<string>>;

/**
 * @brief Generates a binary matrix of the given size.
 *
 * This function generates a binary matrix represented as a vector of vectors,
 * where each subvector is a row in the matrix.
 *
 * @param numbers The size of the binary matrix to generate. If numbers
 *                is less than or equal to 0, returns a matrix with an empty vector.
 * @return A binary matrix represented as a vector of vectors.
 */
IntMatrix generate_binary_matrix(int numbers) {
    if (numbers <= 0) {
        return {{}};
    }
    IntMatrix smaller_matrix = generate_binary_matrix(numbers - 1);
    IntMatrix new_matrix;
    for (const auto& matrix : smaller_matrix) {
        vector<int> row_with_zero = matrix;
        row_with_zero.push_back(0);
        new_matrix.push_back(row_with_zero);

        vector<int> row_with_one = matrix;
        row_with_one.push_back(1);
        new_matrix.push_back(row_with_one);
    }
    return move(new_matrix);
}

/**
 * @brief Multiplies a binary matrix by a string matrix.
 *
 * This function takes a binary matrix and a string matrix, and multiplies them
 * to produce a resulting matrix. Each element in the binary matrix determines
 * whether to include the corresponding string from the string matrix or a wildcard.
 *
 * @param binary_matrix A binary matrix represented as a vector of vectors of integers.
 * @param string_matrix A vector of strings to multiply with the binary matrix.
 * @return A matrix represented as a vector of vectors of strings, where each
 *         subvector is a row in the resulting matrix.
 */
StringMatrix multiply_binary_matrix_by_string_matrix(const IntMatrix& binary_matrix,
                                                     const StringList& string_matrix) {
    StringMatrix result_matrix;

    for (const auto& binary_row : binary_matrix) {
        StringList result_row;
        for (size_t i = 0; i < binary_row.size(); ++i) {
            result_row.push_back(binary_row[i] == 1 ? string_matrix[i] : WILDCARD);
        }
        result_matrix.push_back(result_row);
    }

    // Remove the last row from the result matrix
    if (not result_matrix.empty()) {
        result_matrix.pop_back();
    }

    return move(result_matrix);
}

/**
 * @brief Builds pattern keys using a list of hashes.
 *
 * This function takes a list of hash strings, generates a binary matrix,
 * multiplies it by the hash list, and then generates pattern keys from the result.
 *
 * @param hash_list A vector of hash strings to build pattern keys from.
 * @return A vector of pattern keys generated from the hash list.
 */
StringList build_pattern_keys(const StringList& hash_list) {
    auto binary_matrix = generate_binary_matrix(hash_list.size());
    auto result_matrix = multiply_binary_matrix_by_string_matrix(binary_matrix, hash_list);

    StringList keys;
    for (const auto& matrix_item : result_matrix) {
        string type_hash = matrix_item[0];
        StringList elements(matrix_item.begin() + 1, matrix_item.end());
        keys.push_back(ExpressionHasher::expression_hash(type_hash, elements));
    }

    return move(keys);
}

}  // namespace atomdb
