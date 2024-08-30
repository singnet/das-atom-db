#ifndef EXPRESSIONHASHER_H
#define EXPRESSIONHASHER_H

#define JOINING_CHAR ((char) ' ')
#define MAX_LITERAL_OR_SYMBOL_SIZE ((size_t) 10000)
#define MAX_HASHABLE_STRING_SIZE ((size_t) 100000)
#define HANDLE_HASH_SIZE ((unsigned int) 33)

/**
 * @brief Computes the hash of the given input string.
 *
 * This function takes an input string, computes its hash, and returns
 * the hash as a string.
 *
 * @param input A string representing the input to be hashed.
 * @return A string representing the hash of the input.
 */
const char* compute_hash(const char* input);

/**
 * @brief Generates a hash for a named type.
 *
 * This function takes a name and generates a hash representing the named type.
 *
 * @param name A string representing the name of the type.
 * @return A string representing the hash of the named type.
 */
const char* named_type_hash(const char* name);

/**
 * @brief Generates a hash for a terminal expression.
 *
 * This function takes a type and a name, and combines them
 * to generate a hash representing the terminal expression.
 *
 * @param type A string representing the type of the terminal expression.
 * @param name A string representing the name of the terminal expression.
 * @return A string representing the hash of the terminal expression.
 */
const char* terminal_hash(const char* type, const char* name);

/**
 * @brief Generates a composite hash from an array of element hashes.
 *
 * This function takes an array of element hashes and combines them
 * to generate a single composite hash.
 *
 * @param elements An array of strings, each representing the hash of an element.
 * @param nelements The number of elements in the elements array.
 * @return A string representing the combined composite hash.
 */
const char* composite_hash(const char** elements, unsigned int nelements);

/**
 * @brief Generates a hash for an expression.
 *
 * This function takes a type hash and an array of element hashes, and combines them
 * to generate a single hash representing the entire expression.
 *
 * @param type_hash A string representing the hash of the type.
 * @param elements An array of strings, each representing the hash of an element.
 * @param nelements The number of elements in the elements array.
 * @return A string representing the combined hash of the expression.
 */
const char* expression_hash(const char* type_hash, const char** elements, unsigned int nelements);

#endif
