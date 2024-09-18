#pragma once

#include <openssl/md5.h>

#include <string>

#include "type_aliases.hpp"

#define MAX_HASHABLE_STRING_SIZE 1024
#define MAX_LITERAL_OR_SYMBOL_SIZE 256
#define JOINING_CHAR ((char) ' ')

using namespace std;

namespace atomdb {

class ExpressionHasher {
   public:
    /**
     * @brief Computes the MD5 hash of the given input string.
     *
     * @param input The input string to be hashed.
     * @return A string representing the MD5 hash of the input.
     */
    static const string compute_hash(const string& input) {
        MD5_CTX ctx;
        unsigned char MD5_BUFFER[MD5_DIGEST_LENGTH];
        char HASH[2 * MD5_DIGEST_LENGTH + 1];

        MD5_Init(&ctx);
        MD5_Update(&ctx, input.c_str(), input.length());
        MD5_Final(MD5_BUFFER, &ctx);

        for (unsigned int i = 0; i < MD5_DIGEST_LENGTH; i++) {
            sprintf(HASH + 2 * i, "%02x", MD5_BUFFER[i]);
        }
        HASH[2 * MD5_DIGEST_LENGTH] = '\0';

        return move(string(HASH));
    }

    /**
     * @brief Generates a hash for a named type.
     *
     * @param name The name of the type.
     * @return A string representing the hash of the named type.
     */
    static const string named_type_hash(const string& name) { return compute_hash(name); }

    /**
     * @brief Generates a hash for a terminal expression.
     *
     * @param type The type of the terminal expression.
     * @param name The name of the terminal expression.
     * @return A string representing the hash of the terminal expression.
     * @throws invalid_argument if the terminal name is too large.
     */
    static const string terminal_hash(const string& type, const string& name) {
        if (type.length() + name.length() >= MAX_HASHABLE_STRING_SIZE) {
            throw invalid_argument("Invalid (too large) terminal name");
        }
        string hashable_string = type + JOINING_CHAR + name;
        return compute_hash(hashable_string);
    }

    /**
     * @brief Generates a hash for a composite expression.
     *
     * @param elements A vector of strings representing the elements of the composite expression.
     * @return A string representing the hash of the composite expression.
     */
    static const string composite_hash(const StringList& elements) {
        if (elements.size() == 1) {
            return elements[0];
        }

        string hashable_string;
        for (const auto& element : elements) {
            hashable_string += element + JOINING_CHAR;
        }
        hashable_string.pop_back();  // remove the last joining character

        return compute_hash(hashable_string);
    }

    /**
     * @brief Generates a composite hash from a list of elements.
     *
     * This function takes a vector of elements, each of which can be of any type,
     * and generates a composite hash representing the combined hash of all elements.
     *
     * @param elements A vector of elements of type std::any, representing the components to be
     * hashed.
     * @return A string representing the composite hash generated from the elements.
     */
    static std::string composite_hash(const ListOfAny& elements) {
        StringList hashable_elements;
        for (const auto& element : elements) {
            if (auto str = any_cast<string>(&element)) {
                hashable_elements.push_back(*str);
            } else {
                throw invalid_argument("Invalid composite type element.");
            }
        }
        return composite_hash(hashable_elements);
    }

    /**
     * @brief Generates a composite hash from a base hash.
     *
     * This function takes a base hash string and generates a composite hash
     * by applying additional hashing logic.
     *
     * @param hash_base A string representing the base hash.
     * @return A string representing the composite hash generated from the base hash.
     */
    static const string composite_hash(const string& hash_base) { return hash_base; }

    /**
     * @brief Generates a hash for an expression.
     *
     * @param type_hash The hash of the type of the expression.
     * @param elements A vector of strings representing the elements of the expression.
     * @return A string representing the hash of the expression.
     */
    static const string expression_hash(const string& type_hash, const StringList& elements) {
        StringList composite({type_hash});
        composite.insert(composite.end(), elements.begin(), elements.end());
        return composite_hash(composite);
    }
};

}  // namespace atomdb
