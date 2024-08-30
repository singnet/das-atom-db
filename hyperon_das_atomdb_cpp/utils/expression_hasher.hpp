#ifndef _EXPRESSIONHASHER_H
#define _EXPRESSIONHASHER_H

#include <openssl/md5.h>

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#define MAX_HASHABLE_STRING_SIZE 1024
#define MAX_LITERAL_OR_SYMBOL_SIZE 256
#define JOINING_CHAR ((char) ' ')

class ExpressionHasher {
   public:
    /**
     * @brief Computes the MD5 hash of the given input string.
     *
     * @param input The input string to be hashed.
     * @return A string representing the MD5 hash of the input.
     */
    static std::string compute_hash(const std::string& input) {
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

        return std::string(HASH);
    }

    /**
     * @brief Generates a hash for a named type.
     *
     * @param name The name of the type.
     * @return A string representing the hash of the named type.
     */
    static std::string named_type_hash(const std::string& name) {
        return compute_hash(name);
    }

    /**
     * @brief Generates a hash for a terminal expression.
     *
     * @param type The type of the terminal expression.
     * @param name The name of the terminal expression.
     * @return A string representing the hash of the terminal expression.
     */
    static std::string terminal_hash(const std::string& type, const std::string& name) {
        if (type.length() + name.length() >= MAX_HASHABLE_STRING_SIZE) {
            std::cerr << "Invalid (too large) terminal name" << std::endl;
            exit(1);
        }
        std::string hashable_string = type + JOINING_CHAR + name;
        return compute_hash(hashable_string);
    }

    /**
     * @brief Generates a hash for a composite expression.
     *
     * @param elements A vector of strings representing the elements of the composite expression.
     * @return A string representing the hash of the composite expression.
     */
    static std::string composite_hash(const StringList& elements) {
        unsigned int total_size = 0;
        std::vector<unsigned int> element_size(elements.size());

        for (size_t i = 0; i < elements.size(); i++) {
            unsigned int size = elements[i].length();
            if (size > MAX_LITERAL_OR_SYMBOL_SIZE) {
                std::cerr << "Invalid (too large) composite elements" << std::endl;
                exit(1);
            }
            element_size[i] = size;
            total_size += size;
        }
        if (total_size >= MAX_HASHABLE_STRING_SIZE) {
            std::cerr << "Invalid (too large) composite elements" << std::endl;
            exit(1);
        }

        std::string hashable_string;
        for (size_t i = 0; i < elements.size(); i++) {
            hashable_string += elements[i];
            if (i != elements.size() - 1) {
                hashable_string += JOINING_CHAR;
            }
        }

        return compute_hash(hashable_string);
    }

    /**
     * @brief Generates a hash for an expression.
     *
     * @param type_hash The hash of the type of the expression.
     * @param elements A vector of strings representing the elements of the expression.
     * @return A string representing the hash of the expression.
     */
    static std::string expression_hash(
        const std::string& type_hash, const StringList& elements) {
        StringList composite;
        composite.push_back(type_hash);
        composite.insert(composite.end(), elements.begin(), elements.end());
        return composite_hash(composite);
    }
};

#endif  // _EXPRESSIONHASHER_H
