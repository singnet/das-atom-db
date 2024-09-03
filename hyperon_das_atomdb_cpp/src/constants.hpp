#ifndef _CONTANTS_HPP
#define _CONTANTS_HPP

#include "type_aliases.hpp"

const std::string WILDCARD = "*";
const StringList UNORDERED_LINK_TYPES = {};

enum class FieldIndexType {
    BINARY_TREE,
    TOKEN_INVERTED_LIST
};

#endif  // _CONTANTS_HPP