#pragma once

#include "type_aliases.hpp"
#include "utils/expression_hasher.hpp"

namespace atomdb {

constexpr const char* WILDCARD = "*";

// pre-computed hashes
static const std::string TYPE_HASH = ExpressionHasher::named_type_hash("Type");
static const std::string TYPEDEF_MARK_HASH = ExpressionHasher::named_type_hash(":");

enum class FieldIndexType { BINARY_TREE, TOKEN_INVERTED_LIST };

}  // namespace atomdb