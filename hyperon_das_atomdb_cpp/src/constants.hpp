#pragma once

#include "type_aliases.hpp"
#include "utils/expression_hasher.hpp"

using namespace std;

namespace atomdb {

constexpr const char* WILDCARD = "*";

// pre-computed hashes
static const string WILDCARD_HASH = ExpressionHasher::named_type_hash(WILDCARD);
static const string TYPE_HASH = ExpressionHasher::named_type_hash("Type");
static const string TYPEDEF_MARK_HASH = ExpressionHasher::named_type_hash(":");

enum FieldIndexType { BINARY_TREE = 0, TOKEN_INVERTED_LIST };

}  // namespace atomdb