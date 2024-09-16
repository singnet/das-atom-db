#pragma once

#include <any>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

namespace atomdb {

// Type aliases for readability

template <typename T>
using opt = optional<T>;

using OptCursor = int;
constexpr const int NO_CURSOR = -1;

using StringSet = set<string>;
using StringList = vector<string>;
using StringUnorderedSet = unordered_set<string>;
using ListOfAny = vector<any>;

using Pattern_or_Template = pair<string, opt<StringList>>;
using Pattern_or_Template_List = vector<Pattern_or_Template>;

}  // namespace atomdb
