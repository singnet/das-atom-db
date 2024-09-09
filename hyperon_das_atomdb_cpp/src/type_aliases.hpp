#ifndef _TYPE_ALIASES_HPP
#define _TYPE_ALIASES_HPP

#include <any>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

namespace atomdb {

// Type aliases for readability

template <typename T>
using opt = optional<T>;

using OptCursor = opt<int>;
using StringSet = set<string>;
using StringList = vector<string>;
using StringUnorderedSet = unordered_set<string>;
using ListOfAny = vector<any>;

using Pattern_or_Template = tuple<string, opt<StringList>>;
using Pattern_or_Template_List = vector<Pattern_or_Template>;

}  // namespace atomdb

#endif  // _TYPE_ALIASES_HPP
