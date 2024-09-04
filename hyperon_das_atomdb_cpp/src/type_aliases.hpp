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

namespace atomdb {

// Type aliases for readability

template <typename T>
using opt = std::optional<T>;

using OptCursor = opt<int>;
using StringSet = std::set<std::string>;
using StringList = std::vector<std::string>;
using StringUnorderedSet = std::unordered_set<std::string>;
using ListOfAny = std::vector<std::any>;

using Pattern_or_Template = std::tuple<std::string, opt<StringList>>;
using Pattern_or_Template_List = std::vector<Pattern_or_Template>;

}  // namespace atomdb

#endif  // _TYPE_ALIASES_HPP
