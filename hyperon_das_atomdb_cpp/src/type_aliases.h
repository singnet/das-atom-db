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
using uchar = unsigned char;

template <typename T>
using opt = optional<T>;

using StringSet = set<string>;
using StringUnorderedMap = unordered_map<string, string>;
using StringList = vector<string>;
using StringUnorderedSet = unordered_set<string>;
using IntUnorderedMap = unordered_map<string, long>;
using FloatUnorderedMap = unordered_map<string, double>;
using BoolUnorderedMap = unordered_map<string, bool>;

/**
 * std::vector<std::any> performs well enough in some particular cases, but be cautious when using it.
 * Always test how it performs in your specific use case.
 */
using ListOfAny = vector<any>;

/**
 * NOTE:
 * The following type alias was commented out because std::unordered_map<T, std::any> performs
 * poorly, and it was kept here just as a reminder of the performance implications.
 *
 * using MapOfAny = unordered_map<string, any>;
 */

}  // namespace atomdb
