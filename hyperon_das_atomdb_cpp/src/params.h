#pragma once

#include <variant>

#include "type_aliases.h"

using namespace std;

namespace atomdb {

struct KwArgs {
    bool no_target_format = false;
    bool targets_document = false;
    bool deep_representation = false;
    bool toplevel_only = false;
    bool handles_only = false;
};

struct CustomAttributes {
    StringUnorderedMap strings = {};
    IntUnorderedMap integers = {};
    FloatUnorderedMap floats = {};
    BoolUnorderedMap booleans = {};
};

struct NodeParams {
    string type = "";
    string name = "";
    opt<CustomAttributes> custom_attributes = nullopt;
};

struct LinkParams {
    using Target = variant<NodeParams, LinkParams>;
    using Targets = vector<Target>;

    string type = "";
    opt<Targets> targets = nullopt;
    opt<CustomAttributes> custom_attributes = nullopt;
};

}  // namespace atomdb
