#pragma once

#include <variant>

using namespace std;

namespace atomdb {

struct KwArgs {
    bool no_target_format = false;
    bool targets_documents = false;
    bool deep_representation = false;
    bool toplevel_only = false;
    bool handles_only = false;
};

struct NodeParams {
    string type = "";
    string name = "";
};

struct LinkParams {
    using Target = variant<NodeParams, LinkParams>;
    using Targets = vector<Target>;

    string type = "";
    Targets targets = {};
};

}  // namespace atomdb
