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
    opt<int> cursor = nullopt;
    int chunk_size = 500;
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

    static bool is_node(const Target& target) { return holds_alternative<NodeParams>(target); }
    static bool is_link(const Target& target) { return holds_alternative<LinkParams>(target); }
    static const NodeParams& as_node(const Target& target) { return get<NodeParams>(target); }
    static const LinkParams& as_link(const Target& target) { return get<LinkParams>(target); }
};

}  // namespace atomdb
