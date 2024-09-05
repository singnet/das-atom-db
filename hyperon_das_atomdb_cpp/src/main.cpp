#include <iostream>
#include <unordered_map>
#include <vector>

#include "adapters/ram_only.hpp"
#include "database.hpp"
#include "utils/params.hpp"

using namespace std;
using namespace atomdb;

int main(int argc, char const* argv[]) {
    InMemoryDB db;
    auto node_params = NodeParams(/*type*/ "Person", /*name*/ "John Doe");
    node_params.custom_attributes.set("age", 42);
    auto node = db.add_node(node_params);
    cout << "Node handle: " << node.handle << endl;
    cout << "Node name: " << node.name << endl;
    cout << "Node age: " << node.custom_attributes.get<int>("age").value_or(0) << endl;

    auto link_params =
        LinkParams("Friendship",
                   /*custom_attributes*/ {{"since", "2021-01-01"}, {"location", "New York"}});
    link_params.add_target(NodeParams("Person", "Jane Doe"));
    link_params.add_target(NodeParams("Person", "Samuel L. Jackson"));
    auto nested_link = LinkParams("Fellowship");
    nested_link.add_target(NodeParams("Person", "Jane Doe"));
    nested_link.add_target(NodeParams("Person", "Michael Douglas"));
    link_params.add_target(nested_link);
    auto opt_link = db.add_link(link_params);
    if (opt_link.has_value()) {
        auto link = opt_link.value();
        cout << "Link handle: " << link.handle << endl;
        cout << "Link type: " << link.named_type << endl;
        cout << "Link targets: [";
        string targets;
        for (const auto& target : link.targets) {
            targets += target + ", ";
        }
        if (!targets.empty()) {  // Remove trailing comma and space
            targets.pop_back();
            targets.pop_back();
        }
        cout << targets << "]" << endl;
        cout << "Link location: " << link.custom_attributes.get<string>("location").value_or("") << endl;
    }

    return 0;
}
