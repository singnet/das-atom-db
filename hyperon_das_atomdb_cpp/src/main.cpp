#include <iostream>

#include "adapters/ram_only.hpp"
#include "database.hpp"
#include "utils/params.hpp"

using namespace std;
using namespace atomdb;

int main(int argc, char const* argv[]) {
    InMemoryDB db;

    // Adding a Node
    auto node_params = NodeParams(
        "Person", // type
        "John Doe", // name
        {{"gender", "male"}} // custom attributes
    );
    node_params.custom_attributes.set("age", 42); // custom attribute can be set after initialization

    auto node = db.add_node(node_params);
    
    cout << "Node handle: " << node->handle << endl;
    cout << "Node name: " << node->name << endl;
    cout << "Node age: " << node->custom_attributes.get<int>("age").value_or(-1) << endl;

    // Adding a Link
    /*
    Equivalent in Python for the C++ code below:
    link_params = {
        "type": "Friendship",
        "since": "2021-01-01",
        "location": "New York",
        "targets": [
            { "type": "Person", "name": "Jane Doe" },            # a Person node as a target
            { "type": "Person", "name": "Samuel L. Jackson" },   # another Person node as a target
            {
                "type": "Fellowship",        # a Fellowship link as a target of Friendship
                "targets": [
                    {
                        "type": "Person",    # a Person node as a target of Fellowship
                        "name": "Jane Doe"
                    },
                    {
                        "type": "Person",    # another Person node as a target of Fellowship
                        "name": "Michael Douglas"
                    }
                ]
            }
        ]
    }
    */

    auto link_params = LinkParams(
        "Friendship", // type
        {   // targets - a list of NodeParams and LinkParams
            NodeParams("Person", "John Doe", {{"location", "BH"}}),  // type and name
            NodeParams("Person", "Samuel L. Jackson"),
            LinkParams(
                "Fellowship", // type
                {   // targets
                    NodeParams("Person", "Jane Doe"),
                    NodeParams("Person", "Michael Douglas")
                }
            )
        },
        {   // custom attributes - a list of key-value pairs,
            // where the key is a string and the value can be any type
            {"since", "2021-01-01"},
            {"location", "New York"}
        }
    );
    
    /* Another way to do the same as above
     * targets and custom attributes can be added after initialization
    
    auto link_params = LinkParams( // Initialize the link
        "Friendship",  // type
        {{"since", "2021-01-01"}}  // custom attributes
    );
    link_params.custom_attributes.set("location", "New York");  // Add a custom attribute
    
    // Adding 2 Person Nodes as targets
    link_params.add_target(NodeParams("Person", "Jane Doe"));
    link_params.add_target(NodeParams("Person", "Samuel L. Jackson"));
    
    // Adding a Fellowship Link as a target of Friendship
    auto link_as_target = LinkParams("Fellowship");
    link_as_target.add_target(NodeParams("Person", "Jane Doe")); // target of Fellowship
    link_as_target.add_target(NodeParams("Person", "Michael Douglas")); // target of Fellowship
    link_params.add_target(link_as_target);
    */
    
    // Adding the link to the database
    auto link = db.add_link(link_params);
    
    // Printing the link details
    cout << "Link handle: " << link->handle << endl;
    cout << "Link type: " << link->named_type << endl;
    cout << "Link targets: [";
    string targets;
    for (const auto& target : link->targets) {
        targets += target + ", ";
    }
    if (!targets.empty()) {  // Remove trailing comma and space
        targets.pop_back();
        targets.pop_back();
    }
    cout << targets << "]" << endl;
    cout << "Link location: " << link->custom_attributes.get<string>("location").value_or("") << endl;

    cout << "----------------------------------------" << endl;

    auto atom = db.get_atom(
        link->handle,
        Params(
            {
                {ParamsKeys::NO_TARGET_FORMAT, false},
                {ParamsKeys::TARGETS_DOCUMENTS, true},
                {ParamsKeys::DEEP_REPRESENTATION, true}
            }
        )
    );
    if (atom) {
        if (auto link = dynamic_cast<const Link*>(atom.get())) {
            cout << "Link handle: " << link->handle << endl;
            cout << "Link type: " << link->named_type << endl;
            cout << "Link targets: [";
            string targets;
            for (const auto& target : link->targets) {
                targets += target + ", ";
            }
            if (!targets.empty()) {  // Remove trailing comma and space
                targets.pop_back();
                targets.pop_back();
            }
            cout << targets << "]" << endl;
            cout << "Link location: " << link->custom_attributes.get<string>("location").value_or("") << endl;
            auto targets_documents = link->custom_attributes.get<shared_ptr<vector<shared_ptr<const Atom>>>>(
                    ParamsKeys::TARGETS_DOCUMENTS);
            if (targets_documents.has_value()) {
                cout << "Link targets documents: [" << endl;
                for (const auto& target : *(targets_documents.value())) {
                    if (auto node = dynamic_cast<const Node*>(target.get())) {
                        cout << "    Node handle: " << node->handle << endl;
                        cout << "    Node type: " << node->named_type << endl;
                        cout << "    Node name: " << node->name << endl;
                    } else if (auto link = dynamic_cast<const Link*>(target.get())) {
                        cout << "    Link handle: " << link->handle << endl;
                        cout << "    Link type: " << link->named_type << endl;
                        cout << "    Link targets: [";
                        string targets;
                        for (const auto& target : link->targets) {
                            targets += target + ", ";
                        }
                        if (!targets.empty()) {  // Remove trailing comma and space
                            targets.pop_back();
                            targets.pop_back();
                        }
                        cout << targets << "]" << endl;
                    }
                }
                cout << "]" << endl;
            }
        } else if (auto node = dynamic_cast<const Node*>(atom.get())) {
            cout << "Node handle: " << node->handle << endl;
            cout << "Node type: " << node->named_type << endl;
            cout << "Node name: " << node->name << endl;
        }
    }

    return 0;
}
