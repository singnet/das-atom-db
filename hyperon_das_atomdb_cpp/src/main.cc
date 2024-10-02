#include <iostream>

#include "adapters/ram_only.h"
#include "database.h"
#include "params.h"

using namespace std;
using namespace atomdb;

int main(int argc, char const* argv[]) {
    InMemoryDB db;

    // Adding a Node
    auto node_params = NodeParams{type : "Person", name : "John Doe"};

    auto node = db.add_node({type : "Person", name : "John Doe"});

    cout << "Node handle: " << node->handle << endl;
    cout << "Node name: " << node->name << endl;

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

    // auto link_params = LinkParams(
    //     "Friendship", // type
    //     {   // targets - a list of NodeParams and LinkParams
    //         NodeParams("Person", "John Doe"), //{{"location", "BH"}}),  // type and name
    //         NodeParams("Person", "Samuel L. Jackson"),
    //         LinkParams(
    //             "Fellowship", // type
    //             {   // targets
    //                 NodeParams("Person", "Jane Doe"),
    //                 NodeParams("Person", "Michael Douglas")
    //             }
    //         )
    //     }
    // );

    LinkParams link_params = {
        type : "Friendship",
        targets :
            {NodeParams{type : "Person", name : "Jane Doe"},           // a Person node as a target
             NodeParams{type : "Person", name : "Samuel L. Jackson"},  // another Person node as a target
             LinkParams{
                 type : "Fellowship",  // a Fellowship link as a target of Friendship
                 targets : {NodeParams{
                                type : "Person",  // a Person node as a target of Fellowship
                                name : "Jane Doe"
                            },
                            NodeParams{
                                type : "Person",  // another Person node as a target of Fellowship
                                name : "Michael Douglas"
                            }}
             }}
    };

    /* Another way to do the same as above
     * targets and custom attributes can be added after initialization

    auto link_params = LinkParams( // Initialize the link
        "Friendship",  // type
        {{"since", "2021-01-01"}}  // custom attributes
    );

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
    if (not targets.empty()) {  // Remove trailing comma and space
        targets.pop_back();
        targets.pop_back();
    }
    cout << targets << "]" << endl;

    cout << "----------------------------------------" << endl;

    auto atom = db.get_atom(link->handle,
                            // Flags{Flags::NO_TARGET_FORMAT}
                            {.no_target_format = true});

    cout << "Atom pointer id: " << atom.get() << endl;
    cout << atom.use_count() << endl;
    cout << "Atom size: " << sizeof(atom) << endl;

    {
        auto atom = db.get_atom(link->handle,
                                // Flags{Flags::NO_TARGET_FORMAT}
                                {.no_target_format = true});

        // prints the atoms pointer id
        cout << "Atom pointer id: " << atom.get() << endl;
        cout << atom.use_count() << endl;
        cout << "Atom size: " << sizeof(atom) << endl;
    }

    cout << atom.use_count() << endl;

    atom = db.get_atom(link->handle, {targets_documents : true, deep_representation : true});

    cout << "Atom pointer id: " << atom.get() << endl;
    cout << atom.use_count() << endl;
    cout << "Atom size: " << sizeof(atom) << endl;

    if (atom) {
        if (const auto& link = dynamic_pointer_cast<const Link>(atom)) {
            cout << "Link handle: " << link->handle << endl;
            cout << "Link type: " << link->named_type << endl;
            cout << "Link targets: [";
            string targets;
            for (const auto& target : link->targets) {
                targets += target + ", ";
            }
            if (not targets.empty()) {  // Remove trailing comma and space
                targets.pop_back();
                targets.pop_back();
            }
            cout << targets << "]" << endl;
            if (link->targets_documents.has_value()) {
                cout << "Link targets documents: [" << endl;
                for (const auto& target : *link->targets_documents) {
                    if (const auto& node = dynamic_pointer_cast<const Node>(target)) {
                        cout << "    Node handle: " << node->handle << endl;
                        cout << "    Node type: " << node->named_type << endl;
                        cout << "    Node name: " << node->name << endl;
                    } else if (const auto& link = dynamic_pointer_cast<const Link>(target)) {
                        cout << "    Link handle: " << link->handle << endl;
                        cout << "    Link type: " << link->named_type << endl;
                        cout << "    Link targets: [";
                        string targets;
                        for (const auto& target : link->targets) {
                            targets += target + ", ";
                        }
                        if (not targets.empty()) {  // Remove trailing comma and space
                            targets.pop_back();
                            targets.pop_back();
                        }
                        cout << targets << "]" << endl;
                    }
                }
                cout << "]" << endl;
            }
        } else if (const auto& node = dynamic_pointer_cast<const Node>(atom)) {
            cout << "Node handle: " << node->handle << endl;
            cout << "Node type: " << node->named_type << endl;
            cout << "Node name: " << node->name << endl;
        }
    }

    cout << db.node_exists("Person", "John Doe") << endl;
    db.delete_atom(node->handle);
    cout << db.node_exists("Person", "John Doe") << endl;

    return 0;
}