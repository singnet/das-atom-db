#pragma once

#include <nanobind/nanobind.h>

#include "document_types.h"
#include "type_aliases.h"

using namespace std;
using namespace atomdb;

namespace nb = nanobind;

namespace bind_helpers {

// Tuples and functions used for `pickle` conversion between C++ and Python
using AtomTypeTuple = std::tuple<string,  // id
                                 string,  // handle
                                 string,  // composite_type_hash
                                 string,  // named_type
                                 string   // named_type_hash
                                 >;

using NodeTuple = std::tuple<string,  // id
                             string,  // handle
                             string,  // composite_type_hash
                             string,  // named_type
                             string   // name
                             >;

using LinkTuple = std::tuple<string,                      // id
                             string,                      // handle
                             string,                      // composite_type_hash
                             string,                      // named_type
                             nb::list,                    // composite_type
                             string,                      // named_type_hash
                             vector<string>,              // targets
                             bool,                        // is_top_level
                             map<string, string>,         // keys
                             opt<Link::TargetsDocuments>  // targets_documents
                             >;

/**
 * @brief Converts a composite type list to a Python list.
 * @param ct_list The composite type list to be converted.
 * @return A Python list (`nb::list`) containing the elements of the composite type list.
 */
static nb::list composite_type_to_pylist(const ListOfAny& ct_list) {
    nb::list py_list;
    for (const auto& element : ct_list) {
        if (auto str = any_cast<string>(&element)) {
            py_list.append(*str);
        } else if (auto list = any_cast<ListOfAny>(&element)) {
            py_list.append(composite_type_to_pylist(*list));
        } else {
            throw invalid_argument("Invalid composite type element.");
        }
    }
    return move(py_list);
}

/**
 * @brief Converts a Python list to a composite type list.
 * @param py_list The Python list to be converted.
 * @return A composite type list (`ListOfAny`) containing the elements of the Python list.
 */
static ListOfAny pylist_to_composite_type(const nb::list& py_list) {
    ListOfAny ct_list;
    ct_list.reserve(py_list.size());
    for (const auto& element : py_list) {
        auto e_type = element.type();
        auto type_name = string(nb::type_name(e_type).c_str());
        if (type_name == "str") {
            ct_list.push_back(nb::cast<string>(element));
        } else if (type_name == "list") {
            ct_list.push_back(pylist_to_composite_type(nb::cast<nb::list>(element)));
        } else {
            throw invalid_argument("Invalid composite type element.");
        }
    }
    return move(ct_list);
}

/**
 * @brief Converts an Atom object to a Python dictionary.
 * @param self The Atom object to be converted.
 * @return A Python dictionary (`nb::dict`) containing the attributes of the Atom.
 */
static nb::dict atom_to_dict(const Atom& self) {
    nb::dict dict;
    dict["_id"] = self._id;
    dict["handle"] = self.handle;
    dict["composite_type_hash"] = self.composite_type_hash;
    dict["named_type"] = self.named_type;
    return move(dict);
};

/**
 * @brief Converts an AtomType object to a Python dictionary.
 * @param self The AtomType object to be converted.
 * @return A Python dictionary (`nb::dict`) containing the attributes of the AtomType.
 */
static nb::dict atom_type_to_dict(const AtomType& self) {
    nb::dict dict = atom_to_dict(self);
    dict["named_type_hash"] = self.named_type_hash;
    return move(dict);
};

static nb::dict node_to_dict(const Node& self) {
    nb::dict dict = atom_to_dict(self);
    dict["name"] = self.name;
    return move(dict);
};

static nb::dict link_to_dict(const Link& self) {
    nb::dict dict = atom_to_dict(self);
    dict["composite_type"] = composite_type_to_pylist(self.composite_type);
    dict["named_type_hash"] = self.named_type_hash;
    dict["targets"] = self.targets;
    dict["is_top_level"] = self.is_top_level;
    dict["keys"] = self.keys;
    if (self.targets_documents.has_value()) {
        nb::list targets_documents;
        for (const auto& target : *self.targets_documents) {
            if (const auto& node = dynamic_pointer_cast<const Node>(target)) {
                targets_documents.append(node_to_dict(*node));
            } else if (const auto& link = dynamic_pointer_cast<const Link>(target)) {
                targets_documents.append(link_to_dict(*link));
            }
        }
        dict["targets_documents"] = move(targets_documents);
    } else {
        dict["targets_documents"] = nullptr;
    }
    return move(dict);
};

static AtomTypeTuple atom_type_to_tuple(const AtomType& atom_type) {
    return make_tuple(atom_type._id,
                      atom_type.handle,
                      atom_type.composite_type_hash,
                      atom_type.named_type,
                      atom_type.named_type_hash);
}

static void tuple_to_atom_type(AtomType& atom_type, const AtomTypeTuple& state) {
    new (&atom_type) AtomType(std::get<0>(state),  // id
                              std::get<1>(state),  // handle
                              std::get<2>(state),  // composite_type_hash
                              std::get<3>(state),  // named_type
                              std::get<4>(state)   // named_type_hash
    );
}

static NodeTuple node_to_tuple(const Node& node) {
    return std::make_tuple(node._id, node.handle, node.composite_type_hash, node.named_type, node.name);
}

static void tuple_to_node(Node& node, const NodeTuple& state) {
    new (&node) Node(std::get<0>(state),  // id
                     std::get<1>(state),  // handle
                     std::get<2>(state),  // composite_type_hash
                     std::get<3>(state),  // named_type
                     std::get<4>(state)   // name
    );
}

static LinkTuple link_to_tuple(const Link& link) {
    return std::make_tuple(link._id,
                           link.handle,
                           link.composite_type_hash,
                           link.named_type,
                           composite_type_to_pylist(link.composite_type),
                           link.named_type_hash,
                           link.targets,
                           link.is_top_level,
                           link.keys,
                           link.targets_documents);
}

static void tuple_to_link(Link& link, const LinkTuple& state) {
    new (&link) Link(std::get<0>(state),  // id
                     std::get<1>(state),  // handle
                     std::get<2>(state),  // composite_type_hash
                     std::get<3>(state),  // named_type
                     pylist_to_composite_type(std::get<4>(state)),
                     std::get<5>(state),  // named_type_hash
                     std::get<6>(state),  // targets
                     std::get<7>(state),  // is_top_level
                     std::get<8>(state),  // keys
                     std::get<9>(state)   // targets_documents
    );
}

static void init_link(Link& self,
                      const string& _id,
                      const string& handle,
                      const string& composite_type_hash,
                      const string& named_type,
                      const nb::list& composite_type,
                      const string& named_type_hash,
                      const vector<string>& targets,
                      bool is_top_level,
                      opt<map<string, string>> keys = nullopt,
                      opt<Link::TargetsDocuments> targets_documents = nullopt) {
    if (keys.has_value() or targets_documents.has_value()) {
        new (&self) Link(_id,
                         handle,
                         composite_type_hash,
                         named_type,
                         pylist_to_composite_type(composite_type),
                         named_type_hash,
                         targets,
                         is_top_level,
                         keys.has_value() ? *keys : map<string, string>(),
                         targets_documents);
    } else {
        new (&self) Link(_id,
                         handle,
                         composite_type_hash,
                         named_type,
                         pylist_to_composite_type(composite_type),
                         named_type_hash,
                         targets,
                         is_top_level);
    }
}

}  // namespace bind_helpers
