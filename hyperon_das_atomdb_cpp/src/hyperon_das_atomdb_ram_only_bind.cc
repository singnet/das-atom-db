#include <nanobind/nanobind.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/unordered_set.h>
#include <nanobind/stl/variant.h>
#include <nanobind/stl/vector.h>

#include "adapters/ram_only.h"
#include "constants.h"
#include "database.h"
#include "document_types.h"
#include "exceptions.h"
#include "params.h"
#include "type_aliases.h"

using namespace std;
using namespace atomdb;

namespace nb = nanobind;
using namespace nb::literals;

struct transformer {
    // Tuple types used for `pickle` conversion between C++ and Python
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

    static nb::list composite_type_to_pylist(const ListOfAny& ct_list) {
        nb::list py_list;
        for (const auto& element : ct_list) {
            if (auto str = any_cast<string>(&element)) {
                py_list.append(*str);
            } else if (auto list = any_cast<ListOfAny>(&element)) {
                py_list.append(transformer::composite_type_to_pylist(*list));
            } else {
                throw invalid_argument("Invalid composite type element.");
            }
        }
        return move(py_list);
    }

    static ListOfAny pylist_to_composite_type(const nb::list& py_list) {
        ListOfAny ct_list;
        ct_list.reserve(py_list.size());
        for (const auto& element : py_list) {
            auto e_type = element.type();
            auto type_name = string(nb::type_name(e_type).c_str());
            if (type_name == "str") {
                ct_list.push_back(nb::cast<string>(element));
            } else if (type_name == "list") {
                ct_list.push_back(transformer::pylist_to_composite_type(nb::cast<nb::list>(element)));
            } else {
                throw invalid_argument("Invalid composite type element.");
            }
        }
        return move(ct_list);
    }
};

NB_MODULE(ext, m) {
    // root module ---------------------------------------------------------------------------------
    m.attr("WILDCARD") = WILDCARD;
    m.attr("WILDCARD_HASH") = WILDCARD_HASH;
    m.attr("TYPE_HASH") = TYPE_HASH;
    m.attr("TYPEDEF_MARK_HASH") = TYPEDEF_MARK_HASH;
    nb::enum_<FieldIndexType>(m, "FieldIndexType", nb::is_arithmetic())
        .value("BINARY_TREE", FieldIndexType::BINARY_TREE)
        .value("TOKEN_INVERTED_LIST", FieldIndexType::TOKEN_INVERTED_LIST)
        .export_values();
    nb::class_<AtomDB>(m, "AtomDB")
        .def_static("build_node_handle", &AtomDB::build_node_handle)
        .def_static("node_handle", &AtomDB::build_node_handle)  // retrocompatibility
        .def_static("build_link_handle",
                    [](const string& link_type, const StringList& target_handles) {
                        return AtomDB::build_link_handle(link_type, target_handles);
                    })
        .def_static("link_handle",  // retrocompatibility
                    [](const string& link_type, const StringList& target_handles) {
                        return AtomDB::build_link_handle(link_type, target_handles);
                    })
        .def_static("build_link_handle",
                    [](const string& link_type, const string& target_handle) {
                        return AtomDB::build_link_handle(link_type, target_handle);
                    })
        .def_static("link_handle",  // retrocompatibility
                    [](const string& link_type, const string& target_handle) {
                        return AtomDB::build_link_handle(link_type, target_handle);
                    })
        .def("node_exists", &AtomDB::node_exists)
        .def("link_exists", &AtomDB::link_exists)
        .def(
            "get_atom",
            [](AtomDB& self,
               const string& handle,
               bool no_target_format = false,
               bool targets_documents = false,
               bool deep_representation = false,
               const nb::kwargs& _ = {}) -> shared_ptr<const Atom> {
                return self.get_atom(handle, {
                    no_target_format : no_target_format,
                    targets_documents : targets_documents,
                    deep_representation : deep_representation
                });
            },
            "handle"_a,
            nb::kw_only(),
            "no_target_format"_a = false,
            "targets_documents"_a = false,
            "deep_representation"_a = false,
            "_"_a = nb::kwargs())
        .def("get_node_handle", &AtomDB::get_node_handle, "node_type"_a, "node_name"_a)
        .def("get_node_name", &AtomDB::get_node_name)
        .def("get_node_type", &AtomDB::get_node_type)
        .def("get_node_by_name", &AtomDB::get_node_by_name)
        .def("get_atoms_by_field", &AtomDB::get_atoms_by_field)
        .def("get_atoms_by_index",
             &AtomDB::get_atoms_by_index,
             "index_id"_a,
             "query"_a,
             "cursor"_a = 0,
             "chunk_size"_a = 500)
        .def("get_atoms_by_text_field",
             &AtomDB::get_atoms_by_text_field,
             "text_value"_a,
             "field"_a = "",
             "text_index_id"_a = "")
        .def("get_node_by_name_starting_with", &AtomDB::get_node_by_name_starting_with)
        .def("get_all_nodes", &AtomDB::get_all_nodes, "node_type"_a, "names"_a = false)
        .def(
            "get_all_links",
            [](AtomDB& self, const string& link_type, const nb::kwargs& _ = {})
                -> const StringUnorderedSet { return self.get_all_links(link_type); },
            "link_type"_a,
            "_"_a = nb::kwargs())
        .def("get_link_handle", &AtomDB::get_link_handle)
        .def("get_link_type", &AtomDB::get_link_type)
        .def("get_link_targets", &AtomDB::get_link_targets)
        .def(
            "get_incoming_links_handles",
            [](AtomDB& self,
               const string& atom_handle,
               bool handles_only = true,
               const nb::kwargs& _ = {}) -> const StringList {
                return self.get_incoming_links_handles(atom_handle, {handles_only : handles_only});
            },
            "atom_handle"_a,
            nb::kw_only(),
            "handles_only"_a = true,
            "_"_a = nb::kwargs())
        .def(
            "get_incoming_links_atoms",
            [](AtomDB& self,
               const string& atom_handle,
               bool no_target_format = false,
               bool targets_documents = false,
               bool deep_representation = false,
               bool handles_only = false,
               const nb::kwargs& _ = {}) -> const vector<shared_ptr<const Atom>> {
                return self.get_incoming_links_atoms(atom_handle, {
                    no_target_format : no_target_format,
                    targets_documents : targets_documents,
                    deep_representation : deep_representation,
                    handles_only : handles_only
                });
            },
            "atom_handle"_a,
            nb::kw_only(),
            "no_target_format"_a = false,
            "targets_documents"_a = false,
            "deep_representation"_a = false,
            "handles_only"_a = false,
            "_"_a = nb::kwargs())
        .def(
            "get_matched_links",
            [](AtomDB& self,
               const string& link_type,
               const StringList& target_handles,
               bool toplevel_only = false,
               const nb::kwargs& _ = {}) -> const StringUnorderedSet {
                return self.get_matched_links(
                    link_type, target_handles, {toplevel_only : toplevel_only});
            },
            "link_type"_a,
            "target_handles"_a,
            nb::kw_only(),
            "toplevel_only"_a = false,
            "_"_a = nb::kwargs())
        .def(
            "get_matched_type_template",
            [](AtomDB& self,
               const nb::list& _template,
               bool toplevel_only = false,
               const nb::kwargs& _ = {}) -> const StringUnorderedSet {
                return self.get_matched_type_template(transformer::pylist_to_composite_type(_template),
                                                      {toplevel_only : toplevel_only});
            },
            "_template"_a,
            nb::kw_only(),
            "toplevel_only"_a = false,
            "_"_a = nb::kwargs())
        .def(
            "get_matched_type",
            [](AtomDB& self,
               const string& link_type,
               bool toplevel_only = false,
               const nb::kwargs& _ = {}) -> const StringUnorderedSet {
                return self.get_matched_type(link_type, {toplevel_only : toplevel_only});
            },
            "link_type"_a,
            nb::kw_only(),
            "toplevel_only"_a = false,
            "_"_a = nb::kwargs())
        .def("get_atom_type", &AtomDB::get_atom_type)
        .def(
            "count_atoms",
            [](const AtomDB& self, const opt<const nb::dict>& _) -> const unordered_map<string, int> {
                return self.count_atoms();
            },
            "_"_a = nullopt)
        .def("clear_database", &AtomDB::clear_database)
        .def("add_node", &AtomDB::add_node)
        .def("add_link", &AtomDB::add_link, "link_params"_a, "toplevel"_a = true)
        .def("reindex", &AtomDB::reindex)
        .def("delete_atom", &AtomDB::delete_atom)
        .def("create_field_index",
             &AtomDB::create_field_index,
             "atom_type"_a,
             "fields"_a,
             "named_type"_a = "",
             "composite_type"_a = nullopt,
             "index_type"_a = FieldIndexType::BINARY_TREE)
        .def("bulk_insert", &AtomDB::bulk_insert)
        .def("retrieve_all_atoms", &AtomDB::retrieve_all_atoms)
        .def("commit", &AtomDB::commit);
    // ---------------------------------------------------------------------------------------------
    // adapters submodule --------------------------------------------------------------------------
    nb::module_ adapters = m.def_submodule("adapters");
    nb::class_<InMemoryDB, AtomDB>(adapters, "InMemoryDB")
        .def(nb::init<const string&>(), "database_name"_a = "das");
    // ---------------------------------------------------------------------------------------------
    // exceptions submodule ------------------------------------------------------------------------
    nb::module_ exceptions = m.def_submodule("exceptions");
    nb::exception<AtomDoesNotExist>(exceptions, "AtomDoesNotExist");
    nb::exception<InvalidAtomDB>(exceptions, "InvalidAtomDB");
    nb::exception<InvalidOperationException>(exceptions, "InvalidOperationException");
    // ---------------------------------------------------------------------------------------------
    // document_types submodule --------------------------------------------------------------------
    nb::module_ document_types = m.def_submodule("document_types");
    nb::class_<Atom>(document_types, "Atom")
        .def_ro("_id", &Atom::_id)
        .def_ro("handle", &Atom::handle)
        .def_ro("composite_type_hash", &Atom::composite_type_hash)
        .def_ro("named_type", &Atom::named_type)
        .def("to_string", &Atom::to_string)
        .def("__str__", &Atom::to_string)
        .def("__repr__", &Atom::to_string);
    nb::class_<AtomType, Atom>(document_types, "AtomType")
        .def(nb::init<const string&, const string&, const string&, const string&, const string&>(),
             "_id"_a,
             "handle"_a,
             "composite_type_hash"_a,
             "named_type"_a,
             "named_type_hash"_a)
        .def_ro("named_type_hash", &AtomType::named_type_hash)
        .def("__getstate__",
             [](const AtomType& atom_type) -> transformer::AtomTypeTuple {
                 return std::make_tuple(atom_type._id,
                                        atom_type.handle,
                                        atom_type.composite_type_hash,
                                        atom_type.named_type,
                                        atom_type.named_type_hash);
             })
        .def("__setstate__", [](AtomType& atom_type, const transformer::AtomTypeTuple& state) {
            new (&atom_type) AtomType(std::get<0>(state),  // id
                                      std::get<1>(state),  // handle
                                      std::get<2>(state),  // composite_type_hash
                                      std::get<3>(state),  // named_type
                                      std::get<4>(state)   // named_type_hash
            );
        });
    nb::class_<Node, Atom>(document_types, "Node")
        .def(nb::init<const string&, const string&, const string&, const string&, const string&>(),
             "_id"_a,
             "handle"_a,
             "composite_type_hash"_a,
             "named_type"_a,
             "name"_a)
        .def_ro("name", &Node::name)
        .def("__getstate__",
             [](const Node& node) -> transformer::NodeTuple {
                 return std::make_tuple(
                     node._id, node.handle, node.composite_type_hash, node.named_type, node.name);
             })
        .def("__setstate__", [](Node& node, const transformer::NodeTuple& state) {
            new (&node) Node(std::get<0>(state),  // id
                             std::get<1>(state),  // handle
                             std::get<2>(state),  // composite_type_hash
                             std::get<3>(state),  // named_type
                             std::get<4>(state)   // name
            );
        });
    nb::class_<Link, Atom>(document_types, "Link")
        .def(
            "__init__",
            [](Link& self,
               const string& _id,
               const string& handle,
               const string& composite_type_hash,
               const string& named_type,
               const nb::list& composite_type,
               const string& named_type_hash,
               const vector<string>& targets,
               bool is_top_level) {
                new (&self) Link(_id,
                                 handle,
                                 composite_type_hash,
                                 named_type,
                                 transformer::pylist_to_composite_type(composite_type),
                                 named_type_hash,
                                 targets,
                                 is_top_level);
            },
            "_id"_a,
            "handle"_a,
            "composite_type_hash"_a,
            "named_type"_a,
            "composite_type"_a,
            "named_type_hash"_a,
            "targets"_a,
            "is_top_level"_a)
        .def_prop_ro("composite_type",
                     [](const Link& self) -> const nb::list {
                         return transformer::composite_type_to_pylist(self.composite_type);
                     })
        .def_ro("named_type_hash", &Link::named_type_hash)
        .def_ro("targets", &Link::targets)
        .def_ro("is_top_level", &Link::is_top_level)
        .def_ro("keys", &Link::keys)
        .def_ro("targets_documents", &Link::targets_documents)
        .def("__getstate__",
             [](const Link& link) -> transformer::LinkTuple {
                 return std::make_tuple(link._id,
                                        link.handle,
                                        link.composite_type_hash,
                                        link.named_type,
                                        transformer::composite_type_to_pylist(link.composite_type),
                                        link.named_type_hash,
                                        link.targets,
                                        link.is_top_level,
                                        link.keys,
                                        link.targets_documents);
             })
        .def("__setstate__", [](Link& link, const transformer::LinkTuple& state) {
            new (&link) Link(std::get<0>(state),  // id
                             std::get<1>(state),  // handle
                             std::get<2>(state),  // composite_type_hash
                             std::get<3>(state),  // named_type
                             transformer::pylist_to_composite_type(std::get<4>(state)),
                             std::get<5>(state),  // named_type_hash
                             std::get<6>(state),  // targets
                             std::get<7>(state),  // is_top_level
                             std::get<8>(state),  // keys
                             std::get<9>(state)   // targets_documents
            );
        });
    // ---------------------------------------------------------------------------------------------
    // database submodule --------------------------------------------------------------------------
    nb::module_ database = m.def_submodule("database");
    nb::class_<NodeParams>(database, "NodeParams")
        .def(nb::init<const string&, const string&>(), "type"_a, "name"_a)
        .def_rw("type", &NodeParams::type)
        .def_rw("name", &NodeParams::name);
    nb::class_<LinkParams>(database, "LinkParams")
        .def(nb::init<const string&, const LinkParams::Targets&>(), "type"_a, "targets"_a)
        .def_rw("type", &LinkParams::type)
        .def_rw("targets", &LinkParams::targets)
        .def("add_target",
             [](LinkParams& self, const LinkParams::Target& target) { self.targets.push_back(target); });
    // ---------------------------------------------------------------------------------------------
}
