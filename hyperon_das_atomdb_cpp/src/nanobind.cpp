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

#include "adapters/ram_only.hpp"
#include "adapters/redis_mongo_db.hpp"
#include "constants.hpp"
#include "database.hpp"
#include "document_types.hpp"
#include "exceptions.hpp"
#include "params.hpp"
#include "type_aliases.hpp"

using namespace std;
using namespace atomdb;

namespace nb = nanobind;
using namespace nb::literals;

struct transformer {
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

NB_MODULE(hyperon_das_atomdb, m) {
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
        .def_static("build_link_handle", &AtomDB::build_link_handle)
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
        .def("get_node_handle", &AtomDB::get_node_handle)
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
        .def("get_all_links",
             [](InMemoryDB& self, const string& link_type, const nb::kwargs& _ = {})
                 -> const StringUnorderedSet { return self.get_all_links(link_type); })
        .def("get_link_handle", &AtomDB::get_link_handle)
        .def("get_link_type", &AtomDB::get_link_type)
        .def("get_link_targets", &AtomDB::get_link_targets)
        .def(
            "get_incoming_links_handles",
            [](InMemoryDB& self,
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
            [](InMemoryDB& self,
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
            [](InMemoryDB& self,
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
            [](InMemoryDB& self,
               const ListOfAny& _template,
               bool toplevel_only = false,
               const nb::kwargs& _ = {}) -> const StringUnorderedSet {
                return self.get_matched_type_template(_template, {toplevel_only : toplevel_only});
            },
            "_template"_a,
            nb::kw_only(),
            "toplevel_only"_a = false,
            "_"_a = nb::kwargs())
        .def(
            "get_matched_type",
            [](InMemoryDB& self,
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
        .def("count_atoms", &AtomDB::count_atoms)
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
    nb::class_<InMemoryDB, AtomDB>(adapters, "InMemoryDB").def(nb::init<>());
    nb::class_<RedisMongoDB, InMemoryDB>(adapters, "RedisMongoDB")
        .def("__init__", [](RedisMongoDB* t, const nb::kwargs& _) { new (t) RedisMongoDB(); });
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
        .def_ro("id", &Atom::id)
        .def_ro("handle", &Atom::handle)
        .def_ro("composite_type_hash", &Atom::composite_type_hash)
        .def_ro("named_type", &Atom::named_type)
        .def("to_string", &Atom::to_string)
        .def("__str__", &Atom::to_string)
        .def("__repr__", &Atom::to_string);
    nb::class_<AtomType, Atom>(document_types, "AtomType")
        .def_ro("named_type_hash", &AtomType::named_type_hash)
        .def("__getstate__",
             [](const AtomType& atom_type) {
                 return std::make_tuple(atom_type.id,
                                        atom_type.handle,
                                        atom_type.composite_type_hash,
                                        atom_type.named_type,
                                        atom_type.named_type_hash);
             })
        .def("__setstate__",
             [](AtomType& atom_type,
                const std::tuple<string,  // id
                                 string,  // handle
                                 string,  // composite_type_hash
                                 string,  // named_type
                                 string   // named_type_hash
                                 >& state) {
                 new (&atom_type) AtomType(std::get<0>(state),  // id
                                           std::get<1>(state),  // handle
                                           std::get<2>(state),  // composite_type_hash
                                           std::get<3>(state),  // named_type
                                           std::get<4>(state)   // named_type_hash
                 );
             });
    nb::class_<Node, Atom>(document_types, "Node")
        .def_ro("name", &Node::name)
        .def("__getstate__",
             [](const Node& node) {
                 return std::make_tuple(
                     node.id, node.handle, node.composite_type_hash, node.named_type, node.name);
             })
        .def("__setstate__",
             [](Node& node,
                const std::tuple<string,  // id
                                 string,  // handle
                                 string,  // composite_type_hash
                                 string,  // named_type
                                 string   // name
                                 >& state) {
                 new (&node) Node(std::get<0>(state),  // id
                                  std::get<1>(state),  // handle
                                  std::get<2>(state),  // composite_type_hash
                                  std::get<3>(state),  // named_type
                                  std::get<4>(state)   // name
                 );
             });
    nb::class_<Link, Atom>(document_types, "Link")
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
             [](const Link& link) {
                 return std::make_tuple(link.id,
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
        .def("__setstate__",
             [](Link& link,
                const std::tuple<string,                      // id
                                 string,                      // handle
                                 string,                      // composite_type_hash
                                 string,                      // named_type
                                 nb::list,                    // composite_type
                                 string,                      // named_type_hash
                                 vector<string>,              // targets
                                 bool,                        // is_top_level
                                 map<string, string>,         // keys
                                 opt<Link::TargetsDocuments>  // targets_documents
                                 >& state) {
                 auto composite_type = transformer::pylist_to_composite_type(std::get<4>(state));
                 new (&link) Link(std::get<0>(state),  // id
                                  std::get<1>(state),  // handle
                                  std::get<2>(state),  // composite_type_hash
                                  std::get<3>(state),  // named_type
                                  composite_type,
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
        .def(nb::init<const string&, const string&>())
        .def_rw("type", &NodeParams::type)
        .def_rw("name", &NodeParams::name);
    nb::class_<LinkParams>(database, "LinkParams")
        .def(nb::init<const string&, const LinkParams::Targets&>())
        .def_rw("type", &LinkParams::type)
        .def_rw("targets", &LinkParams::targets)
        .def("add_target",
             [](LinkParams& self, const LinkParams::Target& target) { self.targets.push_back(target); });
    // ---------------------------------------------------------------------------------------------
}
