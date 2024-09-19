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
        return py_list;
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
        .def_static(
            "build_node_handle",
            [](
                const string& node_type, const string& node_name
            ) -> const string {
                return AtomDB::build_node_handle(node_type, node_name);
            }
        )
        .def_static(
            "build_link_handle",
            [](
                const string& link_type, const StringList& target_handles
            ) -> const string {
                return AtomDB::build_link_handle(link_type, target_handles);
            }
        )
        .def(
            "node_exists",
            [](
                const AtomDB& self, const string& node_type, const string& node_name
            ) -> bool {
                return self.node_exists(node_type, node_name);
            }
        )
        .def(
            "link_exists",
            [](
                const AtomDB& self, const string& link_type, const StringList& target_handles
            ) -> bool {
                return self.link_exists(link_type, target_handles);
            }
        )
        .def(
            "get_atom",
            [](
                AtomDB& self,
                const string& handle,
                bool no_target_format = false,
                bool targets_documents = false,
                bool deep_representation = false,
                const nb::kwargs _ = {}
            ) -> shared_ptr<const Atom> {
                return self.get_atom(
                    handle,
                    {
                        no_target_format: no_target_format,
                        targets_documents: targets_documents,
                        deep_representation: deep_representation
                    }
                );
            },
            "handle"_a,
            nb::kw_only(),
            "no_target_format"_a = false,
            "targets_documents"_a = false,
            "deep_representation"_a = false,
            "_"_a = nb::kwargs()
        )
        .def(
            "get_node_handle",
            [](
                InMemoryDB& self,
                const string& node_type,
                const string& node_name
            ) -> const string {
                return self.get_node_handle(node_type, node_name);
            }
        )
        .def(
            "get_node_name",
            [](
                InMemoryDB& self,
                const string& node_handle
            ) -> const string {
                return self.get_node_name(node_handle);
            }
        )
        .def(
            "get_node_type",
            [](
                InMemoryDB& self,
                const string& node_handle
            ) -> const string {
                return self.get_node_type(node_handle);
            }
        )
        .def(
            "get_node_by_name",
            [](
                InMemoryDB& self,
                const string& node_type,
                const string& substring
            ) -> const StringList {
                return self.get_node_by_name(node_type, substring);
            }
        )
        .def(
            "get_atoms_by_field",
            [](
                InMemoryDB& self,
                const vector<unordered_map<string, string>>& query
            ) -> const StringList {
                return self.get_atoms_by_field(query);
            }
        )
        .def(
            "get_atoms_by_index",
            [](
                InMemoryDB& self,
                const string& index_id,
                const vector<unordered_map<string, string>>& query,
                int cursor = 0,
                int chunk_size = 500
            ) -> const pair<const OptCursor, const AtomList> {
                return self.get_atoms_by_index(index_id, query, cursor, chunk_size);
            },
            "index_id"_a,
            "query"_a,
            "cursor"_a = 0,
            "chunk_size"_a = 500
        )
        .def(
            "get_atoms_by_text_field",
            [](
                InMemoryDB& self,
                const string& text_value,
                const string& field = "",
                const string& text_index_id = ""
            ) -> const StringList {
                return self.get_atoms_by_text_field(text_value, field, text_index_id);
            },
            "text_value"_a,
            "field"_a = "",
            "text_index_id"_a = ""
        )
        .def(
            "get_node_by_name_starting_with",
            [](
                InMemoryDB& self,
                const string& node_type,
                const string& startswith
            ) -> const StringList {
                return self.get_node_by_name_starting_with(node_type, startswith);
            }
        )
        .def(
            "get_all_nodes",
            [](
                InMemoryDB& self,
                const string& node_type,
                bool names = false
            ) -> const StringList {
                return self.get_all_nodes(node_type, names);
            },
            "node_type"_a,
            "names"_a = false
        )
        .def(
            "get_all_links",
            [](
                InMemoryDB& self,
                const string& link_type,
                const OptCursor cursor = nullopt,
                const nb::kwargs _ = {}
            ) -> const pair<const OptCursor, const StringList> {
                return self.get_all_links(link_type, { cursor: cursor });
            },
            "link_type"_a,
            nb::kw_only(),
            "cursor"_a = nullopt,
            "_"_a = nb::kwargs()
        )
        .def(
            "get_link_handle",
            [](
                InMemoryDB& self,
                const string& link_type,
                const StringList& target_handles
            ) -> const string {
                return self.get_link_handle(link_type, target_handles);
            }
        )
        .def(
            "get_link_type",
            [](
                InMemoryDB& self,
                const string& link_handle
            ) -> const string {
                return self.get_link_type(link_handle);
            }
        )
        .def(
            "get_link_targets",
            [](
                InMemoryDB& self,
                const string& link_handle
            ) -> const StringList {
                return self.get_link_targets(link_handle);
            }
        )
        .def(
            "is_ordered",
            [](
                InMemoryDB& self,
                const string& link_handle
            ) -> bool {
                return self.is_ordered(link_handle);
            }
        )
        .def(
            "get_incoming_links_handles",
            [](
                InMemoryDB& self,
                const string& atom_handle,
                const OptCursor cursor = nullopt,
                const nb::kwargs _ = {}
            ) -> const pair<const OptCursor, const StringUnorderedSet> {
                return self.get_incoming_links_handles(atom_handle, { cursor: cursor });
            },
            "atom_handle"_a,
            nb::kw_only(),
            "cursor"_a = nullopt,
            "_"_a = nb::kwargs()
        )
        .def(
            "get_incoming_links_atoms",
            [](
                InMemoryDB& self,
                const string& atom_handle,
                const OptCursor cursor = nullopt,
                bool no_target_format = false,
                bool targets_documents = false,
                bool deep_representation = false,
                const nb::kwargs _ = {}
            ) -> const pair<const OptCursor, const vector<shared_ptr<const Atom>>> {
                return self.get_incoming_links_atoms(
                    atom_handle,
                    {
                        no_target_format: no_target_format,
                        targets_documents: targets_documents,
                        deep_representation: deep_representation,
                        cursor: cursor
                    }
                );
            },
            "atom_handle"_a,
            nb::kw_only(),
            "cursor"_a = nullopt,
            "no_target_format"_a = false,
            "targets_documents"_a = false,
            "deep_representation"_a = false,
            "_"_a = nb::kwargs()
        )
        .def(
            "get_matched_links",
            [](
                InMemoryDB& self,
                const string& link_type,
                const StringList& target_handles,
                const OptCursor cursor = nullopt,
                bool toplevel_only = false,
                const nb::kwargs _ = {}
            ) -> const pair<const OptCursor, const Pattern_or_Template_List> {
                return self.get_matched_links(
                    link_type,
                    target_handles,
                    { toplevel_only: toplevel_only, cursor: cursor }
                );            
            },
            "link_type"_a,
            "target_handles"_a,
            nb::kw_only(),
            "cursor"_a = nullopt,
            "toplevel_only"_a = false,
            "_"_a = nb::kwargs()
        )
        .def(
            "get_matched_type_template",
            [](
                InMemoryDB& self,
                const ListOfAny& _template,
                const OptCursor cursor = nullopt,
                bool toplevel_only = false,
                const nb::kwargs _ = {}
            ) -> const pair<const OptCursor, const Pattern_or_Template_List> {
                return self.get_matched_type_template(
                    _template,
                    { toplevel_only: toplevel_only, cursor: cursor }
                );
            },
            "_template"_a,
            nb::kw_only(),
            "cursor"_a = nullopt,
            "toplevel_only"_a = false,
            "_"_a = nb::kwargs()
        )
        .def(
            "get_matched_type",
            [](
                InMemoryDB& self,
                const string& link_type,
                const OptCursor cursor = nullopt,
                bool toplevel_only = false,
                const nb::kwargs _ = {}
            ) -> const pair<const OptCursor, const Pattern_or_Template_List> {
                return self.get_matched_type(
                    link_type,
                    { toplevel_only: toplevel_only, cursor: cursor }
                );
            },
            "link_type"_a,
            nb::kw_only(),
            "cursor"_a = nullopt,
            "toplevel_only"_a = false,
            "_"_a = nb::kwargs()
        )
        .def(
            "get_atom_type",
            [](
                InMemoryDB& self,
                const string& handle
            ) -> opt<const string> {
                return self.get_atom_type(handle);
            }
        )
        .def(
            "count_atoms",
            [](
                InMemoryDB& self
            ) -> unordered_map<string, int> {
                return self.count_atoms();
            }
        )
        .def(
            "clear_database",
            [](
                InMemoryDB& self
            ) {
                self.clear_database();
            }
        )
        .def(
            "add_node",
            [](
                InMemoryDB& self,
                const NodeParams& node_params
            ) -> shared_ptr<const Node> {
                return self.add_node(node_params);
            }
        )
        .def(
            "add_link",
            [](
                InMemoryDB& self,
                const LinkParams& link_params,
                bool toplevel
            ) -> shared_ptr<const Link> {
                return self.add_link(link_params, toplevel);
            },
            "link_params"_a,
            "toplevel"_a = true
        )
        .def(
            "reindex",
            [](
                InMemoryDB& self,
                const unordered_map<string, vector<unordered_map<string, any>>>& pattern_index_templates
            ) {
                self.reindex(pattern_index_templates);
            },
            "pattern_index_templates"_a
        )
        .def(
            "delete_atom",
            [](
                InMemoryDB& self,
                const string& handle
            ) {
                self.delete_atom(handle);
            }
        )
        .def(
            "create_field_index",
            [](
                InMemoryDB& self,
                const string& atom_type,
                const StringList& fields,
                const string& named_type = "",
                const opt<const StringList>& composite_type = nullopt,
                FieldIndexType index_type = FieldIndexType::BINARY_TREE
            ) -> const string {
                return self.create_field_index(atom_type, fields, named_type, composite_type, index_type);
            },
            "atom_type"_a,
            "fields"_a,
            "named_type"_a = "",
            "composite_type"_a = nullopt,
            "index_type"_a = FieldIndexType::BINARY_TREE
        )
        .def(
            "bulk_insert",
            [](
                InMemoryDB& self,
                const vector<shared_ptr<const Atom>>& documents
            ) {
                self.bulk_insert(documents);
            }
        )
        .def(
            "retrieve_all_atoms",
            [](
                InMemoryDB& self
            ) -> const vector<shared_ptr<const Atom>> {
                return self.retrieve_all_atoms();
            }
        )
        .def(
            "commit",
            [](
                InMemoryDB& self,
                const opt<const vector<Atom>>& buffer = nullopt
            ) {
                self.commit(buffer);
            },
            "buffer"_a = nullopt
        );
    // ---------------------------------------------------------------------------------------------
    // adapters submodule --------------------------------------------------------------------------
    nb::module_ adapters = m.def_submodule("adapters");
    nb::class_<InMemoryDB, AtomDB>(adapters, "InMemoryDB")
        .def(nb::init<>());
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
        .def_ro("named_type", &Atom::named_type);
    nb::class_<Node, Atom>(document_types, "Node")
        .def_ro("name", &Node::name)
        .def("to_string", [](const Node& self) -> const string { return self.to_string(); })
        .def("__str__", [](const Node& self) -> const string { return self.to_string(); })
        .def("__repr__", [](const Node& self) -> const string { return self.to_string(); });
    nb::class_<Link, Atom>(document_types, "Link")
        .def_prop_ro(
            "composite_type",
            [](const Link& self) -> const nb::list {
                return transformer::composite_type_to_pylist(self.composite_type);
            }
        )
        .def_ro("named_type_hash", &Link::named_type_hash)
        .def_ro("targets", &Link::targets)
        .def_ro("is_top_level", &Link::is_top_level)
        .def_ro("keys", &Link::keys)
        .def_ro("targets_documents", &Link::targets_documents)
        .def("to_string", [](const Link& self) -> const string { return self.to_string(); })
        .def("__str__", [](const Link& self) -> const string { return self.to_string(); })
        .def("__repr__", [](const Link& self) -> const string { return self.to_string(); });
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
            [](
                LinkParams& self, const LinkParams::Target& target
            ) {
                self.targets.push_back(target);
            }
        )
        .def_static(
            "is_node",
            [](
                const LinkParams::Target& target
            ) -> bool {
                return LinkParams::is_node(target);
            }
        )
        .def_static(
            "is_link",
            [](
                const LinkParams::Target& target
            ) -> bool {
                return LinkParams::is_link(target);
            }
        );
    // ---------------------------------------------------------------------------------------------
}

