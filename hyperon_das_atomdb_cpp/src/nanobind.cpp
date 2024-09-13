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
#include "type_aliases.hpp"

using namespace std;
using namespace atomdb;

namespace nb = nanobind;
using namespace nb::literals;

NB_MODULE(hyperon_das_atomdb_nanobind, m) {
    // root module ---------------------------------------------------------------------------------
    m.attr("WILDCARD") = WILDCARD;
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
                const AtomDB& self, const string& handle
            ) -> shared_ptr<const Atom> {
                return self.get_atom(handle);
            }
        );
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
        .def_ro("composite_type", &Link::composite_type)
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
        .def(nb::init<const string&>())
        .def(nb::init<const string&, const LinkParams::Targets&>())
        .def_rw("type", &LinkParams::type)
        .def_rw("targets", &LinkParams::targets)
        .def("add_target",
            [](
                LinkParams& self, const LinkParams::Target& target
            ) {
                self.add_target(target);
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
    // adapters submodule --------------------------------------------------------------------------
    nb::module_ adapters = m.def_submodule("adapters");
    nb::class_<InMemoryDB, AtomDB>(adapters, "InMemoryDB")
        .def(nb::init<>())
        .def(
            "add_link",
            [](
                InMemoryDB& self, const LinkParams& link_params, bool toplevel
            ) -> shared_ptr<const Link> {
                return self.add_link(link_params, toplevel);
            },
            "link_params"_a, "toplevel"_a = true
        )
        .def(
            "get_atom",
            [](
                InMemoryDB& self,
                const string& handle,
                bool no_target_format = false,
                bool targets_documents = false,
                bool deep_representation = false
            ) -> shared_ptr<const Atom> {
                Params params = Params({
                    {ParamsKeys::NO_TARGET_FORMAT, no_target_format},
                    {ParamsKeys::TARGETS_DOCUMENTS, targets_documents},
                    {ParamsKeys::DEEP_REPRESENTATION, deep_representation}
                });
                return self.get_atom(handle, params);
            },
            "handle"_a,
            nb::kw_only(),
            "no_target_format"_a = false,
            "targets_documents"_a = false,
            "deep_representation"_a = false
        )
        .def(
            "get_node_handle",
            [](
                InMemoryDB& self, const string& node_type, const string& node_name
            ) -> const string {
                return self.get_node_handle(node_type, node_name);
            }
        )
        .def(
            "get_matched_links",
            [](
                InMemoryDB& self,
                const string& link_type,
                const StringList& target_handles,
                opt<int> cursor = nullopt,
                bool toplevel_only = false
            ) -> const pair<const OptCursor, const Pattern_or_Template_List> {
                Params params = Params({{ParamsKeys::TOPLEVEL_ONLY, toplevel_only}});
                if (cursor) 
                    params.set(ParamsKeys::CURSOR, cursor.value());
                return self.get_matched_links(link_type, target_handles, params);
            },
            "link_type"_a,
            "target_handles"_a,
            nb::kw_only(),
            "cursor"_a = nullopt,
            "toplevel_only"_a = false
        );
    // ---------------------------------------------------------------------------------------------
}

