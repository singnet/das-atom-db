#include <nanobind/nanobind.h>
#include <nanobind/operators.h>
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
#include "atom_db_publicist.h"
#include "atom_db_trampoline.h"
#include "bind_helpers.h"
#include "constants.h"
#include "database.h"
#include "document_types.h"
#include "exceptions.h"
#include "params.h"
#include "type_aliases.h"

using namespace std;
using namespace atomdb;
namespace helpers = bind_helpers;

namespace nb = nanobind;
using namespace nb::literals;

#include <iostream>

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
    nb::class_<AtomDB, AtomDBTrampoline>(m, "AtomDB")
        .def(nb::init<>())
        .def_static("build_node_handle", &AtomDB::build_node_handle, "node_type"_a, "node_name"_a)
        .def_static("node_handle",  // retrocompatibility
                    &AtomDB::build_node_handle,
                    "node_type"_a,
                    "node_name"_a)
        .def_static(
            "build_link_handle",
            [](const string& link_type, const StringList& target_handles) {
                return AtomDB::build_link_handle(link_type, target_handles);
            },
            "link_type"_a,
            "target_handles"_a)
        .def_static(
            "link_handle",  // retrocompatibility
            [](const string& link_type, const StringList& target_handles) {
                return AtomDB::build_link_handle(link_type, target_handles);
            },
            "link_type"_a,
            "target_handles"_a)
        .def_static(
            "build_link_handle",
            [](const string& link_type, const string& target_handle) {
                return AtomDB::build_link_handle(link_type, target_handle);
            },
            "link_type"_a,
            "target_handle"_a)
        .def_static(
            "link_handle",  // retrocompatibility
            [](const string& link_type, const string& target_handle) {
                return AtomDB::build_link_handle(link_type, target_handle);
            },
            "link_type"_a,
            "target_handle"_a)
        .def("node_exists", &AtomDB::node_exists, "node_type"_a, "node_name"_a)
        .def("link_exists", &AtomDB::link_exists, "link_type"_a, "target_handles"_a)
        .def(
            "get_atom",
            [](AtomDB& self,
               const string& handle,
               bool no_target_format = false,
               bool targets_document = false,
               bool deep_representation = false,
               const nb::kwargs& _ = {}) -> shared_ptr<const Atom> {
                return self.get_atom(handle, {
                    no_target_format : no_target_format,
                    targets_document : targets_document,
                    deep_representation : deep_representation
                });
            },
            "handle"_a,
            nb::kw_only(),
            "no_target_format"_a = false,
            "targets_document"_a = false,
            "deep_representation"_a = false,
            "_"_a = nb::kwargs())
        .def("get_node_handle", &AtomDB::get_node_handle, "node_type"_a, "node_name"_a)
        .def("get_node_name", &AtomDB::get_node_name, "node_handle"_a)
        .def("get_node_type", &AtomDB::get_node_type, "node_handle"_a)
        .def("get_node_by_name", &AtomDB::get_node_by_name, "node_type"_a, "substring"_a)
        .def("get_atoms_by_field", &AtomDB::get_atoms_by_field, "query"_a)
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
        .def("get_node_by_name_starting_with",
             &AtomDB::get_node_by_name_starting_with,
             "node_type"_a,
             "startswith"_a)
        .def("get_all_nodes", &AtomDB::get_all_nodes, "node_type"_a, "names"_a = false)
        .def(
            "get_all_links",
            [](AtomDB& self, const string& link_type, const nb::kwargs& _ = {})
                -> const StringUnorderedSet { return self.get_all_links(link_type); },
            "link_type"_a,
            "_"_a = nb::kwargs())
        .def("get_link_handle", &AtomDB::get_link_handle, "link_type"_a, "target_handles"_a)
        .def("get_link_type", &AtomDB::get_link_type, "link_handle"_a)
        .def("get_link_targets", &AtomDB::get_link_targets, "link_handle"_a)
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
               bool targets_document = false,
               bool deep_representation = false,
               bool handles_only = false,
               const nb::kwargs& _ = {}) -> const vector<shared_ptr<const Atom>> {
                return self.get_incoming_links_atoms(atom_handle, {
                    no_target_format : no_target_format,
                    targets_document : targets_document,
                    deep_representation : deep_representation,
                    handles_only : handles_only
                });
            },
            "atom_handle"_a,
            nb::kw_only(),
            "no_target_format"_a = false,
            "targets_document"_a = false,
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
                return self.get_matched_type_template(helpers::pylist_to_composite_type(_template),
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
        .def("get_atom_type", &AtomDB::get_atom_type, "handle"_a)
        .def(
            "count_atoms",
            [](const AtomDB& self, const opt<const nb::dict>& parameters = nullopt)
                -> const unordered_map<string, int> { return self.count_atoms(); },
            "parameters"_a = nullopt)
        .def("clear_database", &AtomDB::clear_database)
        .def("add_node", &AtomDB::add_node, "node_params"_a)
        .def("add_link", &AtomDB::add_link, "link_params"_a, "toplevel"_a = true)
        .def("reindex", &AtomDB::reindex, "pattern_index_templates"_a)
        .def("delete_atom", &AtomDB::delete_atom, "handle"_a)
        .def("create_field_index",
             &AtomDB::create_field_index,
             "atom_type"_a,
             "fields"_a,
             "named_type"_a = "",
             "composite_type"_a = nullopt,
             "index_type"_a = FieldIndexType::BINARY_TREE)
        .def("bulk_insert", &AtomDB::bulk_insert, "documents"_a)
        .def("retrieve_all_atoms", &AtomDB::retrieve_all_atoms)
        .def("commit", &AtomDB::commit, "buffer"_a = nullopt)
        .def(
            "_reformat_document",
            [](const AtomDB& self,
               const shared_ptr<const Atom>& document,
               bool no_target_format = false,
               bool targets_document = false,
               bool deep_representation = false,
               const nb::kwargs& _ = {}) {
                return self._reformat_document(document, {
                    no_target_format : no_target_format,
                    targets_document : targets_document,
                    deep_representation : deep_representation
                });
            },
            "document"_a,
            nb::kw_only(),
            "no_target_format"_a = false,
            "targets_document"_a = false,
            "deep_representation"_a = false,
            "_"_a = nb::kwargs())
        .def("_build_node", &AtomDBPublicist::_build_node, "node_params"_a)
        .def("_build_link", &AtomDBPublicist::_build_link, "link_params"_a, "is_toplevel"_a = true)
        .def("_get_atom", &AtomDBPublicist::_get_atom, "handle"_a);
    // ---------------------------------------------------------------------------------------------
    // adapters submodule --------------------------------------------------------------------------
    nb::module_ adapters = m.def_submodule("adapters");
    nb::class_<InMemoryDB, AtomDB>(adapters, "InMemoryDB")
        .def(nb::init<const string&>(), "database_name"_a = "das")
        .def("__repr__", [](const InMemoryDB& self) -> string { return "<Atom database InMemory>"; })
        .def("__str__", [](const InMemoryDB& self) -> string { return "<Atom database InMemory>"; });
    // ---------------------------------------------------------------------------------------------
    // exceptions submodule ------------------------------------------------------------------------
    nb::module_ exceptions = m.def_submodule("exceptions");
    nb::exception<AtomDbBaseException>(exceptions, "AtomDbBaseException");
    nb::exception<AddLinkException>(exceptions, "AddLinkException");
    nb::exception<AddNodeException>(exceptions, "AddNodeException");
    nb::exception<AtomDoesNotExist>(exceptions, "AtomDoesNotExist");
    nb::exception<InvalidAtomDB>(exceptions, "InvalidAtomDB");
    nb::exception<InvalidOperationException>(exceptions, "InvalidOperationException");
    nb::exception<RetryException>(exceptions, "RetryException");
    // ---------------------------------------------------------------------------------------------
    // document_types submodule --------------------------------------------------------------------
    nb::module_ document_types = m.def_submodule("document_types");
    nb::class_<Atom>(document_types, "Atom")
        .def_ro("_id", &Atom::_id)
        .def_prop_ro("id",  // RO property for having access to `_id` as `id`
                     [](const Atom& self) -> string { return self._id; })
        .def_ro("handle", &Atom::handle)
        .def_ro("composite_type_hash", &Atom::composite_type_hash)
        .def_ro("named_type", &Atom::named_type)
        .def_ro("custom_attributes", &Atom::custom_attributes)
        .def("to_string", &Atom::to_string)
        .def("__str__", &Atom::to_string)
        .def("__repr__", &Atom::to_string)
        .def("to_dict", &helpers::atom_to_dict)
        .def(nb::self == nb::self);
    nb::class_<AtomType, Atom>(document_types, "AtomType")
        .def(nb::init<const string&,  // _id,
                      const string&,  // handle,
                      const string&,  // composite_type_hash,
                      const string&,  // named_type,
                      const string&,  // named_type_hash,
                      const opt<const CustomAttributes>&>(),
             "_id"_a,
             "handle"_a,
             "composite_type_hash"_a,
             "named_type"_a,
             "named_type_hash"_a,
             "custom_attributes"_a = nullopt)
        .def_ro("named_type_hash", &AtomType::named_type_hash)
        .def("__getstate__", &helpers::atom_type_to_tuple)
        .def("__setstate__", &helpers::tuple_to_atom_type)
        .def("to_dict", &helpers::atom_type_to_dict)
        .def(nb::self == nb::self);
    nb::class_<Node, Atom>(document_types, "Node")
        .def(nb::init<const string&,  // _id,
                      const string&,  // handle,
                      const string&,  // composite_type_hash,
                      const string&,  // named_type,
                      const string&,  // name,
                      const opt<const CustomAttributes>&>(),
             "_id"_a,
             "handle"_a,
             "composite_type_hash"_a,
             "named_type"_a,
             "name"_a,
             "custom_attributes"_a = nullopt)
        .def_ro("name", &Node::name)
        .def("__getstate__", &helpers::node_to_tuple)
        .def("__setstate__", &helpers::tuple_to_node)
        .def("to_dict", &helpers::node_to_dict)
        .def(nb::self == nb::self);
    nb::class_<Link, Atom>(document_types, "Link")
        .def("__init__",
             &helpers::init_link,
             "_id"_a,
             "handle"_a,
             "composite_type_hash"_a,
             "named_type"_a,
             "composite_type"_a,
             "named_type_hash"_a,
             "targets"_a,
             "is_toplevel"_a,
             "targets_documents"_a = nullopt,
             "custom_attributes"_a = nullopt)
        .def_prop_ro("composite_type",
                     [](const Link& self) -> const nb::list {
                         return helpers::composite_type_to_pylist(self.composite_type);
                     })
        .def_ro("named_type_hash", &Link::named_type_hash)
        .def_ro("targets", &Link::targets)
        .def_ro("is_toplevel", &Link::is_toplevel)
        .def_rw("targets_documents", &Link::targets_documents)
        .def("__getstate__", &helpers::link_to_tuple)
        .def("__setstate__", &helpers::tuple_to_link)
        .def("to_dict", &helpers::link_to_dict)
        .def(nb::self == nb::self);
    // ---------------------------------------------------------------------------------------------
    // database submodule --------------------------------------------------------------------------
    nb::module_ database = m.def_submodule("database");
    nb::class_<CustomAttributes>(database, "CustomAttributes")
        .def(nb::init<const StringUnorderedMap&,
                      const IntUnorderedMap&,
                      const FloatUnorderedMap&,
                      const BoolUnorderedMap&>(),
             "strings"_a = StringUnorderedMap(),
             "integers"_a = IntUnorderedMap(),
             "floats"_a = FloatUnorderedMap(),
             "booleans"_a = BoolUnorderedMap())
        .def_rw("strings", &CustomAttributes::strings)
        .def_rw("integers", &CustomAttributes::integers)
        .def_rw("floats", &CustomAttributes::floats)
        .def_rw("booleans", &CustomAttributes::booleans)
        .def("__getstate__", &helpers::custom_attributes_to_tuple)
        .def("__setstate__", &helpers::tuple_to_custom_attributes);
    nb::class_<NodeParams>(database, "NodeParams")
        .def(nb::init<const string&, const string&, const opt<CustomAttributes>&>(),
             "type"_a,
             "name"_a,
             "custom_attributes"_a = nullopt)
        .def_rw("type", &NodeParams::type)
        .def_rw("name", &NodeParams::name)
        .def_rw("custom_attributes", &NodeParams::custom_attributes)
        .def("to_string", &NodeParams::to_string)
        .def("__str__", &NodeParams::to_string)
        .def("__repr__", &NodeParams::to_string);
    nb::class_<LinkParams>(database, "LinkParams")
        .def(nb::init<const string&, const LinkParams::Targets&, const opt<CustomAttributes>&>(),
             "type"_a,
             "targets"_a,
             "custom_attributes"_a = nullopt)
        .def_rw("type", &LinkParams::type)
        .def_rw("targets", &LinkParams::targets)
        .def_rw("custom_attributes", &LinkParams::custom_attributes)
        .def(
            "add_target",
            [](LinkParams& self, const LinkParams::Target& target) { self.targets.push_back(target); },
            "target"_a)
        .def("to_string", &LinkParams::to_string)
        .def("__str__", &LinkParams::to_string)
        .def("__repr__", &LinkParams::to_string);
    // ---------------------------------------------------------------------------------------------
}
