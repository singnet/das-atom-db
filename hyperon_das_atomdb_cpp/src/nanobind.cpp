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
            [](const string& node_type, const string& node_name) -> string {
                return AtomDB::build_node_handle(node_type, node_name);
            }
        )
        .def_static(
            "build_link_handle",
            [](const string& link_type, const StringList& target_handles) -> string {
                return AtomDB::build_link_handle(link_type, target_handles);
            }
        )
        .def(
            "node_exists",
            [](const AtomDB& self, const string& node_type, const string& node_name) -> bool {
                return self.node_exists(node_type, node_name);
            }
        )
        .def(
            "link_exists",
            [](const AtomDB& self, const string& link_type, const StringList& target_handles) -> bool {
                return self.link_exists(link_type, target_handles);
            }
        )
        .def(
            "get_atom",
            [](const AtomDB& self, const string& handle) -> shared_ptr<const Atom> {
                return self.get_atom(handle);
            }
        );
    // ---------------------------------------------------------------------------------------------
    // exceptions submodule ------------------------------------------------------------------------
    nb::module_ exceptions = m.def_submodule("exceptions");
    nb::exception<AtomDoesNotExist>(exceptions, "AtomDoesNotExist");
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
        .def(
            "to_string",
            [](const Node& self) -> const string {
                return self.to_string();
            }
        )
        .def(
            "__str__",
            [](const Node& self) -> const string {
                return self.to_string();
            }
        )
        .def(
            "__repr__",
            [](const Node& self) -> const string {
                return self.to_string();
            }
        );
    nb::class_<Link, Atom>(document_types, "Link")
        .def_ro("composite_type", &Link::composite_type)
        .def_ro("named_type_hash", &Link::named_type_hash)
        .def_ro("targets", &Link::targets)
        .def_ro("is_top_level", &Link::is_top_level)
        .def_ro("keys", &Link::keys)
        .def_ro("targets_documents", &Link::targets_documents)
        .def(
            "to_string",
            [](const Link& self) -> const string {
                return self.to_string();
            }
        )
        .def(
            "__str__",
            [](const Link& self) -> const string {
                return self.to_string();
            }
        )
        .def(
            "__repr__",
            [](const Link& self) -> const string {
                return self.to_string();
            }
        );
    // ---------------------------------------------------------------------------------------------
    // database submodule --------------------------------------------------------------------------
    nb::module_ database = m.def_submodule("database");
    // database.hpp
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
            [](LinkParams& self, const LinkParams::Target& target) {
                self.add_target(target);
            }
        )
        .def_static(
            "is_node",
            [](const LinkParams::Target& target) -> bool {
                return LinkParams::is_node(target);
            }
        )
        .def_static(
            "is_link",
            [](const LinkParams::Target& target) -> bool {
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
            [](InMemoryDB& self, const LinkParams& link_params, bool toplevel) -> shared_ptr<const Link> {
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
            [](InMemoryDB& self, const string& node_type, const string& node_name) -> string {
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
            ) -> pair<OptCursor, Pattern_or_Template_List> {
                Params params = Params({{ParamsKeys::TOPLEVEL_ONLY, toplevel_only}});
                if (cursor) 
                    params.set(ParamsKeys::CURSOR, cursor.value());
                return self.get_matched_links(link_type, target_handles, params);
            },
            "link_type"_a, "target_handles"_a, nb::kw_only(), "cursor"_a = nullopt, "toplevel_only"_a = false
        );
    // ---------------------------------------------------------------------------------------------
}

// struct Bar {
// public:
//     int x;
//     string s;
//     vector<int> v;
//     unordered_map<string, int> m;

//     int get_x() { return x; }

//     string to_string() {
//         string s = "Bar(x = " + std::to_string(this->x) + ", s = " + this->s + ", v = [";
//         if (v.size() > 0) {
//             for (auto i : this->v) {
//                 s += "'" + std::to_string(i) + "', ";
//             }
//             s.pop_back();
//             s.pop_back();
//         }
//         s += "], m = {";
//         if (m.size() > 0) {
//             for (auto i : this->m) {
//                 s += "'" + i.first + "': " + std::to_string(i.second) + ", ";
//             }
//             s.pop_back();
//             s.pop_back();
//         }
//         s += "})";
//         return move(s);
//     }
// };

// class FooBar {
// public:
//     nb::dict d;
//     unordered_map<string, int> t;
//     FooBar(int x) : x(x), v({}), m({}) {
//         this->p = shared_ptr<Bar>(new Bar{10, "hello", {1, 2, 3}, {{"a", 1}, {"b", 2}, {"c", 3}}});
//         this->bar = Bar{x, "hello", {1, 2, 3}, {{"a", 1}, {"b", 2}, {"c", 3}}};
//         for (const auto [k, v] : unordered_map<const char*, int>{{"a", 1}, {"b", 2}, {"c", 3}}) {
//             this->d[k] = v;
//         }
//         this->t = nb::cast<unordered_map<string, int>>(this->d);
//     }
//     int get() { return x; }
//     void set(int x) { this->x = x; }
//     void add(const string &s) { v.push_back(s); }
//     void merge(const vector<string> &v) { this->v.insert(this->v.end(), v.begin(), v.end()); }
//     string to_string() {
//         string s = "FooBar(x = " + std::to_string(x) + ", v = [";
//         if (v.size() > 0) {
//             for (auto i : v) {
//                 s += "'" + i + "', ";
//             }
//             s.pop_back();
//             s.pop_back();
//         }
//         s += "], m = {";
//         if (m.size() > 0) {
//             for (auto i : m) {
//                 s += "'" + i.first + "': " + std::to_string(i.second) + ", ";
//             }
//             s.pop_back();
//             s.pop_back();
//         }
//         s += "}, shared_ptr<" + p->to_string() + ">, " + bar.to_string() + ")";
//         return s;
//     }
//     void add_to_map(const string &key, int value) { m[key] = value; }
//     unordered_map<string, int> get_map() { return m; }
//     Bar get_ref_from_shared_ptr() { return *p; }
//     shared_ptr<Bar> get_shared_ptr() { return p; }

//     Bar get_bar() { return bar; }
//     Bar bar;
// private:
//     int x;
//     vector<string> v;
//     unordered_map<string, int> m;
//     shared_ptr<Bar> p;
// };

// NB_MODULE(hyperon_das_atomdb_nanobind, m) {
//     nb::class_<FooBar>(m, "FooBar")
//         .def(nb::init<int>())
//         .def("get", [](FooBar &self) -> int { return self.get(); })
//         .def("set", [](FooBar &self, int x) { self.set(x); })
//         .def("add", [](FooBar &self, const string &s) { self.add(s); })
//         .def("merge", [](FooBar &self, const vector<string> &v) { self.merge(v); })
//         .def("to_string", [](FooBar &self) -> string { return self.to_string(); })
//         .def("add_to_map", [](FooBar &self, const string &key, int value) { self.add_to_map(key,
//         value); }) .def("get_map", [](FooBar &self) -> unordered_map<string, int> { return
//         self.get_map(); }) .def("get_ref_from_shared_ptr", [](FooBar &self) -> Bar { return
//         self.get_ref_from_shared_ptr(); }) .def("get_shared_ptr", [](FooBar &self) -> shared_ptr<Bar>
//         { return self.get_shared_ptr(); }) .def("get_bar", [](FooBar &self) -> Bar { return
//         self.get_bar(); }) .def("__str__", [](FooBar &self) -> string { return self.to_string(); })
//         .def("__repr__", [](FooBar &self) -> string { return self.to_string(); })
//         .def_rw("bar", &FooBar::bar)
//         .def_rw("d", &FooBar::d)
//         .def_rw("t", &FooBar::t);
//     nb::class_<Bar>(m, "Bar")
//         .def(nb::init<int, string, vector<int>, unordered_map<string, int>>())
//         .def("to_string", [](Bar &self) -> string { return self.to_string(); })
//         .def("get_x", [](Bar &self) -> int { return self.get_x(); })
//         .def_rw("x", &Bar::x)
//         .def_rw("s", &Bar::s)
//         .def_rw("v", &Bar::v)
//         .def_rw("m", &Bar::m);
// }