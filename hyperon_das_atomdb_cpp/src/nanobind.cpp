#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/unordered_map.h>

using namespace std;

namespace nb = nanobind;

struct Bar {
    int x;
    string s;
    vector<int> v;
    unordered_map<string, int> m;
};

class FooBar {
public:
    FooBar(int x) : x(x), v({}), m({}) {
        p = make_shared<int>(x);
        bar = Bar{x, "hello", {1, 2, 3}, {{"a", 1}, {"b", 2}, {"c", 3}}};
    }
    int get() { return x; }
    void set(int x) { this->x = x; }
    void add(const string &s) { v.push_back(s); }
    void merge(const vector<string> &v) { this->v.insert(this->v.end(), v.begin(), v.end()); }
    string to_string() {
        string s = "FooBar(x = " + std::to_string(x) + ", v = [";
        if (v.size() > 0) {
            for (auto i : v) {
                s += i + ", ";
            }
            s.pop_back();
            s.pop_back();
        }
        s += "], m = {";
        if (m.size() > 0) {
            for (auto i : m) {
                s += i.first + ": " + std::to_string(i.second) + ", ";
            }
            s.pop_back();
            s.pop_back();
        }
        s += "})";
        return s;
    }
    void add_to_map(const string &key, int value) { m[key] = value; }
    unordered_map<string, int> get_map() { return m; }
    int get_shared_ptr() { return *p; }

    Bar get_bar() { return bar; }
    Bar bar;
private:
    int x;
    vector<string> v;
    unordered_map<string, int> m;
    shared_ptr<int> p;
};


NB_MODULE(hyperon_das_atomdb_nanobind, m) {
    nb::class_<FooBar>(m, "FooBar")
        .def(nb::init<int>())
        .def("get", [](FooBar &self) -> int { return self.get(); })
        .def("set", [](FooBar &self, int x) { self.set(x); })
        .def("add", [](FooBar &self, const string &s) { self.add(s); })
        .def("merge", [](FooBar &self, const vector<string> &v) { self.merge(v); })
        .def("to_string", [](FooBar &self) -> string { return self.to_string(); })
        .def("add_to_map", [](FooBar &self, const string &key, int value) { self.add_to_map(key, value); })
        .def("get_map", [](FooBar &self) -> unordered_map<string, int> { return self.get_map(); })
        .def("get_shared_ptr", [](FooBar &self) -> int { return self.get_shared_ptr(); })
        .def("get_bar", [](FooBar &self) -> Bar { return self.get_bar(); })
        .def("__str__", [](FooBar &self) -> string { return self.to_string(); })
        .def("__repr__", [](FooBar &self) -> string { return self.to_string(); })
        .def_rw("bar", &FooBar::bar);
    nb::class_<Bar>(m, "Bar")
        .def(nb::init<int, string, vector<int>, unordered_map<string, int>>())
        .def_rw("x", &Bar::x)
        .def_rw("s", &Bar::s)
        .def_rw("v", &Bar::v)
        .def_rw("m", &Bar::m);
}