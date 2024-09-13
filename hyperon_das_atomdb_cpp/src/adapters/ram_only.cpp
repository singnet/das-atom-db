#include "ram_only.hpp"

#include <algorithm>

#include "exceptions.hpp"
#include "utils/expression_hasher.hpp"
#include "utils/patterns.hpp"

using namespace std;
using namespace atomdb;

// PUBLIC METHODS //////////////////////////////////////////////////////////////////////////////////

//------------------------------------------------------------------------------
const string InMemoryDB::get_node_handle(const string& node_type, const string& node_name) const {
    auto node_handle = AtomDB::build_node_handle(node_type, node_name);
    if (this->db.node.find(node_handle) != this->db.node.end()) {
        return move(node_handle);
    }
    throw AtomDoesNotExist("Nonexistent atom", node_type + node_name);
}

//------------------------------------------------------------------------------
const string InMemoryDB::get_node_name(const string& node_handle) const {
    auto it = this->db.node.find(node_handle);
    if (it != this->db.node.end()) {
        return it->second->name;
    }
    throw AtomDoesNotExist("Nonexistent atom", "node_handle: " + node_handle);
}

//------------------------------------------------------------------------------
const string InMemoryDB::get_node_type(const string& node_handle) const {
    auto it = this->db.node.find(node_handle);
    if (it != this->db.node.end()) {
        return it->second->named_type;
    }
    throw AtomDoesNotExist("Nonexistent atom", "node_handle: " + node_handle);
}

//------------------------------------------------------------------------------
const StringList InMemoryDB::get_node_by_name(const string& node_type, const string& substring) const {
    auto node_type_hash = ExpressionHasher::named_type_hash(node_type);
    StringList node_handles;
    for (const auto& [key, node] : this->db.node) {
        if (node->name.find(substring) != string::npos && node_type_hash == node->composite_type_hash) {
            node_handles.push_back(key);
        }
    }
    return move(node_handles);
}

//------------------------------------------------------------------------------
const StringList InMemoryDB::get_atoms_by_field(
    const vector<unordered_map<string, string>>& query) const {
    throw runtime_error("Not implemented");
}

//------------------------------------------------------------------------------
const pair<const OptCursor, const AtomList> InMemoryDB::get_atoms_by_index(
    const string& index_id,
    const vector<unordered_map<string, string>>& query,
    int curso,
    int chunk_size) const {
    throw runtime_error("Not implemented");
}

//------------------------------------------------------------------------------
const StringList InMemoryDB::get_atoms_by_text_field(const string& text_value,
                                                     const string& field,
                                                     const string& text_index_id) const {
    throw runtime_error("Not implemented");
}

//------------------------------------------------------------------------------
const StringList InMemoryDB::get_node_by_name_starting_with(const string& node_type,
                                                            const string& startswith) const {
    throw runtime_error("Not implemented");
}

//------------------------------------------------------------------------------
const StringList InMemoryDB::get_all_nodes(const string& node_type, bool names) const {
    auto node_type_hash = ExpressionHasher::named_type_hash(node_type);
    StringList node_handles;
    if (names) {
        for (const auto& [_, node] : this->db.node) {
            if (node->composite_type_hash == node_type_hash) {
                node_handles.push_back(node->name);
            }
        }
    } else {
        for (const auto& [handle, node] : this->db.node) {
            if (node->composite_type_hash == node_type_hash) {
                node_handles.push_back(handle);
            }
        }
    }
    return move(node_handles);
}

//------------------------------------------------------------------------------
const pair<const OptCursor, const StringList> InMemoryDB::get_all_links(const string& link_type,
                                                                        const Params& params) const {
    StringList link_handles;
    for (const auto& [_, link] : this->db.link) {
        if (link->named_type == link_type) {
            link_handles.push_back(link->id);
        }
    }
    return {params.get<int>(ParamsKeys::CURSOR), move(link_handles)};
}

//------------------------------------------------------------------------------
const string InMemoryDB::get_link_handle(const string& link_type,
                                         const StringList& target_handles) const {
    auto link_handle = AtomDB::build_link_handle(link_type, target_handles);
    if (this->db.link.find(link_handle) != this->db.link.end()) {
        return move(link_handle);
    }
    string target_handles_str = "[";
    for (const auto& target_handle : target_handles) {
        target_handles_str += target_handle + ", ";
    }
    target_handles_str.pop_back();
    target_handles_str.pop_back();
    target_handles_str += "]";
    throw AtomDoesNotExist("Nonexistent atom", link_type + ":" + target_handles_str);
}

//------------------------------------------------------------------------------
const string InMemoryDB::get_link_type(const string& link_handle) const {
    auto link = this->_get_link(link_handle);
    if (link) {
        return link->named_type;
    }
    throw AtomDoesNotExist("Nonexistent atom", "link_handle: " + link_handle);
}

//------------------------------------------------------------------------------
const StringList InMemoryDB::get_link_targets(const string& link_handle) const {
    auto it = this->db.outgoing_set.find(link_handle);
    if (it != this->db.outgoing_set.end()) {
        return it->second;
    }
    throw AtomDoesNotExist("Nonexistent atom", "link_handle: " + link_handle);
}

//------------------------------------------------------------------------------
bool InMemoryDB::is_ordered(const string& link_handle) const {
    if (this->_get_link(link_handle)) {
        return true;
    }
    throw AtomDoesNotExist("Nonexistent atom", "link_handle: " + link_handle);
}

//------------------------------------------------------------------------------
const pair<const OptCursor, const StringUnorderedSet> InMemoryDB::get_incoming_links_handles(
    const string& atom_handle, const Params& params) const {
    auto it = this->db.incoming_set.find(atom_handle);
    auto links = it != this->db.incoming_set.end() ? it->second : StringUnorderedSet();
    return {params.get<int>(ParamsKeys::CURSOR), move(links)};
}

//------------------------------------------------------------------------------
const pair<const OptCursor, const vector<shared_ptr<const Atom>>> InMemoryDB::get_incoming_links_atoms(
    const string& atom_handle, const Params& params) const {
    const auto& [cursor, links] = this->get_incoming_links_handles(atom_handle, params);
    vector<shared_ptr<const Atom>> atoms;
    for (const auto& link_handle : links) {
        atoms.push_back(this->get_atom(link_handle, params));
    }
    return {cursor, move(atoms)};
}

//------------------------------------------------------------------------------
const pair<const OptCursor, const Pattern_or_Template_List> InMemoryDB::get_matched_links(
    const string& link_type, const StringList& target_handles, const Params& params) const {
    if (link_type != WILDCARD &&
        find(target_handles.begin(), target_handles.end(), WILDCARD) == target_handles.end()) {
        return {params.get<int>(ParamsKeys::CURSOR),
                {make_pair(this->get_link_handle(link_type, target_handles), nullopt)}};
    }

    auto link_type_hash =
        link_type == WILDCARD ? WILDCARD : ExpressionHasher::named_type_hash(link_type);

    StringList handles;
    handles.reserve(target_handles.size() + 1);
    handles.push_back(link_type_hash);
    handles.insert(handles.end(), target_handles.begin(), target_handles.end());
    auto pattern_hash = ExpressionHasher::composite_hash(handles);

    Pattern_or_Template_List patterns_matched;
    auto it = this->db.patterns.find(pattern_hash);
    if (it != this->db.patterns.end()) {
        patterns_matched.reserve(it->second.size());
        patterns_matched.insert(patterns_matched.end(), it->second.begin(), it->second.end());
    }

    if (params.get<bool>(ParamsKeys::TOPLEVEL_ONLY).value_or(false)) {
        return {params.get<int>(ParamsKeys::CURSOR), this->_filter_non_toplevel(patterns_matched)};
    }

    return {params.get<int>(ParamsKeys::CURSOR), move(patterns_matched)};
}

//------------------------------------------------------------------------------
const pair<const OptCursor, const Pattern_or_Template_List> InMemoryDB::get_matched_type_template(
    const ListOfAny& _template, const Params& params) const {
    auto template_hash = ExpressionHasher::composite_hash(_template);
    auto it = this->db.templates.find(template_hash);
    if (it != this->db.templates.end()) {
        Pattern_or_Template_List templates_matched;
        templates_matched.reserve(it->second.size());
        templates_matched.insert(templates_matched.end(), it->second.begin(), it->second.end());
        if (params.get<bool>(ParamsKeys::TOPLEVEL_ONLY).value_or(false)) {
            return {params.get<int>(ParamsKeys::CURSOR), this->_filter_non_toplevel(templates_matched)};
        }
        return {params.get<int>(ParamsKeys::CURSOR), move(templates_matched)};
    }
    return {params.get<int>(ParamsKeys::CURSOR), {}};
}

//------------------------------------------------------------------------------
const pair<const OptCursor, const Pattern_or_Template_List> InMemoryDB::get_matched_type(
    const string& link_type, const Params& params) const {
    auto link_type_hash = ExpressionHasher::named_type_hash(link_type);
    auto it = this->db.templates.find(link_type_hash);
    if (it != this->db.templates.end()) {
        Pattern_or_Template_List templates_matched;
        templates_matched.reserve(it->second.size());
        templates_matched.insert(templates_matched.end(), it->second.begin(), it->second.end());
        if (params.get<bool>(ParamsKeys::TOPLEVEL_ONLY).value_or(false)) {
            return {params.get<int>(ParamsKeys::CURSOR), this->_filter_non_toplevel(templates_matched)};
        }
        return {params.get<int>(ParamsKeys::CURSOR), move(templates_matched)};
    }
    return {params.get<int>(ParamsKeys::CURSOR), {}};
}

//------------------------------------------------------------------------------
const opt<const string> InMemoryDB::get_atom_type(const string& handle) const {
    auto atom = this->_get_atom(handle);
    if (atom) {
        return atom->named_type;
    }
    return nullopt;
}

//------------------------------------------------------------------------------
const unordered_map<string, any> InMemoryDB::get_atom_as_dict(const string& handle, int arity) const {
    auto node = this->_get_node(handle);
    if (node) {
        return {{"handle", node->handle}, {"type", node->named_type}, {"name", node->name}};
    }
    auto link = this->_get_link(handle);
    if (link) {
        return {{"handle", link->handle},
                {"type", link->named_type},
                {"targets", this->_build_targets_list(*link)}};
    }
}

//------------------------------------------------------------------------------
const unordered_map<string, int> InMemoryDB::count_atoms() const {
    auto node_count = this->db.node.size();
    auto link_count = this->db.link.size();
    auto atom_count = node_count + link_count;
    return {{"node_count", node_count}, {"link_count", link_count}, {"atom_count", atom_count}};
}

//------------------------------------------------------------------------------
void InMemoryDB::clear_database() {
    this->db = Database();
    this->all_named_types.clear();
    this->named_type_table.clear();
}

//------------------------------------------------------------------------------
const shared_ptr<const Node> InMemoryDB::add_node(const NodeParams& node_params) {
    auto node = this->_build_node(node_params);
    this->db.node[node->handle] = node;
    this->_update_index(*node);
    return move(node);
}

//------------------------------------------------------------------------------
const shared_ptr<const Link> InMemoryDB::add_link(const LinkParams& link_params, bool toplevel) {
    auto link = this->_build_link(link_params, toplevel);
    this->db.link[link->handle] = link;
    this->_update_index(*link);
    return move(link);
}

//------------------------------------------------------------------------------
void InMemoryDB::reindex(
    const unordered_map<string, vector<unordered_map<string, any>>>& pattern_index_templates) {
    throw runtime_error("Not implemented");
}

//------------------------------------------------------------------------------
void InMemoryDB::delete_atom(const string& handle) {
    auto it = this->db.node.find(handle);
    if (it != this->db.node.end()) {
        this->db.node.erase(it);
        auto it = this->db.incoming_set.find(handle);
        if (it != this->db.incoming_set.end()) {
            auto handles = move(it->second);
            this->db.incoming_set.erase(it);
            for (const auto& h : handles) {
                this->_delete_link_and_update_index(h);
            }
        }
    } else {
        try {
            this->_delete_link_and_update_index(handle);
        } catch (const AtomDoesNotExist& e) {
            // TODO: log error
            throw AtomDoesNotExist("Nonexistent atom", "handle: " + handle);
        }
    }
}

//------------------------------------------------------------------------------
const string InMemoryDB::create_field_index(const string& atom_type,
                                            const StringList& fields,
                                            const string& named_type,
                                            const StringList& composite_type,
                                            FieldIndexType index_type) {
    throw runtime_error("Not implemented");
}

//------------------------------------------------------------------------------
void InMemoryDB::bulk_insert(const vector<unique_ptr<const Atom>>& documents) {
    try {
        for (const auto& document : documents) {
            auto handle = document->id;
            if (auto node = dynamic_cast<const Node*>(document.get())) {
                this->db.node[handle] = make_shared<Node>(*node);
            } else if (auto link = dynamic_cast<const Link*>(document.get())) {
                this->db.link[handle] = make_shared<Link>(*link);
            }
            this->_update_index(*document);
        }
    } catch (const exception& e) {
        // TODO: log error
    }
}

//------------------------------------------------------------------------------
const vector<shared_ptr<const Atom>> InMemoryDB::retrieve_all_atoms() const {
    try {
        vector<shared_ptr<const Atom>> atoms;
        for (const auto& [_, node] : this->db.node) {
            atoms.push_back(node);
        }
        for (const auto& [_, link] : this->db.link) {
            atoms.push_back(link);
        }
        return move(atoms);
    } catch (const exception& e) {
        // TODO: log error
        throw runtime_error("Error retrieving all atoms: " + string(e.what()));
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::commit() { throw runtime_error("Not implemented"); }

// PROTECTED OR PRIVATE METHODS ////////////////////////////////////////////////////////////////////

//------------------------------------------------------------------------------
const shared_ptr<const Atom> InMemoryDB::_get_atom(const string& handle) const {
    auto node = this->_get_node(handle);
    if (node) {
        return move(node);
    }
    return this->_get_link(handle);
}

//------------------------------------------------------------------------------
const shared_ptr<const Node> InMemoryDB::_get_node(const string& handle) const {
    auto it = this->db.node.find(handle);
    if (it != this->db.node.end()) {
        return it->second;
    }
    return nullptr;
}

//------------------------------------------------------------------------------
const shared_ptr<const Link> InMemoryDB::_get_link(const string& handle) const {
    auto it = this->db.link.find(handle);
    if (it != this->db.link.end()) {
        return it->second;
    }
    return nullptr;
}

//------------------------------------------------------------------------------
const shared_ptr<const Link> InMemoryDB::_get_and_delete_link(const string& link_handle) {
    auto it = this->db.link.find(link_handle);
    if (it != this->db.link.end()) {
        auto link_document = make_shared<Link>(*it->second);
        this->db.link.erase(it);
        return move(link_document);
    }
    return nullptr;
}

//------------------------------------------------------------------------------
const ListOfAny InMemoryDB::_build_named_type_hash_template(const ListOfAny& _template) const {
    ListOfAny hash_template;
    for (const auto& element : _template) {
        if (const string* str = any_cast<string>(&element)) {
            hash_template.push_back(_build_atom_type_key_hash(*str));
        } else if (const ListOfAny* vec = any_cast<ListOfAny>(&element)) {
            hash_template.push_back(_build_named_type_hash_template(*vec));
        } else {
            // TODO: log error
            throw invalid_argument("Invalid template element type.");
        }
    }
    return move(hash_template);
}

//------------------------------------------------------------------------------
const string InMemoryDB::_build_named_type_hash_template(const string& _template) {
    return ExpressionHasher::named_type_hash(_template);
}

//------------------------------------------------------------------------------
const string InMemoryDB::_build_atom_type_key_hash(const string& name) {
    string name_hash = ExpressionHasher::named_type_hash(name);
    return ExpressionHasher::expression_hash(TYPEDEF_MARK_HASH, {name_hash, TYPE_HASH});
}

//------------------------------------------------------------------------------
void InMemoryDB::_add_atom_type(const string& atom_type_name, const string& atom_type) {
    if (this->all_named_types.find(atom_type_name) != this->all_named_types.end()) {
        return;
    }

    this->all_named_types.insert(atom_type_name);
    string name_hash = ExpressionHasher::named_type_hash(atom_type_name);
    string type_hash = atom_type == "Type" ? TYPE_HASH : ExpressionHasher::named_type_hash(atom_type);

    string key = ExpressionHasher::expression_hash(TYPEDEF_MARK_HASH, {name_hash, type_hash});

    if (this->db.atom_type.find(key) != this->db.atom_type.end()) {
        return;
    }

    string base_type_hash = ExpressionHasher::named_type_hash("Type");
    StringList composite_type = {TYPEDEF_MARK_HASH, type_hash, base_type_hash};
    string composite_type_hash = ExpressionHasher::composite_hash(composite_type);
    auto new_atom_type = make_shared<AtomType>(key, key, composite_type_hash, atom_type_name, name_hash);
    this->db.atom_type[key] = move(new_atom_type);
    this->named_type_table[name_hash] = atom_type_name;
}

//------------------------------------------------------------------------------
void InMemoryDB::_delete_atom_type(const string& name) {
    string key = this->_build_atom_type_key_hash(name);
    this->db.atom_type.erase(key);
    this->all_named_types.erase(name);
}

//------------------------------------------------------------------------------
void InMemoryDB::_add_outgoing_set(const string& key, const StringList& targets_hash) {
    this->db.outgoing_set[key] = targets_hash;
}

//------------------------------------------------------------------------------
const opt<const StringList> InMemoryDB::_get_and_delete_outgoing_set(const string& handle) {
    auto it = this->db.outgoing_set.find(handle);
    if (it != this->db.outgoing_set.end()) {
        auto handles = move(it->second);
        this->db.outgoing_set.erase(it);
        return move(handles);
    }
    return nullopt;
}

//------------------------------------------------------------------------------
void InMemoryDB::_add_incoming_set(const string& key, const StringList& targets_hash) {
    for (const auto& target_hash : targets_hash) {
        auto it = this->db.incoming_set.find(target_hash);
        if (it == this->db.incoming_set.end()) {
            this->db.incoming_set[target_hash] = StringUnorderedSet({key});
        } else {
            it->second.insert(key);
        }
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::_delete_incoming_set(const string& link_handle, const StringList& atoms_handle) {
    for (const auto& atom_handle : atoms_handle) {
        auto it = this->db.incoming_set.find(atom_handle);
        if (it != this->db.incoming_set.end()) {
            it->second.erase(link_handle);
        }
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::_add_templates(const string& composite_type_hash,
                                const string& named_type_hash,
                                const string& key,
                                const StringList& targets_hash) {
    auto it = this->db.templates.find(composite_type_hash);
    if (it != this->db.templates.end()) {
        it->second.insert(make_pair(key, targets_hash));
    } else {
        this->db.templates[composite_type_hash] =
            Database::Pattern_or_Template_Set({make_pair(key, targets_hash)});
    }

    it = this->db.templates.find(named_type_hash);
    if (it != this->db.templates.end()) {
        it->second.insert(make_pair(key, targets_hash));
    } else {
        this->db.templates[named_type_hash] =
            Database::Pattern_or_Template_Set({make_pair(key, targets_hash)});
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::_delete_templates(const Link& link_document, const StringList& targets_hash) {
    string composite_type_hash = link_document.composite_type_hash;
    string named_type_hash = link_document.named_type_hash;
    string key = link_document.id;

    auto it = this->db.templates.find(composite_type_hash);
    if (it != this->db.templates.end()) {
        it->second.erase(make_pair(key, targets_hash));
    }

    it = this->db.templates.find(named_type_hash);
    if (it != this->db.templates.end()) {
        it->second.erase(make_pair(key, targets_hash));
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::_add_patterns(const string& named_type_hash,
                               const string& key,
                               const StringList& targets_hash) {
    auto hash_list = StringList({named_type_hash});
    hash_list.insert(hash_list.end(), targets_hash.begin(), targets_hash.end());
    StringList pattern_keys = build_pattern_keys(hash_list);
    for (const auto& pattern_key : pattern_keys) {
        auto it = this->db.patterns.find(pattern_key);
        if (it == this->db.patterns.end()) {
            this->db.patterns[pattern_key] =
                Database::Pattern_or_Template_Set({make_pair(key, targets_hash)});
        } else {
            it->second.insert(make_pair(key, targets_hash));
        }
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::_delete_patterns(const Link& link_document, const StringList& targets_hash) {
    string named_type_hash = link_document.named_type_hash;
    string key = link_document.id;
    auto hash_list = StringList({named_type_hash});
    hash_list.insert(hash_list.end(), targets_hash.begin(), targets_hash.end());
    StringList pattern_keys = build_pattern_keys(hash_list);
    for (const auto& pattern_key : pattern_keys) {
        auto it = this->db.patterns.find(pattern_key);
        if (it != this->db.patterns.end()) {
            it->second.erase(make_pair(key, targets_hash));
        }
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::_delete_link_and_update_index(const string& link_handle) {
    auto link_document = this->_get_and_delete_link(link_handle);
    if (link_document) {
        this->_update_index(*link_document, true);
    }
}

//------------------------------------------------------------------------------
const Pattern_or_Template_List InMemoryDB::_filter_non_toplevel(
    const Pattern_or_Template_List& matches) const {
    if (this->db.link.empty()) {
        return matches;
    }
    Pattern_or_Template_List filtered_matched_targets;
    for (const auto& [link_handle, targets] : matches) {
        auto it = this->db.link.find(link_handle);
        if (it != this->db.link.end()) {
            if (it->second->is_top_level) {
                filtered_matched_targets.push_back({link_handle, targets});
            }
        }
    }
    return move(filtered_matched_targets);
}

//------------------------------------------------------------------------------
const vector<string> InMemoryDB::_build_targets_list(const Link& link) {
    vector<string> targets;
    for (const auto& [_, value] : link.keys) {
        targets.push_back(value);
    }
    return move(targets);
}

//------------------------------------------------------------------------------
void InMemoryDB::_delete_atom_index(const Atom& atom) {
    auto atom_handle = atom.id;
    auto it = this->db.incoming_set.find(atom_handle);
    if (it != this->db.incoming_set.end()) {
        auto handles = move(it->second);
        this->db.incoming_set.erase(it);
        for (const auto& handle : handles) {
            this->_delete_link_and_update_index(handle);
        }
    }

    auto outgoing_atoms = this->_get_and_delete_outgoing_set(atom_handle);
    if (outgoing_atoms.has_value()) {
        this->_delete_incoming_set(atom_handle, outgoing_atoms.value());
    }

    if (auto link = dynamic_cast<const Link*>(&atom)) {
        auto targets_hash = this->_build_targets_list(*link);
        this->_delete_templates(*link, targets_hash);
        this->_delete_patterns(*link, targets_hash);
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::_add_atom_index(const Atom& atom) {
    auto atom_type_name = atom.named_type;
    this->_add_atom_type(atom_type_name);
    if (auto link = dynamic_cast<const Link*>(&atom)) {
        auto handle = link->id;
        auto targets_hash = this->_build_targets_list(*link);
        this->_add_outgoing_set(handle, targets_hash);
        this->_add_incoming_set(handle, targets_hash);
        this->_add_templates(link->composite_type_hash, link->named_type_hash, handle, targets_hash);
        this->_add_patterns(link->named_type_hash, handle, targets_hash);
    }
}

//------------------------------------------------------------------------------
void InMemoryDB::_update_index(const Atom& atom, bool delete_atom) {
    if (delete_atom) {
        this->_delete_atom_index(atom);
    } else {
        this->_add_atom_index(atom);
    }
}
