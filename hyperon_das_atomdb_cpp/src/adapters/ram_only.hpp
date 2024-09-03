#ifndef _RAM_ONLY
#define _RAM_ONLY

#include <algorithm>

#include "database.hpp"
#include "document_types.hpp"
#include "exceptions.hpp"
#include "type_aliases.hpp"
#include "utils/expression_hasher.hpp"
#include "utils/patterns.hpp"

class Database {
   public:
    /**
     * @brief Class representing the structure of the in-memory database.
     */

    using Pattern = std::tuple<std::string, StringList>;
    using PatternsSet = std::unordered_set<Pattern>;
    using Template = std::tuple<std::string, StringList>;
    using TemplatesSet = std::unordered_set<Template>;

    // Member variables
    std::unordered_map<std::string, AtomType> atom_type = {};
    std::unordered_map<std::string, Node> node = {};
    std::unordered_map<std::string, Link> link = {};
    std::unordered_map<std::string, StringList> outgoing_set = {};
    std::unordered_map<std::string, StringUnorderedSet> incoming_set = {};
    std::unordered_map<std::string, PatternsSet> patterns = {};
    std::unordered_map<std::string, TemplatesSet> templates = {};

    // Constructor
    Database() {
        atom_type = {};
        node = {};
        link = {};
        outgoing_set = {};
        incoming_set = {};
        patterns = {};
        templates = {};
    };
};

/**
 * @brief An in-memory database implementation.
 *
 * This class provides an in-memory implementation of the AtomDB interface.
 */
class InMemoryDB : public AtomDB {
   protected:
    Database db = Database();
    std::set<std::string> all_named_types = {};
    std::unordered_map<std::string, std::string> named_type_table = {};

    opt<const Atom&> _get_atom(const std::string& handle) const override {
        return this->_get_node(handle).value_or(this->_get_link(handle));
    }

    opt<const Node&> _get_node(const std::string& handle) const {
        auto it = this->db.node.find(handle);
        if (it != this->db.node.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    opt<const Link&> _get_link(const std::string& handle) const {
        auto it = this->db.link.find(handle);
        if (it != this->db.link.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    opt<const Link> _get_and_delete_link(const std::string& link_handle) {
        auto it = this->db.link.find(link_handle);
        if (it != this->db.link.end()) {
            auto link_document = std::move(it->second);
            this->db.link.erase(it);
            return link_document;
        }
        return std::nullopt;
    }

    const std::vector<std::any> _build_named_type_hash_template(
        const std::vector<std::any>& _template) const {
        std::vector<std::any> hash_template;
        for (const auto& element : _template) {
            if (const std::string* str = std::any_cast<std::string>(&element)) {
                hash_template.push_back(_build_atom_type_key_hash(*str));
            } else if (
                const std::vector<std::any>* vec = std::any_cast<std::vector<std::any>>(&element)) {
                hash_template.push_back(_build_named_type_hash_template(*vec));
            } else {
                // TODO: log error
                throw std::invalid_argument("Invalid template element type.");
            }
        }
        return hash_template;
    }

    const std::string _build_named_type_hash_template(const std::string& _template) {
        return ExpressionHasher::named_type_hash(_template);
    }

    static const std::string _build_atom_type_key_hash(const std::string& name) {
        std::string name_hash = ExpressionHasher::named_type_hash(name);
        std::string type_hash = ExpressionHasher::named_type_hash("Type");
        std::string typedef_mark_hash = ExpressionHasher::named_type_hash(":");
        return ExpressionHasher::expression_hash(typedef_mark_hash, {name_hash, type_hash});
    }

    void _add_atom_type(const std::string& atom_type_name, const std::string& atom_type = "Type") {
        if (this->all_named_types.find(atom_type_name) != this->all_named_types.end()) {
            return;
        }

        this->all_named_types.insert(atom_type_name);
        std::string name_hash = ExpressionHasher::named_type_hash(atom_type_name);
        std::string type_hash = ExpressionHasher::named_type_hash(atom_type);
        std::string typedef_mark_hash = ExpressionHasher::named_type_hash(":");

        std::string key = ExpressionHasher::expression_hash(
            typedef_mark_hash, {name_hash, type_hash});

        if (this->db.atom_type.find(key) != this->db.atom_type.end()) {
            return;
        }

        std::string base_type_hash = ExpressionHasher::named_type_hash("Type");
        StringList composite_type = {typedef_mark_hash, type_hash, base_type_hash};
        std::string composite_type_hash = ExpressionHasher::composite_hash(composite_type);
        AtomType new_atom_type = AtomType(key, key, composite_type_hash, atom_type_name, name_hash);
        this->db.atom_type[key] = new_atom_type;
        this->named_type_table[name_hash] = atom_type_name;
    }

    void _delete_atom_type(const std::string& name) {
        std::string key = this->_build_atom_type_key_hash(name);
        this->db.atom_type.erase(key);
        this->all_named_types.erase(name);
    }

    void _add_outgoing_set(const std::string& key, const StringList& targets_hash) {
        this->db.outgoing_set[key] = targets_hash;
    }

    const opt<StringList> _get_and_delete_outgoing_set(const std::string& handle) {
        auto it = this->db.outgoing_set.find(handle);
        if (it != this->db.outgoing_set.end()) {
            auto handles = std::move(it->second);
            this->db.outgoing_set.erase(it);
            return handles;
        }
        return std::nullopt;
    }

    void _add_incoming_set(const std::string& key, const StringList& targets_hash) {
        for (const auto& target_hash : targets_hash) {
            this->db.incoming_set[target_hash].insert(key);
        }
    }

    void _delete_incoming_set(const std::string& link_handle, const StringList& atoms_handle) {
        for (const auto& atom_handle : atoms_handle) {
            if (this->db.incoming_set.find(atom_handle) != this->db.incoming_set.end()) {
                this->db.incoming_set[atom_handle].erase(link_handle);
            }
        }
    }

    void _add_templates(
        const std::string& composite_type_hash,
        const std::string& named_type_hash,
        const std::string& key,
        const StringList& targets_hash) {
        auto template_composite_type_hash = this->db.templates.find(composite_type_hash);
        auto template_named_type_hash = this->db.templates.find(named_type_hash);

        if (template_composite_type_hash != this->db.templates.end()) {
            template_composite_type_hash->second.insert(std::make_tuple(key, targets_hash));
        } else {
            this->db.templates[composite_type_hash] = Database::TemplatesSet(
                {std::make_tuple(key, targets_hash)});
        }

        if (template_named_type_hash != this->db.templates.end()) {
            template_named_type_hash->second.insert(std::make_tuple(key, targets_hash));
        } else {
            this->db.templates[named_type_hash] = Database::PatternsSet(
                {std::make_tuple(key, targets_hash)});
        }
    }

    void _delete_templates(const Link& link_document, const StringList& targets_hash) {
        std::string composite_type_hash = link_document.composite_type_hash;
        std::string named_type_hash = link_document.named_type_hash;
        std::string key = link_document.id;

        auto template_composite_type_hash = this->db.templates.find(composite_type_hash);
        if (template_composite_type_hash != this->db.templates.end()) {
            template_composite_type_hash->second.erase(std::make_tuple(key, targets_hash));
        }

        auto template_named_type_hash = this->db.templates.find(named_type_hash);
        if (template_named_type_hash != this->db.templates.end()) {
            template_named_type_hash->second.erase(std::make_tuple(key, targets_hash));
        }
    }

    void _add_patterns(
        const std::string& named_type_hash,
        const std::string& key,
        const StringList& targets_hash) {
        auto hash_list = StringList({named_type_hash});
        hash_list.insert(hash_list.end(), targets_hash.begin(), targets_hash.end());
        StringList pattern_keys = build_pattern_keys(hash_list);
        for (const auto& pattern_key : pattern_keys) {
            this->db.patterns[pattern_key].insert(std::make_tuple(key, targets_hash));
        }
    }

    void _delete_patterns(const Link& link_document, const StringList& targets_hash) {
        std::string named_type_hash = link_document.named_type_hash;
        std::string key = link_document.id;

        auto hash_list = StringList({named_type_hash});
        hash_list.insert(hash_list.end(), targets_hash.begin(), targets_hash.end());
        StringList pattern_keys = build_pattern_keys(hash_list);

        for (const auto& pattern_key : pattern_keys) {
            auto pattern = this->db.patterns.find(pattern_key);
            if (pattern != this->db.patterns.end()) {
                pattern->second.erase(std::make_tuple(key, targets_hash));
            }
        }
    }

    void _delete_link_and_update_index(const std::string& link_handle) {
        auto link_document = this->_get_and_delete_link(link_handle);
        if (link_document.has_value()) {
            this->_update_index(
                link_document.value(), Params({{FlagsParams::DELETE_ATOM, true}}));
        }
    }

    const Pattern_or_Template_List& _filter_non_toplevel(
        const Pattern_or_Template_List& matches) const {
        if (this->db.link.empty()) {
            return matches;
        }

        Pattern_or_Template_List filtered_matched_targets;
        for (const auto& [link_handle, targets] : matches) {
            auto it = this->db.link.find(link_handle);
            if (it != this->db.link.end()) {
                if (it->second.is_top_level) {
                    filtered_matched_targets.push_back({link_handle, targets});
                }
            }
        }
        return filtered_matched_targets;
    }

    static const std::vector<std::string>& _build_targets_list(const Link& link) {
        std::vector<std::string> targets;
        for (const auto& [_, value] : link.keys) {
            targets.push_back(value);
        }
        return targets;
    }

    // TODO: not used in the code
    // void _update_atom_indexes(const std::vector<Atom>& documents, const Params& params) {
    //     for (const auto& document : documents) {
    //         this->_update_index(document, params);
    //     }
    // }

    void _delete_atom_index(const Atom& atom) {
        auto atom_handle = atom.id;
        auto it = this->db.incoming_set.find(atom_handle);
        if (it != this->db.incoming_set.end()) {
            auto handles = std::move(it->second);
            this->db.incoming_set.erase(it);
            for (const auto& handle : handles) {
                this->_delete_link_and_update_index(handle);
            }
        }

        auto outgoing_atoms = this->_get_and_delete_outgoing_set(atom_handle);
        if (outgoing_atoms.has_value()) {
            this->_delete_incoming_set(atom_handle, outgoing_atoms.value());
        }

        if (const Link* link = dynamic_cast<const Link*>(&atom)) {
            auto targets_hash = this->_build_targets_list(*link);
            this->_delete_templates(*link, targets_hash);
            this->_delete_patterns(*link, targets_hash);
        }
    }

    void _add_atom_index(const Atom& atom) {
        auto atom_type_name = atom.named_type;
        this->_add_atom_type(atom_type_name);
        if (const Link* link = dynamic_cast<const Link*>(&atom)) {
            auto handle = link->id;
            auto targets_hash = this->_build_targets_list(*link);
            this->_add_outgoing_set(handle, targets_hash);
            this->_add_incoming_set(handle, targets_hash);
            this->_add_templates(
                link->composite_type_hash, link->named_type_hash, handle, targets_hash);
            this->_add_patterns(link->named_type_hash, handle, targets_hash);
        }
    }

    void _update_index(const Atom& atom, const Params& params = {}) {
        if (params.get<bool>(FlagsParams::DELETE_ATOM).value_or(false)) {
            this->_delete_atom_index(atom);
        } else {
            this->_add_atom_index(atom);
        }
    }

   public:
    InMemoryDB() {
        this->db = Database();
        this->all_named_types = {};
        this->named_type_table = {};
    };
    ~InMemoryDB() {};

    std::string get_node_handle(
        const std::string& node_type, const std::string& node_name) const override {
        auto node_handle = AtomDB::build_node_handle(node_type, node_name);
        if (this->db.node.find(node_handle) != this->db.node.end()) {
            return node_handle;
        }
        throw AtomDoesNotExist("Nonexistent atom", node_type + node_name);
    }

    std::string get_node_name(const std::string& node_handle) const override {
        auto it = this->db.node.find(node_handle);
        if (it != this->db.node.end()) {
            return it->second.name;
        }
        throw AtomDoesNotExist("Nonexistent atom", "node_handle: " + node_handle);
    }

    std::string get_node_type(const std::string& node_handle) const override {
        auto it = this->db.node.find(node_handle);
        if (it != this->db.node.end()) {
            return it->second.named_type;
        }
        throw AtomDoesNotExist("Nonexistent atom", "node_handle: " + node_handle);
    }

    StringList get_node_by_name(
        const std::string& node_type, const std::string& substring) const override {
        auto node_type_hash = ExpressionHasher::named_type_hash(node_type);
        StringList node_handles;
        for (const auto& [key, node] : this->db.node) {
            if (node.name.find(substring) != std::string::npos &&
                node_type_hash == node.composite_type_hash) {
                node_handles.push_back(key);
            }
        }
        return node_handles;
    }

    StringList get_atoms_by_field(
        const std::vector<std::unordered_map<std::string, std::string>>& query) const override {
        throw std::runtime_error("Not implemented");
    }

    std::pair<OptionalCursor, AtomList> get_atoms_by_index(
        const std::string& index_id,
        const std::vector<std::unordered_map<std::string, std::string>>& query,
        int cursor = 0,
        int chunk_size = 500) const override {
        throw std::runtime_error("Not implemented");
    }

    StringList get_atoms_by_text_field(
        const std::string& text_value,
        const std::string& field = "",
        const std::string& text_index_id = "") const override {
        throw std::runtime_error("Not implemented");
    }

    StringList get_node_by_name_starting_with(
        const std::string& node_type, const std::string& startswith) const override {
        throw std::runtime_error("Not implemented");
    }

    StringList get_all_nodes(const std::string& node_type, bool names = false) const override {
        auto node_type_hash = ExpressionHasher::named_type_hash(node_type);
        StringList node_handles;
        if (names) {
            for (const auto& [_, node] : this->db.node) {
                if (node.composite_type_hash == node_type_hash) {
                    node_handles.push_back(node.name);
                }
            }
        } else {
            for (const auto& [handle, node] : this->db.node) {
                if (node.composite_type_hash == node_type_hash) {
                    node_handles.push_back(handle);
                }
            }
        }
        return node_handles;
    }

    std::pair<OptionalCursor, StringList> get_all_links(
        const std::string& link_type, const Params& params) const override {
        StringList link_handles;
        for (const auto& [_, link] : this->db.link) {
            if (link.named_type == link_type) {
                link_handles.push_back(link.id);
            }
        }
        return {params.get<int>(FlagsParams::CURSOR), link_handles};
    }

    std::string get_link_handle(
        const std::string& link_type, const StringList& target_handles) const override {
        auto link_handle = AtomDB::build_link_handle(link_type, target_handles);
        if (this->db.link.find(link_handle) != this->db.link.end()) {
            return link_handle;
        }
        std::string target_handles_str = "[";
        for (const auto& target_handle : target_handles) {
            target_handles_str += target_handle + ",";
        }
        target_handles_str += "]";
        throw AtomDoesNotExist("Nonexistent atom", link_type + ":" + target_handles_str);
    }

    std::string get_link_type(const std::string& link_handle) const override {
        auto link = this->_get_link(link_handle);
        if (link.has_value()) {
            return link.value().named_type;
        }
        throw AtomDoesNotExist("Nonexistent atom", "link_handle: " + link_handle);
    }

    StringList get_link_targets(const std::string& link_handle) const override {
        auto it = this->db.outgoing_set.find(link_handle);
        if (it != this->db.outgoing_set.end()) {
            return it->second;
        }
        throw AtomDoesNotExist("Nonexistent atom", "link_handle: " + link_handle);
    }

    bool is_ordered(const std::string& link_handle) const override {
        if (this->_get_link(link_handle).has_value()) {
            return true;
        }
        throw AtomDoesNotExist("Nonexistent atom", "link_handle: " + link_handle);
    }

    std::pair<OptionalCursor, StringUnorderedSet> get_incoming_links_handles(
        const std::string& atom_handle, const Params& params) const override {
        auto it = this->db.incoming_set.find(atom_handle);
        auto links = it != this->db.incoming_set.end() ? it->second : StringUnorderedSet();
        return {params.get<int>(FlagsParams::CURSOR), links};
    }

    std::pair<OptionalCursor, AtomList> get_incoming_links_atoms(
        const std::string& atom_handle, const Params& params) const override {
        const auto& [cursor, links] = this->get_incoming_links_handles(atom_handle, params);
        AtomList atoms;
        for (const auto& link_handle : links) {
            atoms.push_back(this->get_atom(link_handle, params));
        }
        return {cursor, atoms};
    }

    MatchedElements get_matched_links(const std::string& link_type,
                                      const StringList& target_handles,
                                      const Params& params) const override {
        if (link_type != WILDCARD && std::find(target_handles.begin(),
                                               target_handles.end(),
                                               WILDCARD) == target_handles.end()) {
            return {
                params.get<int>(FlagsParams::CURSOR),
                {std::make_tuple(this->get_link_handle(link_type, target_handles), std::nullopt)}};
        }

        auto link_type_hash =
            link_type == WILDCARD ? WILDCARD : ExpressionHasher::named_type_hash(link_type);

        if (std::find(UNORDERED_LINK_TYPES.begin(), UNORDERED_LINK_TYPES.end(), link_type) !=
            UNORDERED_LINK_TYPES.end()) {
            // TODO: log error
            throw InvalidOperationException(
                "Queries with unordered links are not implemented",
                "link_type: " + link_type);
        }

        StringList handles({link_type_hash});
        handles.insert(handles.end(), target_handles.begin(), target_handles.end());
        auto pattern_hash = ExpressionHasher::composite_hash(handles);

        Pattern_or_Template_List patterns_matched;
        auto it = this->db.patterns.find(pattern_hash);
        if (it != this->db.patterns.end()) {
            for (const auto& pattern_tuple : it->second) {
                patterns_matched.push_back(pattern_tuple);
            }
        }

        if (params.get<bool>(FlagsParams::TOPLEVEL_ONLY).value_or(false)) {
            return {
                params.get<int>(FlagsParams::CURSOR),
                this->_filter_non_toplevel(patterns_matched)};
        }

        return {params.get<int>(FlagsParams::CURSOR), patterns_matched};
    }

    MatchedElements get_matched_type_template(
        const std::vector<std::any>& _template, const Params& params) const override {
        auto hash_base = this->_build_named_type_hash_template(_template);
        auto template_hash = ExpressionHasher::composite_hash(hash_base);
        auto it = this->db.templates.find(template_hash);
        if (it != this->db.templates.end()) {
            Pattern_or_Template_List templates_matched;
            templates_matched.reserve(it->second.size());
            templates_matched.insert(templates_matched.end(), it->second.begin(), it->second.end());
            if (params.get<bool>(FlagsParams::TOPLEVEL_ONLY).value_or(false)) {
                return {params.get<int>(FlagsParams::CURSOR),
                        this->_filter_non_toplevel(templates_matched)};
            }
            return {params.get<int>(FlagsParams::CURSOR), templates_matched};
        }
        return {params.get<int>(FlagsParams::CURSOR), {}};
    }

    MatchedElements get_matched_type(
        const std::string& link_type, const Params& params) const override {
        auto link_type_hash = ExpressionHasher::named_type_hash(link_type);
        auto it = this->db.templates.find(link_type_hash);
        if (it != this->db.templates.end()) {
            Pattern_or_Template_List templates_matched;
            templates_matched.reserve(it->second.size());
            templates_matched.insert(templates_matched.end(), it->second.begin(), it->second.end());
            if (params.get<bool>(FlagsParams::TOPLEVEL_ONLY).value_or(false)) {
                return {params.get<int>(FlagsParams::CURSOR),
                        this->_filter_non_toplevel(templates_matched)};
            }
            return {params.get<int>(FlagsParams::CURSOR), templates_matched};
        }
        return {params.get<int>(FlagsParams::CURSOR), {}};
    }

    opt<std::string> get_atom_type(const std::string& handle) const override {
        auto atom = this->_get_atom(handle);
        if (atom.has_value()) {
            return atom.value().named_type;
        }
        return std::nullopt;
    }

    std::unordered_map<std::string, std::any> get_atom_as_dict(
        const std::string& handle, int arity = 0) const override {
        auto node = this->_get_node(handle);
        if (node.has_value()) {
            return {
                {"handle", node->value().handle},
                {"type", node->value().named_type},
                {"name", node->value().name}};
        }
        auto link = this->_get_link(handle);
        if (link.has_value()) {
            return {
                {"handle", link->value().handle},
                {"type", link->value().named_type},
                {"targets", this->_build_targets_list(link.value())}};
        }
    }

    std::unordered_map<std::string, int> count_atoms() const override {
        auto node_count = this->db.node.size();
        auto link_count = this->db.link.size();
        auto atom_count = node_count + link_count;
        return {
            {"node_count", node_count},
            {"link_count", link_count},
            {"atom_count", atom_count}};
    }

    void clear_database() override {
        this->db = Database();
        this->all_named_types.clear();
        this->named_type_table.clear();
    }

    opt<Node> add_node(const Params& node_params) override {
        auto node = this->_build_node(node_params);
        if (!node.has_value()) {
            return std::nullopt;
        }
        this->db.node[node.value().handle] = node.value();
        this->_update_index(node.value());
        return node;
    }

    opt<Link> add_link(const Params& link_params, bool toplevel = true) override {
        auto link = this->_build_link(link_params, toplevel);
        if (!link.has_value()) {
            return std::nullopt;
        }
        this->db.link[link.value().handle] = link.value();
        this->_update_index(link.value());
        return link;
    }

    void reindex(
        const std::unordered_map<
            std::string,
            std::vector<
                std::unordered_map<std::string, void*>>>& pattern_index_templates) override {
        throw std::runtime_error("Not implemented");
    }

    void delete_atom(const std::string& handle) override {
        auto it = this->db.node.find(handle);
        if (it != this->db.node.end()) {
            this->db.node.erase(it);
            auto it = this->db.incoming_set.find(handle);
            if (it != this->db.incoming_set.end()) {
                for (const auto& h : it->second) {
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

    std::string create_field_index(
        const std::string& atom_type,
        const StringList& fields, const std::string& named_type = "",
        const StringList& composite_type = {},
        FieldIndexType index_type = FieldIndexType::BINARY_TREE) override {
        throw std::runtime_error("Not implemented");
    }

    void bulk_insert(const std::vector<Atom>& documents) override {
        try {
            for (const auto& document : documents) {
                auto handle = document.id;
                if (const Node* node = dynamic_cast<const Node*>(&document)) {
                    this->db.node[handle] = *node;
                } else if (const Link* link = dynamic_cast<const Link*>(&document)) {
                    this->db.link[handle] = *link;
                }
                this->_update_index(document);
            }
        } catch (const std::exception& e) {
            // TODO: log error
        }
    }

    std::vector<Atom> retrieve_all_atoms() const override {
        try {
            std::vector<Atom> atoms;
            for (const auto& [_, node] : this->db.node) {
                atoms.push_back(node);
            }
            for (const auto& [_, link] : this->db.link) {
                atoms.push_back(link);
            }
            return atoms;
        } catch (const std::exception& e) {
            // TODO: log error
            throw std::runtime_error("Error retrieving all atoms: " + std::string(e.what()));
        }
    }

    void commit() override {
        throw std::runtime_error("Not implemented");
    }
};

#endif  // _RAM_ONLY
