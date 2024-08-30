#ifndef _RAM_ONLY
#define _RAM_ONLY

#include <any>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "../basic_types.hpp"
#include "../database.hpp"
#include "../utils/expression_hasher.hpp"

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

    const Atom& _get_atom(const std::string& handle) override {
        return Atom(handle, handle, handle, handle);
    }

    std::optional<const Link> _get_link(const std::string& handle) const {
        if (this->db.link.find(handle) != this->db.link.end()) {
            return this->db.link.at(handle);
        }
        return std::nullopt;
    }

    std::optional<const Link> _get_and_delete_link(const std::string& link_handle) {
        auto it = this->db.link.find(link_handle);
        if (it != this->db.link.end()) {
            auto link_document = std::move(it->second);
            this->db.link.erase(it);
            return link_document;
        }
        return std::nullopt;
    }

    const StringList _build_named_type_hash_template(  // TODO: revisit this
        const StringList& _template) {
        StringList hash_template;
        for (const auto& element : _template) {
            hash_template.push_back(_build_named_type_hash_template(element));
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

    std::optional<StringList> _get_and_delete_outgoing_set(const std::string& handle) {
        if (this->db.outgoing_set.find(handle) != this->db.outgoing_set.end()) {
            auto outgoing_set = this->db.outgoing_set.at(handle);
            this->db.outgoing_set.erase(handle);
            return outgoing_set;
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
        std::string key = link_document._id;

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
        const std::string& key, const StringList& targets_hash) {
        StringList pattern_keys = build_pattern_keys({named_type_hash, targets_hash});

        for (const auto& pattern_key : pattern_keys) {
            this->db.patterns[pattern_key].insert(std::make_tuple(key, targets_hash));
        }
    }

    void _delete_patterns(const Link& link_document, const StringList& targets_hash) {
        std::string named_type_hash = link_document.named_type_hash;
        std::string key = link_document._id;

        StringList pattern_keys = build_pattern_keys({named_type_hash, targets_hash});

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
            this->_update_index(link_document.value(), true);
        }
    }

    const MatchedTargetsList& _filter_non_toplevel(const MatchedTargetsList& matches) {
        if (this->db.link.empty()) {
            return matches;
        }

        MatchedTargetsList filtered_matched_targets;
        for (const auto& [link_handle, targets] : matches) {
            if (this->db.link.find(link_handle) != this->db.link.end()) {
                if (this->db.link.at(link_handle).is_top_level) {
                    filtered_matched_targets.push_back({link_handle, targets});
                }
            }
        }
        return filtered_matched_targets;
    }

   public:
    InMemoryDB() {
        this->db = Database();
        this->all_named_types = {};
        this->named_type_table = {};
    };
    ~InMemoryDB() {};

    std::string get_node_handle(const std::string& node_type, const std::string& node_name) override {
        return AtomDB::build_node_handle(node_type, node_name);
    }

    std::string get_node_name(const std::string& node_handle) override {
        return node_handle;
    }

    std::string get_node_type(const std::string& node_handle) override {
        return node_handle;
    }

    std::string get_link_handle(
        const std::string& link_type, const StringList& target_handles) override {
        return AtomDB::build_link_handle(link_type, target_handles);
    }

    void commit() override {}

    StringList get_atoms_by_text_field(
        const std::string& text_value,
        const std::string& field = "",
        const std::string& text_index_id = "") override {
        return {};
    }

    StringList get_node_by_name_starting_with(
        const std::string& node_type, const std::string& startswith) override {
        return {};
    }

    StringList get_all_nodes(const std::string& node_type, bool names = false) override {
        return {};
    }

    std::pair<int, StringList> get_all_links(const std::string& link_type) override {
        return {};
    }

    std::string get_link_type(const std::string& link_handle) override {
        return link_handle;
    }

    StringList get_link_targets(const std::string& link_handle) override {
        return {link_handle};
    }

    bool is_ordered(const std::string& link_handle) override {
        return false;
    }

    std::pair<int, IncomingLinks> get_incoming_links(const std::string& atom_handle) override {
        return {0, {}};
    }

    MatchedLinksResult get_matched_links(
        const std::string& link_type, const StringList& target_handles) override {
        return {0, {}};
    }

    MatchedTypesResult get_matched_type_template(const StringList& template_) override {
        return {0, {}};
    }

    MatchedTypesResult get_matched_type(const std::string& link_type) override {
        return {0, {}};
    }

    std::string get_atom_type(const std::string& handle) override {
        return handle;
    }

    std::unordered_map<std::string, std::string> get_atom_as_dict(
        const std::string& handle, int arity = 0) override {
        return {{handle, handle}};
    }

    std::unordered_map<std::string, int> count_atoms() override {
        return {{"node", 0}, {"link", 0}, {"total", 0}};
    }

    void clear_database() override {}

    Node add_node(const std::string& node_type, const std::string& node_name) override {
        return Node(node_name, node_name, node_name, node_type, node_name);
    }

    Link add_link(
        const std::string& link_type,
        const std::vector<Atom>& targets,
        bool toplevel = true) override {
        return this->_build_link(link_type, targets, toplevel);
    }

    void reindex(
        const std::unordered_map<
            std::string,
            std::vector<
                std::unordered_map<std::string, void*>>>& pattern_index_templates) override {}

    void delete_atom(const std::string& handle) override {}

    std::string create_field_index(
        const std::string& atom_type,
        const StringList& fields, const std::string& named_type = "",
        const StringList& composite_type = {},
        FieldIndexType index_type = FieldIndexType::BINARY_TREE) override {
        return atom_type;
    }

    void bulk_insert(const std::vector<Atom>& documents) override {}

    std::vector<Atom> retrieve_all_atoms() override {
        return {};
    }

    void commit() override {}
};

#endif  // _RAM_ONLY
