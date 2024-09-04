#ifndef _RAM_ONLY_HPP
#define _RAM_ONLY_HPP

#include "database.hpp"
#include "document_types.hpp"
#include "type_aliases.hpp"

namespace atomdb {

/**
 * @brief Represents an in-memory database for storing and managing atoms, nodes, and links.
 */
class Database {
   public:
    using Pattern = std::tuple<std::string, StringList>;
    using PatternsSet = std::unordered_set<Pattern>;
    using Template = std::tuple<std::string, StringList>;
    using TemplatesSet = std::unordered_set<Template>;

    std::unordered_map<std::string, AtomType> atom_type;
    std::unordered_map<std::string, Node> node;
    std::unordered_map<std::string, Link> link;
    std::unordered_map<std::string, StringList> outgoing_set;
    std::unordered_map<std::string, StringUnorderedSet> incoming_set;
    std::unordered_map<std::string, PatternsSet> patterns;
    std::unordered_map<std::string, TemplatesSet> templates;

    Database()
        : atom_type({}),
          node({}),
          link({}),
          outgoing_set({}),
          incoming_set({}),
          patterns({}),
          templates({}) {}

    ~Database() {
        atom_type.clear();
        node.clear();
        link.clear();
        outgoing_set.clear();
        incoming_set.clear();
        patterns.clear();
        templates.clear();
    };
};

/**
 * @brief Represents an in-memory database for storing and managing atoms, nodes, and links.
 *
 * The InMemoryDB class inherits from the AtomDB class and provides an implementation for
 * managing various types of data within an in-memory data structure. It supports operations
 * such as adding, retrieving, and clearing atoms, nodes, links, and their relationships.
 *
 * This class uses unordered maps to store different types of data, including atom types,
 * nodes, links, and sets for managing relationships between atoms. It is designed to be
 * efficient for in-memory operations, making it suitable for applications that require
 * fast access to data without the overhead of persistent storage.
 *
 * The InMemoryDB class is intended to be used in scenarios where data does not need to be
 * persisted across sessions, or where the data can be reconstructed from other sources if
 * needed. It provides a flexible and efficient way to manage complex data structures in
 * memory.
 */
class InMemoryDB : public AtomDB {
   public:
    InMemoryDB() {
        this->db = Database();
        this->all_named_types = {};
        this->named_type_table = {};
    };
    ~InMemoryDB() {
        this->all_named_types.clear();
        this->named_type_table.clear();
    };

    std::string get_node_handle(const std::string& node_type,
                                const std::string& node_name) const override;

    std::string get_node_name(const std::string& node_handle) const override;

    std::string get_node_type(const std::string& node_handle) const override;

    StringList get_node_by_name(const std::string& node_type,
                                const std::string& substring) const override;

    StringList get_atoms_by_field(
        const std::vector<std::unordered_map<std::string, std::string>>& query) const override;

    std::pair<OptCursor, AtomList> get_atoms_by_index(
        const std::string& index_id,
        const std::vector<std::unordered_map<std::string, std::string>>& query,
        int cursor = 0,
        int chunk_size = 500) const override;

    StringList get_atoms_by_text_field(const std::string& text_value,
                                       const std::string& field = "",
                                       const std::string& text_index_id = "") const override;

    StringList get_node_by_name_starting_with(const std::string& node_type,
                                              const std::string& startswith) const override;

    StringList get_all_nodes(const std::string& node_type, bool names = false) const override;

    std::pair<OptCursor, StringList> get_all_links(const std::string& link_type,
                                                   const Params& params = {}) const override;

    std::string get_link_handle(const std::string& link_type,
                                const StringList& target_handles) const override;

    std::string get_link_type(const std::string& link_handle) const override;

    StringList get_link_targets(const std::string& link_handle) const override;

    bool is_ordered(const std::string& link_handle) const override;

    std::pair<OptCursor, StringUnorderedSet> get_incoming_links_handles(
        const std::string& atom_handle, const Params& params = {}) const override;

    std::pair<OptCursor, AtomList> get_incoming_links_atoms(const std::string& atom_handle,
                                                            const Params& params = {}) const override;

    std::pair<OptCursor, Pattern_or_Template_List> get_matched_links(
        const std::string& link_type,
        const StringList& target_handles,
        const Params& params = {}) const override;

    std::pair<OptCursor, Pattern_or_Template_List> get_matched_type_template(
        const ListOfAny& _template, const Params& params = {}) const override;

    std::pair<OptCursor, Pattern_or_Template_List> get_matched_type(
        const std::string& link_type, const Params& params = {}) const override;

    opt<std::string> get_atom_type(const std::string& handle) const override;

    std::unordered_map<std::string, std::any> get_atom_as_dict(const std::string& handle,
                                                               int arity = 0) const override;

    std::unordered_map<std::string, int> count_atoms() const override;

    void clear_database() override;

    opt<Node> add_node(const Params& node_params) override;

    opt<Link> add_link(const Params& link_params, bool toplevel = true) override;

    void reindex(
        const std::unordered_map<std::string, std::vector<std::unordered_map<std::string, std::any>>>&
            pattern_index_templates) override;

    void delete_atom(const std::string& handle) override;

    std::string create_field_index(const std::string& atom_type,
                                   const StringList& fields,
                                   const std::string& named_type = "",
                                   const StringList& composite_type = {},
                                   FieldIndexType index_type = FieldIndexType::BINARY_TREE) override;

    void bulk_insert(const std::vector<Atom>& documents) override;

    std::vector<Atom> retrieve_all_atoms() const override;

    void commit() override;

   protected:
    Database db;
    std::set<std::string> all_named_types;
    std::unordered_map<std::string, std::string> named_type_table;

    opt<const Atom&> _get_atom(const std::string& handle) const override;

    opt<const Node&> _get_node(const std::string& handle) const;

    opt<const Link&> _get_link(const std::string& handle) const;

    opt<const Link> _get_and_delete_link(const std::string& link_handle);

    /**
     * @brief Builds a named type hash template from the given template.
     * @param _template A vector of elements of type std::any representing the template.
     * @return A vector of elements of type std::any representing the named type hash template.
     *
     * Both `_template` and the returned vector are expected to contain elements of type
     * `std::string` or `std::vector<std::string>`, allowing multiple levels of nesting.
     * Example:
     *
     * @code
     * ```
     * {
     *     "986a251e2a3e4c19a856a279dab2495f",
     *     "3399d0d460c849c892f223b97d79635a",
     *     {
     *         "81ab7b04e75642a2a8acfdbb7033dc23",
     *         "4aac2093cb6040e7acf09af2f58e2e22"
     *     },
     *     "fc16eec8ae914f818a3342534632de33",
     *     {
     *         "5c7e4ba338ef47abb41a1f497c35ea57",
     *         {
     *             "9cf28d0712fc4759adcde1ec4801b53a",
     *             "b1b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3"
     *         }
     *     }
     * }
     * ```
     * @endcode
     */
    const ListOfAny _build_named_type_hash_template(const ListOfAny& _template) const;

    const std::string _build_named_type_hash_template(const std::string& _template);

    static const std::string _build_atom_type_key_hash(const std::string& name);

    void _add_atom_type(const std::string& atom_type_name, const std::string& atom_type = "Type");

    void _delete_atom_type(const std::string& name);

    void _add_outgoing_set(const std::string& key, const StringList& targets_hash);

    const opt<StringList> _get_and_delete_outgoing_set(const std::string& handle);

    void _add_incoming_set(const std::string& key, const StringList& targets_hash);

    void _delete_incoming_set(const std::string& link_handle, const StringList& atoms_handle);

    void _add_templates(const std::string& composite_type_hash,
                        const std::string& named_type_hash,
                        const std::string& key,
                        const StringList& targets_hash);

    void _delete_templates(const Link& link_document, const StringList& targets_hash);

    void _add_patterns(const std::string& named_type_hash,
                       const std::string& key,
                       const StringList& targets_hash);

    void _delete_patterns(const Link& link_document, const StringList& targets_hash);

    void _delete_link_and_update_index(const std::string& link_handle);

    const Pattern_or_Template_List _filter_non_toplevel(const Pattern_or_Template_List& matches) const;

    static const std::vector<std::string> _build_targets_list(const Link& link);

    void _delete_atom_index(const Atom& atom);

    void _add_atom_index(const Atom& atom);

    void _update_index(const Atom& atom, const Params& params = {});

    // TODO: not used anywhere in the code - remove?
    // void _update_atom_indexes(const std::vector<Atom>& documents, const Params& params = {}) {
    //     for (const auto& document : documents) {
    //         this->_update_index(document, params);
    //     }
    // }
};

}  // namespace atomdb

#endif  // _RAM_ONLY_HPP
