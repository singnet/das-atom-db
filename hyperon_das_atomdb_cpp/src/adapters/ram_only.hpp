#pragma once

#include "database.hpp"
#include "document_types.hpp"
#include "type_aliases.hpp"

using namespace std;

namespace atomdb {

/**
 * @brief Represents an in-memory database for storing and managing atoms, nodes, and links.
 */
class Database {
   public:
    unordered_map<string, shared_ptr<AtomType>> atom_type;
    unordered_map<string, shared_ptr<Node>> node;
    unordered_map<string, shared_ptr<Link>> link;
    unordered_map<string, StringUnorderedSet> outgoing_set;
    unordered_map<string, StringUnorderedSet> incoming_set;
    unordered_map<string, StringUnorderedSet> patterns;
    unordered_map<string, StringUnorderedSet> templates;

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
    InMemoryDB() {};
    ~InMemoryDB() {
        this->all_named_types.clear();
        this->named_type_table.clear();
    };

    const string get_node_handle(const string& node_type, const string& node_name) const override;

    const string get_node_name(const string& node_handle) const override;

    const string get_node_type(const string& node_handle) const override;

    const StringList get_node_by_name(const string& node_type, const string& substring) const override;

    const StringList get_atoms_by_field(
        const vector<unordered_map<string, string>>& query) const override;

    const pair<const int, const AtomList> get_atoms_by_index(
        const string& index_id,
        const vector<unordered_map<string, string>>& query,
        int cursor = 0,
        int chunk_size = 500) const override;

    const StringList get_atoms_by_text_field(const string& text_value,
                                             const string& field = "",
                                             const string& text_index_id = "") const override;

    const StringList get_node_by_name_starting_with(const string& node_type,
                                                    const string& startswith) const override;

    const StringList get_all_nodes(const string& node_type, bool names = false) const override;

    const pair<const OptCursor, const StringList> get_all_links(
        const string& link_type, const KwArgs& kwargs = {}) const override;

    const string get_link_handle(const string& link_type,
                                 const StringList& target_handles) const override;

    const string get_link_type(const string& link_handle) const override;

    const StringUnorderedSet get_link_targets(const string& link_handle) const override;

    const StringList get_incoming_links_handles(const string& atom_handle,
                                                const KwArgs& kwargs = {}) const override;

    const vector<shared_ptr<const Atom>> get_incoming_links_atoms(
        const string& atom_handle, const KwArgs& kwargs = {}) const override;

    const StringUnorderedSet get_matched_links(const string& link_type,
                                               const StringList& target_handles,
                                               const KwArgs& kwargs = {}) const override;

    const StringUnorderedSet get_matched_type_template(const ListOfAny& _template,
                                                       const KwArgs& kwargs = {}) const override;

    const StringUnorderedSet get_matched_type(const string& link_type,
                                              const KwArgs& kwargs = {}) const override;

    const opt<const string> get_atom_type(const string& handle) const override;

    // const unordered_map<string, anything> get_atom_as_dict(const string& handle,
    //                                                   int arity = 0) const override;

    const unordered_map<string, int> count_atoms() const override;

    void clear_database() override;

    const shared_ptr<const Node> add_node(const NodeParams& node_params) override;

    const shared_ptr<const Link> add_link(const LinkParams& link_params, bool toplevel = true) override;

    void reindex(const unordered_map<string, vector<unordered_map<string, any>>>&
                     pattern_index_templates) override;

    void delete_atom(const string& handle) override;

    const string create_field_index(const string& atom_type,
                                    const StringList& fields,
                                    const string& named_type = "",
                                    const opt<const StringList>& composite_type = nullopt,
                                    FieldIndexType index_type = FieldIndexType::BINARY_TREE) override;

    void bulk_insert(const vector<shared_ptr<const Atom>>& documents) override;

    const vector<shared_ptr<const Atom>> retrieve_all_atoms() const override;

    void commit(const opt<const vector<Atom>>& buffer = nullopt) override;

   protected:
    Database db;
    set<string> all_named_types;
    unordered_map<string, string> named_type_table;

    const shared_ptr<const Atom> _get_atom(const string& handle) const override;

    const shared_ptr<const Node> _get_node(const string& handle) const;

    const shared_ptr<const Link> _get_link(const string& handle) const;

    const shared_ptr<const Link> _get_and_delete_link(const string& link_handle);

    /**
     * @brief Builds a named type hash template from the given template.
     * @param _template A vector of elements of type any representing the template.
     * @return A vector of elements of type any representing the named type hash template.
     *
     * Both `_template` and the returned vector are expected to contain elements of type
     * `string` or `vector<string>`, allowing multiple levels of nesting.
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

    const string _build_named_type_hash_template(const string& _template) const;

    const string _build_atom_type_key_hash(const string& name) const;

    void _add_atom_type(const string& atom_type_name, const string& atom_type = "Type");

    void _delete_atom_type(const string& name);

    void _add_outgoing_set(const string& key, const StringUnorderedSet& targets_hash);

    const opt<const StringUnorderedSet> _get_and_delete_outgoing_set(const string& handle);

    void _add_incoming_set(const string& key, const StringUnorderedSet& targets_hash);

    void _delete_incoming_set(const string& link_handle, const StringUnorderedSet& atoms_handles);

    void _add_templates(const string& composite_type_hash,
                        const string& named_type_hash,
                        const string& key);

    void _delete_templates(const Link& link_document);

    void _add_patterns(const string& named_type_hash,
                       const string& key,
                       const StringUnorderedSet& targets_hash);

    void _delete_patterns(const Link& link_document, const StringUnorderedSet& targets_hash);

    void _delete_link_and_update_index(const string& link_handle);

    const StringUnorderedSet _filter_non_toplevel(const StringUnorderedSet& matches) const;

    const StringUnorderedSet _build_targets_list(const Link& link) const;

    void _delete_atom_index(const Atom& atom);

    void _add_atom_index(const Atom& atom);

    void _update_index(const Atom& atom, bool delete_atom = false);

    // TODO: not used anywhere in the code - remove?
    // void _update_atom_indexes(const vector<Atom>& documents, const KwArgs& kwargs = {}) {
    //     for (const auto& document : documents) {
    //         this->_update_index(document, params);
    //     }
    // }
};

}  // namespace atomdb
