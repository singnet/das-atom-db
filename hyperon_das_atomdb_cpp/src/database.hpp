#ifndef _DATABASE_HPP
#define _DATABASE_HPP

#include <variant>

#include "constants.hpp"
#include "document_types.hpp"
#include "exceptions.hpp"
#include "type_aliases.hpp"
#include "utils/expression_hasher.hpp"
#include "utils/params.hpp"

using namespace std;

namespace atomdb {

class AtomParams {
   public:
    using CustomAttributesInitializer = vector<pair<string, any>>;

    AtomParams() = default;
    AtomParams(const string& type, const CustomAttributesInitializer& custom_attributes = {})
        : type(type) {
        if (type.empty()) {
            throw invalid_argument("'type' cannot be empty.");
        }
        for (const auto& [key, value] : custom_attributes) {
            this->custom_attributes.set(key, value);
        }
    }

    virtual ~AtomParams() = default;

    string type;
    Params custom_attributes = {};
};

class NodeParams : public AtomParams {
   public:
    NodeParams() = default;
    NodeParams(const string& type,
               const string& name,
               const CustomAttributesInitializer& custom_attributes = {})
        : name(name), AtomParams(type, custom_attributes) {
        if (name.empty()) {
            throw invalid_argument("'name' cannot be empty.");
        }
    }

    string name;
};

class LinkParams : public AtomParams {
   public:
    using Target = variant<NodeParams, LinkParams>;
    using Targets = vector<Target>;

    LinkParams() = default;
    LinkParams(const string& type, const CustomAttributesInitializer& custom_attributes = {})
        : AtomParams(type, custom_attributes) {}
    LinkParams(const string& type,
               const Targets& targets,
               const CustomAttributesInitializer& custom_attributes = {})
        : LinkParams(type, custom_attributes) {
        if (targets.empty()) {
            throw invalid_argument("'targets' cannot be empty.");
        }
        this->targets.reserve(targets.size());
        this->targets.insert(this->targets.end(), targets.begin(), targets.end());
    }

    void add_target(const Target& atom) { targets.push_back(atom); }

    static bool is_node(const Target& target) { return holds_alternative<NodeParams>(target); }
    static bool is_link(const Target& target) { return holds_alternative<LinkParams>(target); }
    static const NodeParams& as_node(const Target& target) { return get<NodeParams>(target); }
    static const LinkParams& as_link(const Target& target) { return get<LinkParams>(target); }

    Targets targets = {};
};

class AtomDB {
   public:
    virtual ~AtomDB() = default;

    /**
     * @brief Build a node handle with the specified type and name.
     * @param node_type The node type.
     * @param node_name The node name.
     * @return The node handle.
     */
    static const string build_node_handle(const string& node_type, const string& node_name) {
        return ExpressionHasher::terminal_hash(node_type, node_name);
    }

    /**
     * @brief Build a link handle with the specified type and targets.
     * @param link_type The link type.
     * @param target_handles A list of link target identifiers.
     * @return The link handle.
     */
    static const string build_link_handle(const string& link_type, const StringList& target_handles) {
        string link_type_hash = ExpressionHasher::named_type_hash(link_type.c_str());
        return ExpressionHasher::expression_hash(link_type_hash, target_handles);
    }

    /**
     * @brief Checks if a node with the specified type and name exists.
     * @param node_type A string representing the type of the node.
     * @param node_name A string representing the name of the node.
     * @return A boolean value indicating whether the node exists (true) or not (false).
     */
    bool node_exists(const string& node_type, const string& node_name) const;

    /**
     * @brief Checks if a link with the specified type and target handles exists.
     * @param link_type A string representing the type of the link.
     * @param target_handles A vector of strings representing the handles of the link's targets.
     * @return A boolean value indicating whether the link exists (true) or not (false).
     */
    bool link_exists(const string& link_type, const StringList& target_handles) const;

    /**
     * @brief Retrieves an atom from the database using its handle and optional params.
     * @param handle A string representing the handle of the atom to be retrieved.
     * @param params An optional Params object containing additional retrieval options, as follows:
     *               `no_target_format` (`bool`, optional): If True, return the document without
     *                   transforming it to the target format. Defaults to False.
     *               `targets_document` (`bool`, optional): If True, include the `targets_document`
     *                   in the response. Defaults to False.
     *               `deep_representation` (`bool`, optional): If True, include a deep
     * representation of the targets. Defaults to False.
     * @return An Atom object representing the retrieved atom.
     */
    const shared_ptr<const Atom> get_atom(const string& handle, const Params& params = {}) const;

    // PURE VIRTUAL PUBLIC METHODS /////////////////////////////////////////////////////////////////

    /**
     * @brief Get the handle of the node with the specified type and name.
     * @param node_type The node type.
     * @param node_name The node name.
     * @return The node handle.
     */
    virtual const string get_node_handle(const string& node_type, const string& node_name) const = 0;

    /**
     * @brief Get the name of the node with the specified handle.
     * @param node_handle The node handle.
     * @return The node name.
     */
    virtual const string get_node_name(const string& node_handle) const = 0;

    /**
     * @brief Get the type of the node with the specified handle.
     * @param node_handle The node handle.
     * @return The node type.
     */
    virtual const string get_node_type(const string& node_handle) const = 0;

    /**
     * @brief Get the name of (a) node(s) of the specified type containing the given substring.
     * @param node_type The node type.
     * @param substring The substring to search for in node names.
     * @return List of handles of nodes whose names matched the criteria.
     */
    virtual const StringList get_node_by_name(const string& node_type,
                                              const string& substring) const = 0;

    /**
     * @brief Query the database by field and value.
     * @param query List of dicts containing 'field' and 'value' keys.
     * @return List of node IDs.
     */
    virtual const StringList get_atoms_by_field(
        const vector<unordered_map<string, string>>& query) const = 0;

    /**
     * @brief Retrieves atoms from the database using the specified index.
     * @param index_id The ID of the index to use for retrieving atoms from the database.
     * @param query A vector of unordered maps representing the query parameters.
     * @param cursor An integer representing the cursor position for pagination (default is 0).
     * @param chunk_size An integer representing the number of atoms to retrieve in one chunk
     *                   (default is 500).
     * @return A pair containing an optional cursor and a list of atoms. The optional cursor is
     *         used for pagination or further retrieval operations, and the list contains the
     *         retrieved atoms.
     */
    virtual const pair<const OptCursor, const AtomList> get_atoms_by_index(
        const string& index_id,
        const vector<unordered_map<string, string>>& query,
        int cursor = 0,
        int chunk_size = 500) const = 0;

    /**
     * @brief Query the database by a text field.
     * @param text_value Value to search for.
     * @param field Field to be used to search.
     * @param text_index_id TOKEN_INVERTED_LIST index id to search for.
     * @return List of node IDs ordered by the closest match.
     */
    virtual const StringList get_atoms_by_text_field(const string& text_value,
                                                     const string& field = "",
                                                     const string& text_index_id = "") const = 0;

    /**
     * @brief Query the database by node name starting with 'startswith' value.
     * @param node_type The node type.
     * @param startswith The starting substring to search for.
     * @return List of node IDs.
     */
    virtual const StringList get_node_by_name_starting_with(const string& node_type,
                                                            const string& startswith) const = 0;

    /**
     * @brief Get all nodes of a specific type.
     * @param node_type The node type.
     * @param names If True, return node names instead of handles. Default is False.
     * @return A list of node handles or names, depending on the value of 'names'.
     */
    virtual const StringList get_all_nodes(const string& node_type, bool names = false) const = 0;

    /**
     * @brief Retrieves all links of the specified type from the database.
     * @param link_type A string representing the type of the links to retrieve.
     * @param params An optional Params object containing additional retrieval options.
     * @return A pair containing an optional cursor and a list of strings representing the links.
     */
    virtual const pair<const OptCursor, const StringList> get_all_links(
        const string& link_type, const Params& params = {}) const = 0;

    /**
     * @brief Get the handle of the link with the specified type and targets.
     * @param link_type The link type.
     * @param target_handles A list of link target identifiers.
     * @return The link handle.
     */
    virtual const string get_link_handle(const string& link_type,
                                         const StringList& target_handles) const = 0;

    /**
     * @brief Get the type of the link with the specified handle.
     * @param link_handle The link handle.
     * @return The link type.
     */
    virtual const string get_link_type(const string& link_handle) const = 0;

    /**
     * @brief Get the target handles of a link specified by its handle.
     * @param link_handle The link handle.
     * @return A list of target identifiers of the link.
     */
    virtual const StringList get_link_targets(const string& link_handle) const = 0;

    /**
     * @brief Check if a link specified by its handle is ordered.
     * @param link_handle The link handle.
     * @return True if the link is ordered, False otherwise.
     */
    virtual bool is_ordered(const string& link_handle) const = 0;

    /**
     * @brief Retrieves incoming link handles for the specified atom.
     * @param atom_handle A string representing the handle of the atom.
     * @param params An optional Params object containing additional retrieval options.
     * @return A pair containing an optional cursor and a set of strings representing the incoming
     *         link handles.
     */
    virtual const pair<const OptCursor, const StringUnorderedSet> get_incoming_links_handles(
        const string& atom_handle, const Params& params = {}) const = 0;

    /**
     * @brief Retrieves incoming link atoms for the specified atom.
     * @param atom_handle A string representing the handle of the atom.
     * @param params An optional Params object containing additional retrieval options.
     * @return A pair containing an optional cursor and a list of Atom objects representing the
     *         incoming links.
     */
    virtual const pair<const OptCursor, const vector<shared_ptr<const Atom>>> get_incoming_links_atoms(
        const string& atom_handle, const Params& params = {}) const = 0;

    /**
     * @brief Retrieves matched links of the specified type and target handles.
     * @param link_type A string representing the type of the links to retrieve.
     * @param target_handles A list of strings representing the target handles.
     * @param params An optional Params object containing additional retrieval options.
     * @return A pair containing an optional cursor and a list of patterns or templates representing
     *         the matched links.
     */
    virtual const pair<const OptCursor, const Pattern_or_Template_List> get_matched_links(
        const string& link_type, const StringList& target_handles, const Params& params = {}) const = 0;

    /**
     * @brief Retrieves matched type templates based on the specified template.
     * @param _template A vector of elements of type any representing the template.
     * @param params An optional Params object containing additional retrieval options.
     * @return A pair containing an optional cursor and a list of patterns or templates representing
     *         the matched type templates.
     */
    virtual const pair<const OptCursor, const Pattern_or_Template_List> get_matched_type_template(
        const ListOfAny& _template, const Params& params = {}) const = 0;

    /**
     * @brief Retrieves matched types based on the specified link type.
     * @param link_type A string representing the type of the links to retrieve.
     * @param params An optional Params object containing additional retrieval options.
     * @return A pair containing an optional cursor and a list of patterns or templates representing
     *         the matched types.
     */
    virtual const pair<const OptCursor, const Pattern_or_Template_List> get_matched_type(
        const string& link_type, const Params& params = {}) const = 0;

    /**
     * @brief Retrieves the type of the atom with the specified handle.
     * @param handle A string representing the handle of the atom.
     * @return An optional string containing the type of the atom if found, otherwise nullopt.
     */
    virtual const opt<const string> get_atom_type(const string& handle) const = 0;

    /**
     * @brief Retrieves an atom as a dictionary representation.
     * @param handle The atom handle.
     * @param arity The arity of the atom. Defaults to 0.
     * @return A dictionary representation of the atom.
     */
    virtual const unordered_map<string, any> get_atom_as_dict(const string& handle,
                                                              int arity = 0) const = 0;

    /**
     * @brief Count the total number of atoms in the database.
     * @return A dictionary containing the count of node atoms, link atoms, and total atoms.
     */
    virtual const unordered_map<string, int> count_atoms() const = 0;

    /**
     * @brief Clear the entire database, removing all data.
     */
    virtual void clear_database() = 0;

    /**
     * @brief Adds a node to the database.
     * @param node_params A NodeParams object containing the parameters for the node.
     * @return Node object representing the created node.
     */
    virtual const shared_ptr<const Node> add_node(const NodeParams& node_params) = 0;

    /**
     * @brief Adds a link to the database.
     * @param link_params A LinkParams object containing the parameters for the link.
     * @param toplevel A boolean indicating whether the link is a top-level link (default is true).
     * @return An optional Link object representing the created link. If the link could not be
     * created, the optional will contain nullopt.
     */
    virtual const shared_ptr<const Link> add_link(const LinkParams& link_params,
                                                  bool toplevel = true) = 0;

    /**
     * @brief Reindexes the inverted pattern index according to the passed templates.
     *
     * This function reindexes the inverted pattern index based on the specified pattern
     * templates. The pattern templates are specified by atom type in a map, where each
     * atom type maps to a pattern template.
     *
     * @param pattern_index_templates A map where the keys are atom types and the values
     *        are pattern templates. Each pattern template is a vector of maps, where each
     *        map specifies a pattern template with:
     *        - "named_type": A boolean indicating whether the named type should be included.
     *        - "selected_positions": A vector of integers specifying the selected positions.
     *
     * Pattern templates are applied to each link entered in the atom space to determine
     * which entries should be created in the inverted pattern index. Entries in the inverted
     * pattern index are like patterns where the link type and each of its targets may be
     * replaced by wildcards. For instance, given a similarity link Similarity(handle1, handle2),
     * it could be used to create any of the following entries in the inverted pattern index:
     *
     * - *(handle1, handle2)
     *
     * - Similarity(*, handle2)
     *
     * - Similarity(handle1, *)
     *
     * - Similarity(*, *)
     *
     * If we create all possibilities of index entries for all links, the pattern index size
     * will grow exponentially, so we limit the entries we want to create by each type of link.
     * This is what a pattern template for a given link type is. For instance, if we apply this
     * pattern template:
     *
     * @code
     * `{ "named_type": false, "selected_positions": {0, 1} }`
     * @endcode
     *
     * to Similarity(handle1, handle2), we'll create only the following entries:
     *
     * - Similarity(*, handle2)
     *
     * - Similarity(handle1, *)
     *
     * - Similarity(*, *)
     *
     * If we apply this pattern template instead:
     *
     * @code
     * `{ "named_type": true, "selected_positions": {1} }`
     * @endcode
     *
     * We'll have:
     *
     * - *(handle1, handle2)
     *
     * - Similarity(handle1, *)
     */
    virtual void reindex(
        const unordered_map<string, vector<unordered_map<string, any>>>& pattern_index_templates) = 0;

    /**
     * @brief Delete an atom from the database.
     * @param handle Atom handle.
     */
    virtual void delete_atom(const string& handle) = 0;

    /**
     * @brief Create an index for the specified fields in the database.
     * @param atom_type The type of the atom for which the index is created.
     * @param fields A list of fields to be indexed.
     * @param named_type The named type of the atom.
     * @param composite_type A list representing the composite type of the atom.
     * @param index_type The type of the index to create.
     * @return The ID of the created index.
     */
    virtual const string create_field_index(const string& atom_type,
                                      const StringList& fields,
                                      const string& named_type = "",
                                      const StringList& composite_type = {},
                                      FieldIndexType index_type = FieldIndexType::BINARY_TREE) = 0;

    /**
     * @brief Insert multiple documents into the database.
     * @param documents A list of atoms, each representing a document to be inserted into the db.
     */
    virtual void bulk_insert(const vector<unique_ptr<const Atom>>& documents) = 0;

    /**
     * @brief Retrieve all atoms from the database.
     * @return A list of dictionaries representing the atoms.
     */
    virtual const vector<shared_ptr<const Atom>> retrieve_all_atoms() const = 0;

    /**
     * @brief Commit the current state of the database.
     */
    virtual void commit() = 0;

   protected:
    AtomDB() = default;

    // PROTECTED METHODS ///////////////////////////////////////////////////////////////////////////

    /**
     * @brief Reformats a document based on the provided params.
     * @param document A reference to the Atom object representing the document to be reformatted.
     * @param params A reference to a Params object containing the reformatting options.
     * @return A reference to the reformatted Atom object.
     */
    const shared_ptr<const Atom> _reformat_document(const shared_ptr<const Atom>& document,
                                                    const Params& params = {}) const;

    /**
     * @brief Builds a node with the specified parameters.
     * @param node_params A NodeParams object containing the parameters for the node.
     * @return Node object representing the created node.
     */
    shared_ptr<Node> _build_node(const NodeParams& node_params);

    /**
     * @brief Builds a link with the specified parameters.
     * @param link_params A LinkParams object containing the parameters for the link.
     * @param is_top_level A boolean indicating whether the link is a top-level link.
     * @return An optional Link object representing the constructed link.
     */
    shared_ptr<Link> _build_link(const LinkParams& link_params, bool is_top_level = true);

    /**
     * @brief Retrieves an atom from the database using its handle.
     * @param handle A string representing the handle of the atom to be retrieved.
     * @return A const shared pointer to the Atom object representing the retrieved atom.
     */
    virtual const shared_ptr<const Atom> _get_atom(const string& handle) const = 0;
};
}  // namespace atomdb

#endif  // _DATABASE_HPP
