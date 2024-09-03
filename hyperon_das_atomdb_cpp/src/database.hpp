#ifndef _DATABASE_HPP
#define _DATABASE_HPP

#include "constants.hpp"
#include "document_types.hpp"
#include "exceptions.hpp"
#include "type_aliases.hpp"
#include "utils/expression_hasher.hpp"
#include "utils/params.hpp"

class AtomDB {
   public:
    virtual ~AtomDB() = default;

    /**
     * @brief Get the handle of the node with the specified type and name.
     * @param node_type The node type.
     * @param node_name The node name.
     * @return The node handle.
     */
    static std::string build_node_handle(
        const std::string& node_type, const std::string& node_name) {
        return ExpressionHasher::terminal_hash(node_type, node_name);
    }

    /**
     * @brief Get the handle of the link with the specified type and targets.
     * @param link_type The link type.
     * @param target_handles A list of link target identifiers.
     * @return The link handle.
     */
    static std::string build_link_handle(
        const std::string& link_type, const StringList& target_handles) {
        std::string link_type_hash = ExpressionHasher::named_type_hash(link_type.c_str());
        std::vector<const char*> elements;
        for (const auto& target_handle : target_handles) {
            elements.push_back(target_handle.c_str());
        }
        return ExpressionHasher::expression_hash(link_type_hash, target_handles);
    }

    /**
     * @brief Checks if a node with the specified type and name exists.
     *
     * This function checks whether a node with the given type and name exists in the database.
     *
     * @param node_type A string representing the type of the node.
     * @param node_name A string representing the name of the node.
     * @return A boolean value indicating whether the node exists (true) or not (false).
     */
    bool node_exists(const std::string& node_type, const std::string& node_name) const {
        try {
            this->get_node_handle(node_type, node_name);
            return true;
        } catch (const AtomDoesNotExist& e) {
            return false;
        }
    }

    /**
     * @brief Checks if a link with the specified type and target handles exists.
     *
     * This function checks whether a link with the given type and target handles exists in the database.
     *
     * @param link_type A string representing the type of the link.
     * @param target_handles A vector of strings representing the handles of the link's targets.
     * @return A boolean value indicating whether the link exists (true) or not (false).
     */
    bool link_exists(const std::string& link_type, const StringList& target_handles) const {
        try {
            this->get_link_handle(link_type, target_handles);
            return true;
        } catch (const AtomDoesNotExist& e) {
            return false;
        }
    }

    /**
     * @brief Retrieves an atom from the database using its handle and optional params.
     *
     * This function takes a handle and optional params, and retrieves the corresponding atom from the database.
     *
     * @param handle A string representing the handle of the atom to be retrieved.
     * @param params An optional Params object containing additional retrieval options.
     * @return An Atom object representing the retrieved atom.
     */
    const Atom get_atom(const std::string& handle, const Params& params = {}) const {
        opt<Atom> document = _get_atom(handle);
        if (!document.has_value()) {
            throw AtomDoesNotExist("Nonexistent atom", "handle: " + handle);
        }
        if (params.get<bool>(FlagsParams::NO_TARGET_FORMAT).value_or(false)) {
            return document.value();
        }
        return _reformat_document(document.value(), params);
    }

    /**
     * @brief Get the handle of the node with the specified type and name.
     * @param node_type The node type.
     * @param node_name The node name.
     * @return The node handle.
     */
    virtual std::string get_node_handle(
        const std::string& node_type, const std::string& node_name) const = 0;

    /**
     * @brief Get the name of the node with the specified handle.
     * @param node_handle The node handle.
     * @return The node name.
     */
    virtual std::string get_node_name(const std::string& node_handle) const = 0;

    /**
     * @brief Get the type of the node with the specified handle.
     * @param node_handle The node handle.
     * @return The node type.
     */
    virtual std::string get_node_type(const std::string& node_handle) const = 0;

    /**
     * @brief Get the name of a node of the specified type containing the given substring.
     * @param node_type The node type.
     * @param substring The substring to search for in node names.
     * @return List of handles of nodes whose names matched the criteria.
     */
    virtual StringList get_node_by_name(
        const std::string& node_type, const std::string& substring) const = 0;

    /**
     * @brief Query the database by field and value.
     * @param query List of dicts containing 'field' and 'value' keys.
     * @return List of node IDs.
     */
    virtual StringList get_atoms_by_field(
        const std::vector<std::unordered_map<std::string, std::string>>& query) const = 0;

    /**
     * @brief Queries the database to return all atoms matching a specific index ID.
     * @param index_id The ID of the index to query against.
     * @param query A list of ordered dictionaries, each containing field-value pairs.
     * @param cursor An optional cursor indicating the starting point within the result set.
     * @param chunk_size An optional size indicating the maximum number of atom IDs to retrieve.
     * @return A tuple containing the cursor position and a list of retrieved atoms.
     */
    virtual std::pair<OptionalCursor, AtomList> get_atoms_by_index(
        const std::string& index_id,
        const std::vector<std::unordered_map<std::string, std::string>>& query,
        int cursor = 0,
        int chunk_size = 500) const = 0;

    /**
     * @brief Query the database by a text field.
     * @param text_value Value to search for.
     * @param field Field to be used to search.
     * @param text_index_id TOKEN_INVERTED_LIST index id to search for.
     * @return List of node IDs ordered by the closest match.
     */
    virtual StringList get_atoms_by_text_field(
        const std::string& text_value,
        const std::string& field = "",
        const std::string& text_index_id = "") const = 0;

    /**
     * @brief Query the database by node name starting with 'startswith' value.
     * @param node_type The node type.
     * @param startswith The starting substring to search for.
     * @return List of node IDs.
     */
    virtual StringList get_node_by_name_starting_with(
        const std::string& node_type, const std::string& startswith) const = 0;

    /**
     * @brief Get all nodes of a specific type.
     * @param node_type The node type.
     * @param names If True, return node names instead of handles. Default is False.
     * @return A list of node handles or names, depending on the value of 'names'.
     */
    virtual StringList get_all_nodes(
        const std::string& node_type, bool names = false) const = 0;

    /**
     * @brief Get all links of a specific type.
     * @param link_type The type of the link.
     * @return A tuple containing a cursor and a list of link handles.
     */
    virtual std::pair<OptionalCursor, StringList> get_all_links(
        const std::string& link_type, const Params& params) const = 0;

    /**
     * @brief Get the handle of the link with the specified type and targets.
     * @param link_type The link type.
     * @param target_handles A list of link target identifiers.
     * @return The link handle.
     */
    virtual std::string get_link_handle(
        const std::string& link_type, const StringList& target_handles) const = 0;

    /**
     * @brief Get the type of the link with the specified handle.
     * @param link_handle The link handle.
     * @return The link type.
     */
    virtual std::string get_link_type(const std::string& link_handle) const = 0;

    /**
     * @brief Get the target handles of a link specified by its handle.
     * @param link_handle The link handle.
     * @return A list of target identifiers of the link.
     */
    virtual StringList get_link_targets(const std::string& link_handle) const = 0;

    /**
     * @brief Check if a link specified by its handle is ordered.
     * @param link_handle The link handle.
     * @return True if the link is ordered, False otherwise.
     */
    virtual bool is_ordered(const std::string& link_handle) const = 0;

    /**
     * @brief Retrieve incoming links for a specified atom handle.
     * @param atom_handle The handle of the atom for which to retrieve incoming links.
     * @return A tuple containing the count of incoming links and a list of incoming links.
     */
    virtual std::pair<OptionalCursor, StringUnorderedSet> get_incoming_links_handles(
        const std::string& atom_handle, const Params& params) const = 0;

    virtual std::pair<OptionalCursor, AtomList> get_incoming_links_atoms(
        const std::string& atom_handle, const Params& params) const = 0;

    /**
     * @brief Retrieve links that match a specified link type and target handles.
     * @param link_type The type of the link to match.
     * @param target_handles A list of target handles to match.
     * @return A tuple containing a cursor and a list of matching link handles.
     */
    virtual MatchedElements get_matched_links(const std::string& link_type,
                                              const StringList& target_handles,
                                              const Params& params) const = 0;

    /**
     * @brief Retrieve links that match a specified type template.
     * @param _template A list representing the type template to match.
     * @return A tuple containing a cursor and a list of matching link handles.
     */
    virtual MatchedElements get_matched_type_template(
        const std::vector<std::any>& _template, const Params& params) const = 0;

    /**
     * @brief Retrieve links that match a specified link type.
     * @param link_type The type of the link to match.
     * @return A tuple containing a cursor and a list of matching link handles.
     */
    virtual MatchedElements get_matched_type(
        const std::string& link_type, const Params& params) const = 0;

    /**
     * @brief Retrieve the atom's type by its handle.
     * @param handle The handle of the atom to retrieve the type for.
     * @return The type of the atom.
     */
    virtual opt<std::string> get_atom_type(const std::string& handle) const = 0;

    /**
     * @brief Get an atom as a dictionary representation.
     * @param handle The atom handle.
     * @param arity The arity of the atom. Defaults to 0.
     * @return A dictionary representation of the atom.
     */
    virtual std::unordered_map<std::string, std::any> get_atom_as_dict(
        const std::string& handle, int arity = 0) const = 0;

    /**
     * @brief Count the total number of atoms in the database.
     * @return A dictionary containing the count of node atoms, link atoms, and total atoms.
     */
    virtual std::unordered_map<std::string, int> count_atoms() const = 0;

    /**
     * @brief Clear the entire database, removing all data.
     */
    virtual void clear_database() = 0;

    /**
     * @brief Adds a node to the database.
     *
     * This function creates a node using the specified parameters and adds it to the database.
     *
     * @param node_params A Params object containing the parameters for the node.
     * @return An optional Node object representing the created node. If the node could not be created,
     *         the optional will contain std::nullopt.
     */
    virtual opt<Node> add_node(const Params& node_params) = 0;

    /**
     * @brief Adds a link to the database.
     *
     * This function creates a link using the specified parameters and optionally marks it as a top-level link.
     *
     * @param link_params A Params object containing the parameters for the link.
     * @param toplevel A boolean indicating whether the link is a top-level link (default is true).
     * @return An optional Link object representing the created link. If the link could not be created,
     *         the optional will contain std::nullopt.
     */
    virtual opt<Link> add_link(const Params& link_params, bool toplevel = true) = 0;

    /**
     * @brief Reindex inverted pattern index according to passed templates.
     * @param pattern_index_templates Indexes are specified by atom type in a dict mapping from
     *                                atom type to a list of field-value pairs.
     */
    virtual void reindex(
        const std::unordered_map<
            std::string,
            std::vector<
                std::unordered_map<std::string,
                                   void*>>>& pattern_index_templates) = 0;  // TODO: Replace void* with appropriate type

    /**
     * @brief Delete an atom from the database.
     * @param handle Atom handle.
     */
    virtual void delete_atom(const std::string& handle) = 0;

    /**
     * @brief Create an index for the specified fields in the database.
     * @param atom_type The type of the atom for which the index is created.
     * @param fields A list of fields to be indexed.
     * @param named_type The named type of the atom.
     * @param composite_type A list representing the composite type of the atom.
     * @param index_type The type of the index to create.
     * @return The ID of the created index.
     */
    virtual std::string create_field_index(
        const std::string& atom_type,
        const StringList& fields, const std::string& named_type = "",
        const StringList& composite_type = {},
        FieldIndexType index_type = FieldIndexType::BINARY_TREE) = 0;

    /**
     * @brief Insert multiple documents into the database.
     * @param documents A list of atoms, each representing a document to be inserted into the db.
     */
    virtual void bulk_insert(const std::vector<Atom>& documents) = 0;

    /**
     * @brief Retrieve all atoms from the database.
     * @return A list of dictionaries representing the atoms.
     */
    virtual std::vector<Atom> retrieve_all_atoms() const = 0;

    /**
     * @brief Commit the current state of the database.
     */
    virtual void commit() = 0;

   protected:
    AtomDB() = default;

    /**
     * @brief Reformats a document based on the provided params.
     *
     * This function takes a document and a set of params, and reformats the document
     * according to the specified params.
     *
     * @param document A reference to the Atom object representing the document to be reformatted.
     * @param params A reference to a Params object containing the reformatting options.
     * @return A reference to the reformatted Atom object.
     */
    const Atom& _reformat_document(Atom& document, const Params& params = {}) const {
        if (Link* link = dynamic_cast<Link*>(&document)) {
            auto cursor = params.get<int>(FlagsParams::CURSOR);
            auto targets_documents = params.get<bool>(FlagsParams::TARGETS_DOCUMENTS).value_or(false);
            auto deep_representation = params.get<bool>(FlagsParams::DEEP_REPRESENTATION).value_or(false);
            if (targets_documents || deep_representation) {
                auto targets_documents = std::make_shared<std::vector<Atom>>();
                targets_documents->reserve(link->targets.size());
                for (const auto& target : link->targets) {
                    if (deep_representation) {
                        targets_documents->push_back(get_atom(target, params));
                    } else {
                        targets_documents->push_back(get_atom(target));
                    }
                }
                link->extra_params.set(FlagsParams::TARGETS_DOCUMENTS, targets_documents);
            }
        }
        return document;
    }

    /**
     * @brief Builds a node with the specified type and name.
     *
     * This function creates a node object using the provided type and name.
     *
     * @param node_type A string representing the type of the node.
     * @param node_name A string representing the name of the node.
     * @return A Node object representing the created node.
     */
    opt<Node> _build_node(const Params& node_params) {
        auto node_type = node_params.get<std::string>("type");
        auto node_name = node_params.get<std::string>("name");
        if (!node_type.has_value() || !node_name.has_value()) {
            // TODO: log error ???
            throw std::invalid_argument("'type' and 'name' are required.");
        }
        std::string handle = this->build_node_handle(node_type.value(), node_name.value());
        std::string composite_type_hash = ExpressionHasher::named_type_hash(node_type.value());
        Node node = Node(
            handle,               // id
            handle,               // handle
            composite_type_hash,  // composite_type_hash
            node_type.value(),    // named_type
            node_name.value()     // name
        );
        return node;
    }

    /**
     * @brief Builds a link with the specified parameters.
     *
     * This function constructs a Link object using the provided parameters.
     *
     * @param link_type A string representing the type of the link.
     * @param targets A vector of Atom objects representing the targets of the link.
     * @param toplevel A boolean indicating whether the link is a top-level link.
     * @return A Link object representing the constructed link.
     */
    opt<Link> _build_link(const Params& link_params, bool is_top_level = true) {
        auto link_type = link_params.get<std::string>("type");
        auto targets = link_params.get<std::vector<Params>>("targets");
        if (!link_type.has_value() || !targets.has_value()) {
            // TODO: log error ???
            throw std::invalid_argument("'type' and 'targets' are required.");
        }
        std::string link_type_hash = ExpressionHasher::named_type_hash(link_type.value());
        StringList target_handles = {};
        std::vector<CompositeType> composite_type = {CompositeType(link_type_hash)};
        StringList composite_type_hash = {link_type_hash};
        std::string atom_hash;
        std::string atom_handle;
        for (const Params& target : targets.value()) {
            if (!target.contains("targets")) {
                auto node = this->add_node(target);
                if (!node.has_value()) {
                    return std::nullopt;
                }
                atom_handle = node.value().id;
                atom_hash = node.value().composite_type_hash;
                composite_type.push_back(atom_hash);
            } else {
                auto link = this->add_link(target, false);
                if (!link.has_value()) {
                    return std::nullopt;
                }
                atom_handle = link.value().id;
                atom_hash = link.value().composite_type_hash;
                composite_type.push_back(CompositeType(link.value().composite_type));
            }
            composite_type_hash.push_back(atom_hash);
            target_handles.push_back(atom_handle);
        }

        std::string handle = ExpressionHasher::expression_hash(link_type_hash, target_handles);

        Link link = Link(
            handle,                                                 // id
            handle,                                                 // handle
            ExpressionHasher::composite_hash(composite_type_hash),  // composite_type_hash
            link_type.value(),                                      // named_type
            composite_type,                                         // composite_type
            link_type_hash,                                         // named_type_hash
            target_handles,                                         // targets
            is_top_level                                            // is_top_level
        );

        uint n = 0;
        for (const auto& target_handle : target_handles) {
            link.keys["key_" + std::to_string(n)] = target_handle;
            n++;
        }

        return link;
    }

    /**
     * @brief Retrieves an atom from the database using its handle.
     *
     * This function takes a handle and retrieves the corresponding atom from the database.
     *
     * @param handle A string representing the handle of the atom to be retrieved.
     * @return An Atom object representing the retrieved atom.
     */
    virtual opt<const Atom&> _get_atom(const std::string& handle) const = 0;
};

#endif  // _DATABASE_HPP
