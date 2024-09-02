#ifndef _DATABASE_HPP
#define _DATABASE_HPP

#include <string>
#include <unordered_map>
#include <vector>

#include "basic_types.hpp"
#include "exceptions.hpp"
#include "utils/expression_hasher.hpp"
#include "utils/params.hpp"

const std::string WILDCARD = "*";
const StringList UNORDERED_LINK_TYPES = {};

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
    bool node_exists(const std::string& node_type, const std::string& node_name) {
        try {
            get_node_handle(node_type, node_name);
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
    bool link_exists(const std::string& link_type, const StringList& target_handles) {
        try {
            get_link_handle(link_type, target_handles);
            return true;
        } catch (const AtomDoesNotExist& e) {
            return false;
        }
    }

    /**
     * @brief Retrieves an atom from the database using its handle and optional flags.
     *
     * This function takes a handle and optional flags, and retrieves the corresponding atom from the database.
     *
     * @param handle A string representing the handle of the atom to be retrieved.
     * @param flags An optional Flags object containing additional retrieval options.
     * @return An Atom object representing the retrieved atom.
     */
    const Atom& get_atom(const std::string& handle, const Params& flags = Params()) {
        Atom document = _get_atom(handle);
        if (flags.get<bool>(FlagsParams::NO_TARGET_FORMAT, false)) return document;
        return _reformat_document(document, flags);
    }

    /**
     * @brief Get the handle of the node with the specified type and name.
     * @param node_type The node type.
     * @param node_name The node name.
     * @return The node handle.
     */
    virtual std::string get_node_handle(
        const std::string& node_type, const std::string& node_name) = 0;

    /**
     * @brief Get the name of the node with the specified handle.
     * @param node_handle The node handle.
     * @return The node name.
     */
    virtual std::string get_node_name(const std::string& node_handle) = 0;

    /**
     * @brief Get the type of the node with the specified handle.
     * @param node_handle The node handle.
     * @return The node type.
     */
    virtual std::string get_node_type(const std::string& node_handle) = 0;

    /**
     * @brief Get the name of a node of the specified type containing the given substring.
     * @param node_type The node type.
     * @param substring The substring to search for in node names.
     * @return List of handles of nodes whose names matched the criteria.
     */
    virtual StringList get_node_by_name(
        const std::string& node_type, const std::string& substring) = 0;

    /**
     * @brief Query the database by field and value.
     * @param query List of dicts containing 'field' and 'value' keys.
     * @return List of node IDs.
     */
    virtual StringList get_atoms_by_field(
        const std::vector<std::unordered_map<std::string, std::string>>& query) = 0;

    /**
     * @brief Queries the database to return all atoms matching a specific index ID.
     * @param index_id The ID of the index to query against.
     * @param query A list of ordered dictionaries, each containing field-value pairs.
     * @param cursor An optional cursor indicating the starting point within the result set.
     * @param chunk_size An optional size indicating the maximum number of atom IDs to retrieve.
     * @return A tuple containing the cursor position and a list of retrieved atoms.
     */
    virtual std::pair<int, std::vector<Atom>> get_atoms_by_index(
        const std::string& index_id,
        const std::vector<std::unordered_map<std::string, std::string>>& query,
        int cursor = 0,
        int chunk_size = 500) = 0;

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
        const std::string& text_index_id = "") = 0;

    /**
     * @brief Query the database by node name starting with 'startswith' value.
     * @param node_type The node type.
     * @param startswith The starting substring to search for.
     * @return List of node IDs.
     */
    virtual StringList get_node_by_name_starting_with(
        const std::string& node_type, const std::string& startswith) = 0;

    /**
     * @brief Get all nodes of a specific type.
     * @param node_type The node type.
     * @param names If True, return node names instead of handles. Default is False.
     * @return A list of node handles or names, depending on the value of 'names'.
     */
    virtual StringList get_all_nodes(
        const std::string& node_type, bool names = false) = 0;

    /**
     * @brief Get all links of a specific type.
     * @param link_type The type of the link.
     * @return A tuple containing a cursor and a list of link handles.
     */
    virtual std::pair<int, StringList> get_all_links(
        const std::string& link_type) = 0;

    /**
     * @brief Get the handle of the link with the specified type and targets.
     * @param link_type The link type.
     * @param target_handles A list of link target identifiers.
     * @return The link handle.
     */
    virtual std::string get_link_handle(
        const std::string& link_type, const StringList& target_handles) = 0;

    /**
     * @brief Get the type of the link with the specified handle.
     * @param link_handle The link handle.
     * @return The link type.
     */
    virtual std::string get_link_type(const std::string& link_handle) = 0;

    /**
     * @brief Get the target handles of a link specified by its handle.
     * @param link_handle The link handle.
     * @return A list of target identifiers of the link.
     */
    virtual StringList get_link_targets(const std::string& link_handle) = 0;

    /**
     * @brief Check if a link specified by its handle is ordered.
     * @param link_handle The link handle.
     * @return True if the link is ordered, False otherwise.
     */
    virtual bool is_ordered(const std::string& link_handle) = 0;

    /**
     * @brief Retrieve incoming links for a specified atom handle.
     * @param atom_handle The handle of the atom for which to retrieve incoming links.
     * @return A tuple containing the count of incoming links and a list of incoming links.
     */
    virtual std::pair<int, IncomingLinks> get_incoming_links(const std::string& atom_handle) = 0;

    /**
     * @brief Retrieve links that match a specified link type and target handles.
     * @param link_type The type of the link to match.
     * @param target_handles A list of target handles to match.
     * @return A tuple containing a cursor and a list of matching link handles.
     */
    virtual MatchedLinksResult get_matched_links(
        const std::string& link_type, const StringList& target_handles) = 0;

    /**
     * @brief Retrieve links that match a specified type template.
     * @param template_ A list representing the type template to match.
     * @return A tuple containing a cursor and a list of matching link handles.
     */
    virtual MatchedTypesResult get_matched_type_template(
        const StringList& template_) = 0;

    /**
     * @brief Retrieve links that match a specified link type.
     * @param link_type The type of the link to match.
     * @return A tuple containing a cursor and a list of matching link handles.
     */
    virtual MatchedTypesResult get_matched_type(const std::string& link_type) = 0;

    /**
     * @brief Retrieve the atom's type by its handle.
     * @param handle The handle of the atom to retrieve the type for.
     * @return The type of the atom.
     */
    virtual std::string get_atom_type(const std::string& handle) = 0;

    /**
     * @brief Get an atom as a dictionary representation.
     * @param handle The atom handle.
     * @param arity The arity of the atom. Defaults to 0.
     * @return A dictionary representation of the atom.
     */
    virtual std::unordered_map<std::string, std::string> get_atom_as_dict(
        const std::string& handle, int arity = 0) = 0;

    /**
     * @brief Count the total number of atoms in the database.
     * @return A dictionary containing the count of node atoms, link atoms, and total atoms.
     */
    virtual std::unordered_map<std::string, int> count_atoms() = 0;

    /**
     * @brief Clear the entire database, removing all data.
     */
    virtual void clear_database() = 0;

    /**
     * @brief Adds a node to the database.
     * @param node_type The node type.
     * @param node_name The node name.
     * @return The information about the added node, including its unique key and other details.
     */
    virtual Node add_node(const std::string& node_type, const std::string& node_name) = 0;

    /**
     * @brief Adds a link to the database.
     *
     * This function creates a link of the specified type, connecting the given target atoms,
     * and optionally marks it as a top-level link.
     *
     * @param link_type A string representing the type of the link.
     * @param targets A vector of Atom objects representing the targets of the link.
     * @param toplevel A boolean indicating whether the link is a top-level link (default is true).
     * @return A Link object representing the created link.
     */
    virtual Link add_link(
        const std::string& link_type,
        const std::vector<Atom>& targets,
        bool toplevel = true) = 0;

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
    virtual std::vector<Atom> retrieve_all_atoms() = 0;

    /**
     * @brief Commit the current state of the database.
     */
    virtual void commit() = 0;

   protected:
    AtomDB() = default;

    /**
     * @brief Reformats a document based on the provided flags.
     *
     * This function takes a document and a set of flags, and reformats the document
     * according to the specified flags.
     *
     * @param document A reference to the Atom object representing the document to be reformatted.
     * @param flags A reference to a Flags object containing the reformatting options.
     * @return A reference to the reformatted Atom object.
     */
    const Atom& _reformat_document(Atom& document, const Params& flags = Params()) {
        if (Link* link = dynamic_cast<Link*>(&document)) {
            auto cursor = flags.get<int>(FlagsParams::CURSOR);
            auto targets_documents = flags.get<bool>(FlagsParams::TARGETS_DOCUMENTS, false);
            auto deep_representation = flags.get<bool>(FlagsParams::DEEP_REPRESENTATION, false);
            if (targets_documents || deep_representation) {
                std::vector<Atom> targets_documents;
                for (const auto& target : link->targets) {
                    if (deep_representation) {
                        targets_documents.push_back(get_atom(target, flags));
                    } else {
                        targets_documents.push_back(get_atom(target));
                    }
                }
                link->targets_documents = targets_documents;
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
    Node _build_node(const std::string& node_type, const std::string& node_name) {
        std::string handle = AtomDB::build_node_handle(node_type, node_name);
        return Node(
            handle, handle, ExpressionHasher::named_type_hash(node_type), node_type, node_name);
    }

    /**
     * @brief Builds a link with the specified type and targets.
     *
     * This function creates a link object using the provided type and targets.
     *
     * @param link_type A string representing the type of the link.
     * @param target_handles A list of strings representing the handles of the link targets.
     * @param is_top_level A boolean value indicating whether the link is top-level.
     * @return A Link object representing the created link.
     */
    Link _build_link(
        const std::string& link_type, const std::vector<Atom>& targets, bool is_top_level = true) {
        std::string link_type_hash = ExpressionHasher::named_type_hash(link_type);
        StringList target_handles = {};
        std::vector<CompositeType> composite_type = {CompositeType(link_type_hash)};
        StringList composite_type_hash = {link_type_hash};
        std::string atom_hash;
        std::string atom_handle;
        for (const Atom& target : targets) {
            if (const Node* node = dynamic_cast<const Node*>(&target)) {
                Node atom = this->add_node(node->named_type, node->name);
                atom_handle = atom.id;
                atom_hash = atom.composite_type_hash;
                composite_type.push_back(atom_hash);
            } else if (const Link* link = dynamic_cast<const Link*>(&target)) {
                Link atom = this->add_link(link->named_type, link->targets, false);
                atom_handle = atom.id;
                atom_hash = atom.composite_type_hash;
                composite_type.push_back(CompositeType(atom.composite_type));
            }
            composite_type_hash.push_back(atom_hash);
            target_handles.push_back(atom_handle);
        }

        std::string handle = ExpressionHasher::expression_hash(link_type_hash, target_handles);

        Link link = Link(
            handle,                                                 // id
            handle,                                                 // handle
            ExpressionHasher::composite_hash(composite_type_hash),  // composite_type_hash
            link_type,                                              // named_type
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
    virtual const Atom& _get_atom(const std::string& handle) = 0;
};

#endif  // _DATABASE_HPP
