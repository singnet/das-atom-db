/**
 * @file document_types.h
 * @brief Defines various classes representing atomic entities and their relationships in the atom
 * database.
 *
 * This header file contains the definitions of several classes that represent different types of
 * atomic entities and their relationships within the atom database. The classes include:
 * - Atom: Represents a basic atomic entity with attributes such as ID, handle, composite type hash,
 *   named type, and optional custom attributes.
 * - AtomType: Extends the Atom class by adding a named type hash attribute.
 * - Node: Represents a node in the atom database, extending the Atom class by adding a name
 *   attribute.
 * - Link: Represents a link in the atom database, encapsulating a composite type, named type hash,
 *   and a list of target hashes. It supports nested composite types and validates their structure.
 *
 * Each class provides constructors for initialization, comparison operators for equality checks,
 * and methods to convert the objects to string representations. The classes also include validation
 * mechanisms to ensure the integrity of the attributes.
 *
 * The file also defines several type aliases for vectors of these atomic entities, facilitating
 * their usage in collections.
 */
#pragma once

#include <map>
#include <stdexcept>
#include <string>

#include "params.h"

using namespace std;

namespace atomdb {

/**
 * @class Atom
 * @brief Represents an atomic entity with various attributes.
 *
 * The Atom class encapsulates the properties of an atomic entity, including its ID, handle,
 * composite type hash, named type, and optional custom attributes. It provides constructors
 * for initialization, comparison operators, and a method to convert the object to a string
 * representation.
 */
class Atom {
   public:
    string _id;
    string handle;
    string composite_type_hash;
    string named_type;
    opt<CustomAttributes> custom_attributes;

    /**
     * @brief Default constructor for the Atom class.
     */
    Atom() = default;

    /**
     * @brief Constructs an Atom object with the given parameters.
     * @param id The unique identifier for the atom.
     * @param handle The handle for the atom.
     * @param composite_type_hash The hash representing the composite type of the atom.
     * @param named_type The named type of the atom.
     * @param custom_attributes Optional custom attributes for the atom.
     * @throws invalid_argument if any of the required parameters are empty.
     */
    Atom(const string& id,
         const string& handle,
         const string& composite_type_hash,
         const string& named_type,
         const opt<const CustomAttributes>& custom_attributes = nullopt)
        : _id(id),
          handle(handle),
          composite_type_hash(composite_type_hash),
          named_type(named_type),
          custom_attributes(custom_attributes) {
        if (id.empty()) {
            throw invalid_argument("Atom ID cannot be empty.");
        }
        if (handle.empty()) {
            throw invalid_argument("Atom handle cannot be empty.");
        }
        if (composite_type_hash.empty()) {
            throw invalid_argument("Composite type hash cannot be empty.");
        }
        if (named_type.empty()) {
            throw invalid_argument("Named type cannot be empty.");
        }
    }

    virtual ~Atom() = default;

    /**
     * @brief Compares this Atom with another for equality.
     * @param other The Atom to compare with.
     * @return True if both Atoms are equal, otherwise false.
     */
    bool operator==(const Atom& other) const noexcept {
        return this->_id == other._id and this->handle == other.handle and
               this->composite_type_hash == other.composite_type_hash and
               this->named_type == other.named_type and
               this->custom_attributes == other.custom_attributes;
    }

    /**
     * @brief Converts the object to a string representation.
     * @return A string representing the object, including its ID, handle, composite type hash,
     *         named type, and custom attributes.
     */
    virtual const string to_string() const noexcept {
        string result = "_id: '" + this->_id + "'";
        result += ", handle: '" + this->handle + "'";
        result += ", composite_type_hash: '" + this->composite_type_hash + "'";
        result += ", named_type: '" + this->named_type + "'";
        result += ", custom_attributes: ";
        if (this->custom_attributes.has_value()) {
            result += "CustomAttributes(";
            if (not this->custom_attributes->strings.empty()) {
                result += "strings: {";
                for (const auto& [key, value] : this->custom_attributes->strings) {
                    result += key + ": '" + value + "', ";
                }
                result.pop_back();
                result.pop_back();
                result += "}, ";
            }
            if (not this->custom_attributes->integers.empty()) {
                result += "integers: {";
                for (const auto& [key, value] : this->custom_attributes->integers) {
                    result += key + ": " + std::to_string(value) + ", ";
                }
                result.pop_back();
                result.pop_back();
                result += "}, ";
            }
            if (not this->custom_attributes->floats.empty()) {
                result += "floats: {";
                for (const auto& [key, value] : this->custom_attributes->floats) {
                    result += key + ": " + std::to_string(value) + ", ";
                }
                result.pop_back();
                result.pop_back();
                result += "}, ";
            }
            if (not this->custom_attributes->booleans.empty()) {
                result += "booleans: {";
                for (const auto& [key, value] : this->custom_attributes->booleans) {
                    result += key + ": " + (value ? "true" : "false") + ", ";
                }
                result.pop_back();
                result.pop_back();
                result += "}, ";
            }
            result.pop_back();
            result.pop_back();
            result += ")";
        } else {
            result += "NULL";
        }
        return move(result);
    }
};

/**
 * @class AtomType
 * @brief Represents a specialized type of Atom with an additional named type hash.
 *
 * The AtomType class extends the Atom class by adding a named type hash attribute.
 * It provides constructors for initialization, an equality operator, and a string
 * representation method.
 */
class AtomType : public Atom {
   public:
    string named_type_hash;

    /**
     * @brief Default constructor for the AtomType class.
     */
    AtomType() = default;

    /**
     * @brief Constructs an AtomType object with the specified parameters.
     * @param id The identifier for the atom type.
     * @param handle The handle for the atom type.
     * @param composite_type_hash The hash of the composite type.
     * @param named_type The named type of the atom.
     * @param named_type_hash The hash of the named type.
     * @param custom_attributes Optional custom attributes for the atom type.
     * @throws invalid_argument if named_type_hash is empty.
     */
    AtomType(const string& id,
             const string& handle,
             const string& composite_type_hash,
             const string& named_type,
             const string& named_type_hash,
             const opt<const CustomAttributes>& custom_attributes = nullopt)
        : named_type_hash(named_type_hash),
          Atom(id, handle, composite_type_hash, named_type, custom_attributes) {
        if (named_type_hash.empty()) {
            throw invalid_argument("Named type hash cannot be empty.");
        }
    }

    /**
     * @brief Compares this AtomType with another for equality.
     * @param other The AtomType to compare with.
     * @return True if both AtomType objects are equal, false otherwise.
     */
    bool operator==(const AtomType& other) const {
        return Atom::operator==(other) and this->named_type_hash == other.named_type_hash;
    }

    /**
     * @brief Converts the AtomType object to a string representation.
     * @return A string representing the AtomType object.
     */
    const string to_string() const noexcept override {
        string result = "AtomType(" + Atom::to_string();
        result += ", named_type_hash: '" + this->named_type_hash + "')";
        return move(result);
    }
};

/**
 * @class Node
 * @brief Represents a node in the atom database, inheriting from Atom.
 *
 * The Node class extends the Atom class by adding a name attribute. It includes constructors,
 * equality operators, and a string representation method. The name attribute must not be empty.
 */
class Node : public Atom {
   public:
    string name;

    /**
     * @brief Default constructor for the Node class.
     */
    Node() = default;

    /**
     * @brief Constructs a Node object with the given parameters.
     * @param id The identifier for the Node.
     * @param handle The handle for the Node.
     * @param composite_type_hash The hash representing the composite type.
     * @param named_type The named type of the Node.
     * @param name The name of the Node.
     * @param custom_attributes Optional custom attributes for the Node.
     * @throws invalid_argument if the name is empty.
     */
    Node(const string& id,
         const string& handle,
         const string& composite_type_hash,
         const string& named_type,
         const string& name,
         const opt<const CustomAttributes>& custom_attributes = nullopt)
        : name(name), Atom(id, handle, composite_type_hash, named_type, custom_attributes) {
        if (name.empty()) {
            throw invalid_argument("Node name cannot be empty.");
        }
    }

    /**
     * @brief Compares this Node with another Node for equality.
     * @param other The Node to compare with.
     * @return True if both Nodes are equal, false otherwise.
     */
    bool operator==(const Node& other) const {
        return Atom::operator==(other) and this->name == other.name;
    }

    /**
     * @brief Converts the Node object to a string representation.
     * @return A string representing the Node object.
     */
    const string to_string() const noexcept override {
        string result = "Node(" + Atom::to_string();
        result += ", name: '" + this->name + "')";
        return move(result);
    }
};

/**
 * @class Link
 * @brief Represents a link in the atom database, inheriting from Atom.
 *
 * The Link class encapsulates a composite type, a named type hash, and a list of target hashes.
 * It supports nested composite types and validates their structure. The class also provides
 * functionality to compare links, convert them to string representations, and manage target
 * documents.
 */
class Link : public Atom {
   public:
    /// @brief Alias for a vector of shared pointers to constant Atom objects.
    using TargetsDocuments = vector<shared_ptr<const Atom>>;

    /**
     * `composite_type` is designed to hold a list of elements, where each element can either be a
     * `string` (single hash) or another list of `strings`, allowing multiple levels of nesting.
     * Example:
     ```
       [
          "5b19fd94cb294ecebae572045771f1ac",
          "f74c835e804a481c8918e16671cabcde",
          [
             "5df522aeaaf04617b96128bd5ca7ad17",
             [
                "0a9aada9568e48febfc4395657a82840",
                "72645c1ec2ef46d88542d54484cb45e9"
             ],
             "d3d4832042af4fdab5983c63a66885d2"
          ]
       ]
     ```
     */
    ListOfAny composite_type;

    string named_type_hash;
    vector<string> targets;
    bool is_toplevel = true;
    opt<TargetsDocuments> targets_documents = nullopt;

    /**
     * @brief Default constructor for the Link class.
     */
    Link() = default;

    /**
     * @brief Constructs a Link object with the specified parameters.
     * @param id The identifier for the link.
     * @param handle The handle for the link.
     * @param composite_type_hash The hash of the composite type.
     * @param named_type The named type of the link.
     * @param composite_type The composite type list.
     * @param named_type_hash The hash of the named type.
     * @param targets The vector of target strings.
     * @param is_toplevel Boolean indicating if the link is top-level.
     * @param targets_documents Optional targets documents.
     * @param custom_attributes Optional custom attributes.
     * @throws invalid_argument if composite_type is empty, composite_type is invalid, named_type_hash
     * is empty, or targets is empty.
     */
    Link(const string& id,
         const string& handle,
         const string& composite_type_hash,
         const string& named_type,
         const ListOfAny& composite_type,
         const string& named_type_hash,
         const vector<string>& targets,
         bool is_toplevel,
         const opt<const TargetsDocuments>& targets_documents = nullopt,
         const opt<const CustomAttributes>& custom_attributes = nullopt)
        : composite_type(composite_type),
          named_type_hash(named_type_hash),
          targets(targets),
          is_toplevel(is_toplevel),
          targets_documents(nullopt),
          Atom(id, handle, composite_type_hash, named_type, custom_attributes) {
        if (composite_type.empty()) {
            throw invalid_argument("Composite type cannot be empty.");
        }
        if (not Validator::validate_composite_type(composite_type)) {
            throw invalid_argument(
                "Invalid composite type. All elements must be strings or lists of strings.");
        }
        if (named_type_hash.empty()) {
            throw invalid_argument("Named type hash cannot be empty.");
        }
        if (targets.empty()) {
            throw invalid_argument("Link targets cannot be empty.");
        }
        if (targets_documents.has_value()) {
            this->targets_documents = TargetsDocuments();
            this->targets_documents->reserve(targets_documents->size());
            for (const auto& target : *(targets_documents)) {
                if (const auto& node = dynamic_pointer_cast<const Node>(target)) {
                    this->targets_documents->push_back(make_shared<Node>(*node));
                } else if (const auto& link = dynamic_pointer_cast<const Link>(target)) {
                    this->targets_documents->push_back(make_shared<Link>(*link));
                }
            }
        }
    }

    /**
     * @brief Compares this Link object with another for equality.
     * @param other The Link object to compare with.
     * @return True if the objects are equal, false otherwise.
     */
    bool operator==(const Link& other) const {
        bool composite_type_are_equal = this->composite_type_list_to_string(this->composite_type) ==
                                        this->composite_type_list_to_string(other.composite_type);
        return composite_type_are_equal and Atom::operator==(other) and
               this->named_type_hash == other.named_type_hash and this->targets == other.targets and
               this->is_toplevel == other.is_toplevel and
               this->targets_documents == other.targets_documents;
    }

    /**
     * @brief Converts the Link object to a string representation.
     * @return A string representing the Link object, including its composite type, named type hash,
     *         targets, top-level status, and target documents.
     */
    const string to_string() const noexcept override {
        string result = "Link(" + Atom::to_string();
        result += ", composite_type: " + composite_type_list_to_string(this->composite_type);
        result += ", named_type_hash: '" + this->named_type_hash + "'";
        result += ", targets: [";
        if (not this->targets.empty()) {
            for (const auto& target : this->targets) {
                result += "'" + target + "', ";
            }
            result.pop_back();
            result.pop_back();
        }
        result += "]";
        result += ", is_toplevel: ";
        result += this->is_toplevel ? "true" : "false";
        result += ", targets_documents: ";
        if (this->targets_documents.has_value()) {
            result += "[";
            if (not this->targets_documents->empty()) {
                for (const auto& target : *(this->targets_documents)) {
                    if (const auto& node = dynamic_pointer_cast<const Node>(target)) {
                        result += string(node->to_string()) + ", ";
                    } else if (const auto& link = dynamic_pointer_cast<const Link>(target)) {
                        result += string(link->to_string()) + ", ";
                    }
                }
                result.pop_back();
                result.pop_back();
            }
            result += "]";
        } else {
            result += "NULL";
        }
        result += ")";
        return move(result);
    }

    /**
     * @brief Converts a composite type list to a string representation.
     * @param composite_type The list of any type elements to be converted.
     * @return A string representation of the composite type list.
     */
    const string composite_type_list_to_string(const ListOfAny& composite_type) const noexcept {
        string result = "[";
        if (not composite_type.empty()) {
            for (const auto& element : composite_type) {
                if (auto str = any_cast<string>(&element)) {
                    result += "'" + *str + "', ";
                } else if (auto list = any_cast<ListOfAny>(&element)) {
                    result += composite_type_list_to_string(*list) + ", ";
                }
            }
            result.pop_back();
            result.pop_back();
        }
        result += "]";
        return move(result);
    }

   private:
    /**
     * @struct Validator
     * @brief Provides validation functions for composite types.
     *
     * The Validator struct contains static methods to validate the structure of composite types,
     * ensuring that elements are either strings or nested lists of the same type.
     */
    struct Validator {
        /**
         * @brief Validates the structure of a composite type.
         *
         * This function checks whether the given composite type adheres to the expected structure.
         * A composite type is a list where each element can be either a string or another list of
         * the same type. The function ensures that all elements in the composite type meet these
         * criteria.
         *
         * @param composite_type A list of elements of type std::any representing the composite type
         *                       to be validated.
         * @return true if the composite type is valid, false otherwise.
         */
        static bool validate_composite_type(const ListOfAny& composite_type) {
            for (const auto& element : composite_type) {
                if (auto str = any_cast<string>(&element)) {
                    continue;
                } else if (auto list = any_cast<ListOfAny>(&element)) {
                    if (not Validator::validate_composite_type(*list)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            return true;
        }
    };
};

using AtomList = vector<Atom>;
using AtomTypeList = vector<AtomType>;
using NodeList = vector<Node>;
using LinkList = vector<Link>;

}  // namespace atomdb
