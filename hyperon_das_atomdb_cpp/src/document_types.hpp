#pragma once

#include <map>
#include <stdexcept>
#include <string>

#include "utils/params.hpp"

using namespace std;

namespace atomdb {

/**
 * @brief Represents a basic unit of data in the system.
 *
 * The Atom class serves as a fundamental building block within the system,
 * encapsulating the essential properties and behaviors of a data unit.
 */
class Atom {
   public:
    string id;
    string handle;
    string composite_type_hash;
    string named_type;
    Params custom_attributes = {};

    Atom() = default;
    Atom(const string& id,
         const string& handle,
         const string& composite_type_hash,
         const string& named_type,
         const Params& custom_attributes = {})
        : id(id),
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

    const string to_string() const noexcept {
        string result = "id: '" + id + "'";
        result += ", handle: '" + handle + "'";
        result += ", composite_type_hash: '" + composite_type_hash + "'";
        result += ", named_type: '" + named_type + "'";
        return move(result);
    }
};

/**
 * @brief Represents a specific type of atom in the system.
 *
 * The AtomType class inherits from the Atom class and encapsulates additional properties
 * and behaviors specific to a particular type of atom. This class is used to define
 * and manage different types of atoms within the system.
 */
class AtomType : public Atom {
   public:
    string named_type_hash;

    AtomType() = default;
    AtomType(const string& id,
             const string& handle,
             const string& composite_type_hash,
             const string& named_type,
             const string& named_type_hash,
             const Params& custom_attributes = {})
        : named_type_hash(named_type_hash),
          Atom(id, handle, composite_type_hash, named_type, custom_attributes) {
        if (named_type_hash.empty()) {
            throw invalid_argument("Named type hash cannot be empty.");
        }
    }

    const string to_string() const noexcept {
        string result = "AtomType(" + Atom::to_string();
        result += ", named_type_hash: '" + named_type_hash + "')";
        return move(result);
    }
};

/**
 * @brief Represents a node in the system.
 *
 * The Node class inherits from the Atom class and represents a node within the system.
 * It encapsulates additional properties and behaviors specific to nodes.
 */
class Node : public Atom {
   public:
    string name;

    Node() = default;
    Node(const string& id,
         const string& handle,
         const string& composite_type_hash,
         const string& named_type,
         const string& name,
         const Params& custom_attributes = {})
        : name(name), Atom(id, handle, composite_type_hash, named_type, custom_attributes) {
        if (name.empty()) {
            throw invalid_argument("Node name cannot be empty.");
        }
    }

    const string to_string() const noexcept {
        string result = "Node(" + Atom::to_string();
        result += ", name: '" + name + "')";
        return move(result);
    }
};

/**
 * @brief Represents a composite type in the database.
 */
class CompositeType {
   public:
    using CompositeTypeList = vector<CompositeType>;

    string single_hash = "";
    CompositeTypeList list_of_composite_types = {};

    CompositeType(const string& single_hash) : single_hash(single_hash) {
        if (single_hash.empty()) {
            throw invalid_argument("'single_hash' cannot be empty.");
        }
        if (not list_of_composite_types.empty()) {
            throw invalid_argument(
                "'list_of_composite_types' must be empty when 'single_hash' is not empty.");
        }
    }

    CompositeType(const CompositeTypeList& list_of_composite_types)
        : list_of_composite_types(list_of_composite_types) {
        if (list_of_composite_types.empty()) {
            throw invalid_argument("'list_of_composite_types' cannot be empty.");
        }
        if (!single_hash.empty()) {
            throw invalid_argument(
                "'single_hash' must be empty when 'list_of_composite_types' is not empty.");
        }
    }
};

/**
 * @brief Represents a link in the system.
 *
 * The Link class inherits from the Atom class and represents a link within the system.
 * It encapsulates additional properties and behaviors specific to links, such as connections
 * between different atoms.
 */
class Link : public Atom {
   public:
    /**
     * `composite_type` is designed to hold a list of elements, where each element can either be a
     * `string` (single hash) or another list of CompositeType, allowing multiple levels of nesting.
     */
    CompositeType::CompositeTypeList composite_type;

    string named_type_hash;
    vector<string> targets;
    bool is_top_level = true;
    map<string, string> keys = {};
    opt<vector<shared_ptr<const Atom>>> targets_documents = nullopt;

    Link() = default;
    Link(const string& id,
         const string& handle,
         const string& composite_type_hash,
         const string& named_type,
         const CompositeType::CompositeTypeList& composite_type,
         const string& named_type_hash,
         const vector<string>& targets,
         bool is_top_level = true,
         map<string, string> keys = {},
         const Params& custom_attributes = {})
        : composite_type(composite_type),
          named_type_hash(named_type_hash),
          targets(targets),
          is_top_level(is_top_level),
          keys(keys),
          targets_documents(nullopt),
          Atom(id, handle, composite_type_hash, named_type, custom_attributes) {
        if (composite_type.empty()) {
            throw invalid_argument("Composite type cannot be empty.");
        }
        if (named_type_hash.empty()) {
            throw invalid_argument("Named type hash cannot be empty.");
        }
        if (targets.empty()) {
            throw invalid_argument("Link targets cannot be empty.");
        }
    }

    const string to_string() const noexcept {
        string result = "Link(" + Atom::to_string();
        result += ", composite_type: " + composite_type_list_to_string(composite_type);
        result += ", named_type_hash: '" + named_type_hash + "'";
        result += ", targets: [";
        if (not targets.empty()) {
            for (const auto& target : targets) {
                result += "'" + target + "', ";
            }
            result.pop_back();
            result.pop_back();
        }
        result += "]";
        result += ", is_top_level: ";
        result += is_top_level ? "true" : "false";
        result += ", keys: {";
        if (not keys.empty()) {
            for (const auto& [key, value] : keys) {
                result += "'" + key + "': '" + value + "', ";
            }
            result.pop_back();
            result.pop_back();
        }
        result += "}";
        result += ", targets_documents: [";
        if (targets_documents.has_value()) {
            if (not targets_documents->empty()) {
                for (const auto& target : *targets_documents) {
                    if (const auto& node = dynamic_pointer_cast<const Node>(target)) {
                        result += string(node->to_string()) + ", ";
                    } else if (const auto& link = dynamic_pointer_cast<const Link>(target)) {
                        result += string(link->to_string()) + ", ";
                    }
                }
                result.pop_back();
                result.pop_back();
            }
        }
        result += "])";
        return move(result);
    }

    const string composite_type_list_to_string(
        const CompositeType::CompositeTypeList& composite_type) const noexcept {
        string result = "[";
        for (const auto& element : composite_type) {
            if (not element.single_hash.empty()) {
                result += "'" + element.single_hash + "', ";
            } else {
                result += composite_type_list_to_string(element.list_of_composite_types) + ", ";
            }
        }
        result.pop_back();
        result.pop_back();
        result += "]";
        return move(result);
    }
};

using AtomList = vector<Atom>;
using AtomTypeList = vector<AtomType>;
using NodeList = vector<Node>;
using LinkList = vector<Link>;

}  // namespace atomdb
