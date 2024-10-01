#pragma once

#include <map>
#include <stdexcept>
#include <string>

#include "params.hpp"

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

    Atom() = default;
    Atom(const string& id,
         const string& handle,
         const string& composite_type_hash,
         const string& named_type)
        : id(id), handle(handle), composite_type_hash(composite_type_hash), named_type(named_type) {
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

    virtual const string to_string() const noexcept {
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
             const string& named_type_hash)
        : named_type_hash(named_type_hash), Atom(id, handle, composite_type_hash, named_type) {
        if (named_type_hash.empty()) {
            throw invalid_argument("Named type hash cannot be empty.");
        }
    }

    const string to_string() const noexcept override {
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
         const string& name)
        : name(name), Atom(id, handle, composite_type_hash, named_type) {
        if (name.empty()) {
            throw invalid_argument("Node name cannot be empty.");
        }
    }

    const string to_string() const noexcept override {
        string result = "Node(" + Atom::to_string();
        result += ", name: '" + name + "')";
        return move(result);
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
    using TargetsDocuments = vector<shared_ptr<const Atom>>;

    /**
     * `composite_type` is designed to hold a list of elements, where each element can either be a
     * `string` (single hash) or another list of `strings`, allowing multiple levels of nesting.
     */
    ListOfAny composite_type;

    string named_type_hash;
    vector<string> targets;
    bool is_top_level = true;
    map<string, string> keys = {};
    opt<TargetsDocuments> targets_documents = nullopt;

    Link() = default;
    Link(const string& id,
         const string& handle,
         const string& composite_type_hash,
         const string& named_type,
         const ListOfAny& composite_type,
         const string& named_type_hash,
         const vector<string>& targets,
         bool is_top_level = true,
         map<string, string> keys = {},
         opt<TargetsDocuments> targets_documents = {})
        : composite_type(composite_type),
          named_type_hash(named_type_hash),
          targets(targets),
          is_top_level(is_top_level),
          keys(keys),
          targets_documents(nullopt),
          Atom(id, handle, composite_type_hash, named_type) {
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
            if (not targets_documents->empty()) {
                this->targets_documents = TargetsDocuments();
                this->targets_documents->reserve(targets_documents->size());
                for (const auto& target : *targets_documents) {
                    if (const auto& node = dynamic_pointer_cast<const Node>(target)) {
                        this->targets_documents->push_back(make_shared<Node>(*node));
                    } else if (const auto& link = dynamic_pointer_cast<const Link>(target)) {
                        this->targets_documents->push_back(make_shared<Link>(*link));
                    }
                }
            }
        }
    }

    const string to_string() const noexcept override {
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
        result += ", targets_documents: ";
        if (targets_documents.has_value()) {
            result += "[";
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
            result += "]";
        } else {
            result += "NULL";
        }
        result += ")";
        return move(result);
    }

    const string composite_type_list_to_string(const ListOfAny& composite_type) const noexcept {
        string result = "[";
        for (const auto& element : composite_type) {
            if (auto str = any_cast<string>(&element)) {
                result += "'" + *str + "', ";
            } else if (auto list = any_cast<ListOfAny>(&element)) {
                result += composite_type_list_to_string(*list) + ", ";
            }
        }
        result.pop_back();
        result.pop_back();
        result += "]";
        return move(result);
    }

   private:
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
