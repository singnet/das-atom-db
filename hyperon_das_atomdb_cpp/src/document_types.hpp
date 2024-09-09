#ifndef _DOCUMENT_TYPES_HPP
#define _DOCUMENT_TYPES_HPP

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

    string id;
    string handle;
    string composite_type_hash;
    string named_type;
    Params custom_attributes = {};
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

    string named_type_hash;
};

/**
 * @brief Represents a node in the system.
 *
 * The Node class inherits from the Atom class and represents a node within the system.
 * It encapsulates additional properties and behaviors specific to nodes.
 */
class Node : public Atom {
   public:
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

    string name;
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
         const Params& custom_attributes = {})
        : composite_type(composite_type),
          named_type_hash(named_type_hash),
          targets(targets),
          is_top_level(is_top_level),
          keys(keys),
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

    /**
     * `composite_type` is designed to hold a list of elements, where each element can either be a
     * `string` or another list of the same type, allowing multiple levels of nesting.
     */
    ListOfAny composite_type;

    string named_type_hash;
    vector<string> targets;
    bool is_top_level = true;
    map<string, string> keys = {};
};

using AtomList = vector<Atom>;
using AtomTypeList = vector<AtomType>;
using NodeList = vector<Node>;
using LinkList = vector<Link>;

}  // namespace atomdb

#endif  // _DOCUMENT_TYPES_HPP
