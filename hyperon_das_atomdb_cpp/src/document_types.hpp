#ifndef _DOCUMENT_TYPES_HPP
#define _DOCUMENT_TYPES_HPP

#include <map>
#include <stdexcept>
#include <string>

#include "utils/params.hpp"

/**
 * @brief Represents a basic unit of data in the system.
 *
 * The Atom class serves as a fundamental building block within the system,
 * encapsulating the essential properties and behaviors of a data unit.
 */
class Atom {
   public:
    Atom(const std::string& id,
         const std::string& handle,
         const std::string& composite_type_hash,
         const std::string& named_type,
         const Params& extra_params = {})
        : id(id),
          handle(handle),
          composite_type_hash(composite_type_hash),
          named_type(named_type),
          extra_params(extra_params) {
        if (id.empty()) {
            throw std::invalid_argument("Atom ID cannot be empty.");
        }
        if (handle.empty()) {
            throw std::invalid_argument("Atom handle cannot be empty.");
        }
        if (composite_type_hash.empty()) {
            throw std::invalid_argument("Composite type hash cannot be empty.");
        }
        if (named_type.empty()) {
            throw std::invalid_argument("Named type cannot be empty.");
        }
    }

    virtual ~Atom() = default;

    std::string id;
    std::string handle;
    std::string composite_type_hash;
    std::string named_type;
    Params extra_params = {};
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
    AtomType(const std::string& id,
             const std::string& handle,
             const std::string& composite_type_hash,
             const std::string& named_type,
             const std::string& named_type_hash,
             const Params& extra_params = {})
        : named_type_hash(named_type_hash),
          Atom(id, handle, composite_type_hash, named_type, extra_params) {
        if (named_type_hash.empty()) {
            throw std::invalid_argument("Named type hash cannot be empty.");
        }
    }

    std::string named_type_hash;
};

/**
 * @brief Represents a node in the system.
 *
 * The Node class inherits from the Atom class and represents a node within the system.
 * It encapsulates additional properties and behaviors specific to nodes.
 */
class Node : public Atom {
   public:
    Node(const std::string& id,
         const std::string& handle,
         const std::string& composite_type_hash,
         const std::string& named_type,
         const std::string& name,
         const Params& extra_params = {})
        : name(name),
          Atom(id, handle, composite_type_hash, named_type, extra_params) {
        if (name.empty()) {
            throw std::invalid_argument("Node name cannot be empty.");
        }
    }

    std::string name;
};

/**
 * @brief Represents a composite type in the system.
 *
 * The CompositeType class encapsulates the properties and behaviors of a composite type,
 * which is a type composed of multiple elements. This class provides methods to manage
 * and manipulate these elements.
 */
class CompositeType {
   public:
    CompositeType(const std::string& single_hash)
        : single_hash(single_hash) {
        if (single_hash.empty()) {
            throw std::invalid_argument("Single hash cannot be empty.");
        }
        if (!list_of_composite_types.empty()) {
            throw std::invalid_argument("List of composite types must be empty.");
        }
    }

    CompositeType(const std::vector<CompositeType>& list_of_composite_types)
        : list_of_composite_types(list_of_composite_types) {
        if (list_of_composite_types.empty()) {
            throw std::invalid_argument("List of composite types cannot be empty.");
        }
        if (!single_hash.empty()) {
            throw std::invalid_argument("Single hash must be empty.");
        }
    }

    std::string single_hash = "";
    std::vector<CompositeType> list_of_composite_types = {};
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
    Link(const std::string& id,
         const std::string& handle,
         const std::string& composite_type_hash,
         const std::string& named_type,
         std::vector<CompositeType>& composite_type,
         const std::string& named_type_hash,
         const std::vector<std::string>& targets,
         bool is_top_level = true,
         const std::map<std::string, std::string>& keys = {},
         const Params& extra_params = {})
        : composite_type(composite_type),
          named_type_hash(named_type_hash),
          targets(targets),
          is_top_level(is_top_level),
          keys(keys),
          Atom(id, handle, composite_type_hash, named_type, extra_params) {
        if (composite_type.empty()) {
            throw std::invalid_argument("Composite type cannot be empty.");
        }
        if (named_type_hash.empty()) {
            throw std::invalid_argument("Named type hash cannot be empty.");
        }
        if (targets.empty()) {
            throw std::invalid_argument("Link targets cannot be empty.");
        }
    }

    std::vector<CompositeType> composite_type;
    std::string named_type_hash;
    std::vector<std::string> targets;
    bool is_top_level = true;
    std::map<std::string, std::string> keys = {};
};

using AtomList = std::vector<Atom>;
using AtomTypeList = std::vector<AtomType>;
using NodeList = std::vector<Node>;
using LinkList = std::vector<Link>;

#endif  // _DOCUMENT_TYPES_HPP
