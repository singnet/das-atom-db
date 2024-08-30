#ifndef _BASIC_TYPES_HPP
#define _BASIC_TYPES_HPP

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

enum class FieldIndexType {
    BINARY_TREE,
    TOKEN_INVERTED_LIST
};

class Atom {
   public:
    Atom(const std::string& id,
         const std::string& handle,
         const std::string& composite_type_hash,
         const std::string& named_type)
        : _id(id),
          handle(handle),
          composite_type_hash(composite_type_hash),
          named_type(named_type) {
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

    std::string _id;
    std::string handle;
    std::string composite_type_hash;
    std::string named_type;
};

class Node : public Atom {
   public:
    Node(const std::string& id,
         const std::string& handle,
         const std::string& composite_type_hash,
         const std::string& named_type,
         const std::string& name)
        : name(name),
          Atom(id, handle, composite_type_hash, named_type) {
        if (name.empty()) {
            throw std::invalid_argument("Node name cannot be empty.");
        }
    }

    std::string name;
};

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
         const std::map<std::string, std::string>& keys = {})
        : composite_type(composite_type),
          named_type_hash(named_type_hash),
          targets(targets),
          is_top_level(is_top_level),
          keys(keys),
          Atom(id, handle, composite_type_hash, named_type) {
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
    std::vector<Atom> targets_documents = {};
};

using IncomingLinks = std::vector<std::string>;
using MatchedTargetsList = std::vector<std::pair<std::string, std::vector<std::string>>>;
using HandlesList = std::vector<std::string>;
using MatchedLinksResult = std::pair<int, HandlesList>;
using MatchedTypesResult = std::pair<int, MatchedTargetsList>;

#endif  // _BASIC_TYPES_HPP
