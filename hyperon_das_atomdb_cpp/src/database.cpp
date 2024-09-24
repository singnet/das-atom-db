#include "database.hpp"

using namespace std;
using namespace atomdb;

// PUBLIC METHODS //////////////////////////////////////////////////////////////////////////////////

//------------------------------------------------------------------------------
bool AtomDB::node_exists(const string& node_type, const string& node_name) const {
    try {
        this->get_node_handle(node_type, node_name);
        return true;
    } catch (const AtomDoesNotExist& e) {
        return false;
    }
}

//------------------------------------------------------------------------------
bool AtomDB::link_exists(const string& link_type, const StringList& target_handles) const {
    try {
        this->get_link_handle(link_type, target_handles);
        return true;
    } catch (const AtomDoesNotExist& e) {
        return false;
    }
}

//------------------------------------------------------------------------------
const shared_ptr<const Atom> AtomDB::get_atom(const string& handle, const KwArgs& kwargs) const {
    auto document = _get_atom(handle);
    if (not document) {
        throw AtomDoesNotExist("Nonexistent atom", "handle: " + handle);
    }
    if (not kwargs.no_target_format) {
        return _reformat_document(document, kwargs);
    }
    return move(document);
}

// PROTECTED OR PRIVATE METHODS ////////////////////////////////////////////////////////////////////

//------------------------------------------------------------------------------
const shared_ptr<const Atom> AtomDB::_reformat_document(const shared_ptr<const Atom>& document,
                                                        const KwArgs& kwargs) const {
    if (const auto& link = dynamic_pointer_cast<const Link>(document)) {
        auto targets_documents = kwargs.targets_documents;
        auto deep_representation = kwargs.deep_representation;
        if (targets_documents or deep_representation) {
            shared_ptr<Link> link_copy = make_shared<Link>(*link);
            link_copy->targets_documents = vector<shared_ptr<const Atom>>();
            link_copy->targets_documents->reserve(link->targets.size());
            for (const auto& target : link->targets) {
                if (deep_representation) {
                    link_copy->targets_documents->push_back(this->get_atom(target, kwargs));
                } else {
                    link_copy->targets_documents->push_back(this->get_atom(target));
                }
            }
            return move(link_copy);
        }
    }
    return document;
}

//------------------------------------------------------------------------------
shared_ptr<Node> AtomDB::_build_node(const NodeParams& node_params) {
    const auto& node_type = node_params.type;
    const auto& node_name = node_params.name;
    if (node_type.empty() or node_name.empty()) {
        // TODO: log error ???
        throw invalid_argument("'type' and 'name' are required.");
    }
    string handle = this->build_node_handle(node_type, node_name);
    string composite_type_hash = ExpressionHasher::named_type_hash(node_type);
    return make_shared<Node>(handle,               // id
                             handle,               // handle
                             composite_type_hash,  // composite_type_hash
                             node_type,            // named_type
                             node_name             // name
    );
}

//------------------------------------------------------------------------------
shared_ptr<Link> AtomDB::_build_link(const LinkParams& link_params, bool is_top_level) {
    const auto& link_type = link_params.type;
    const auto& targets = link_params.targets;
    if (link_type.empty() or targets.empty()) {
        // TODO: log error ???
        throw invalid_argument("'type' and 'targets' are required.");
    }
    string link_type_hash = ExpressionHasher::named_type_hash(link_type);
    StringList target_handles = {};
    ListOfAny composite_type_list = {link_type_hash};
    StringList composite_type_elements = {link_type_hash};
    string atom_hash;
    string atom_handle;
    for (const auto& target : targets) {
        if (auto node_params = get_if<NodeParams>(&target)) {
            auto node = this->add_node(*node_params);
            atom_handle = node->id;
            atom_hash = node->composite_type_hash;
            composite_type_list.push_back(atom_hash);
        } else if (auto link_params = get_if<LinkParams>(&target)) {
            auto link = this->add_link(*link_params, false);
            atom_handle = link->id;
            atom_hash = link->composite_type_hash;
            composite_type_list.push_back(link->composite_type);
        } else {
            throw invalid_argument("Invalid target type. Must be NodeParams or LinkParams.");
        }
        composite_type_elements.push_back(atom_hash);
        target_handles.push_back(atom_handle);
    }

    string handle = ExpressionHasher::expression_hash(link_type_hash, target_handles);
    string composite_type_hash = ExpressionHasher::composite_hash(composite_type_elements);

    auto link = make_shared<Link>(handle,               // id
                                  handle,               // handle
                                  composite_type_hash,  // composite_type_hash
                                  link_type,            // named_type
                                  composite_type_list,  // composite_type
                                  link_type_hash,       // named_type_hash
                                  target_handles,       // targets
                                  is_top_level          // is_top_level
    );

    uint n = 0;
    for (const auto& target_handle : target_handles) {
        link->keys["key_" + to_string(n)] = target_handle;
        n++;
    }

    return move(link);
}
