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
Atom AtomDB::get_atom(const string& handle, const Params& params) const {
    auto document = _get_atom(handle);
    if (!document.has_value()) {
        throw AtomDoesNotExist("Nonexistent atom", "handle: " + handle);
    }
    if (params.get<bool>(ParamsKeys::NO_TARGET_FORMAT).value_or(false)) {
        return document.value();
    }
    return _reformat_document(document.value(), params);
}

// PROTECTED OR PRIVATE METHODS ////////////////////////////////////////////////////////////////////

//------------------------------------------------------------------------------
const Atom AtomDB::_reformat_document(const Atom& document, const Params& params) const {
    if (const Link* link = dynamic_cast<const Link*>(&document)) {
        auto targets_documents = params.get<bool>(ParamsKeys::TARGETS_DOCUMENTS).value_or(false);
        auto deep_representation = params.get<bool>(ParamsKeys::DEEP_REPRESENTATION).value_or(false);
        if (targets_documents || deep_representation) {
            auto targets_documents = make_shared<vector<Atom>>();
            targets_documents->reserve(link->targets.size());
            for (const auto& target : link->targets) {
                if (deep_representation) {
                    targets_documents->push_back(get_atom(target, params));
                } else {
                    targets_documents->push_back(get_atom(target));
                }
            }
            Link link_copy = *link;
            link_copy.custom_attributes.set(ParamsKeys::TARGETS_DOCUMENTS, targets_documents);
            return link_copy;
        }
    }
    return document;
}

//------------------------------------------------------------------------------
Node AtomDB::_build_node(const NodeParams& node_params) {
    const auto& node_type = node_params.type;
    const auto& node_name = node_params.name;
    if (node_type.empty() || node_name.empty()) {
        // TODO: log error ???
        throw invalid_argument("'type' and 'name' are required.");
    }
    string handle = this->build_node_handle(node_type, node_name);
    string composite_type_hash = ExpressionHasher::named_type_hash(node_type);
    Node node = Node(handle,                        // id
                     handle,                        // handle
                     composite_type_hash,           // composite_type_hash
                     node_type,                     // named_type
                     node_name,                     // name
                     node_params.custom_attributes  // custom_attributes
    );
    return node;
}

//------------------------------------------------------------------------------
opt<Link> AtomDB::_build_link(const LinkParams& link_params, bool is_top_level) {
    const auto& link_type = link_params.type;
    const auto& targets = link_params.targets;
    if (link_type.empty() || targets.empty()) {
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
        if (LinkParams::is_node(target)) {
            auto node = this->add_node(LinkParams::as_node(target));
            atom_handle = node.id;
            atom_hash = node.composite_type_hash;
            composite_type_list.push_back(atom_hash);
        } else if (LinkParams::is_link(target)) {
            auto link = this->add_link(LinkParams::as_link(target), false);
            if (!link.has_value()) {
                return nullopt;
            }
            atom_handle = link.value().id;
            atom_hash = link.value().composite_type_hash;
            composite_type_list.push_back(link.value().composite_type);
        } else {
            throw invalid_argument("Invalid target type. Must be NodeParams or LinkParams.");
        }
        composite_type_elements.push_back(atom_hash);
        target_handles.push_back(atom_handle);
    }

    string handle = ExpressionHasher::expression_hash(link_type_hash, target_handles);
    string composite_type_hash = ExpressionHasher::composite_hash(composite_type_elements);

    Link link = Link(handle,                        // id
                     handle,                        // handle
                     composite_type_hash,           // composite_type_hash
                     link_type,                     // named_type
                     composite_type_list,           // composite_type
                     link_type_hash,                // named_type_hash
                     target_handles,                // targets
                     is_top_level,                  // is_top_level
                     {},                            // keys
                     link_params.custom_attributes  // custom_attributes
    );

    uint n = 0;
    for (const auto& target_handle : target_handles) {
        link.keys["key_" + to_string(n)] = target_handle;
        n++;
    }

    return link;
}
