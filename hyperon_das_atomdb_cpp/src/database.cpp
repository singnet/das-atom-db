#include "database.hpp"

using namespace atomdb;

// PUBLIC METHODS //////////////////////////////////////////////////////////////////////////////////

//------------------------------------------------------------------------------
bool AtomDB::node_exists(const std::string& node_type, const std::string& node_name) const {
    try {
        this->get_node_handle(node_type, node_name);
        return true;
    } catch (const AtomDoesNotExist& e) {
        return false;
    }
}

//------------------------------------------------------------------------------
bool AtomDB::link_exists(const std::string& link_type, const StringList& target_handles) const {
    try {
        this->get_link_handle(link_type, target_handles);
        return true;
    } catch (const AtomDoesNotExist& e) {
        return false;
    }
}

//------------------------------------------------------------------------------
Atom AtomDB::get_atom(const std::string& handle, const Params& params) const {
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
            auto targets_documents = std::make_shared<std::vector<Atom>>();
            targets_documents->reserve(link->targets.size());
            for (const auto& target : link->targets) {
                if (deep_representation) {
                    targets_documents->push_back(get_atom(target, params));
                } else {
                    targets_documents->push_back(get_atom(target));
                }
            }
            Link link_copy = *link;
            link_copy.extra_params.set(ParamsKeys::TARGETS_DOCUMENTS, targets_documents);
            return link_copy;
        }
    }
    return document;
}

//------------------------------------------------------------------------------
opt<Node> AtomDB::_build_node(const Params& node_params) {
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
        node_name.value(),    // name
        node_params           // extra_params
    );
    return node;
}

//------------------------------------------------------------------------------
opt<Link> AtomDB::_build_link(const Params& link_params, bool is_top_level = true) {
    auto link_type = link_params.get<std::string>("type");
    auto targets = link_params.get<std::vector<Params>>("targets");
    if (!link_type.has_value() || !targets.has_value()) {
        // TODO: log error ???
        throw std::invalid_argument("'type' and 'targets' are required.");
    }
    std::string link_type_hash = ExpressionHasher::named_type_hash(link_type.value());
    StringList target_handles = {};
    std::vector<CompositeType> composite_type_list = {CompositeType(link_type_hash)};
    StringList composite_type_elements = {link_type_hash};
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
            composite_type_list.push_back(atom_hash);
        } else {
            auto link = this->add_link(target, false);
            if (!link.has_value()) {
                return std::nullopt;
            }
            atom_handle = link.value().id;
            atom_hash = link.value().composite_type_hash;
            composite_type_list.push_back(CompositeType(link.value().composite_type));
        }
        composite_type_elements.push_back(atom_hash);
        target_handles.push_back(atom_handle);
    }

    std::string handle = ExpressionHasher::expression_hash(link_type_hash, target_handles);
    std::string composite_type_hash = ExpressionHasher::composite_hash(composite_type_elements);

    Link link = Link(
        handle,               // id
        handle,               // handle
        composite_type_hash,  // composite_type_hash
        link_type.value(),    // named_type
        composite_type_list,  // composite_type
        link_type_hash,       // named_type_hash
        target_handles,       // targets
        is_top_level,         // is_top_level
        {},                   // keys
        link_params           // extra_params
    );

    uint n = 0;
    for (const auto& target_handle : target_handles) {
        link.keys["key_" + std::to_string(n)] = target_handle;
        n++;
    }

    return link;
}
