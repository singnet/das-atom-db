

/**
 * @file params.h
 * @brief This header file defines several Plain Old Data (POD) types used for configuration and
 *        parameterization within the atomdb namespace.
 *
 * The types defined in this file include:
 * - KwArgs: A structure containing boolean flags for various configuration options.
 * - CustomAttributes: A structure for holding custom attributes categorized by type, with an
 *   equality operator for comparison.
 * - NodeParams: A structure representing parameters for a node, including type, name, and optional
 *   custom attributes, with a method to convert its contents to a string representation.
 * - LinkParams: A structure representing parameters for a link, encapsulating the type of the link,
 *   its targets, and optional custom attributes, with a method to convert its contents to a string
 *   representation.
 *
 * The structures are designed to be simple and efficient, providing essential functionality for
 * managing configuration and parameters in a clear and organized manner. The use of unordered maps
 * for custom attributes allows for flexible and efficient storage and retrieval of attribute data.
 */
#pragma once

#include <variant>

#include "type_aliases.h"

using namespace std;

namespace atomdb {

using CustomAttributesKey = string;
using CustomAttributesValue = variant<string, long, double, bool>;
using CustomAttributes = unordered_map<CustomAttributesKey, CustomAttributesValue>;

/**
 * @brief Retrieves a custom attribute from the given custom attributes.
 * @param custom_attributes The custom attributes map.
 * @param key The key for the custom attribute to retrieve.
 * @return An optional value of type T if the custom attribute exists.
 */
template <typename T>
static const opt<T> get_custom_attribute(const CustomAttributes& custom_attributes,
                                         const CustomAttributesKey& key) {
    if (custom_attributes.find(key) != custom_attributes.end()) {
        return std::get<T>(custom_attributes.at(key));
    }
    return nullopt;
}

/**
 * @brief Converts custom attributes to a string representation.
 * @param custom_attributes The custom attributes to be converted.
 * @return A string representation of the custom attributes.
 */
static string custom_attributes_to_string(const CustomAttributes& custom_attributes) {
    if (custom_attributes.empty()) {
        return "{}";
    }
    string result = "{";
    for (const auto& [key, value] : custom_attributes) {
        result += key + ": ";
        if (auto str = std::get_if<string>(&value)) {
            result += "'" + *str + "'";
        } else if (auto integer = std::get_if<long>(&value)) {
            result += std::to_string(*integer);
        } else if (auto floating = std::get_if<double>(&value)) {
            result += std::to_string(*floating);
        } else if (auto boolean = std::get_if<bool>(&value)) {
            result += *boolean ? "true" : "false";
        }
        result += ", ";
    }
    result.pop_back();
    result.pop_back();
    result += "}";
    return move(result);
}

/**
 * @brief A Plain Old Data (POD) type representing various boolean flags for configuration options.
 *
 * This structure contains several boolean flags that control different aspects of the
 * configuration, such as target formatting, document handling, representation depth,
 * and scope of operation.
 */
struct KwArgs {
    bool no_target_format = false;
    bool targets_document = false;
    bool deep_representation = false;
    bool toplevel_only = false;
    bool handles_only = false;
};

/**
 * @brief A Plain Old Data (POD) type representing parameters for a node.
 *
 * This struct contains the type, name, and optional custom attributes of a node.
 * It provides a method to convert its contents to a string representation.
 */
struct NodeParams {
    string type = "";
    string name = "";
    opt<CustomAttributes> custom_attributes = nullopt;

    /**
     * @brief Converts the NodeParams object to a string representation.
     * @return A string representing the NodeParams object, including its type, name, and custom
     * attributes.
     */
    const string to_string() const {
        string result = "NodeParams(";
        result += "type: '" + this->type + "'";
        result += ", name: '" + this->name + "'";
        result += ", custom_attributes: ";
        result += (this->custom_attributes.has_value()
                       ? custom_attributes_to_string(this->custom_attributes.value())
                       : "NULL");
        result += ")";
        return move(result);
    }
};

/**
 * @brief Plain Old Data (POD) type representing parameters for a link.
 *
 * This struct encapsulates the type of the link, its targets, and optional custom attributes.
 * It provides a method to convert its contents to a string representation.
 */
struct LinkParams {
    using Target = variant<NodeParams, LinkParams>;
    using Targets = vector<Target>;

    string type = "";
    Targets targets = {};
    opt<CustomAttributes> custom_attributes = nullopt;

    const string to_string() const {
        string result = "LinkParams(";
        result += "type: '" + this->type + "'";
        result += ", targets: ";
        result += "[";
        if (not this->targets.empty()) {
            for (const auto& target : this->targets) {
                if (auto node_params = std::get_if<NodeParams>(&target)) {
                    result += node_params->to_string() + ", ";
                } else if (auto link_params = std::get_if<LinkParams>(&target)) {
                    result += link_params->to_string() + ", ";
                }
            }
            result.pop_back();
            result.pop_back();
        }
        result += "]";
        result += ", custom_attributes: ";
        result += (this->custom_attributes.has_value()
                       ? custom_attributes_to_string(this->custom_attributes.value())
                       : "NULL");
        result += ")";
        return move(result);
    }
};

}  // namespace atomdb
