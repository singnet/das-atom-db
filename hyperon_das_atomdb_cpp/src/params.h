

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
 * @brief A Plain Old Data (POD) type which holds custom attributes categorized by type.
 *
 * This structure contains unordered maps for storing string, integer, float, and boolean attributes.
 * It also provides an equality operator to compare two CustomAttributes objects.
 */
struct CustomAttributes {
    StringUnorderedMap strings = {};
    IntUnorderedMap integers = {};
    FloatUnorderedMap floats = {};
    BoolUnorderedMap booleans = {};

    /**
     * @brief Compares this CustomAttributes object with another for equality.
     * @param other The CustomAttributes object to compare against.
     * @return true if all attributes (strings, integers, floats, booleans) are equal, false otherwise.
     */
    bool operator==(const CustomAttributes& other) const noexcept {
        return this->strings == other.strings and this->integers == other.integers and
               this->floats == other.floats and this->booleans == other.booleans;
    }
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
            result += "None";
        }
        result += ")";
        return result;
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
                if (auto node_params = get_if<NodeParams>(&target)) {
                    result += node_params->to_string() + ", ";
                } else if (auto link_params = get_if<LinkParams>(&target)) {
                    result += link_params->to_string() + ", ";
                }
            }
            result.pop_back();
            result.pop_back();
        }
        result += "]";

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
            result += "None";
        }
        result += ")";
        return result;
    }
};

}  // namespace atomdb
