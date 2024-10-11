#pragma once

#include <variant>

#include "type_aliases.h"

using namespace std;

namespace atomdb {

struct KwArgs {
    bool no_target_format = false;
    bool targets_document = false;
    bool deep_representation = false;
    bool toplevel_only = false;
    bool handles_only = false;
};

struct CustomAttributes {
    StringUnorderedMap strings = {};
    IntUnorderedMap integers = {};
    FloatUnorderedMap floats = {};
    BoolUnorderedMap booleans = {};

    bool operator==(const CustomAttributes& other) const noexcept {
        return this->strings == other.strings and this->integers == other.integers and
               this->floats == other.floats and this->booleans == other.booleans;
    }
};

struct NodeParams {
    string type = "";
    string name = "";
    opt<CustomAttributes> custom_attributes = nullopt;

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
