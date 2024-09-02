#ifndef _PARAMS_HPP
#define _PARAMS_HPP

#include <any>
#include <optional>
#include <string>
#include <unordered_map>

using ParamKey = const char*;
using ParamValue = std::any;
using ParamsMap = std::unordered_map<ParamKey, ParamValue>;

/**
 * @brief Namespace containing flag parameters.
 *
 * The FlagsParams namespace defines various parameters that can be used as params
 * within the application. Each parameter represents a specific flag that can be
 * utilized to control different aspects of the application's behavior.
 */
namespace FlagsParams {
constexpr ParamKey CURSOR = "cursor";
constexpr ParamKey DEEP_REPRESENTATION = "deep_representation";
constexpr ParamKey DELETE_ATOM = "delete_atom";
constexpr ParamKey NO_TARGET_FORMAT = "no_target_format";
constexpr ParamKey TARGETS_DOCUMENTS = "targets_documents";
};  // namespace FlagsParams

/**
 * @brief Namespace containing atom parameters.
 *
 * The AtomParams namespace defines various parameters that can be used to specify
 * properties and behaviors of atoms within the application. Each parameter represents
 * a specific attribute or setting related to atoms.
 */
namespace AtomParams {
constexpr ParamKey COMPOSITE_TYPE = "composite_type";
constexpr ParamKey COMPOSITE_TYPE_HASH = "composite_type_hash";
constexpr ParamKey HANDLE = "handle";
constexpr ParamKey ID = "id";
constexpr ParamKey IS_TOP_LEVEL = "is_top_level";
constexpr ParamKey KEYS = "keys";
constexpr ParamKey NAME = "name";
constexpr ParamKey NAMED_TYPE = "named_type";
constexpr ParamKey NAMED_TYPE_HASH = "named_type_hash";
constexpr ParamKey TARGETS = "targets";
constexpr ParamKey TARGETS_DOCUMENTS = "targets_documents";
};  // namespace AtomParams

/**
 * @brief A class representing a collection of params.
 *
 * This class is a specialized unordered map that stores parameters and their associated values.
 * It inherits privately from std::unordered_map, using ParamKey as keys and ParamValue as values.
 */
class Params : private ParamsMap {
   public:
    /**
     * @brief Constructs a Params object with the specified initial params.
     * @param params An unordered map where the keys are ParamKey and the values are of type
     *               ParamValue, representing the initial set of params.
     *               Defaults to an empty map if not provided.
     * @return A Params object initialized with the specified params.
     */
    Params(const ParamsMap& params = {}) {
        for (const auto& [key, value] : params) {
            this->set(key, value);
        }
    };

    /**
     * @brief Checks if the specified parameter exists in the collection.
     * @param key The parameter to check for existence.
     * @return A boolean value indicating whether the parameter exists (true) or not (false).
     */
    bool contains(ParamKey key) const {
        return this->find(key) != this->end();
    };

    /**
     * @brief Retrieves the value associated with the specified parameter.
     * @tparam T The type of the value to be retrieved.
     * @param key The parameter whose value is to be retrieved.
     * @param default_value <optional> The default value to return if the key is not found.
     *                      Defaults to std::nullopt.
     * @return The value associated with the specified parameter, or the default value if the
     *         key is not found.
     */
    template <typename T>
    std::optional<T> get(ParamKey key, std::optional<T> default_value = std::nullopt) const {
        if (this->contains(key)) {
            return std::any_cast<T>(this->at(key));
        }
        return default_value;
    };

    /**
     * @brief Sets the value for the specified parameter.
     * @tparam T The type of the value to be set.
     * @param key The parameter whose value is to be set.
     * @param value The value to be associated with the specified parameter.
     */
    template <typename T>
    void set(ParamKey key, T value) {
        this->insert_or_assign(key, value);
    };

    /**
     * @brief Removes and returns the value associated with the specified parameter.
     * @tparam T The type of the value to be retrieved.
     * @param key The parameter whose value is to be removed and returned.
     * @param default_value <optional> The default value to return if the key is not found.
     *                      Defaults to std::nullopt.
     * @return The value associated with the specified parameter, or the default value if the
     *         key is not found.
     */
    template <typename T>
    std::optional<T> pop(ParamKey key, std::optional<T> default_value = std::nullopt) {
        if (this->contains(key)) {
            T value = std::move(std::any_cast<T>(this->at(key)));
            this->erase(key);
            return value;
        }
        return default_value;
    };
};

#endif  // _PARAMS_HPP
