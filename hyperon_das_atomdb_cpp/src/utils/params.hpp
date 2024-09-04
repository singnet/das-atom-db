#ifndef _PARAMS_HPP
#define _PARAMS_HPP

#include "type_aliases.hpp"

namespace atomdb {

using ParamKey = const char*;
using ParamValue = std::any;
using ParamsMap = std::unordered_map<ParamKey, ParamValue>;

/**
 * @brief Namespace containing parameters keys.
 *
 * The ParamsKeys namespace defines various parameters that can be used as params
 * within the application. Each parameter represents a specific flag that can be
 * utilized to control different aspects of the application's behavior.
 */
namespace ParamsKeys {
constexpr ParamKey CURSOR = "cursor";
constexpr ParamKey DEEP_REPRESENTATION = "deep_representation";
constexpr ParamKey DELETE_ATOM = "delete_atom";
constexpr ParamKey NO_TARGET_FORMAT = "no_target_format";
constexpr ParamKey TARGETS_DOCUMENTS = "targets_documents";
constexpr ParamKey TOPLEVEL_ONLY = "toplevel_only";
};  // namespace ParamsKeys

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
     * @brief Retrieves the value associated with the specified parameter key.
     * @tparam T The type of the value to be retrieved.
     * @param key The parameter key whose value is to be retrieved.
     * @return An optional containing the value associated with the specified parameter key,
     *         or an empty optional if the key is not found.
     */
    template <typename T>
    opt<T> get(ParamKey key) const {
        if (this->contains(key)) {
            return std::any_cast<T>(this->at(key));
        }
        return std::nullopt;
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
     * @brief Removes and returns the value associated with the specified parameter key.
     * @tparam T The type of the value to be retrieved.
     * @param key The parameter key whose value is to be removed and returned.
     * @return An optional containing the value associated with the specified parameter key,
     *         or an empty optional if the key is not found.
     */
    template <typename T>
    opt<T> pop(ParamKey key) {
        if (this->contains(key)) {
            T value = std::move(std::any_cast<T>(this->at(key)));
            this->erase(key);
            return value;
        }
        return std::nullopt;
    };
};

}

#endif  // _PARAMS_HPP
