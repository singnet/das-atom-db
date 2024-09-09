#ifndef _PARAMS_HPP
#define _PARAMS_HPP

#include "type_aliases.hpp"

using namespace std;

namespace atomdb {

using ParamKey = string;
using ParamValue = any;
using ParamsMap = unordered_map<ParamKey, ParamValue>;

/**
 * @brief Namespace containing parameters keys.
 *
 * The ParamsKeys namespace defines various parameters that can be used as params
 * within the application. Each parameter represents a specific flag that can be
 * utilized to control different aspects of the application's behavior.
 */
namespace ParamsKeys {
static const ParamKey CURSOR = "cursor";
static const ParamKey DEEP_REPRESENTATION = "deep_representation";
static const ParamKey DELETE_ATOM = "delete_atom";
static const ParamKey NO_TARGET_FORMAT = "no_target_format";
static const ParamKey TARGETS_DOCUMENTS = "targets_documents";
static const ParamKey TOPLEVEL_ONLY = "toplevel_only";
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
    bool contains(const ParamKey& key) const { return this->find(key) != this->end(); };

    /**
     * @brief Retrieves the value associated with the specified parameter key.
     * @tparam T The type of the value to be retrieved.
     * @param key The parameter key whose value is to be retrieved.
     * @return An optional containing the value associated with the specified parameter key,
     *         or an empty optional if the key is not found.
     */
    template <typename T>
    opt<const T> get(const ParamKey& key) const {
        auto it = this->find(key);
        if (it != this->end()) {
            try {
                return any_cast<T>(it->second);
            } catch (const bad_any_cast& e) {
                if constexpr (is_same_v<T, string>) {
                    return string(any_cast<const char*>(it->second));
                }
                throw e;
            }
        }
        return nullopt;
    };

    /**
     * @brief Sets the value for the specified parameter.
     * @param key The parameter whose value is to be set.
     * @param value The value to be associated with the specified parameter.
     */
    void set(const ParamKey& key, any value) { this->insert_or_assign(key, value); };

    /**
     * @brief Removes and returns the value associated with the specified parameter key.
     * @tparam T The type of the value to be retrieved.
     * @param key The parameter key whose value is to be removed and returned.
     * @return An optional containing the value associated with the specified parameter key,
     *         or an empty optional if the key is not found.
     */
    template <typename T>
    opt<T> pop(const ParamKey& key) {
        auto temp_value = this->get<T>(key);
        if (temp_value.has_value()) {
            T value = move(temp_value.value());
            this->erase(key);
            return value;
        }
        return nullopt;
    };
};

}  // namespace atomdb

#endif  // _PARAMS_HPP
