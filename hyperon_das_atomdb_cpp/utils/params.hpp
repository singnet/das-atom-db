#ifndef _PARAMS_HPP
#define _PARAMS_HPP

#include <any>
#include <optional>
#include <string>
#include <unordered_map>

/**
 * @brief Namespace containing flag parameters.
 *
 * The FlagsParams namespace defines various parameters that can be used as params
 * within the application. Each parameter represents a specific flag that can be
 * utilized to control different aspects of the application's behavior.
 */
namespace FlagsParams {
constexpr const char* CURSOR = "cursor";
constexpr const char* DEEP_REPRESENTATION = "deep_representation";
constexpr const char* NO_TARGET_FORMAT = "no_target_format";
constexpr const char* TARGETS_DOCUMENTS = "targets_documents";
};  // namespace FlagsParams

/**
 * @brief Namespace containing atom parameters.
 *
 * The AtomParams namespace defines various parameters that can be used to specify
 * properties and behaviors of atoms within the application. Each parameter represents
 * a specific attribute or setting related to atoms.
 */
namespace AtomParams {
constexpr const char* COMPOSITE_TYPE = "composite_type";
constexpr const char* COMPOSITE_TYPE_HASH = "composite_type_hash";
constexpr const char* HANDLE = "handle";
constexpr const char* ID = "id";
constexpr const char* IS_TOP_LEVEL = "is_top_level";
constexpr const char* KEYS = "keys";
constexpr const char* NAME = "name";
constexpr const char* NAMED_TYPE = "named_type";
constexpr const char* NAMED_TYPE_HASH = "named_type_hash";
constexpr const char* TARGETS = "targets";
constexpr const char* TARGETS_DOCUMENTS = "targets_documents";
};  // namespace AtomParams

/**
 * @brief A class representing a collection of params.
 *
 * This class is a specialized unordered map that stores parameters and their associated values.
 * It inherits privately from std::unordered_map, using const char* as keys and std::any as values.
 */
class Params : private std::unordered_map<const char*, std::any> {
   public:
    /**
     * @brief Constructs a Params object with the specified initial params.
     * @param params An unordered map where the keys are const char* and the values are of type
     *               std::any, representing the initial set of params.
     *               Defaults to an empty map if not provided.
     * @return A Params object initialized with the specified params.
     */
    Params(const std::unordered_map<const char*, std::any>& params = {}) {
        for (const auto& [key, value] : params) {
            this->set(key, value);
        }
    };

    /**
     * @brief Checks if the specified parameter exists in the collection.
     * @param key The parameter to check for existence.
     * @return A boolean value indicating whether the parameter exists (true) or not (false).
     */
    bool contains(const char* key) const {
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
    std::optional<T> get(const char* key, std::optional<T> default_value = std::nullopt) const {
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
    void set(const char* key, T value) {
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
    std::optional<T> pop(const char* key, std::optional<T> default_value = std::nullopt) {
        if (this->contains(key)) {
            T value = std::any_cast<T>(this->at(key));
            this->erase(key);
            return value;
        }
        return default_value;
    };
};

#endif  // _PARAMS_HPP
