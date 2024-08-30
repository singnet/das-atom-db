#ifndef _FLAGS_HPP
#define _FLAGS_HPP

#include <any>
#include <string>
#include <unordered_map>

/**
 * @brief Enumeration of flag parameters.
 *
 * This enum class defines the various parameters that can be used as flags
 * within the application. Each enumerator represents a specific flag parameter.
 */
enum class FlagsParams {
    NO_TARGET_FORMAT,
    TARGETS_DOCUMENTS,
    DEEP_REPRESENTATION,
    CURSOR
};

/**
 * @brief A class representing a collection of flags.
 *
 * This class is a specialized unordered map that stores flag parameters and their associated values.
 * It inherits privately from std::unordered_map, using FlagsParams as keys and std::any as values.
 */
class Flags : private std::unordered_map<FlagsParams, std::any> {
   public:
    /**
     * @brief Constructs a Flags object with the specified initial flags.
     * @param flags An unordered map where the keys are FlagsParams and the values are of type std::any,
     *              representing the initial set of flags. Defaults to an empty map if not provided.
     * @return A Flags object initialized with the specified flags.
     */
    Flags(const std::unordered_map<FlagsParams, std::any>& flags = {}) {
        for (const auto& [key, value] : flags) {
            this->set(key, value);
        }
    };

    /**
     * @brief Checks if the specified flag parameter exists in the collection.
     * @param key The flag parameter to check for existence.
     * @return A boolean value indicating whether the flag parameter exists (true) or not (false).
     */
    bool contains(FlagsParams key) const {
        return this->find(key) != this->end();
    };

    /**
     * @brief Retrieves the value associated with the specified flag parameter.
     * @tparam T The type of the value to be retrieved.
     * @param key The flag parameter whose value is to be retrieved.
     * @param default_value The default value to return if the key is not found.
     * @return The value associated with the specified flag parameter, or the default value if the
     *         key is not found.
     */
    template <typename T>
    T get(FlagsParams key, T default_value) const {
        if (this->contains(key)) {
            return std::any_cast<T>(this->at(key));
        }
        return default_value;
    };

    /**
     * @brief Sets the value for the specified flag parameter.
     * @tparam T The type of the value to be set.
     * @param key The flag parameter whose value is to be set.
     * @param value The value to be associated with the specified flag parameter.
     */
    template <typename T>
    void set(FlagsParams key, T value) {
        this->insert_or_assign(key, value);
    };

    /**
     * @brief Removes and returns the value associated with the specified flag parameter.
     * @tparam T The type of the value to be retrieved.
     * @param key The flag parameter whose value is to be removed and returned.
     * @param default_value The default value to return if the key is not found.
     * @return The value associated with the specified flag parameter, or the default value if the
     *         key is not found.
     */
    template <typename T>
    T pop(FlagsParams key, T default_value) {
        if (this->find(key) != this->end()) {
            T value = std::any_cast<T>(this->at(key));
            this->erase(key);
            return value;
        }
        return default_value;
    };
};

#endif  // _FLAGS_HPP
