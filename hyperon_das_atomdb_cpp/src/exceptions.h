#pragma once

#include <exception>
#include <string>

using namespace std;

namespace atomdb {

class AtomDbBaseException : public exception {
   public:
    AtomDbBaseException(const string& message, const string& details)
        : message(message), details(details) {}

    virtual const char* what() const noexcept override {
        return (this->message + ": " + this->details).c_str();
    }

   protected:
    string message;
    string details;
};

/**
 * @brief Exception thrown when an atom does not exist in the database.
 */
class AtomDoesNotExist : public AtomDbBaseException {
   public:
    using AtomDbBaseException::AtomDbBaseException;
    // virtual const char* what() const noexcept override { return string("WTF!!!").c_str(); }
    const string get_info() const noexcept {
        return (this->message + ": " + this->details);
    }
};

/**
 * @brief Exception thrown when adding a node to the database fails.
 */
class AddNodeException : public AtomDbBaseException {
    using AtomDbBaseException::AtomDbBaseException;
};

/**
 * @brief Exception thrown when adding a link to the database fails.
 */
class AddLinkException : public AtomDbBaseException {
    using AtomDbBaseException::AtomDbBaseException;
};

/**
 * @brief Exception thrown when an invalid operation is performed on the database.
 */
class InvalidOperationException : public AtomDbBaseException {
    using AtomDbBaseException::AtomDbBaseException;
};

/**
 * @brief Exception raised for retryable errors.
 */
class RetryException : public AtomDbBaseException {
    using AtomDbBaseException::AtomDbBaseException;
};

/**
 * @brief Exception raised for invalid Atom DB operations.
 */
class InvalidAtomDB : public AtomDbBaseException {
    using AtomDbBaseException::AtomDbBaseException;
};

}  // namespace atomdb
