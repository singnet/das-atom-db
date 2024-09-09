#ifndef _EXCEPTIONS_HPP
#define _EXCEPTIONS_HPP

#include <exception>
#include <string>

using namespace std;

namespace atomdb {

class AtomDoesNotExist : public exception {
   public:
    AtomDoesNotExist(const string& message, const string details) : message(message), details(details) {}

    const char* what() const noexcept override { return (message + ": " + details).c_str(); }

   private:
    string message;
    string details;
};

class InvalidOperationException : public exception {
   public:
    InvalidOperationException(const string& message, const string details)
        : message(message), details(details) {}

    const char* what() const noexcept override { return (message + ": " + details).c_str(); }

   private:
    string message;
    string details;
};

}  // namespace atomdb

#endif  // _EXCEPTIONS_HPP
