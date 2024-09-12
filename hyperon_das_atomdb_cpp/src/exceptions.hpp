#ifndef _EXCEPTIONS_HPP
#define _EXCEPTIONS_HPP

#include <exception>
#include <string>

using namespace std;

namespace atomdb {

class BaseException : public exception {
   public:
    BaseException(const string& message, const string details) : message(message), details(details) {}

    const char* what() const noexcept override { return (message + ": " + details).c_str(); }

   private:
    string message;
    string details;
};

class AtomDoesNotExist : public BaseException {
    using BaseException::BaseException;
};

class InvalidOperationException : public BaseException {
    using BaseException::BaseException;
};

}  // namespace atomdb

#endif  // _EXCEPTIONS_HPP
