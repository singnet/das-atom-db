#ifndef _EXCEPTIONS_HPP
#define _EXCEPTIONS_HPP

#include <exception>
#include <string>

class AtomDoesNotExist : public std::exception {
   public:
    AtomDoesNotExist(const std::string& message, const std::string details)
        : message(message), details(details) {}

    const char* what() const noexcept override {
        return (message + ": " + details).c_str();
    }

   private:
    std::string message;
    std::string details;
};

class InvalidOperationException : public std::exception {
   public:
    InvalidOperationException(const std::string& message, const std::string details)
        : message(message), details(details) {}

    const char* what() const noexcept override {
        return (message + ": " + details).c_str();
    }

   private:
    std::string message;
    std::string details;
};

#endif  // _EXCEPTIONS_HPP
