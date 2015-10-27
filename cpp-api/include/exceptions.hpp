#ifndef SCALARIS_EXCEPTIONS_HPP
#define SCALARIS_EXCEPTIONS_HPP

#include <stdexcept>

namespace scalaris {

  class AbortError : public std::exception {
  };

  class NotFoundError : public std::exception {
  };

  class NotSupportedError : public std::exception {
  };

  class MalFormedJsonError : public std::exception {
  };

  class ConnectionError : public std::exception {
    std::string msg;
  public:
    ConnectionError(const std::string& _msg) : msg(_msg) {}

    virtual const char *what() const _NOEXCEPT override { return msg.c_str(); }
  };

  class CommitFailedError : public std::exception {
    std::string msg;
  public:
    CommitFailedError(const std::string& _msg) : msg(_msg) {}

    virtual const char *what() const _NOEXCEPT override { return msg.c_str(); }
  };

  class ReadFailedError : public std::exception {
    std::string msg;
  public:
    ReadFailedError(const std::string& _msg) : msg(_msg) {}

    virtual const char *what() const _NOEXCEPT override { return msg.c_str(); }
  };

  class WriteFailedError : public std::exception {
    std::string msg;
  public:
    WriteFailedError(const std::string& _msg) : msg(_msg) {}

    virtual const char *what() const _NOEXCEPT override { return msg.c_str(); }
  };

}

#endif
