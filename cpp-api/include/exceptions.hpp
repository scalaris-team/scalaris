// Copyright 2015-2017 Zuse Institute Berlin
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

#ifndef SCALARIS_EXCEPTIONS_HPP
#define SCALARIS_EXCEPTIONS_HPP

#include <stdexcept>

namespace scalaris {

  /// Exception to report when a key was not found
  class NotFoundError : public std::exception {
  };

  /// Exception to report when a rpc-call is not supported
  class NotSupportedError : public std::exception {
  };

  /// Exception to report mal-formed JSON objects
  class MalFormedJsonError : public std::exception {
  };

  /// Exception to report errors when connecting to Scalaris
  class ConnectionError : public std::exception {
    std::string msg;
  public:
    /**
     * Creates a ConnectionError instance
     * @param _msg the detailed error message
     */
    ConnectionError(const std::string& _msg) : msg(_msg) {}

    /**
     * returns a detailed error description
     */
    virtual const char *what() const noexcept override { return msg.c_str(); }
  };

  /// Exception to report transaction failures
  class CommitFailedError : public std::exception {
    std::string msg;
  public:
    /**
     * Creates a CommitFailedError instance
     * @param _msg the detailed error message
     */
    CommitFailedError(const std::string& _msg) : msg(_msg) {}

    /**
     * returns a detailed error description
     */
    virtual const char *what() const noexcept override { return msg.c_str(); }
  };

  /// Exception to report read errors
  class ReadFailedError : public std::exception {
    std::string msg;
  public:
    /**
     * Creates a ReadFailedError instance
     * @param _msg the detailed error message
     */
    ReadFailedError(const std::string& _msg) : msg(_msg) {}

    /**
     * returns a detailed error description
     */
    virtual const char *what() const noexcept override { return msg.c_str(); }
  };

  /// Exception to report write errors
  class WriteFailedError : public std::exception {
    std::string msg;
  public:
    /**
     * Creates a WriteFailedError instance
     * @param _msg the detailed error message
     */
    WriteFailedError(const std::string& _msg) : msg(_msg) {}

    /**
     * returns a detailed error description
     */
    virtual const char *what() const noexcept override { return msg.c_str(); }
  };

}

#endif
