// Copyright 2015 Zuse Institute Berlin
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

    virtual const char *what() const noexcept override { return msg.c_str(); }
  };

  class CommitFailedError : public std::exception {
    std::string msg;
  public:
    CommitFailedError(const std::string& _msg) : msg(_msg) {}

    virtual const char *what() const noexcept override { return msg.c_str(); }
  };

  class ReadFailedError : public std::exception {
    std::string msg;
  public:
    ReadFailedError(const std::string& _msg) : msg(_msg) {}

    virtual const char *what() const noexcept override { return msg.c_str(); }
  };

  class WriteFailedError : public std::exception {
    std::string msg;
  public:
    WriteFailedError(const std::string& _msg) : msg(_msg) {}

    virtual const char *what() const noexcept override { return msg.c_str(); }
  };

}

#endif
