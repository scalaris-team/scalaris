// Copyright 2015-2018 Zuse Institute Berlin
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

#pragma once

#include <array>
#include <iostream>
#include <string>
#include <stdexcept>

#include <boost/asio.hpp>
#include "converter.hpp"
#include "exceptions.hpp"
#include "json/json.h"

namespace scalaris {

  /// represents a connection to Scalaris to execute JSON-RPC requests
  class Connection {
  protected:
    bool closed=false;
    std::string hostname;
    std::string link;
  protected:
    /**
     * creates a connection instance
     * @param _hostname the host name of the Scalaris instance
     * @param _link the URL for JSON-RPC
     * @param port the TCP port of the Scalaris instance
     */
    Connection(std::string _hostname,
               std::string _link  = "jsonrpc.yaws"
               );

    ~Connection() = default;

  public:
    /**
     * performs a JSON-RPC request
     * @param methodname the name of the function to call
     * @param args the list of arguments of the function call
     */
    template<typename... Args>
    Json::Value rpc(const std::string& methodname, Args... args) {
      std::array<Json::Value, sizeof...(args)> arg_list = {{Converter<Args>::to_value(args)... }};

      Json::Value params = Json::arrayValue;
      for(size_t i = 0; i< sizeof...(args); i++)
        params.append(arg_list[i]);
      return exec_call(methodname, params);
    }

  protected:
    virtual Json::Value exec_call(const std::string& methodname,
                                  Json::Value params,
                                  bool reconnect = true) = 0;
  };
} // namespace scalaris
