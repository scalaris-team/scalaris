// Copyright 2015, 2016 Zuse Institute Berlin
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

#ifndef SCALARIS_CONNECTION_HPP
#define SCALARIS_CONNECTION_HPP

#include <array>
#include <iostream>
#include <string>
#include <stdexcept>

#include <boost/asio.hpp>
#include "exceptions.hpp"
#include "json/json.h"

namespace scalaris {

  class Connection {
    boost::asio::io_service ioservice;
    boost::asio::ip::tcp::socket socket;
    std::string hostname;
    std::string link;
    bool closed=false;
  public:
    Connection(std::string _hostname,
               std::string _link  = "jsonrpc.yaws",
               std::string port = Connection::get_port());

    ~Connection();

    bool isOpen() const;

    void close();

    static std::string get_port();

    template<typename... Args>
    Json::Value rpc(const std::string& methodname, Args... args) {
      std::array<Json::Value, sizeof...(args)> arg_list = {{args... }};

      Json::Value params = Json::arrayValue;
      for(size_t i = 0; i< sizeof...(args); i++)
        params.append(arg_list[i]);
      return exec_call(methodname, params);
    }

  private:
    Json::Value exec_call(const std::string& methodname, Json::Value params);
    Json::Value process_result(const Json::Value& value);
  };
}

#endif
