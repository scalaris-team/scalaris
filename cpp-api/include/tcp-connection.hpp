// Copyright 2017-2018 Zuse Institute Berlin
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

#include "connection.hpp"
#include "converter.hpp"
#include "exceptions.hpp"
#include "json/json.h"

#include <boost/asio.hpp>

#include <array>
#include <iostream>
#include <stdexcept>
#include <string>

namespace scalaris {

  /// represents a TCP connection to Scalaris to execute JSON-RPC requests
  class TCPConnection : public Connection {
    boost::asio::io_service ioservice;
    boost::asio::ip::tcp::socket socket;

    bool hasToConnect = true;

  public:
    TCPConnection() = default;

    /**
     * creates a connection instance
     * @param _hostname the host name of the Scalaris instance
     * @param _link the pathURL for JSON-RPC
     * @param _port the TCP port of the Scalaris instance
     */
    TCPConnection(const std::string& _hostname,
                  const std::string& _link = "jsonrpc.yaws",
                  unsigned _port = 8000);

    ~TCPConnection();

    bool needsConnect() const override { return hasToConnect; };

    /// checks whether the TCP connection is alive
    bool isOpen() const;

    /// closes the TCP connection
    void close();

    /// returns the server port of the TCP connection
    virtual unsigned getPort();

    /// connects to the specified server
    /// it can also be used, if the connection failed
    void connect() override;

    std::string toString() const override {
      std::stringstream s;
      s << "http://" << hostname << ":" << port << "/" << link;
      return s.str();
    };

  private:
    virtual Json::Value exec_call(const std::string& methodname,
                                  Json::Value params,
                                  bool reconnect = true) override;
    Json::Value process_result(const Json::Value& value);
  };
} // namespace scalaris
