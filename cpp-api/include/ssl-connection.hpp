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
#include <boost/asio/ssl.hpp>
#include "converter.hpp"
#include "exceptions.hpp"
#include "json/json.h"

#include "connection.hpp"

namespace scalaris {

  /// represents a SSL connection to Scalaris to execute JSON-RPC requests
  class SSLConnection : public Connection {
    boost::asio::io_service ioservice;
    boost::asio::ssl::context ctx;
    boost::asio::ssl::stream<boost::asio::ip::tcp::socket> socket;
    std::string password = {""};

    bool hasToConnect = true;
  public:
    /**
     * creates a connection instance
     * @param _hostname the host name of the Scalaris instance
     * @param _location the path location for JSON-RPC
     * @param port the TCP port of the Scalaris instance
     */
    SSLConnection(std::string _hostname,
                  std::string _link  = "jsonrpc.yaws");

    ~SSLConnection();

    bool needsConnect() const override { return hasToConnect; };

    /// checks whether the TCP connection is alive
    bool isOpen() const;

    /// closes the TCP connection
    void close();

    /// returns the server port of the TCP connection
    virtual unsigned get_port();

    /// connects to the specified server
    /// it can also be used, if the connection failed
    void connect() override;

    void set_verify_file(const std::string& file);
    void set_certificate_file(const std::string& file);
    void set_private_key(const std::string& file);
    void set_rsa_private_key(const std::string& file);
    void set_password(const std::string& file);

    std::string toString() const override {
      std::stringstream s;
      s << "https://" << hostname << ":" << port << "/" << link;
      return s.str();
    };

  private:
    virtual Json::Value exec_call(const std::string& methodname,
                                  Json::Value params, bool reconnect = true) override;
    Json::Value process_result(const Json::Value& value);

    bool verify_callback(bool preverified, boost::asio::ssl::verify_context& ctx);

    std::string password_callback(std::size_t max_length,
                                  boost::asio::ssl::context::password_purpose purpose);
  };
}
