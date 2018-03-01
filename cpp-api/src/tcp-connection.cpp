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

#include "tcp-connection.hpp"

#include <boost/algorithm/string.hpp>

namespace scalaris {

  TCPConnection::TCPConnection(std::string _hostname, std::string _link)
      : socket(ioservice), Connection(_hostname, _link) {
    connect();
  }

  void TCPConnection::connect() {
    using boost::asio::ip::tcp;

    // Determine the location of the server.
    tcp::resolver resolver(ioservice);
    tcp::resolver::query query(hostname, std::to_string(get_port()));
    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

    // create a new socket in case of reconnect
    socket = boost::asio::ip::tcp::socket(ioservice);

    // Try each endpoint until we successfully establish a connection.
    boost::system::error_code ec;
    boost::asio::connect(socket, endpoint_iterator, ec);
    if (ec) {
      std::cout << ec.message() << std::endl;
      throw ConnectionError(ec.message());
    }

    // boost::asio::socket_base::keep_alive option(true);
    // socket.set_option(option);
  }

  TCPConnection::~TCPConnection() {
    if (!closed) {
      boost::system::error_code ec;
      socket.close(ec);
    }
  }

  bool TCPConnection::isOpen() const { return socket.is_open(); };

  void TCPConnection::close() {
    socket.close();
    closed = true;
  };

  unsigned TCPConnection::get_port() {
    char* port = getenv("SCALARIS_UNITTEST_YAWS_PORT");
    if (port == NULL)
      return 8000;
    else
      return atoi(port);
  }

  Json::Value TCPConnection::exec_call(const std::string& methodname,
                                       Json::Value params) {

    // FIXME: reconnect when the connection is closed
    Json::Value call;
    call["method"] = methodname;
    call["params"] = params;
    call["id"] = 0;

    Json::FastWriter w;
    std::string json_call_str = w.write(call);
    boost::trim_right(json_call_str);

    boost::asio::streambuf request;
    std::ostream request_stream(&request);
    request_stream << "POST /" << link << " HTTP/1.1\r\n";
    request_stream << "Host: " << hostname << "\r\n";
    request_stream << "Content-Type: application/json-rpc\r\n";
    request_stream << "Content-Length: " << json_call_str.size() << "\r\n";
    request_stream << "Connection: keep-alive\r\n\r\n";
    request_stream << json_call_str << "\r\n\r\n";

    // Send the request.
    boost::system::error_code ec;
    boost::asio::write(socket, request, ec);
    if (ec) {
      std::cout << ec.message() << std::endl;
      throw ConnectionError(ec.message());
    }

    try {
      boost::asio::streambuf response;
      boost::asio::read_until(socket, response, "\r\n");
      // Check that response is OK.
      std::istream response_stream(&response);
      std::string http_version;
      response_stream >> http_version;
      unsigned int status_code;
      response_stream >> status_code;
      std::string status_message;
      std::getline(response_stream, status_message);
      if (!response_stream || http_version.substr(0, 5) != "HTTP/") {
        std::cout << "Invalid response\n";
        throw ConnectionError("Invalid response");
      }
      if (status_code != 200) {
        std::cout << "Response returned with status code " << status_code
                  << "\n";
        std::stringstream error;
        error << "Response returned with status code " << status_code;
        throw ConnectionError(error.str());
      }

      // Read the response headers, which are terminated by a blank line.
      boost::asio::read_until(socket, response, "\r\n\r\n");
      // Process the response headers.
      std::string header;
      std::stringstream header_stream;
      while (std::getline(response_stream, header) && header != "\r")
        header_stream << header << "\n";
      header_stream << "\n";

      std::stringstream json_result;
      boost::asio::read_until(socket, response, "\n");
      // Write whatever content we already have to output.
      if (response.size() > 0)
        json_result << &response;

      Json::CharReaderBuilder reader_builder;
      reader_builder["collectComments"] = false;
      Json::Value value;
      std::string errs;
      bool ok =
          Json::parseFromStream(reader_builder, json_result, &value, &errs);
      if (!ok)
        throw ConnectionError(errs);
      return process_result(value);
    } catch (const std::exception& e) {
      throw ConnectionError(e.what());
    }
  }

  Json::Value TCPConnection::process_result(const Json::Value& value) {
    if (value.isObject()) {
      Json::Value id = value["id"];
      if (!id.isIntegral() or id.asInt() != 0) {
        std::stringstream error;
        error << value.toStyledString() << " is no id=0";
        throw ConnectionError(error.str());
      }
      Json::Value jsonrpc = value["jsonrpc"];
      if (!jsonrpc.isString()) {
        std::stringstream error;
        error << value.toStyledString() << " has no string member: jsonrpc";
        throw ConnectionError(error.str());
      }
      Json::Value result = value["result"];
      if (!result.isObject()) {
        std::stringstream error;
        error << value.toStyledString() << " has no object member: result";
        throw ConnectionError(error.str());
      }
      return result;
    } else {
      std::stringstream error;
      error << value.toStyledString() << " is no json object";
      throw ConnectionError(error.str());
    }
  }

} // namespace scalaris
