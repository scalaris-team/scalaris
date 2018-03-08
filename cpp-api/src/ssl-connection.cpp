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

#include "ssl-connection.hpp"

#include <boost/algorithm/string.hpp>

namespace scalaris {

  SSLConnection::SSLConnection(std::string hostname, std::string link)
      : Connection(hostname, link),
        ctx(boost::asio::ssl::context_base::method::sslv23),
        socket(ioservice, ctx) {
    try {connect();}
    catch(scalaris::ConnectionError) {}
  }

  void SSLConnection::connect() {
    using boost::asio::ip::tcp;

    socket.set_verify_mode(boost::asio::ssl::verify_peer);
    socket.set_verify_callback(
        [this](bool preverified, boost::asio::ssl::verify_context& ctx) {
          return this->verify_callback(preverified, ctx);
        });

    // ctx.load_verify_file("ca.pem");
    ctx.set_password_callback(
        [this](std::size_t max_length,
               boost::asio::ssl::context::password_purpose purpose) {
          return this->password_callback(max_length, purpose);
        });

    // Determine the location of the server.
    tcp::resolver resolver(ioservice);
    tcp::resolver::query query(hostname, std::to_string(get_port()));
    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

    // Try each endpoint until we successfully establish a connection.
    boost::system::error_code ec;
    boost::asio::connect(socket.lowest_layer(), endpoint_iterator, ec);
    hasToConnect = false;
    if (ec) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << ec.message()
                << std::endl;
      hasToConnect = true;
      throw ConnectionError(ec.message());
    }

    socket.handshake(boost::asio::ssl::stream_base::client, ec);
    if (ec) {
      std::cout << __FILE__ << ":" << __LINE__ << " " << ec.message()
                << std::endl;
      hasToConnect = true;
      throw ConnectionError(ec.message());
    }
  }

  SSLConnection::~SSLConnection() {
    if (!closed) {
      socket.lowest_layer().close();
    }
  }

  void SSLConnection::set_verify_file(const std::string& file) {
    // Load a certification authority file for performing verification.
    ctx.load_verify_file(file); //"ca.pem"
  }

  void SSLConnection::set_certificate_file(const std::string& file) {
    //  Use a certificate from a file.
    ctx.use_certificate_file(file, boost::asio::ssl::context_base::file_format::pem);
  }
  void SSLConnection::set_private_key(const std::string& file) {
    // Use a private key from a file.
    ctx.use_private_key_file(file, boost::asio::ssl::context_base::file_format::pem);
  }

  void SSLConnection::set_rsa_private_key(const std::string& file) {
    // Use an RSA private key from a file.
    ctx.use_rsa_private_key_file(file, boost::asio::ssl::context_base::file_format::pem);
  }

  void SSLConnection::set_password(const std::string& pw) { password = pw; }

  bool SSLConnection::isOpen() const {
    return socket.lowest_layer().is_open();
  };

  void SSLConnection::close() {
    socket.lowest_layer().close();
    closed = true;
  };

  unsigned SSLConnection::get_port() {
    char* port = getenv("SCALARIS_UNITTEST_YAWS_PORT");
    if (port == NULL)
      return 8000;
    else
      return atoi(port);
  }

  bool SSLConnection::verify_callback(bool preverified,
                                      boost::asio::ssl::verify_context& ctx) {
    // @todo
    return true;
  }

  std::string SSLConnection::password_callback(
      std::size_t max_length,
      boost::asio::ssl::context::password_purpose purpose) {
    // @todo
    // for_reading or for_writing
    return password;
  }

  Json::Value SSLConnection::exec_call(const std::string& methodname,
                                       Json::Value params, bool reconnect) {
    // FIXME add reconnect
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
      std::cout << __FILE__ << ":" << __LINE__ << " " << ec.message()
                << std::endl;
      hasToConnect = true;
      throw ConnectionError(ec.message());
    }

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
      hasToConnect = true;
      throw ConnectionError("Invalid response");
    }
    if (status_code != 200) {
      std::cout << "Response returned with status code " << status_code << "\n";
      std::stringstream error;
      error << "Response returned with status code " << status_code;
      hasToConnect = true;
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
    // Write whatever content we already have to output.
    if (response.size() > 0)
      json_result << &response;

    Json::CharReaderBuilder reader_builder;
    reader_builder["collectComments"] = false;
    Json::Value value;
    std::string errs;
    bool ok = Json::parseFromStream(reader_builder, json_result, &value, &errs);
    if (!ok) {
      hasToConnect = true;
      throw ConnectionError(errs);
    }

    return process_result(value);
  }

  Json::Value SSLConnection::process_result(const Json::Value& value) {
    // @todo
    if (value.isObject()) {
      Json::Value id = value["id"];
      if (!id.isIntegral() or id.asInt() != 0) {
        std::stringstream error;
        error << value.toStyledString() << " is no id=0";
        hasToConnect = true;
        throw ConnectionError(error.str());
      }
      Json::Value jsonrpc = value["jsonrpc"];
      if (!jsonrpc.isString()) {
        std::stringstream error;
        error << value.toStyledString() << " has no string member: jsonrpc";
        hasToConnect = true;
        throw ConnectionError(error.str());
      }
      Json::Value result = value["result"];
      if (!result.isObject()) {
        std::stringstream error;
        error << value.toStyledString() << " has no object member: result";
        hasToConnect = true;
        throw ConnectionError(error.str());
      }
      return result;
    } else {
      std::stringstream error;
      error << value.toStyledString() << " is no json object";
      hasToConnect = true;
      throw ConnectionError(error.str());
    }
  }

} // namespace scalaris
