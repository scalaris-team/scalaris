// Copyright 2018 Zuse Institute Berlin
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

#include <iostream>
#include <string>
#include <stdexcept>

#include <boost/asio.hpp>
#include "exceptions.hpp"
#include "json/json.h"

#include "connection.hpp"

namespace scalaris {
  /// \brief Executes a single read or write.
  ///
  /// It uses kv_on_cseq to perform reads and writes.
  class Rbr {
    Connection& c;
  public:
    /**
     * Creates a <code>Rbr</code> instance
     * @param _c the Connection object
     */
    Rbr(Connection& _c) : c(_c) {}

    std::string JsontoString(Json::Value v) {
      std::stringstream sstream;
      Json::StreamWriterBuilder builder;
      std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
      writer->write(v, &sstream);
      std::string result = sstream.str();
      return result;
    }
    /**
     * Reads a key-value pair
     * @param key the key to lookup in Scalaris
     */
    Json::Value read(const std::string key) {
      Json::Value val = c.rpc("rbr_read", key);

      // std::cout << "read: " << Json::StyledWriter().write(val) << std::endl;

      if(!val.isObject())
        throw MalFormedJsonError();
      Json::Value status = val["status"];

      if(!status.isString())
        throw MalFormedJsonError();

      std::string status_str = status.asString();
        if(status_str.compare("ok") != 0)
            throw ReadFailedError(val["reason"].asString());

      Json::Value value = val["value"];
      if(!value.isObject())
        throw MalFormedJsonError();

      Json::Value value_type = value["type"];
      if(!value_type.isString())
        throw MalFormedJsonError();
      Json::Value value_type_str = value_type.asString();
      if(value_type_str.compare("as_is") != 0)
        throw ReadFailedError("unsupported value type");

      Json::Value value_value = value["value"];

      return value_value;
    }

    /**
     * Writes a key-value pair
     * @param key the key to update in Scalaris
     * @param value the value to store under <code>key</code>
     */
    void write(const std::string key, const Json::Value& value) {
      Json::Value val = c.rpc("rbr_write", key, value);

      //std::cout << "write: " << Json::StyledWriter().write(val) << std::endl;

      if(!val.isObject())
        throw MalFormedJsonError();
      Json::Value status = val["status"];
      if(!status.isString())
        throw MalFormedJsonError();
      std::string status_str = status.asString();
      if(status_str.compare("ok") != 0)
        throw WriteFailedError(val["reason"].asString());
    }
  };
}
