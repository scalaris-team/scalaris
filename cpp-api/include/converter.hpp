// Copyright 2016-2018 Zuse Institute Berlin
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

#ifndef SCALARIS_CONVERTER_HPP
#define SCALARIS_CONVERTER_HPP

#include <string>

#include "req_list.hpp"
#include "json/json.h"

namespace scalaris {

  /// converts C++ values to JSON values
  /* for compound types it recursively walks over the member objects */
  template<typename T>
  class Converter {};

  /// \brief converts integers to JSON integers
  template<>
  class Converter<int> {
  public:
    /**
     * converts an integer to a JSON value
     * @param arg the integer value
     */
    static Json::Value to_value(int arg) { return Json::Value(arg); }
  };

  /// \brief converts unsigned integers to JSON integers
  template<>
  class Converter<unsigned int> {
  public:
    /**
     * converts an integer to a JSON value
     * @param arg the integer value
     */
    static Json::Value to_value(unsigned int arg) { return Json::Value(arg); }
  };

  /// converts std::string to JSON strings
  template<>
  class Converter<std::string> {
  public:
    /**
     * converts a std::string to a JSON value
     * @param arg the std::string value
     */
    static Json::Value to_value(std::string arg) { return Json::Value(arg); }
  };

  /// converts RequestLists to JSON lists
  template<>
  class Converter<RequestList> {
  public:
    /**
     * converts a request list to a JSON value
     * @param arg the request list value
     */
    static Json::Value to_value(RequestList arg) { return arg.get_json_value(); }
  };

  /// converts std::pair<T,U> to JSON structs
  template<typename T, typename U>
  class Converter<std::pair<T,U> > {
  public:
    /**
     * converts a std::pair to a JSON value
     * @param arg the std::pair value
     */
    static Json::Value to_value(std::pair<T,U> arg) {
      Json::Value result;
      result["first"] = Converter<T>::to_value(std::get<0>(arg));
      result["second"] = Converter<T>::to_value(std::get<1>(arg));
      return result;
    }
  };

  /// converts std::vector<T> to JSON arrays
  template<typename T>
  class Converter<std::vector<T> > {
  public:
    /**
     * converts a std::vector to a JSON value
     * @param arg the std::vector value
     */
    static Json::Value to_value(std::vector<T> arg) {
      Json::Value result;

      for(T& t: arg)
        result.append(Converter<T>::to_value(t));

      return result;
    }
  };

  /// converts Json::Values to JSON values
  template<>
  class Converter<Json::Value> {
  public:
    /**
     * converts a request list to a JSON value
     * @param arg the request list value
     */
    static Json::Value to_value(Json::Value arg) { return arg; }
  };

}

#endif
