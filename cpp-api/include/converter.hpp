// Copyright 2016 Zuse Institute Berlin
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

  template<typename T>
  class Converter {};

  template<>
  class Converter<int> {
  public:
    static Json::Value to_value(int arg) { return Json::Value(arg); }
  };

  template<>
  class Converter<std::string> {
  public:
    static Json::Value to_value(std::string arg) { return Json::Value(arg); }
  };

  template<>
  class Converter<RequestList> {
  public:
    static Json::Value to_value(RequestList arg) { return arg.get_json_value(); }
  };

  template<typename T, typename U>
  class Converter<std::pair<T,U> > {
  public:
    static Json::Value to_value(std::pair<T,U> arg) {
      Json::Value result;
      result["first"] = Converter<T>::to_value(std::get<0>(arg));
      result["second"] = Converter<T>::to_value(std::get<1>(arg));
      return result;
    }
  };

  template<typename T>
  class Converter<std::vector<T> > {
  public:
    static Json::Value to_value(std::vector<T> arg) {
      Json::Value result;

      for(T& t: arg)
        result.append(Converter<T>::to_value(t));

      return result;
    }
  };
}

#endif
