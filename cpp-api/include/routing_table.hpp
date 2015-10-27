#ifndef SCALARIS_ROUTINGTABLE_HPP
#define SCALARIS_ROUTINGTABLE_HPP

#include <iostream>
#include <string>
#include <stdexcept>

#include <boost/asio.hpp>
#include "json/json.h"
#include "req_list.hpp"

namespace scalaris {
  class RoutingTable {
    Connection& c;
  public:
    RoutingTable(Connection& _c) : c(_c) {}

    int get_replication_factor() {
      Json::Value result = c.rpc("get_replication_factor");

      if(!result.isObject()) {
        std::stringstream error;
        error << result.toStyledString() << " is no object";
        throw std::runtime_error(error.str());
      }
      Json::Value result_status = result["status"];
      if(!result_status.isString() or (result_status.asString().compare("ok") != 0)) {
        std::stringstream error;
        error << result.toStyledString() << ": status != ok";
        throw std::runtime_error(error.str());
      }
      Json::Value result_value = result["value"];
      if(!result_value.isIntegral()) {
        std::stringstream error;
        error << result.toStyledString() << " has numerical value field";
        throw std::runtime_error(error.str());
      }
      return result_value.asInt();
    }
  };
}

#endif
