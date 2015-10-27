#ifndef SCALARIS_TRANSACTIONSINGLEOP_HPP
#define SCALARIS_TRANSACTIONSINGLEOP_HPP

#include <iostream>
#include <string>
#include <stdexcept>

#include <boost/asio.hpp>
#include "exceptions.hpp"
#include "json/json.h"
#include "req_list.hpp"

namespace scalaris {
  class TransactionSingleOp {
    Connection& c;
  public:
    TransactionSingleOp(Connection& _c) : c(_c) {}

    std::string read(const std::string key) {
      RequestList reqlist;
      reqlist.add_read(key);
      reqlist.add_commit();

      Json::Value val = c.rpc("req_list", reqlist);

      if(!val.isObject())
        throw MalFormedJsonError();
      Json::Value val_results = val["results"];
      if(!val_results.isArray())
        throw MalFormedJsonError();
      Json::Value val_tlog = val["tlog"];
      if(!val_tlog.isString())
        throw MalFormedJsonError();

      // 1. success of read
      {
        Json::Value tmp = val_results[0];
        if(!tmp.isObject())
          throw MalFormedJsonError();
        Json::Value status = tmp["status"];
        if(!status.isString())
          throw MalFormedJsonError();
        std::string status_str = status.asString();
        if(status_str.compare("ok") != 0)
            throw ReadFailedError(tmp["reason"].asString());
      }

      // 2. success of commit
      {
        Json::Value tmp = val_results[0];
        if(!tmp.isObject())
          throw MalFormedJsonError();
        Json::Value status = tmp["status"];
        if(!status.isString())
          throw MalFormedJsonError();
        std::string status_str = status.asString();
        if(status_str.compare("ok") != 0)
            throw CommitFailedError(tmp["reason"].asString());
      }

        Json::Value value = val_results[0]["value"];
        if(!value.isObject())
          throw MalFormedJsonError();
        Json::Value type = value["type"];
        if(!type.isString())
          throw MalFormedJsonError();
        std::string type_str = type.asString();
        if(type_str.compare("as_is") != 0)
          throw NotSupportedError();
        //type is as_is
        Json::Value the_value = value["value"];
        if(!the_value.isString())
          throw MalFormedJsonError();
        return the_value.asString();
    }

    void write(const std::string key, const std::string value) {
      RequestList reqlist;
      reqlist.add_write(key, value);
      reqlist.add_commit();
      Json::Value val = c.rpc("req_list", reqlist);

      if(!val.isObject())
        throw MalFormedJsonError();
      Json::Value val_results = val["results"];
      if(!val_results.isArray())
        throw MalFormedJsonError();
      Json::Value val_tlog = val["tlog"];
      if(!val_tlog.isString())
        throw MalFormedJsonError();

      // 1. success of write
      {
        Json::Value tmp = val_results[0];
        if(!tmp.isObject())
          throw MalFormedJsonError();
        Json::Value status = tmp["status"];
        if(!status.isString())
          throw MalFormedJsonError();
        std::string status_str = status.asString();
        if(status_str.compare("ok") != 0)
            throw WriteFailedError(tmp["reason"].asString());
      }

      // 2. success of commit
      {
        Json::Value tmp = val_results[0];
        if(!tmp.isObject())
          throw MalFormedJsonError();
        Json::Value status = tmp["status"];
        if(!status.isString())
          throw MalFormedJsonError();
        std::string status_str = status.asString();
        if(status_str.compare("ok") != 0)
            throw CommitFailedError(tmp["reason"].asString());
      }
    }
  };
}

#endif
