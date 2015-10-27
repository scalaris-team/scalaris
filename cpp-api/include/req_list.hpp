#ifndef SCALARIS_REQUESTLIST_HPP
#define SCALARIS_REQUESTLIST_HPP

#include <iostream>
#include <string>
#include <stdexcept>

#include <boost/asio.hpp>
#include "json/json.h"

namespace scalaris {
  class RequestList {
    Json::Value reqlist;
    bool _is_commit = false;
  public:
    RequestList() : reqlist(Json::arrayValue) {}

    void add_read(const std::string key) {
      Json::Value read_request = { Json::objectValue };
      read_request["read"] = key;
      reqlist.append(read_request);
    }

    void add_write(const std::string key, const std::string value) {
      Json::Value write_op = { Json::objectValue };
      write_op[key] = as_is(value);
      Json::Value write_request = { Json::objectValue };
      write_request["write"] = write_op;
      reqlist.append(write_request);

    }
    void add_commit() {
      Json::Value commit_request = { Json::objectValue };
      commit_request["commit"] = "";
      reqlist.append(commit_request);
      _is_commit = true;
    }

    bool is_commit() {
      return _is_commit;
    }

    bool is_empty() {
      return _is_commit;
    }

    int size() {
      return reqlist.size();
    }

    operator Json::Value() const { return reqlist; }

  private:
    Json::Value as_is(const std::string& val) {
      Json::Value result = { Json::objectValue };
      result["type"] = "as_is";
      result["value"] = val;

      return result;
    }
  };
}

#endif
