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

#include "rbr.hpp"
#include "tcp-connection.hpp"
#include "json/json.h"

#define BOOST_TEST_DYN_LINK

#include <boost/filesystem/fstream.hpp>
#include <boost/test/test_tools.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>

#include <iostream>

using namespace boost::unit_test;
using namespace std;
using namespace scalaris;

BOOST_AUTO_TEST_SUITE(RbrSuite)

BOOST_AUTO_TEST_CASE(read_unknown_key) {
  TCPConnection c = {"localhost"};
  Rbr r = {c};

  BOOST_CHECK_THROW(std::string val = r.read("_no_such_key").toStyledString(), ReadFailedError);
}

BOOST_AUTO_TEST_CASE(write) {
  TCPConnection c = {"localhost"};
  Rbr r = {c};

  r.write("bar", Json::Value("foo"));
}

BOOST_AUTO_TEST_CASE(write_read) {
  {
    TCPConnection c = {"localhost"};
    Rbr r = {c};

    r.write("write_read", Json::Value("foo"));
  }
  {
    TCPConnection c = {"localhost"};
    Rbr r = {c};

    Json::Value res = r.read("write_read");
    BOOST_CHECK(res.compare(Json::Value("foo")) == 0);
  }
}

BOOST_AUTO_TEST_SUITE_END()
