// Copyright 2015-2017 Zuse Institute Berlin
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
#include "routing_table.hpp"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/filesystem/fstream.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>

#include <iostream>

using namespace boost::unit_test;
using namespace std;
using namespace scalaris;

BOOST_AUTO_TEST_SUITE(MasterSuite)

BOOST_AUTO_TEST_CASE(create_connection) {
  SSLConnection c = {"localhost"};

  c.set_verify_file("../ca-chain.cert.pem");
  c.set_certificate_file("../host2.cert.pem");
  c.set_private_key("../host2.key.pem");
  //c.set_rsa_private_key(const std::string& file);
  c.set_password("secretpassword");

  BOOST_CHECK(c.isOpen());
}

//BOOST_AUTO_TEST_CASE(close_connection) {
//  SSLConnection c = {"localhost"};
//  BOOST_CHECK(c.isOpen());
//  c.close();
//  BOOST_CHECK(!c.isOpen());
//}

BOOST_AUTO_TEST_SUITE_END()
