// Copyright 2015 Zuse Institute Berlin
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

#include "connection.hpp"
#include "transaction_single_op.hpp"
#include "json/json.h"

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>
#include <boost/test/test_tools.hpp>
#include <boost/filesystem/fstream.hpp>

#include <iostream>

using namespace boost::unit_test;
using namespace std;
using namespace scalaris;

BOOST_AUTO_TEST_SUITE(TransactionSingleOpSuite)

BOOST_AUTO_TEST_CASE( read_unknown_key )
{
  Connection c = { "localhost", "8000" };
  TransactionSingleOp op = { c };

  BOOST_CHECK_THROW(std::string val = op.read("_no_such_key"), ReadFailedError);
}

BOOST_AUTO_TEST_CASE( write )
{
  Connection c = { "localhost", "8000" };
  TransactionSingleOp op = { c };

  op.write("bar", "foo");
}

BOOST_AUTO_TEST_CASE( write_read )
{
  {
    Connection c = { "localhost", "8000" };
    TransactionSingleOp op = { c };

    op.write("write_read", "foo");
  }
  {
    Connection c = { "localhost", "8000" };
    TransactionSingleOp op = { c };

    std::string res = op.read("write_read");
    BOOST_CHECK(res.compare("foo") == 0);
  }
}

BOOST_AUTO_TEST_SUITE_END()
