#include "connection.hpp"
#include "routing_table.hpp"
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

BOOST_AUTO_TEST_SUITE(RoutingTableSuite)

BOOST_AUTO_TEST_CASE( get_replication_factor )
{
  Connection c("localhost", "8000");
  RoutingTable rt(c);
  int r = rt.get_replication_factor();
  BOOST_CHECK(r > 0);
}

BOOST_AUTO_TEST_SUITE_END()
