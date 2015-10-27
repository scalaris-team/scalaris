#include "connection.hpp"
#include "routing_table.hpp"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>
#include <boost/filesystem/fstream.hpp>

#include <iostream>

using namespace boost::unit_test;
using namespace std;
using namespace scalaris;

BOOST_AUTO_TEST_SUITE(MasterSuite)

BOOST_AUTO_TEST_CASE( create_connection )
{
  Connection c("localhost", "8000");

  BOOST_CHECK(c.isOpen());
}

BOOST_AUTO_TEST_CASE( close_connection )
{
  Connection c("localhost", "8000");
  BOOST_CHECK(c.isOpen());
  c.close();
  BOOST_CHECK(!c.isOpen());
}

BOOST_AUTO_TEST_SUITE_END()
