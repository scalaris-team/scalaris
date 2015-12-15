#include <iostream>

#include <boost/program_options.hpp>

#include "connection.hpp"
#include "transaction_single_op.hpp"

using namespace std;
using namespace scalaris;
namespace po = boost::program_options;

namespace std {
  istream& operator>>(istream& in, std::pair<std::string,std::string>& ss) {
  string s;
  in >> s;
  const size_t sep = s.find(',');
  if (sep==string::npos) {
    ss.first = s;
    ss.second = string();
  } else {
    ss.first  = s.substr(0,sep);
    ss.second = s.substr(sep+1);
  }
  return in;
}}

void read(std::string key) {
  // cout << "reading : " << key << endl;
  try {
    Connection c { "localhost" };
    TransactionSingleOp op = { c };
    string value = op.read(key);
    cout << value << endl;
  } catch (ConnectionError& e) {
    cout << "ConnectionError: " << e.what() << endl;
  } catch (MalFormedJsonError& e) {
    cout << "MalFormedJsonError: " << e.what() << endl;
  } catch (ReadFailedError& e) {
    cout << "ReadFailedError: " << e.what() << endl;
  } catch (CommitFailedError& e) {
    cout << "CommitFailedError: " << e.what() << endl;
  } catch (NotSupportedError& e) {
    cout << "NotSupportedError: " << e.what() << endl;
  }
}

void write(std::string key, std::string value) {
  // cout << "writing : " << key << ", " << value << endl;
  try {
    Connection c { "localhost" };
    TransactionSingleOp op = { c };
    op.write(key, value);
  } catch (ConnectionError& e) {
    cout << "ConnectionError: " << e.what() << endl;
  } catch (MalFormedJsonError& e) {
    cout << "MalFormedJsonError: " << e.what() << endl;
  } catch (WriteFailedError& e) {
    cout << "WriteFailedError: " << e.what() << endl;
  } catch (CommitFailedError& e) {
    cout << "CommitFailedError: " << e.what() << endl;
  } catch (NotSupportedError& e) {
    cout << "NotSupportedError: " << e.what() << endl;
  }
}

int main(int argc, char **argv) {
  // Declare the supported options.
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("read", po::value<std::string>(), "read key")
    ("write", po::value<pair<string, string>>(), "write key,value")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    cout << desc << "\n";
    return 1;
  } else if (vm.count("read")) {
    read(vm["read"].as<string>());
  } else if (vm.count("write")) {
    pair<string,string> kv = vm["write"].as<pair<string,string>>();
    write(get<0>(kv), get<1>(kv));
  } else {
    cout << "Compression level was not set.\n";
  }
  return 0;
}
