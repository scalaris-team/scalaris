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

template<typename F>
void exec_call(F& f){
  try {
    Connection c { "localhost" };
    TransactionSingleOp op = { c };
    f(op);
  } catch (std::runtime_error& e) {
    cout << "std::runtime_error: " << e.what() << endl;
    exit(EXIT_FAILURE);
  } catch (ConnectionError& e) {
    cout << "ConnectionError: " << e.what() << endl;
    exit(EXIT_FAILURE);
  } catch (MalFormedJsonError& e) {
    cout << "MalFormedJsonError: " << e.what() << endl;
    exit(EXIT_FAILURE);
  } catch (ReadFailedError& e) {
    cout << "ReadFailedError: " << e.what() << endl;
    exit(EXIT_FAILURE);
  } catch (NotFoundError& e) {
    cout << "NotFoundError: " << e.what() << endl;
    exit(EXIT_FAILURE);
  } catch (WriteFailedError& e) {
    cout << "WriteFailedError: " << e.what() << endl;
    exit(EXIT_FAILURE);
  } catch (CommitFailedError& e) {
    cout << "CommitFailedError: " << e.what() << endl;
    exit(EXIT_FAILURE);
  } catch (NotSupportedError& e) {
    cout << "NotSupportedError: " << e.what() << endl;
    exit(EXIT_FAILURE);
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
    return 0;
  } else if (vm.count("read")) {
    string key = vm["read"].as<string>();
    auto p = [key](TransactionSingleOp& op) {
      std::string value = op.read(key);
      cout << value << endl;
    };
    exec_call(p);
  } else if (vm.count("write")) {
    pair<string,string> kv = vm["write"].as<pair<string,string>>();
    string key = get<0>(kv);
    string value = get<1>(kv);
    auto p = [key,value](TransactionSingleOp& op) {
      op.write(key,value);
    };
    exec_call(p);
  } else {
    cout << desc << "\n";
    return 0;
  }
  return 0;
}
