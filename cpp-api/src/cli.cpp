#include <chrono>
#include <iostream>

#include <boost/program_options.hpp>

#include "rbr.hpp"
#include "tcp-connection.hpp"
#include "transaction_single_op.hpp"

using namespace std;
using namespace scalaris;
namespace po = boost::program_options;

const int N = 100;

namespace std {
  istream& operator>>(istream& in, std::pair<std::string, std::string>& ss) {
    string s;
    in >> s;
    const size_t sep = s.find(',');
    if (sep == string::npos) {
      ss.first = s;
      ss.second = string();
    } else {
      ss.first = s.substr(0, sep);
      ss.second = s.substr(sep + 1);
    }
    return in;
  }
} // namespace std

template <typename F> void exec_call(F& f) {
  try {
    TCPConnection c{"localhost"};
    TransactionSingleOp op = {c};
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

// TODO merge exec_call copies

template <typename F> void exec_call2(F& f) {
  try {
    TCPConnection c{"localhost"};
    Rbr op = {c};
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

int main(int argc, char** argv) {
  // Declare the supported options.
  po::options_description desc("Allowed options");
  desc.add_options()
      // clang-format off
    ("help", "produce help message")
    ("read", po::value<std::string>(), "read key")
    ("write", po::value<pair<string, string>>(),"write key,value")
    ("rbr-read", po::value<std::string>(), "rbr-read key")
    ("rbr-write", po::value<pair<string, string>>(),"rbr-write key,value")
    ("bench", "bench");
  // clang-format on

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  try {
    if (vm.count("help")) {
      cout << desc << "\n";
      return 0;
    } else if (vm.count("read")) {
      try {
        string key = vm["read"].as<string>();
        auto p = [key](TransactionSingleOp& op) {
          std::string value = op.read(key);
          cout << value << endl;
        };
        exec_call(p);
      } catch (const boost::exception_detail::clone_impl<
               boost::exception_detail::error_info_injector<
                   boost::bad_any_cast>>& e) {
        cout << "could not convert read parameter to a string" << endl;
        exit(EXIT_FAILURE);
      }
    } else if (vm.count("write")) {
      try {
        pair<string, string> kv = vm["write"].as<pair<string, string>>();
        string key = get<0>(kv);
        string value = get<1>(kv);
        auto p = [key, value](TransactionSingleOp& op) {
          op.write(key, value);
        };
        exec_call(p);
      } catch (const boost::exception_detail::clone_impl<
               boost::exception_detail::error_info_injector<
                   boost::bad_any_cast>>& e) {
        cout << "could not convert write parameter to a pair of strings"
             << endl;
        exit(EXIT_FAILURE);
      }
    } else if (vm.count("rbr-read")) {
      try {
        string key = vm["rbr-read"].as<string>();
        auto p = [key](Rbr& r) {
          std::string value = r.read(key).toStyledString();
          cout << value << endl;
        };
        exec_call2(p);
      } catch (const boost::exception_detail::clone_impl<
               boost::exception_detail::error_info_injector<
                   boost::bad_any_cast>>& e) {
        cout << "could not convert read parameter to a string" << endl;
        exit(EXIT_FAILURE);
      }
    } else if (vm.count("rbr-write")) {
      try {
        pair<string, string> kv = vm["rbr-write"].as<pair<string, string>>();
        string key = get<0>(kv);
        string value = get<1>(kv);
        auto p = [key, value](Rbr& r) { r.write(key, value); };
        exec_call2(p);
      } catch (const boost::exception_detail::clone_impl<
               boost::exception_detail::error_info_injector<
                   boost::bad_any_cast>>& e) {
        cout << "could not convert write parameter to a pair of strings"
             << endl;
        exit(EXIT_FAILURE);
      }
    } else if (vm.count("bench")) {
      try {
        string key = "bench-key";
        string value = "bench-value";
        vector<double> latencies;
        latencies.resize(N);
        for (int i = 0; i < N; i++) {
          auto start = std::chrono::high_resolution_clock::now();
          {
            auto p = [key, value](TransactionSingleOp& op) {
              op.write(key, value);
            };
            exec_call(p);
          }
          auto stop = std::chrono::high_resolution_clock::now();
          const std::chrono::duration<double> diff = stop - start;
          const double duration = diff.count();
          latencies[i] = duration;
        }
        sort(latencies.begin(), latencies.end());
        const double latency = latencies[N / 2] * 1000;
        printf("executed %d key-value pairs updates using the transaction "
               "mechanism\n",
               N);
        printf("median latency=%fms\n", latency);
      } catch (const boost::exception_detail::clone_impl<
               boost::exception_detail::error_info_injector<
                   boost::bad_any_cast>>& e) {
        cout << "could not convert write parameter to a pair of strings"
             << endl;
        exit(EXIT_FAILURE);
      }
    } else {
      cout << desc << "\n";
      return 0;
    }
  } catch (const Json::LogicError& e) {
    cout << "Json error: " << e.what() << endl;
    exit(EXIT_FAILURE);
  }

  return 0;
}
