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

#ifndef SCALARIS_ROUTINGTABLE_HPP
#define SCALARIS_ROUTINGTABLE_HPP

#include <stdexcept>

#include "connection.hpp"

namespace scalaris {
  class RoutingTable {
    Connection& c;
  public:
    RoutingTable(Connection& _c);

    int get_replication_factor() throw (std::runtime_error);
  };
}

#endif
