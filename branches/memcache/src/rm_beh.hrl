% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable law or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Common types and function specs for ring maintenance implementations.
%% @end
%% @version $Id$

-ifdef(with_export_type_support).
-export_type([state/0, custom_message/0]).
-endif.

-export([init/4, on/3,
         zombie_node/3, crashed_node/3,
         new_pred/3, new_succ/3,
         leave/2, remove_pred/4, remove_succ/4,
         updated_node/4,
         get_web_debug_info/2,
         check_config/0]).

-spec init(Table::tid(), Me::node:node_type(), Pred::node:node_type(),
           Succ::node:node_type()) -> state().

-spec on(custom_message(), state(), NeighbTable::tid()) -> state().

-spec zombie_node(State::state(), NeighbTable::tid(), Node::node:node_type()) -> state().

-spec crashed_node(State::state(), NeighbTable::tid(), DeadPid::comm:mypid()) -> state().

-spec new_pred(State::state(), NeighbTable::tid(), NewPred::node:node_type()) -> state().

-spec new_succ(State::state(), NeighbTable::tid(), NewSucc::node:node_type()) -> state().

-spec leave(State::state(), NeighbTable::tid()) -> ok.

-spec remove_pred(State::state(), NeighbTable::tid(),
                  OldPred::node:node_type(), PredsPred::node:node_type()) -> state().

-spec remove_succ(State::state(), NeighbTable::tid(),
                  OldSucc::node:node_type(), SuccsSucc::node:node_type()) -> state().

-spec updated_node(State::state(), NeighbTable::tid(),
                   OldMe::node:node_type(), NewMe::node:node_type()) -> state().

-spec get_web_debug_info(State::state(), NeighbTable::tid()) -> [{string(), string()}].
