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
%% @doc    Common types and function specs for routing table implementations.
%% @end
%% @version $Id$

% note: define key_t(), rt_t() and external_rt_t() in the RT-implementation!
-opaque key()::key_t().
-opaque rt()::rt_t().
-type external_rt()::external_rt_t(). %% @todo: make opaque

-include("client_types.hrl").

-ifdef(with_export_type_support).
-export_type([key/0, rt/0, custom_message/0, external_rt/0]).
-endif.

-export([empty/1, empty_ext/1,
         hash_key/1, get_random_node_id/0, next_hop/2,
         init_stabilize/2, update/3,
         filter_dead_node/2, to_pid_list/1, get_size/1, get_replica_keys/1,
         n/0, dump/1, to_list/1, export_rt_to_dht_node/2,
         handle_custom_message/2,
         check/4, check/5,
         check_config/0]).

% note: can not use wrapper methods for all methods to make dialyzer happy
% about the opaque types since it doesn't recognize the module's own opaque
% type if returned by a method outside the module's scope, e.g. node:id/1
% if required, e.g. a method is used internally and externally, use a wrapper
% in the implementation

-spec empty(nodelist:neighborhood()) -> rt().
-spec empty_ext(nodelist:neighborhood()) -> external_rt().
-spec hash_key(client_key()) -> key().
-spec get_random_node_id() -> key().
-spec next_hop(dht_node_state:state(), key()) -> comm:mypid().

-spec init_stabilize(nodelist:neighborhood(), rt()) -> rt().
-spec filter_dead_node(rt(), comm:mypid()) -> rt().

-spec to_pid_list(rt()) -> [comm:mypid()].
-spec get_size(rt() | external_rt()) -> non_neg_integer().
-spec get_replica_keys(key()) -> [key()].

-spec n() -> non_neg_integer().
-spec dump(RT::rt()) -> KeyValueList::[{Index::string(), Node::string()}].

-spec to_list(dht_node_state:state()) -> nodelist:snodelist().
-spec export_rt_to_dht_node(rt(), Neighbors::nodelist:neighborhood()) -> external_rt().

-spec check(OldRT::rt(), NewRT::rt(), Neighbors::nodelist:neighborhood(),
            ReportToFD::boolean()) -> ok.
-spec check(OldRT::rt(), NewRT::rt(), OldNeighbors::nodelist:neighborhood(),
            NewNeighbors::nodelist:neighborhood(), ReportToFD::boolean()) -> ok.

-spec check_config() -> boolean().
