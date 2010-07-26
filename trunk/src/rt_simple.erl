% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc sample routing table
%% @end
%% @version $Id$
-module(rt_simple).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(rt_beh).
-include("scalaris.hrl").

% routingtable behaviour
-export([empty/1, empty_ext/1,
         hash_key/1, get_random_node_id/0, next_hop/2, init_stabilize/3,
         filter_dead_node/2, to_pid_list/1, get_size/1, get_keys_for_replicas/1,
         dump/1, to_list/1, export_rt_to_dht_node/4, n/0,
         update_pred_succ_in_dht_node/3, handle_custom_message/2,
         check/6, check/5, check_fd/2,
         check_config/0]).

%% userdevguide-begin rt_simple:types
% @type key(). Identifier.
-opaque(key()::non_neg_integer()).
% @type rt(). Routing Table.
-opaque(rt()::Succ::node:node_type()).
-type(external_rt()::rt()).
-opaque(custom_message() :: any()).
%% userdevguide-end rt_simple:types

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_simple:empty
%% @doc Creates an "empty" routing table containing the successor.
-spec empty(node:node_type()) -> rt().
empty(Succ) -> Succ.
%% userdevguide-end rt_simple:empty

%% userdevguide-begin rt_simple:hash_key
%% @doc Hashes the key to the identifier space.
-spec hash_key(iodata() | integer()) -> key().
hash_key(Key) -> hash_key_(Key).

%% @doc Hashes the key to the identifier space (internal).
%%      Note: Needed for dialyzer to cope with the opaque key() type and the
%%      use of key() in get_keys_for_replicas/1.
-spec hash_key_(iodata() | integer()) -> non_neg_integer().
hash_key_(Key) when is_integer(Key) ->
    <<N:128>> = erlang:md5(erlang:term_to_binary(Key)),
    N;
hash_key_(Key) ->
    <<N:128>> = erlang:md5(Key),
    N.
%% userdevguide-end rt_simple:hash_key

%% userdevguide-begin rt_simple:get_random_node_id
%% @doc Generates a random node id, i.e. a random 128-bit string.
-spec get_random_node_id() -> key().
get_random_node_id() -> hash_key(randoms:getRandomId()).
%% userdevguide-end rt_simple:get_random_node_id

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_simple:init_stabilize
%% @doc Triggered by a new stabilization round, renews the routing table.
-spec init_stabilize(key(), node:node_type(), rt()) -> rt().
init_stabilize(_Id, Succ, _RT) -> empty(Succ).
%% userdevguide-end rt_simple:init_stabilize

%% userdevguide-begin rt_simple:filter_dead_node
%% @doc Removes dead nodes from the routing table (rely on periodic
%%      stabilization here).
-spec filter_dead_node(rt(), comm:mypid()) -> rt().
filter_dead_node(RT, _DeadPid) -> RT.
%% userdevguide-end rt_simple:filter_dead_node

%% userdevguide-begin rt_simple:to_pid_list
%% @doc Returns the pids of the routing table entries.
-spec to_pid_list(rt() | external_rt()) -> [comm:mypid()].
to_pid_list(Succ) -> [node:pidX(Succ)].
%% userdevguide-end rt_simple:to_pid_list

%% userdevguide-begin rt_simple:get_size
%% @doc Returns the size of the routing table.
-spec get_size(rt() | external_rt()) -> 1.
get_size(_RT) -> 1.
%% userdevguide-end rt_simple:get_size

%% userdevguide-begin rt_simple:n
%% @doc Returns the size of the address space.
-spec n() -> non_neg_integer().
n() -> 16#100000000000000000000000000000000.
%% userdevguide-end rt_simple:n

%% userdevguide-begin rt_simple:get_keys_for_replicas
%% @doc Returns the replicas of the given key.
-spec get_keys_for_replicas(iodata() | integer()) -> [key()].
get_keys_for_replicas(Key) ->
    HashedKey = hash_key_(Key),
    [HashedKey,
     HashedKey bxor 16#40000000000000000000000000000000,
     HashedKey bxor 16#80000000000000000000000000000000,
     HashedKey bxor 16#C0000000000000000000000000000000
    ].
%% userdevguide-end rt_simple:get_keys_for_replicas

%% userdevguide-begin rt_simple:dump
%% @doc Dumps the RT state for output in the web interface.
-spec dump(RT::rt()) -> KeyValueList::[{Index::non_neg_integer(), Node::string()}].
dump(Succ) -> [{0, lists:flatten(io_lib:format("~p", [Succ]))}].
%% userdevguide-end rt_simple:dump

%% @doc Checks whether config parameters of the rt_simple process exist and are
%%      valid (there are no config parameters).
-spec check_config() -> true.
check_config() -> true.

-include("rt_generic.hrl").

%% userdevguide-begin rt_simple:handle_custom_message
%% @doc There are no custom messages here.
-spec handle_custom_message(custom_message(), rt_loop:state_init()) -> unknown_event.
handle_custom_message(_Message, _State) -> unknown_event.
%% userdevguide-end rt_simple:handle_custom_message

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_simple:empty_ext
-spec empty_ext(node:node_type()) -> external_rt().
empty_ext(Succ) -> empty(Succ).
%% userdevguide-end rt_simple:empty_ext

%% userdevguide-begin rt_simple:next_hop
%% @doc Returns the next hop to contact for a lookup.
-spec next_hop(dht_node_state:state(), key()) -> comm:mypid().
next_hop(State, _Key) -> node:pidX(dht_node_state:get(State, rt)).
%% userdevguide-end rt_simple:next_hop

%% userdevguide-begin rt_simple:export_rt_to_dht_node
%% @doc Converts the internal RT to the external RT used by the dht_node. Both
%%      are the same here.
-spec export_rt_to_dht_node(rt(), ID::key(), Pred::node:node_type(),
                            Succ::node:node_type()) -> external_rt().
export_rt_to_dht_node(RT, _Id, _Pred, _Succ) -> RT.
%% userdevguide-end rt_simple:export_rt_to_dht_node

%% userdevguide-begin rt_simple:update_pred_succ_in_dht_node
%% @doc Updates the successor in the (external) routing table state.
-spec update_pred_succ_in_dht_node(Pred::node:node_type(), Succ::node:node_type(),
                                   RT::external_rt()) -> external_rt().
update_pred_succ_in_dht_node(_Pred, Succ, Succ) -> Succ.
%% userdevguide-end rt_simple:update_pred_succ_in_dht_node

%% userdevguide-begin rt_simple:to_list
%% @doc Converts the (external) representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
-spec to_list(dht_node_state:state()) -> nodelist:snodelist().
to_list(State) -> [dht_node_state:get(State, rt)].
%% userdevguide-end rt_simple:to_list
