% @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc routing table behaviour
%% @end
%% @version $Id$
-module(rt_beh).
-author('schuett@zib.de').
-vsn('$Id$').

% for behaviour
-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

%% userdevguide-begin rt_beh:behaviour
-ifdef(have_callback_support).
-include("scalaris.hrl").
-include("client_types.hrl").
-type rt() :: term().
-type external_rt() :: term().
-type key() :: term().

-callback empty(nodelist:neighborhood()) -> rt().
-callback empty_ext(nodelist:neighborhood()) -> external_rt().
-callback hash_key(client_key()) -> key().
-callback get_random_node_id() -> key().
-callback next_hop(dht_node_state:state(), key()) -> comm:mypid().

-callback init_stabilize(nodelist:neighborhood(), rt()) -> rt().
-callback update(OldRT::rt(), OldNeighbors::nodelist:neighborhood(),
                 NewNeighbors::nodelist:neighborhood())
        -> {trigger_rebuild, rt()} | {ok, rt()}.
-callback filter_dead_node(rt(), comm:mypid()) -> rt().

-callback to_pid_list(rt()) -> [comm:mypid()].
-callback get_size(rt() | external_rt()) -> non_neg_integer().
-callback get_replica_keys(key()) -> [key()].

-callback n() -> number().
-callback get_range(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE) -> number().
-callback get_split_key(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE, SplitFraction::{Num::0..100, Denom::0..100}) -> key().

-callback dump(RT::rt()) -> KeyValueList::[{Index::string(), Node::string()}].

-callback to_list(dht_node_state:state()) -> nodelist:snodelist().
-callback export_rt_to_dht_node(rt(), Neighbors::nodelist:neighborhood()) -> external_rt().
-callback handle_custom_message(comm:message(), rt_loop:state_active()) -> rt_loop:state_active() | unknown_event.

-callback check(OldRT::rt(), NewRT::rt(), Neighbors::nodelist:neighborhood(),
            ReportToFD::boolean()) -> ok.
-callback check(OldRT::rt(), NewRT::rt(), OldNeighbors::nodelist:neighborhood(),
            NewNeighbors::nodelist:neighborhood(), ReportToFD::boolean()) -> ok.

-callback check_config() -> boolean().

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     % create a default routing table
     {empty, 1}, {empty_ext, 1},
     % mapping: key space -> identifier space
     {hash_key, 1}, {get_random_node_id, 0},
     % routing
     {next_hop, 2},
     % trigger for new stabilization round
     {init_stabilize, 2},
     % adapt RT to changed neighborhood
     {update, 3},
     % dead nodes filtering
     {filter_dead_node, 2},
     % statistics
     {to_pid_list, 1}, {get_size, 1},
     % gets all (replicated) keys for a given (hashed) key
     % (for symmetric replication)
     {get_replica_keys, 1},
     % address space size, range and split key
     % (may all throw 'throw:not_supported' if unsupported by the RT)
     {n, 0}, {get_range, 2}, {get_split_key, 3},
     % for debugging and web interface
     {dump, 1},
     % for bulkowner
     {to_list, 1},
     % convert from internal representation to version for dht_node
     {export_rt_to_dht_node, 2},
     % handle messages specific to a certain routing-table implementation
     {handle_custom_message, 2},
     % common methods
     {check, 4}, {check, 5},
     {check_config, 0}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
%% userdevguide-end rt_beh:behaviour
