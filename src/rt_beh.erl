% @copyright 2007-2013 Zuse Institute Berlin

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

-export([tester_create_segment/1,
         tester_is_segment/1]).
-export_type([segment/0]).

-type segment() :: pos_integer(). % 1..config:read(replication_factor)

%% userdevguide-begin rt_beh:behaviour
-ifdef(have_callback_support).
-include("scalaris.hrl").
-include("client_types.hrl").
-type rt() :: term().
-type external_rt() :: term().
-type key() :: term().

-callback empty_ext(nodelist:neighborhood()) -> external_rt().
-callback init() -> ok.
-callback activate(nodelist:neighborhood()) -> rt().
-callback hash_key(client_key() | binary()) -> key().
-callback get_random_node_id() -> key().
-callback next_hop(nodelist:neighborhood(), external_rt(), key()) -> succ | comm:mypid().
-callback succ(external_rt(), nodelist:neighborhood()) -> comm:mypid().

-callback init_stabilize(nodelist:neighborhood(), rt()) -> rt().
-callback update(OldRT::rt(), OldNeighbors::nodelist:neighborhood(),
                 NewNeighbors::nodelist:neighborhood())
        -> {trigger_rebuild, rt()} | {ok, rt()}.
-callback filter_dead_node(rt(), DeadPid::comm:mypid(), Reason::fd:reason()) -> rt().

-callback to_pid_list(rt()) -> [comm:mypid()].
-callback get_size(rt()) -> non_neg_integer().
-callback get_size_ext(external_rt()) -> non_neg_integer().
-callback get_replica_keys(key()) -> [key()].
-callback get_replica_keys(key(), pos_integer()) -> [key()].
-callback get_key_segment(key()) -> pos_integer().
-callback get_key_segment(key(), pos_integer()) -> pos_integer().

-callback n() -> number().
-callback get_range(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE) -> number().
-callback get_split_key(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE,
                        SplitFraction::{Num::number(), Denom::pos_integer()})
        -> key() | ?PLUS_INFINITY_TYPE.
-callback get_split_keys(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE,
                         Parts::pos_integer()) -> [key()].
-callback get_random_in_interval(intervals:simple_interval2()) -> key().

-callback dump(RT::rt()) -> KeyValueList::[{Index::string(), Node::string()}].

-callback to_list(dht_node_state:state()) -> [{key(), comm:mypid()}].
-callback export_rt_to_dht_node(rt(), Neighbors::nodelist:neighborhood()) -> external_rt().
-callback handle_custom_message_inactive(comm:message(), msg_queue:msg_queue()) -> msg_queue:msg_queue().
-callback handle_custom_message(comm:message(), rt_loop:state_active()) -> rt_loop:state_active() | unknown_event.

-callback check(OldRT::rt(), NewRT::rt(), OldERT::external_rt(), Neighbors::nodelist:neighborhood(),
            ReportToFD::boolean()) -> NewERT::external_rt().
-callback check(OldRT::rt(), NewRT::rt(), OldERT::external_rt(),
            OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood(),
            ReportToFD::boolean()) -> NewERT::external_rt().

-callback check_config() -> boolean().
-callback wrap_message(Key::key(), Msg::comm:message(), MyERT::external_rt(),
                       Neighbors::nodelist:neighborhood(),
                       Hops::non_neg_integer()) -> comm:message().
-callback unwrap_message(Msg::comm:message(), State::dht_node_state:state()) ->
    comm:message().

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     % create a default routing table
     {empty_ext, 1},
     % initialize a routing table
     {init, 0},
     % activate a routing table
     {activate, 1},
     % mapping: key space -> identifier space
     {hash_key, 1}, {get_random_node_id, 0},
     % routing
     {next_hop, 3},
     % return the succ
     {succ, 2},
     % trigger for new stabilization round
     {init_stabilize, 2},
     % adapt RT to changed neighborhood
     {update, 3},
     % dead nodes filtering
     {filter_dead_node, 3},
     % statistics
     {to_pid_list, 1}, {get_size, 1}, {get_size_ext, 1},
     % gets all (replicated) keys for a given (hashed) key
     % (for symmetric replication)
     {get_replica_keys, 1}, {get_replica_keys, 2},
     % get the segment of the ring a key belongs to (1-4)
     {get_key_segment, 1}, {get_key_segment, 2},
     % address space size, range and split key
     % (may all throw 'throw:not_supported' if unsupported by the RT)
     {n, 0}, {get_range, 2}, {get_split_key, 3},
     % get a random key wihtin the requested interval
     {get_random_in_interval, 1},
     % for debugging and web interface
     {dump, 1},
     % for bulkowner
     {to_list, 1},
     % convert from internal representation to version for dht_node
     {export_rt_to_dht_node, 2},
     % handle messages specific to a certain routing-table implementation if
     % rt_loop is in on_inactive state
     {handle_custom_message_inactive, 2},
     % handle messages specific to a certain routing-table implementation
     {handle_custom_message, 2},
     % common methods
     {check, 5}, {check, 6},
     {check_config, 0},
     % wrap and unwrap lookup messages
     {wrap_message, 5},
     {unwrap_message, 2}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
%% userdevguide-end rt_beh:behaviour

-spec tester_create_segment(pos_integer()) -> segment().
tester_create_segment(Int) ->
    Int rem config:read(replication_factor) + 1.

-spec tester_is_segment(segment()) -> boolean().
tester_is_segment(Segment) when Segment < 1 ->
    false;
tester_is_segment(Segment) ->
    Segment =< config:read(replication_factor).
