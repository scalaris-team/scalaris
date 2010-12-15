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
%% @doc Simple implementation of a routing table with linear routing.
%% @end
%% @version $Id$
-module(rt_simple).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(rt_beh).
-include("scalaris.hrl").

%% userdevguide-begin rt_simple:types
-type key_t() :: 0..16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF. % 128 bit numbers
-type rt_t() :: Succ::node:node_type().
-type external_rt_t() :: Succ::node:node_type().
-type custom_message() :: none().
%% userdevguide-end rt_simple:types

% Note: must include rt_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("rt_beh.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_simple:empty
%% @doc Creates an "empty" routing table containing the successor.
empty(Neighbors) -> nodelist:succ(Neighbors).
%% userdevguide-end rt_simple:empty

%% @doc Hashes the key to the identifier space.
hash_key(Key) -> hash_key_(Key).

%% @doc Hashes the key to the identifier space (internal function to allow
%%      use in e.g. get_random_node_id without dialyzer complaining about the
%%      opaque key type).
-spec hash_key_(client_key()) -> key_t().
hash_key_(Key) when is_integer(Key) ->
    <<N:128>> = erlang:md5(erlang:term_to_binary(Key)),
    N;
hash_key_(Key) ->
    <<N:128>> = erlang:md5(Key),
    N.
%% userdevguide-end rt_simple:hash_key

%% userdevguide-begin rt_simple:get_random_node_id
%% @doc Generates a random node id, i.e. a random 128-bit number.
get_random_node_id() ->
    case config:read(key_creator) of
        random -> hash_key_(randoms:getRandomId());
        random_with_bit_mask ->
            {Mask1, Mask2} = config:read(key_creator_bitmask),
            (hash_key_(randoms:getRandomId()) band Mask2) bor Mask1
    end.
%% userdevguide-end rt_simple:get_random_node_id

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_simple:init_stabilize
%% @doc Triggered by a new stabilization round, renews the routing table.
init_stabilize(Neighbors, _RT) -> empty(Neighbors).
%% userdevguide-end rt_simple:init_stabilize

%% userdevguide-begin rt_simple:update
%% @doc Updates the routing table due to a changed node ID, pred and/or succ.
-spec update(OldRT::rt(), OldNeighbors::nodelist:neighborhood(),
             NewNeighbors::nodelist:neighborhood()) -> {ok, rt()}.
update(_OldRT, _OldNeighbors, NewNeighbors) ->
    {ok, nodelist:succ(NewNeighbors)}.
%% userdevguide-end rt_simple:update

%% userdevguide-begin rt_simple:filter_dead_node
%% @doc Removes dead nodes from the routing table (rely on periodic
%%      stabilization here).
filter_dead_node(RT, _DeadPid) -> RT.
%% userdevguide-end rt_simple:filter_dead_node

%% userdevguide-begin rt_simple:to_pid_list
%% @doc Returns the pids of the routing table entries.
to_pid_list(Succ) -> [node:pidX(Succ)].
%% userdevguide-end rt_simple:to_pid_list

%% userdevguide-begin rt_simple:get_size
%% @doc Returns the size of the routing table.
get_size(_RT) -> 1.
%% userdevguide-end rt_simple:get_size

%% userdevguide-begin rt_simple:n
%% @doc Returns the size of the address space.
n() -> n_().
%% @doc Helper for n/0 to make dialyzer happy with internal use of n/0.
-spec n_() -> key_t().
n_() -> 16#100000000000000000000000000000000.
%% userdevguide-end rt_simple:n

%% @doc Keep a key in the address space. See n/0.
-spec normalize(Key::key_t()) -> key_t().
normalize(Key) -> Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.

%% @doc Gets the number of keys in the interval (Begin, End]. In the special
%%      case of Begin==End, the whole key range as specified by n/0 is returned.
get_range(Begin, End) -> get_range_(Begin, End).

%% @doc Helper for get_range/2 to make dialyzer happy with internal use of
%%      get_range/2 in the other methods, e.g. get_split_key/2.
-spec get_range_(Begin::key_t(), End::key_t()) -> key_t().
get_range_(Begin, End) ->
    if
        End == Begin -> n_(); % I am the only node
        End > Begin  -> End - Begin;
        End < Begin  -> (n_() - Begin - 1) + End
    end.

%% @doc Gets the key that splits the interval (Begin, End] in two equal halves
%%      (their ranges may differ by at most one key). In the special case of
%%      Begin==End, the whole key range is split in halves.
%%      Beware: if the key range is smaller than 2 the split key will be Begin!
get_split_key(Begin, End) ->
    normalize(Begin + (get_range_(Begin, End) div 2)).

%% userdevguide-begin rt_simple:get_replica_keys
%% @doc Returns the replicas of the given key.
get_replica_keys(Key) ->
    [Key,
     Key bxor 16#40000000000000000000000000000000,
     Key bxor 16#80000000000000000000000000000000,
     Key bxor 16#C0000000000000000000000000000000
    ].
%% userdevguide-end rt_simple:get_replica_keys

%% userdevguide-begin rt_simple:dump
%% @doc Dumps the RT state for output in the web interface.
dump(Succ) -> [{"0", lists:flatten(io_lib:format("~p", [Succ]))}].
%% userdevguide-end rt_simple:dump

%% @doc Checks whether config parameters of the rt_simple process exist and are
%%      valid.
check_config() ->
    config:is_in(key_creator, [random, random_with_bit_mask]) and
        case config:read(key_creator) of
            random -> true;
            random_with_bit_mask ->
                config:is_tuple(key_creator_bitmask, 2,
                                fun({Mask1, Mask2}) ->
                                        erlang:is_integer(Mask1) andalso
                                            erlang:is_integer(Mask2) end,
                                "{int(), int()}")
        end.

%% userdevguide-begin rt_simple:handle_custom_message
%% @doc There are no custom messages here.
-spec handle_custom_message
        (custom_message() | any(), rt_loop:state_active()) -> unknown_event.
handle_custom_message(_Message, _State) -> unknown_event.
%% userdevguide-end rt_simple:handle_custom_message

%% userdevguide-begin rt_simple:check
%% @doc Notifies the dht_node and failure detector if the routing table changed.
%%      Provided for convenience (see check/5).
check(OldRT, NewRT, Neighbors, ReportToFD) ->
    check(OldRT, NewRT, Neighbors, Neighbors, ReportToFD).

%% @doc Notifies the dht_node if the (external) routing table changed.
%%      Also updates the failure detector if ReportToFD is set.
%%      Note: the external routing table only changes the internal RT has
%%      changed.
check(OldRT, NewRT, _OldNeighbors, NewNeighbors, ReportToFD) ->
    case OldRT =:= NewRT of
        true -> ok;
        _ ->
            Pid = pid_groups:get_my(dht_node),
            RT_ext = export_rt_to_dht_node(NewRT, NewNeighbors),
            comm:send_local(Pid, {rt_update, RT_ext}),
            % update failure detector:
            case ReportToFD of
                true ->
                    NewPids = to_pid_list(NewRT),
                    OldPids = to_pid_list(OldRT),
                    fd:update_subscriptions(OldPids, NewPids);
                _ -> ok
            end
    end.
%% userdevguide-end rt_simple:check

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_simple:empty_ext
empty_ext(Neighbors) -> empty(Neighbors).
%% userdevguide-end rt_simple:empty_ext

%% userdevguide-begin rt_simple:next_hop
%% @doc Returns the next hop to contact for a lookup.
next_hop(State, _Key) -> node:pidX(dht_node_state:get(State, rt)).
%% userdevguide-end rt_simple:next_hop

%% userdevguide-begin rt_simple:export_rt_to_dht_node
%% @doc Converts the internal RT to the external RT used by the dht_node. Both
%%      are the same here.
export_rt_to_dht_node(RT, _Neighbors) -> RT.
%% userdevguide-end rt_simple:export_rt_to_dht_node

%% userdevguide-begin rt_simple:to_list
%% @doc Converts the (external) representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
to_list(State) -> [dht_node_state:get(State, rt)].
%% userdevguide-end rt_simple:to_list
