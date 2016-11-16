% @copyright 2008-2016 Zuse Institute Berlin

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
-type key() :: 0..16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF. % 128 bit numbers
-opaque rt() :: Succ::node:node_type().
-type external_rt() :: Succ::node:node_type(). %% @todo: make opaque
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
-spec empty(nodelist:neighborhood()) -> rt().
empty(Neighbors) -> nodelist:succ(Neighbors).
%% userdevguide-end rt_simple:empty

%% @doc This function is called during the startup of the rt_loop process and
%%      is allowed to send trigger messages.
%%      Noop in rt_simple.
-spec init() -> ok.
init() -> ok.

%% @doc Activate the routing table.
%%      This function is called during the activation of the routing table process.
-spec activate(nodelist:neighborhood()) -> rt().
activate(Neighbors) -> empty(Neighbors).

%% @doc Hashes the key to the identifier space.
-spec hash_key(client_key() | binary()) -> key().
hash_key(Key) when not is_binary(Key) ->
    hash_key(client_key_to_binary(Key));
hash_key(Key) ->
    <<N:128>> = ?CRYPTO_MD5(Key),
    N.
%% userdevguide-end rt_simple:hash_key

%% userdevguide-begin rt_simple:get_random_node_id
%% @doc Generates a random node id, i.e. a random 128-bit number.
-spec get_random_node_id() -> key().
get_random_node_id() ->
    case config:read(key_creator) of
        random -> hash_key(randoms:getRandomString());
        random_with_bit_mask ->
            {Mask1, Mask2} = config:read(key_creator_bitmask),
            (hash_key(randoms:getRandomString()) band Mask2) bor Mask1
    end.
%% userdevguide-end rt_simple:get_random_node_id

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_simple:init_stabilize
%% @doc Triggered by a new stabilization round, renews the routing table.
-spec init_stabilize(nodelist:neighborhood(), rt()) -> rt().
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
-spec filter_dead_node(rt(), DeadPid::comm:mypid(), Reason::fd:reason()) -> rt().
filter_dead_node(RT, _DeadPid, _Reason) -> RT.
%% userdevguide-end rt_simple:filter_dead_node

%% userdevguide-begin rt_simple:to_pid_list
%% @doc Returns the pids of the routing table entries.
-spec to_pid_list(rt()) -> [comm:mypid()].
to_pid_list(Succ) -> [node:pidX(Succ)].
%% userdevguide-end rt_simple:to_pid_list

%% userdevguide-begin rt_simple:get_size
%% @doc Returns the size of the routing table.
-spec get_size(rt()) -> non_neg_integer().
get_size(_RT) -> 1.

%% @doc Returns the size of the external routing table.
-spec get_size_ext(external_rt()) -> non_neg_integer().
get_size_ext(_RT) -> 1.
%% userdevguide-end rt_simple:get_size

%% userdevguide-begin rt_simple:n
%% @doc Returns the size of the address space.
-spec n() -> 16#100000000000000000000000000000000.
n() -> 16#100000000000000000000000000000000.
%% userdevguide-end rt_simple:n

%% @doc Keep a key in the address space. See n/0.
-spec normalize(Key::key()) -> key().
normalize(Key) -> Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.

%% @doc Gets the number of keys in the interval (Begin, End]. In the special
%%      case of Begin==End, the whole key range as specified by n/0 is returned.
-spec get_range(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE) -> non_neg_integer().
get_range(Begin, Begin) -> n(); % I am the only node
get_range(?MINUS_INFINITY, ?PLUS_INFINITY) -> n(); % special case, only node
get_range(Begin, End) when End > Begin -> End - Begin;
get_range(Begin, End) when End < Begin -> (n() - Begin) + End.

%% @doc Gets the key that splits the interval (Begin, End] so that the first
%%      interval will be (Num/Denom) * range(Begin, End). In the special case of
%%      Begin==End, the whole key range is split.
%%      Beware: SplitFactor must be in [0, 1]; the final key will be rounded
%%      down and may thus be Begin.
-spec get_split_key(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE,
                    SplitFraction::{Num::number(), Denom::pos_integer()}) -> key() | ?PLUS_INFINITY_TYPE.
get_split_key(Begin, _End, {Num, _Denom}) when Num == 0 -> Begin;
get_split_key(_Begin, End, {Num, Denom}) when Num == Denom -> End;
get_split_key(Begin, End, {Num, Denom}) ->
    normalize(Begin + trunc(get_range(Begin, End) * Num) div Denom).

%% @doc Splits the range between Begin and End into up to Parts equal parts and
%%      returning the according split keys.
-spec get_split_keys(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE,
                     Parts::pos_integer()) -> [key()].
get_split_keys(Begin, End, Parts) ->
    get_split_keys_helper(Begin, End, Parts).

-spec get_split_keys_helper(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE,
                            Parts::pos_integer()) -> [key()].
get_split_keys_helper(_Begin, _End, 1) ->
    [];
get_split_keys_helper(Begin, End, Parts) ->
    SplitKey = get_split_key(Begin, End, {1, Parts}),
    if SplitKey =:= Begin ->
           get_split_keys_helper(Begin, End, Parts - 1);
       true ->
           [SplitKey | get_split_keys_helper(SplitKey, End, Parts - 1)]
    end.

%% @doc Gets input similar to what intervals:get_bounds/1 returns and
%%      calculates a random key in this range. Fails with an exception if there
%%      is no key.
-spec get_random_in_interval(intervals:simple_interval2()) -> key().
get_random_in_interval(I) ->
    hd(get_random_in_interval(I, 1)).

%% @doc Gets input similar to what intervals:get_bounds/1 returns and
%%      calculates Count number of random keys in this range (duplicates may
%%      exist!). Fails with an exception if there is no key.
-spec get_random_in_interval(intervals:simple_interval2(), Count::pos_integer()) -> [key(),...].
get_random_in_interval({LBr, L, R, RBr}, Count) ->
    case intervals:wraps_around(LBr, L, R, RBr) of
        false -> get_random_in_interval2(LBr, L, R, RBr, Count);
        true  -> [normalize(Key) || Key <- get_random_in_interval2(LBr, L, ?PLUS_INFINITY + R, RBr, Count)]
    end.

% TODO: return a failure constant if the interval is empty? (currently fails with an exception)
-spec get_random_in_interval2(intervals:left_bracket(), key(), non_neg_integer(),
                              intervals:right_bracket(), Count::pos_integer()) -> [non_neg_integer()].
get_random_in_interval2('(', L, R, ']', Count) ->
    L2 = L + 1,
    R2 = R + 1,
    randoms:rand_uniform(L2, R2, Count);
get_random_in_interval2('[', L, R, ')', Count) ->
    randoms:rand_uniform(L, R, Count);
get_random_in_interval2('[', X, X, ']', Count) ->
    lists:duplicate(Count, X);
get_random_in_interval2('[', L, R, ']', Count) ->
    R2 = R + 1,
    randoms:rand_uniform(L, R2, Count);
get_random_in_interval2('(', L, R, ')', Count) ->
    L2 = L + 1,
    randoms:rand_uniform(L2, R, Count).

%% userdevguide-begin rt_simple:get_replica_keys
%% @doc Returns the replicas of the given key.
-spec get_replica_keys(key()) -> [key()].
get_replica_keys(Key) ->
    get_replica_keys(Key, config:read(replication_factor)).

-spec get_replica_keys(key(), pos_integer()) -> [key()].
get_replica_keys(Key, ReplicationFactor) ->
    case ReplicationFactor of
        2 ->
            [Key,
             Key bxor 16#80000000000000000000000000000000
            ];
        4 ->
            [Key,
             Key bxor 16#40000000000000000000000000000000,
             Key bxor 16#80000000000000000000000000000000,
             Key bxor 16#C0000000000000000000000000000000
            ];
        8 ->
            [Key,
             Key bxor 16#20000000000000000000000000000000,
             Key bxor 16#40000000000000000000000000000000,
             Key bxor 16#60000000000000000000000000000000,
             Key bxor 16#80000000000000000000000000000000,
             Key bxor 16#A0000000000000000000000000000000,
             Key bxor 16#C0000000000000000000000000000000,
             Key bxor 16#E0000000000000000000000000000000
            ];
        16 ->
            [Key,
             Key bxor 16#10000000000000000000000000000000,
             Key bxor 16#20000000000000000000000000000000,
             Key bxor 16#30000000000000000000000000000000,
             Key bxor 16#40000000000000000000000000000000,
             Key bxor 16#50000000000000000000000000000000,
             Key bxor 16#60000000000000000000000000000000,
             Key bxor 16#70000000000000000000000000000000,

             Key bxor 16#80000000000000000000000000000000,
             Key bxor 16#90000000000000000000000000000000,
             Key bxor 16#A0000000000000000000000000000000,
             Key bxor 16#B0000000000000000000000000000000,
             Key bxor 16#C0000000000000000000000000000000,
             Key bxor 16#D0000000000000000000000000000000,
             Key bxor 16#E0000000000000000000000000000000,
             Key bxor 16#F0000000000000000000000000000000
            ];
        R ->
            Step = n() div R,
            MappedToFirstSegment = Key rem Step,
            [MappedToFirstSegment + I * Step || I <- lists:seq(0, R-1)]
    end.
%% userdevguide-end rt_simple:get_replica_keys
-spec get_key_segment(key()) -> pos_integer().
get_key_segment(Key) ->
    get_key_segment(Key, config:read(replication_factor)).

-spec get_key_segment(key(), pos_integer()) -> pos_integer().
get_key_segment(Key, ReplicationFactor) ->
    case ReplicationFactor of
        2  -> (Key bsr 127) + 1;
        4  -> (Key bsr 126) + 1;
        8  -> (Key bsr 125) + 1;
        16 -> (Key bsr 124) + 1;
        R ->
            Step = n() div R,
            Key div Step + 1
    end.

%% userdevguide-begin rt_simple:dump
%% @doc Dumps the RT state for output in the web interface.
-spec dump(RT::rt()) -> KeyValueList::[{Index::string(), Node::string()}].
dump(Succ) -> [{"0", webhelpers:safe_html_string("~p", [Succ])}].
%% userdevguide-end rt_simple:dump

%% @doc Checks whether config parameters of the rt_simple process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_in(key_creator, [random, random_with_bit_mask]) and
        case config:read(key_creator) of
            random -> true;
            random_with_bit_mask ->
                config:cfg_is_tuple(key_creator_bitmask, 2,
                                fun({Mask1, Mask2}) ->
                                        erlang:is_integer(Mask1) andalso
                                            erlang:is_integer(Mask2) end,
                                "{int(), int()}");
            _ -> false
        end.

%% @doc No special handling of messages, i.e. all messages are queued.
-spec handle_custom_message_inactive(comm:message(), msg_queue:msg_queue()) ->
    msg_queue:msg_queue().
handle_custom_message_inactive(Msg, MsgQueue) ->
    msg_queue:add(MsgQueue, Msg).

%% userdevguide-begin rt_simple:handle_custom_message
%% @doc There are no custom messages here.
-spec handle_custom_message
        (custom_message() | any(), rt_loop:state_active()) -> unknown_event.
handle_custom_message(_Message, _State) -> unknown_event.
%% userdevguide-end rt_simple:handle_custom_message

%% userdevguide-begin rt_simple:check
%% @doc Notifies the dht_node and failure detector if the routing table changed.
%%      Provided for convenience (see check/5).
-spec check(OldRT::rt(), NewRT::rt(), OldERT::external_rt(),
                Neighbors::nodelist:neighborhood(),
                ReportToFD::boolean()) -> NewERT::external_rt().
check(OldRT, NewRT, OldERT, Neighbors, ReportToFD) ->
    check(OldRT, NewRT, OldERT, Neighbors, Neighbors, ReportToFD).

%% @doc Notifies the dht_node if the (external) routing table changed.
%%      Also updates the failure detector if ReportToFD is set.
%%      Note: the external routing table only changes the internal RT has
%%      changed.
-spec check(OldRT::rt(), NewRT::rt(), OldERT::external_rt(),
                OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood(),
                ReportToFD::boolean()) -> NewERT::external_rt().
check(OldRT, NewRT, OldERT, _OldNeighbors, NewNeighbors, ReportToFD) ->
    case OldRT =:= NewRT of
        true -> OldERT;
        _ ->
            Pid = node:pidX(nodelist:node(NewNeighbors)),
            NewERT = export_rt_to_dht_node(NewRT, NewNeighbors),
            comm:send_local(comm:make_local(Pid), {rt_update, NewERT}),
            % update failure detector:
            case ReportToFD of
                true ->
                    NewPids = to_pid_list(NewRT),
                    OldPids = to_pid_list(OldRT),
                    fd:update_subscriptions(self(), OldPids, NewPids);
                _ -> ok
            end,
            NewERT
    end.
%% userdevguide-end rt_simple:check

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_simple:empty_ext
-spec empty_ext(nodelist:neighborhood()) -> external_rt().
empty_ext(Neighbors) -> empty(Neighbors).
%% userdevguide-end rt_simple:empty_ext

%% userdevguide-begin rt_simple:next_hop
%% @doc Returns the next hop to contact for a lookup.
-spec next_hop(nodelist:neighborhood(), external_rt(), key()) -> succ | comm:mypid().
next_hop(Neighbors, RT, Id) ->
    case intervals:in(Id, nodelist:succ_range(Neighbors)) of
        true -> succ;
        _    -> node:pidX(RT)
    end.
%% userdevguide-end rt_simple:next_hop


%% @doc Return the succ
%%      No need to lookup the succ in the ERT, only dht_node pids are used anyway.
-spec succ(ERT::external_rt(), Neighbors::nodelist:neighborhood()) -> comm:mypid().
succ(_ERT, Neighbors) ->
    node:pidX(nodelist:succ(Neighbors)).

%% userdevguide-begin rt_simple:export_rt_to_dht_node
%% @doc Converts the internal RT to the external RT used by the dht_node. Both
%%      are the same here.
-spec export_rt_to_dht_node(rt(), Neighbors::nodelist:neighborhood()) -> external_rt().
export_rt_to_dht_node(RT, _Neighbors) -> RT.
%% userdevguide-end rt_simple:export_rt_to_dht_node

%% userdevguide-begin rt_simple:to_list
%% @doc Converts the (external) representation of the routing table to a list
%%      {Id, Pid} tuples in the order of the fingers, i.e. first=succ,
%%      second=shortest finger, third=next longer finger,...
-spec to_list(dht_node_state:state()) -> [{key(), comm:mypid()}].
to_list(State) ->
    Succ = dht_node_state:get(State, rt), % ERT = Succ
    [{node:id(Succ), node:pidX(Succ)}].
%% userdevguide-end rt_simple:to_list

%% userdevguide-begin rt_simple:wrap_message
%% @doc Wrap lookup messages. This is a noop in rt_simple.
-spec wrap_message(Key::key(), Msg::comm:message(), MyERT::external_rt(),
                   Neighbors::nodelist:neighborhood(),
                   Hops::non_neg_integer()) -> comm:message().
wrap_message(_Key, Msg, _MyERT, _Neighbors, _Hops) -> Msg.
%% userdevguide-end rt_simple:wrap_message

%% userdevguide-begin rt_simple:unwrap_message
%% @doc Unwrap lookup messages. This is a noop in rt_simple.
-spec unwrap_message(Msg::comm:message(), State::dht_node_state:state()) -> comm:message().
unwrap_message(Msg, _State) -> Msg.
%% userdevguide-end rt_simple:unwrap_message
