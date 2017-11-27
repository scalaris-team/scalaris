% @copyright 2007-2017 Zuse Institute Berlin

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
%% @doc Routing Table
%% @end
-module(rt_chord).
-author('schuett@zib.de').

-behaviour(rt_beh).
-include("scalaris.hrl").

%% userdevguide-begin rt_chord:types
-type key() :: 0..16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF. % 128 bit numbers
-type index() :: {pos_integer(), non_neg_integer()}.
-opaque rt() :: gb_trees:tree(index(), {Node::node:node_type(), PidRT::comm:mypid()}).
-opaque external_rt() :: gb_trees:tree(NodeId::key(), PidRT::comm:mypid()).
-type custom_message() ::
       {rt_get_node, Source_PID::comm:mypid(), Index::index()} |
       {rt_get_node_response, Index::index(), Node::node:node_type()}.
%% userdevguide-end rt_chord:types

-define(SEND_OPTIONS, [{channel, prio}]).

-export([add_range/2]).

% Note: must include rt_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("rt_beh.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:empty
%% @doc Creates an empty routing table.
-spec empty(nodelist:neighborhood()) -> rt().
empty(_Neighbors) -> gb_trees:empty().
%% userdevguide-end rt_chord:empty

%% @doc This function is called during the startup of the rt_loop process and
%%      is allowed to send trigger messages.
%%      Noop in chord.
-spec init() -> ok.
init() -> ok.

%% @doc Activate the routing table.
%%      This function is called during the activation of the routing table process.
-spec activate(nodelist:neighborhood()) -> rt().
activate(Neighbors) -> empty(Neighbors).

%% @doc Hashes the key to the identifier space.
-spec hash_key(binary() | client_key()) -> key().
hash_key(Key) when not is_binary(Key) ->
    hash_key(client_key_to_binary(Key));
hash_key(Key) ->
    <<N:128>> = ?CRYPTO_MD5(Key),
    N.

%% @doc Generates a random node id, i.e. a random 128-bit number, based on the
%%      parameters set in the config file (key_creator and key_creator_bitmask).
-spec get_random_node_id() -> key().
get_random_node_id() ->
    case config:read(key_creator) of
        random -> hash_key(randoms:getRandomString());
        random_with_bit_mask ->
            {Mask1, Mask2} = config:read(key_creator_bitmask),
            (hash_key(randoms:getRandomString()) band Mask2) bor Mask1;
        modr ->
            %% put random key into first quarter
            Key = hash_key(randoms:getRandomString()) div config:read(replication_factor),
            %% select the quarter based on the availability zone id
            Quarter = config:read(availability_zone_id) rem config:read(replication_factor),
            %% calculate the final key
            Key + Quarter * n() div config:read(replication_factor)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:init_stabilize
%% @doc Starts the stabilization routine.
-spec init_stabilize(nodelist:neighborhood(), rt()) -> rt().
init_stabilize(Neighbors, RT) ->
    % calculate the longest finger
    case first_index(Neighbors) of
        null -> ok;
        {Key, Index} ->
            % trigger a lookup for Key
            api_dht_raw:unreliable_lookup(
              Key, {?send_to_group_member, routing_table,
                    {rt_get_node, comm:this(), Index}})
    end,
    RT.
%% userdevguide-end rt_chord:init_stabilize

%% userdevguide-begin rt_chord:filter_dead_node
%% @doc Removes dead nodes from the routing table.
-spec filter_dead_node(rt(), DeadPid::comm:mypid(), Reason::fd:reason()) -> rt().
filter_dead_node(RT, DeadPid, _Reason) ->
    DeadIndices = [Index || {Index, {Node, _PidRT}}  <- gb_trees:to_list(RT),
                            node:same_process(Node, DeadPid)],
    lists:foldl(fun(Index, Tree) -> gb_trees:delete(Index, Tree) end,
                RT, DeadIndices).
%% userdevguide-end rt_chord:filter_dead_node

%% @doc Returns the pids of the routing table entries.
-spec to_pid_list(rt()) -> [comm:mypid()].
to_pid_list(RT) ->
    [node:pidX(Node) || {Node, _PidRT} <- gb_trees:values(RT)].

%% @doc Returns the size of the routing table.
-spec get_size(rt()) -> non_neg_integer().
get_size(RT) ->
    gb_trees:size(RT).

%% @doc Returns the size of the external routing table.
-spec get_size_ext(external_rt()) -> non_neg_integer().
get_size_ext(RT) ->
    gb_trees:size(RT).

%% @doc Keep a key in the address space. See n/0.
-spec normalize(non_neg_integer()) -> key().
normalize(Key) -> Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.

%% @doc Returns the size of the address space.
-spec n() -> 16#100000000000000000000000000000000.
n() -> 16#100000000000000000000000000000000.

%% @doc Adds the given range, i.e. Range/n() parts of the key space, to the
%%      given key.
-spec add_range(key(), Range::non_neg_integer()) -> key().
add_range(Key, Range) -> normalize(Key + Range).

%% @doc Gets the number of keys in the interval (Begin, End]. In the special
%%      case of Begin==End, the whole key range as specified by n/0 is returned.
-spec get_range(Begin::key(), End::key() | ?PLUS_INFINITY_TYPE) -> non_neg_integer().
get_range(Begin, Begin) -> n(); % I am the only node
get_range(?MINUS_INFINITY, ?PLUS_INFINITY) -> n(); % special case, only node
get_range(Begin, End) when End > Begin -> End - Begin;
get_range(Begin, End) when End < Begin -> (n() - Begin) + End.

%% @doc Gets the key that splits the interval (Begin, End] so that the first
%%      interval will (roughly) be (Num/Denom) * range(Begin, End). In the
%%      special case of Begin==End, the whole key range is split.
%%      Beware: (Num/Denom) must be in [0, 1]; the final key will be rounded
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

%% @doc Returns the replicas of the given key.
-spec get_replica_keys(key()) -> [key()].
get_replica_keys(Key) ->
    rt_simple:get_replica_keys(Key).

-spec get_replica_keys(key(), pos_integer()) -> [key()].
get_replica_keys(Key, ReplicationFactor) ->
    rt_simple:get_replica_keys(Key, ReplicationFactor).

-spec get_key_segment(key()) -> pos_integer().
get_key_segment(Key) ->
    rt_simple:get_key_segment(Key).

-spec get_key_segment(key(), pos_integer()) -> pos_integer().
get_key_segment(Key, ReplicationFactor) ->
    rt_simple:get_key_segment(Key, ReplicationFactor).

%% @doc Dumps the RT state for output in the web interface.
-spec dump(RT::rt()) -> KeyValueList::[{Index::string(), Node::string()}].
dump(RT) ->
    [{webhelpers:safe_html_string("~p", [Index]),
      webhelpers:safe_html_string("~p", [Node])} || {Index, {Node, _PidRT}} <- gb_trees:to_list(RT)].

%% userdevguide-begin rt_chord:stabilize
%% @doc Updates one entry in the routing table and triggers the next update.
%%      Changed indicates whether a new node was inserted (the RT structure may
%%      change independently from this indicator!).
-spec stabilize(Neighbors::nodelist:neighborhood(), OldRT::rt(), Index::index(),
                Node::node:node_type(), PidRT::comm:mypid()) -> {NewRT::rt(), Changed::boolean()}.
stabilize(Neighbors, RT, Index, Node, PidRT) ->
    MyId = nodelist:nodeid(Neighbors),
    Succ = nodelist:succ(Neighbors),
    case (node:id(Succ) =/= node:id(Node))   % reached succ?
        andalso (not intervals:in(           % there should be nothing shorter
                   node:id(Node),            %   than succ
                   nodelist:succ_range(Neighbors))) of
        true ->
            NextIndex = next_index(Index),
            NextKey = calculateKey(MyId, NextIndex),
            CurrentKey = calculateKey(MyId, Index),
            case CurrentKey =/= NextKey of
                true ->
                    Msg = {rt_get_node, comm:this(), NextIndex},
                    api_dht_raw:unreliable_lookup(
                      NextKey, {?send_to_group_member, routing_table, Msg});
                _ -> ok
            end,
            Changed = (Index =:= first_index() orelse
                       (gb_trees:lookup(prev_index(Index), RT) =/= {value, {Node, PidRT}})),
            {gb_trees:enter(Index, {Node, PidRT}, RT), Changed};
        false ->
            %% there should be nothing shorter than succ
            case intervals:in(node:id(Node), nodelist:succ_range(Neighbors)) of
                %% ignore message
                false -> {RT, false};
                %% add succ to RT
                true -> {gb_trees:enter(Index, {Node, PidRT}, RT), true}
            end
    end.
%% userdevguide-end rt_chord:stabilize

%% userdevguide-begin rt_chord:update
%% @doc Updates the routing table due to a changed neighborhood.
-spec update(OldRT::rt(), OldNeighbors::nodelist:neighborhood(),
             NewNeighbors::nodelist:neighborhood())
        -> {ok | trigger_rebuild, rt()}.
update(OldRT, OldNeighbors, NewNeighbors) ->
    NewPred = nodelist:pred(NewNeighbors),
    OldSucc = nodelist:succ(OldNeighbors),
    NewSucc = nodelist:succ(NewNeighbors),
    NewNodeId = nodelist:nodeid(NewNeighbors),
    % only re-build if a new successor occurs or the new node ID is not between
    % Pred and Succ any more (which should not happen since this must come from
    % a slide!)
    case node:same_process(OldSucc, NewSucc) andalso
             intervals:in(NewNodeId, node:mk_interval_between_nodes(NewPred, NewSucc)) of
        true ->
            % -> if not rebuilding, update the node IDs though
            UpdNodes = nodelist:create_pid_to_node_dict(
                         dict:new(), [nodelist:preds(NewNeighbors),
                                      nodelist:succs(NewNeighbors)]),
            NewRT = gb_trees:map(
                      fun(_K, {Node, PidRT}) ->
                              % check neighbors for newer version of the node
                              case dict:find(node:pidX(Node), UpdNodes) of
                                  {ok, N} -> {node:newer(Node, N), PidRT};
                                  error -> {Node, PidRT}
                              end
                      end, OldRT),
            {ok, NewRT};
        false ->
            % to be on the safe side ...
            {trigger_rebuild, empty(NewNeighbors)}
    end.
%% userdevguide-end rt_chord:update


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Finger calculation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @private
-spec calculateKey(key(), index()) -> key().
calculateKey(Id, {I, J}) ->
    % N / K^I * (J + 1)
    Offset = (n() div util:pow(config:read(chord_base), I)) * (J + 1),
    %io:format("~p: ~p + ~p~n", [{I, J}, Id, Offset]),
    normalize(Id + Offset).

%% @doc Gets the first index not pointing to the own node.
-spec first_index(Neighbors::nodelist:neighborhood()) -> {key(), index()} | null.
first_index(Neighbors) ->
    MyRange = nodelist:node_range(Neighbors),
    case intervals:is_all(MyRange) of
        true  -> null;
        false -> Id = nodelist:nodeid(Neighbors),
                 first_index_(Id, MyRange, first_index())
    end.

%% @doc Helper for first_index/1.
-spec first_index_(Id::key(), MyRange::intervals:interval(), index())
        -> {key(), index()}.
first_index_(Id, MyRange, Index) ->
    Key = calculateKey(Id, Index),
    case intervals:in(Key, MyRange) of
        true  -> first_index_(Id, MyRange, next_index(Index));
        false -> {Key, Index}
    end.

%% @doc Returns the first possible index, i.e. the index of the longest finger,
%%      for the configured chord_base.
-spec first_index() -> index().
first_index() ->
   {1, config:read(chord_base) - 2}.

%% @doc Calculates the next index, i.e. the index for the next shorter finger,
%%      for the configured chord_base.
-spec next_index(index()) -> index().
next_index({I, 0}) ->
    {I + 1, config:read(chord_base) - 2};
next_index({I, J}) ->
    {I, J - 1}.

%% @doc Calculates the previous index, i.e. the index for the next longer finger,
%%      for the configured chord_base.
-spec prev_index(index()) -> index().
prev_index({I, J}) ->
    MaxJ = config:read(chord_base) - 2,
    if J =:= MaxJ andalso I > 1 -> {I - 1, 0};
       J =/= MaxJ -> {I, J + 1}
    end.

%% @doc Checks whether config parameters of the rt_chord process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(chord_base) and
        config:cfg_is_greater_than_equal(chord_base, 2) and
        config:cfg_is_integer(replication_factor) and
        config:cfg_is_greater_than_equal(replication_factor, 2) and
        config:cfg_is_in(key_creator, [random, random_with_bit_mask, modr]) and
        case config:read(key_creator) of
            random -> true;
            random_with_bit_mask ->
                config:cfg_is_tuple(key_creator_bitmask, 2,
                                fun({Mask1, Mask2}) ->
                                        erlang:is_integer(Mask1) andalso
                                            erlang:is_integer(Mask2) end,
                                "{int(), int()}");
            modr -> config:cfg_is_integer(replication_factor) and
                        config:cfg_is_integer(availability_zone_id);
            _ -> false
        end.

%% @doc No special handling of messages, i.e. all messages are queued.
-spec handle_custom_message_inactive(custom_message(), msg_queue:msg_queue()) ->
    msg_queue:msg_queue().
handle_custom_message_inactive(Msg, MsgQueue) ->
    msg_queue:add(MsgQueue, Msg).

%% userdevguide-begin rt_chord:handle_custom_message
%% @doc Chord reacts on 'rt_get_node_response' messages in response to its
%%      'rt_get_node' messages.
-spec handle_custom_message(custom_message(), rt_loop:state_active()) ->
                                   rt_loop:state_active() | unknown_event.
handle_custom_message({rt_get_node, Source_PID, Index}, State) ->
    MyNode = nodelist:node(rt_loop:get_neighb(State)),
    comm:send(Source_PID, {rt_get_node_response, Index, MyNode, comm:this()}, ?SEND_OPTIONS),
    State;
handle_custom_message({rt_get_node_response, Index, Node, PidRT}, State) ->
    OldRT = rt_loop:get_rt(State),
    OldERT = rt_loop:get_ert(State),
    Neighbors = rt_loop:get_neighb(State),
    NewERT = case stabilize(Neighbors, OldRT, Index, Node, PidRT) of
                 {NewRT, true} ->
                     check_do_update(OldRT, NewRT, OldERT, Neighbors, true);
                 {NewRT, false} ->
                     OldERT
             end,
    rt_loop:set_ert(rt_loop:set_rt(State, NewRT), NewERT);
handle_custom_message(_Message, _State) ->
    unknown_event.
%% userdevguide-end rt_chord:handle_custom_message

%% userdevguide-begin rt_chord:check
%% @doc Notifies the dht_node and failure detector if the routing table changed.
%%      Provided for convenience (see check/5).
-spec check(OldRT::rt(), NewRT::rt(), OldERT::external_rt(), Neighbors::nodelist:neighborhood(),
            ReportToFD::boolean()) -> NewERT::external_rt().
check(OldRT, OldRT, OldERT, _Neighbors, _ReportToFD) ->
    OldERT;
check(OldRT, NewRT, OldERT, Neighbors, ReportToFD) ->
    check_do_update(OldRT, NewRT, OldERT, Neighbors, ReportToFD).

%% @doc Notifies the dht_node if the (external) routing table changed.
%%      Also updates the failure detector if ReportToFD is set.
%%      Note: the external routing table also changes if the neighborhood changes.
-spec check(OldRT::rt(), NewRT::rt(), OldERT::external_rt(),
            OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood(),
            ReportToFD::boolean()) -> NewERT::external_rt().
check(OldRT, NewRT, OldERT, OldNeighbors, NewNeighbors, ReportToFD) ->
    case OldNeighbors =:= NewNeighbors andalso OldRT =:= NewRT of
        true -> OldERT;
        _ -> check_do_update(OldRT, NewRT, OldERT, NewNeighbors, ReportToFD)
    end.

%% @doc Helper for check/4 and check/5.
-spec check_do_update(OldRT::rt(), NewRT::rt(), OldERT::external_rt(),
                      NewNeighbors::nodelist:neighborhood(),
                      ReportToFD::boolean()) -> ERT::external_rt().
check_do_update(OldRT, NewRT, OldERT, NewNeighbors, ReportToFD) ->
    % update failure detector:
    case ReportToFD of
        true ->
            NewPids = to_pid_list(NewRT),
            OldPids = to_pid_list(OldRT),
            fd:update_subscriptions(self(), OldPids, NewPids);
        _ -> ok
    end,
    case pid_groups:get_my(dht_node) of
        failed ->
            % TODO: can this really happen?!
            OldERT;
        Pid ->
            NewERT = export_rt_to_dht_node(NewRT, NewNeighbors),
            comm:send_local(Pid, {rt_update, NewERT}),
            NewERT
    end.
%% userdevguide-end rt_chord:check

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:empty_ext
-spec empty_ext(nodelist:neighborhood()) -> external_rt().
empty_ext(_Neighbors) -> gb_trees:empty().
%% userdevguide-end rt_chord:empty_ext

%% userdevguide-begin rt_chord:next_hop
%% @doc Returns the next hop to contact for a lookup.
%%      Note, that this code will be called from the dht_node process and
%%      it will thus have an external_rt!
-spec next_hop(nodelist:neighborhood(), external_rt(), key()) -> succ | comm:mypid().
next_hop(Neighbors, RT, Id) ->
    case intervals:in(Id, nodelist:succ_range(Neighbors)) of
        true ->
            succ;
        false ->
            % check routing table:
            RTSize = get_size_ext(RT),
            case util:gb_trees_largest_smaller_than(Id, RT) of
                {value, _Key, Node} ->
                    Node;
                nil when RTSize =:= 0 ->
                    Succ = nodelist:succ(Neighbors),
                    node:pidX(Succ);
                nil -> % forward to largest finger
                    {_Key, Node} = gb_trees:largest(RT),
                    Node
            end
    end.
%% userdevguide-end rt_chord:next_hop

%% @doc Return the succ, but get the pid from ERT if possible
%%      (to hopefully get a rt_loop pid instead of a dht_node state pid)
-spec succ(ERT::external_rt(), Neighbors::nodelist:neighborhood()) -> comm:mypid().
succ(ERT, Neighbors) ->
    Succ = nodelist:succ(Neighbors),
    case gb_trees:lookup(node:id(Succ), ERT) of
        {value, Pid} -> Pid;
        none -> node:pidX(Succ)
    end.

%% userdevguide-begin rt_chord:export_rt_to_dht_node
-spec export_rt_to_dht_node(rt(), Neighbors::nodelist:neighborhood()) -> external_rt().
export_rt_to_dht_node(RT, Neighbors) ->
    Id = nodelist:nodeid(Neighbors),
    %% include whole neighbourhood external routing table (ert)
    %% note: we are subscribed at the RM for changes to whole neighborhood
    Preds = nodelist:preds(Neighbors),
    Succs = nodelist:succs(Neighbors),
    EnterDhtNode = fun(Node, Tree) ->
                           gb_trees:enter(node:id(Node), node:pidX(Node), Tree)
                   end,
    Tree0 = lists:foldl(EnterDhtNode, gb_trees:empty(), Preds),
    Tree1 = lists:foldl(EnterDhtNode, Tree0, Succs),
    ERT = util:gb_trees_foldl(
            fun (_Key, {Node, PidRT}, Acc) ->
                     % only store the id and the according PidRT Pid
                     case node:id(Node) of
                         Id -> Acc;
                         _  -> gb_trees:enter(node:id(Node), PidRT, Acc)
                     end
            end, Tree1, RT),
    ERT.
%% userdevguide-end rt_chord:export_rt_to_dht_node

%% @doc Converts the (external) representation of the routing table to a list of
%%      {Id, Pid} tuples, in the order of the fingers, i.e. first=succ,
%%      second=shortest finger, third=next longer finger,...
-spec to_list(dht_node_state:state()) -> [{key(), comm:mypid()}].
to_list(State) ->
    ERT = dht_node_state:get(State, rt),
    MyNodeId = dht_node_state:get(State, node_id),
    lists:usort(fun({AId, _APid}, {BId, _BPid}) ->
                        nodelist:succ_ord_id(AId, BId, MyNodeId)
                end, gb_trees:to_list(ERT)).

%% userdevguide-begin rt_chord:wrap_message
%% @doc Wrap lookup messages. This is a noop in Chord.
-spec wrap_message(Key::key(), Msg::comm:message(), MyERT::external_rt(),
                   Neighbors::nodelist:neighborhood(),
                   Hops::non_neg_integer()) -> comm:message().
wrap_message(_Key, Msg, _ERT, _Neighbors, _Hops) -> Msg.
%% userdevguide-end rt_chord:wrap_message

%% userdevguide-begin rt_chord:unwrap_message
%% @doc Unwrap lookup messages. This is a noop in Chord.
-spec unwrap_message(Msg::comm:message(), State::dht_node_state:state()) -> comm:message().
unwrap_message(Msg, _State) -> Msg.
%% userdevguide-end rt_chord:unwrap_message
