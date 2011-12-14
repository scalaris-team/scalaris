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
%% @doc Routing Table
%% @end
%% @version $Id$
-module(rt_chord).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(rt_beh).
-include("scalaris.hrl").

%% userdevguide-begin rt_chord:types
-type key_t() :: 0..16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF. % 128 bit numbers
-type rt_t() :: gb_tree().
-type external_rt_t() :: gb_tree().
-type index() :: {pos_integer(), non_neg_integer()}.
-opaque custom_message() ::
       {rt_get_node, Source_PID::comm:mypid(), Index::index()} |
       {rt_get_node_response, Index::index(), Node::node:node_type()}.
%% userdevguide-end rt_chord:types

-define(SEND_OPTIONS, [{channel, prio}]).

% Note: must include rt_beh.hrl AFTER the type definitions for erlang < R13B04
% to work.
-include("rt_beh.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:empty
%% @doc Creates an empty routing table.
empty(_Neighbors) -> gb_trees:empty().
%% userdevguide-end rt_chord:empty

%% @doc Hashes the key to the identifier space.
hash_key(Key) -> hash_key_(Key).

%% @doc Hashes the key to the identifier space (internal function to allow
%%      use in e.g. get_random_node_id without dialyzer complaining about the
%%      opaque key type).
-spec hash_key_(client_key()) -> key_t().
hash_key_(Key) ->
    <<N:128>> = erlang:md5(client_key_to_binary(Key)),
    N.

%% @doc Generates a random node id, i.e. a random 128-bit number, based on the
%%      parameters set in the config file (key_creator and key_creator_bitmask).
get_random_node_id() ->
    case config:read(key_creator) of
        random -> hash_key_(randoms:getRandomString());
        random_with_bit_mask ->
            {Mask1, Mask2} = config:read(key_creator_bitmask),
            (hash_key_(randoms:getRandomString()) band Mask2) bor Mask1
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:init_stabilize
%% @doc Starts the stabilization routine.
init_stabilize(Neighbors, RT) ->
    % calculate the longest finger
    Id = nodelist:nodeid(Neighbors),
    Key = calculateKey(Id, first_index()),
    % trigger a lookup for Key
    api_dht_raw:unreliable_lookup(Key, {send_to_group_member, routing_table,
                                        {rt_get_node, comm:this(), first_index()}}),
    RT.
%% userdevguide-end rt_chord:init_stabilize

%% userdevguide-begin rt_chord:filter_dead_node
%% @doc Removes dead nodes from the routing table.
filter_dead_node(RT, DeadPid) ->
    DeadIndices = [Index || {Index, Node}  <- gb_trees:to_list(RT),
                            node:same_process(Node, DeadPid)],
    lists:foldl(fun(Index, Tree) -> gb_trees:delete(Index, Tree) end,
                RT, DeadIndices).
%% userdevguide-end rt_chord:filter_dead_node

%% @doc Returns the pids of the routing table entries.
to_pid_list(RT) ->
    [node:pidX(Node) || Node <- gb_trees:values(RT)].

%% @doc Returns the size of the routing table.
get_size(RT) ->
    gb_trees:size(RT).

%% @doc Keep a key in the address space. See n/0.
-spec normalize(Key::key_t()) -> key_t().
normalize(Key) -> Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.

%% @doc Returns the size of the address space.
n() -> n_().
%% @doc Helper for n/0 to make dialyzer happy with internal use of n/0.
-spec n_() -> 16#100000000000000000000000000000000.
n_() -> 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF + 1.

%% @doc Gets the number of keys in the interval (Begin, End]. In the special
%%      case of Begin==End, the whole key range as specified by n/0 is returned.
get_range(Begin, End) -> get_range_(Begin, End).

%% @doc Helper for get_range/2 to make dialyzer happy with internal use of
%%      get_range/2 in the other methods, e.g. get_split_key/3.
-spec get_range_(Begin::key_t(), End::key_t() | ?PLUS_INFINITY_TYPE) -> number().
get_range_(Begin, Begin) -> n_(); % I am the only node
get_range_(?MINUS_INFINITY, ?PLUS_INFINITY) -> n_(); % special case, only node
get_range_(Begin, End) when End > Begin -> End - Begin;
get_range_(Begin, End) when End < Begin -> (n_() - Begin) + End.

%% @doc Gets the key that splits the interval (Begin, End] so that the first
%%      interval will (roughly) be (Num/Denom) * range(Begin, End). In the
%%      special case of Begin==End, the whole key range is split in halves.
%%      Beware: (Num/Denom) must be in [0, 1]; the final key will be rounded
%%      down and may thus be Begin.
get_split_key(Begin, _End, {Num, _Denom}) when Num == 0 -> Begin;
get_split_key(_Begin, End, {Num, Denom}) when Num == Denom -> End;
get_split_key(Begin, End, {Num, Denom}) ->
    normalize(Begin + (get_range_(Begin, End) * Num) div Denom).

%% @doc Returns the replicas of the given key.
get_replica_keys(Key) ->
    [Key,
     Key bxor 16#40000000000000000000000000000000,
     Key bxor 16#80000000000000000000000000000000,
     Key bxor 16#C0000000000000000000000000000000
    ].

%% @doc Dumps the RT state for output in the web interface.
dump(RT) ->
    [{lists:flatten(io_lib:format("~p", [Index])),
      lists:flatten(io_lib:format("~p", [Node]))} || {Index, Node} <- gb_trees:to_list(RT)].

%% userdevguide-begin rt_chord:stabilize
%% @doc Updates one entry in the routing table and triggers the next update.
-spec stabilize(Neighbors::nodelist:neighborhood(), OldRT::rt(),
                Index::index(), Node::node:node_type()) -> NewRT::rt().
stabilize(Neighbors, RT, Index, Node) ->
    MyId = nodelist:nodeid(Neighbors),
    Succ = nodelist:succ(Neighbors),
    case (node:id(Succ) =/= node:id(Node))   % reached succ?
        andalso (not intervals:in(           % there should be nothing shorter
                   node:id(Node),            %   than succ
                   nodelist:succ_range(Neighbors))) of
        true ->
            NewRT = gb_trees:enter(Index, Node, RT),
            NextKey = calculateKey(MyId, next_index(Index)),
            CurrentKey = calculateKey(MyId, Index),
            case CurrentKey =/= NextKey of
                true ->
                    Msg = {rt_get_node, comm:this(), next_index(Index)},
                    api_dht_raw:unreliable_lookup(
                      NextKey, {send_to_group_member, routing_table, Msg});
                _ -> ok
            end,
            NewRT;
        _ -> RT
    end.
%% userdevguide-end rt_chord:stabilize

%% userdevguide-begin rt_chord:update
%% @doc Updates the routing table due to a changed node ID, pred and/or succ.
-spec update(OldRT::rt(), OldNeighbors::nodelist:neighborhood(),
             NewNeighbors::nodelist:neighborhood()) -> {trigger_rebuild, rt()}.
update(_OldRT, _OldNeighbors, NewNeighbors) ->
    % to be on the safe side ...
    {trigger_rebuild, empty(NewNeighbors)}.
%% userdevguide-end rt_chord:update

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Finger calculation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @private
-spec calculateKey(key() | key_t(), index()) -> key_t().
calculateKey(Id, {I, J}) ->
    % N / K^I * (J + 1)
    Offset = (n() div util:pow(config:read(chord_base), I)) * (J + 1),
    %io:format("~p: ~p + ~p~n", [{I, J}, Id, Offset]),
    normalize(Id + Offset).

-spec first_index() -> index().
first_index() ->
   {1, config:read(chord_base) - 2}.

-spec next_index(index()) -> index().
next_index({I, 0}) ->
    {I + 1, config:read(chord_base) - 2};
next_index({I, J}) ->
    {I, J - 1}.

%% @doc Checks whether config parameters of the rt_chord process exist and are
%%      valid.
check_config() ->
    config:cfg_is_integer(chord_base) and
        config:cfg_is_greater_than_equal(chord_base, 2) and
        config:cfg_is_integer(rt_size_use_neighbors) and
        config:cfg_is_greater_than_equal(rt_size_use_neighbors, 0) and
        config:cfg_is_in(key_creator, [random, random_with_bit_mask]) and
        case config:read(key_creator) of
            random -> true;
            random_with_bit_mask ->
                config:cfg_is_tuple(key_creator_bitmask, 2,
                                fun({Mask1, Mask2}) ->
                                        erlang:is_integer(Mask1) andalso
                                            erlang:is_integer(Mask2) end,
                                "{int(), int()}")
        end.

%% userdevguide-begin rt_chord:handle_custom_message
%% @doc Chord reacts on 'rt_get_node_response' messages in response to its
%%      'rt_get_node' messages.
-spec handle_custom_message
        (custom_message(), rt_loop:state_active()) -> rt_loop:state_active();
        (any(), rt_loop:state_active()) -> unknown_event.
handle_custom_message({rt_get_node, Source_PID, Index}, State) ->
    MyNode = nodelist:node(rt_loop:get_neighb(State)),
    comm:send(Source_PID, {rt_get_node_response, Index, MyNode}, ?SEND_OPTIONS),
    State;
handle_custom_message({rt_get_node_response, Index, Node}, State) ->
    OldRT = rt_loop:get_rt(State),
    Neighbors = rt_loop:get_neighb(State),
    NewRT = stabilize(Neighbors, OldRT, Index, Node),
    check(OldRT, NewRT, rt_loop:get_neighb(State), true),
    rt_loop:set_rt(State, NewRT);
handle_custom_message(_Message, _State) ->
    unknown_event.
%% userdevguide-end rt_chord:handle_custom_message

%% userdevguide-begin rt_chord:check
%% @doc Notifies the dht_node and failure detector if the routing table changed.
%%      Provided for convenience (see check/5).
check(OldRT, NewRT, Neighbors, ReportToFD) ->
    check(OldRT, NewRT, Neighbors, Neighbors, ReportToFD).

%% @doc Notifies the dht_node if the (external) routing table changed.
%%      Also updates the failure detector if ReportToFD is set.
%%      Note: the external routing table also changes if the Pred or Succ
%%      change.
check(OldRT, NewRT, OldNeighbors, NewNeighbors, ReportToFD) ->
    case OldRT =:= NewRT andalso
             nodelist:pred(OldNeighbors) =:= nodelist:pred(NewNeighbors) andalso
             nodelist:succ(OldNeighbors) =:= nodelist:succ(NewNeighbors) of
        true -> ok;
        _ ->
            Pid = pid_groups:get_my(dht_node),
            RT_ext = export_rt_to_dht_node(NewRT, NewNeighbors),
            case Pid of
                failed -> ok;
                _      -> comm:send_local(Pid, {rt_update, RT_ext})
            end,
            % update failure detector:
            case ReportToFD of
                true ->
                    NewPids = to_pid_list(NewRT),
                    OldPids = to_pid_list(OldRT),
                    fd:update_subscriptions(OldPids, NewPids);
                _ -> ok
            end
    end.
%% userdevguide-end rt_chord:check

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:empty_ext
empty_ext(_Neighbors) -> gb_trees:empty().
%% userdevguide-end rt_chord:empty_ext

%% userdevguide-begin rt_chord:next_hop
%% @doc Returns the next hop to contact for a lookup.
%%      If the routing table has less entries than the rt_size_use_neighbors
%%      config parameter, the neighborhood is also searched in order to find a
%%      proper next hop.
%%      Note, that this code will be called from the dht_node process and
%%      it will thus have an external_rt!
next_hop(State, Id) ->
    Neighbors = dht_node_state:get(State, neighbors),
    case intervals:in(Id, nodelist:succ_range(Neighbors)) of
        true -> node:pidX(nodelist:succ(Neighbors));
        _ ->
            % check routing table:
            RT = dht_node_state:get(State, rt),
            RTSize = get_size(RT),
            NodeRT = case util:gb_trees_largest_smaller_than(Id, RT) of
                         {value, _Key, N} ->
                             N;
                         nil when RTSize =:= 0 ->
                             nodelist:succ(Neighbors);
                         nil -> % forward to largest finger
                             {_Key, N} = gb_trees:largest(RT),
                             N
                     end,
            FinalNode =
                case RTSize < config:read(rt_size_use_neighbors) of
                    false -> NodeRT;
                    _     ->
                        % check neighborhood:
                        nodelist:largest_smaller_than(Neighbors, Id, NodeRT)
                end,
            node:pidX(FinalNode)
    end.
%% userdevguide-end rt_chord:next_hop

%% userdevguide-begin rt_chord:export_rt_to_dht_node
export_rt_to_dht_node(RT, Neighbors) ->
    Id = nodelist:nodeid(Neighbors),
    Pred = nodelist:pred(Neighbors),
    Succ = nodelist:succ(Neighbors),
    Tree = gb_trees:enter(node:id(Succ), Succ,
                          gb_trees:enter(node:id(Pred), Pred, gb_trees:empty())),
    util:gb_trees_foldl(fun (_K, V, Acc) ->
                                 % only store the ring id and the according node structure
                                 case node:id(V) =:= Id of
                                     true  -> Acc;
                                     false -> gb_trees:enter(node:id(V), V, Acc)
                                 end
                        end, Tree, RT).
%% userdevguide-end rt_chord:export_rt_to_dht_node

%% @doc Converts the (external) representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
to_list(State) ->
    RT = dht_node_state:get(State, rt),
    Neighbors = dht_node_state:get(State, neighbors),
    nodelist:mk_nodelist([nodelist:succ(Neighbors) | gb_trees:values(RT)],
                         nodelist:node(Neighbors)).
