% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

% routingtable behaviour
-export([empty/1, empty_ext/1,
         hash_key/1, get_random_node_id/0, next_hop/2,
         init_stabilize/3, update/7,
         filter_dead_node/2, to_pid_list/1, get_size/1, get_replica_keys/1,
         n/0, dump/1, to_list/1, export_rt_to_dht_node/4,
         handle_custom_message/2,
         check/5, check/6, check/7,
         check_config/0]).

-ifdef(with_export_type_support).
-export_type([key/0, rt/0, custom_message/0, external_rt/0, index/0]).
-endif.

%% userdevguide-begin rt_chord:types
-opaque(key()::non_neg_integer()).
-opaque(rt()::gb_tree()).
-type(external_rt()::gb_tree()).        %% @todo: make opaque
-type(index() :: {pos_integer(), non_neg_integer()}).
-opaque(custom_message() ::
       {rt_get_node_response, Index::index(), Node::node:node_type()}).
%% userdevguide-end rt_chord:types

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:empty
%% @doc Creates an empty routing table.
-spec empty(node:node_type()) -> rt().
empty(_Succ) -> gb_trees:empty().
%% userdevguide-end rt_chord:empty

%% @doc Hashes the key to the identifier space.
-spec hash_key(iodata() | integer()) -> key().
hash_key(Key) -> hash_key_(Key).

%% @doc Hashes the key to the identifier space (internal function to allow
%%      use in e.g. get_random_node_id without dialyzer complaining about the
%%      opaque key type).
-spec hash_key_(iodata() | integer()) -> integer().
hash_key_(Key) when is_integer(Key) ->
    <<N:128>> = erlang:md5(erlang:term_to_binary(Key)),
    N;
hash_key_(Key) ->
    <<N:128>> = erlang:md5(Key),
    N.

%% @doc Generates a random node id, i.e. a random 128-bit number, based on the
%%      parameters set in the config file (key_creator and key_creator_bitmask).
-spec get_random_node_id() -> key().
get_random_node_id() ->
    case config:read(key_creator) of
        random -> hash_key_(randoms:getRandomId());
        random_with_bit_mask ->
            {Mask1, Mask2} = config:read(key_creator_bitmask),
            (hash_key_(randoms:getRandomId()) band Mask2) bor Mask1
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:init_stabilize
%% @doc Starts the stabilization routine.
-spec init_stabilize(key(), node:node_type(), rt()) -> rt().
init_stabilize(Id, _Succ, RT) ->
    % calculate the longest finger
    Key = calculateKey(Id, first_index()),
    % trigger a lookup for Key
    lookup:unreliable_lookup(Key, {rt_get_node, comm:this(), first_index()}),
    RT.
%% userdevguide-end rt_chord:init_stabilize

%% userdevguide-begin rt_chord:filter_dead_node
%% @doc Removes dead nodes from the routing table.
-spec filter_dead_node(rt(), comm:mypid()) -> rt().
filter_dead_node(RT, DeadPid) ->
    DeadIndices = [Index || {Index, Node}  <- gb_trees:to_list(RT),
                            node:same_process(Node, DeadPid)],
    lists:foldl(fun(Index, Tree) -> gb_trees:delete(Index, Tree) end,
                RT, DeadIndices).
%% userdevguide-end rt_chord:filter_dead_node

%% @doc Returns the pids of the routing table entries.
-spec to_pid_list(rt()) -> [comm:mypid()].
to_pid_list(RT) ->
    [node:pidX(Node) || Node <- gb_trees:values(RT)].

%% @doc Returns the size of the routing table.
-spec get_size(rt() | external_rt()) -> non_neg_integer().
get_size(RT) ->
    gb_trees:size(RT).

%% @doc Keep a key in the address space. See n/0.
-spec normalize(Key::non_neg_integer()) -> key().
normalize(Key) ->
    Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.

%% @doc Returns the size of the address space.
-spec n() -> non_neg_integer().
n() ->
    16#100000000000000000000000000000000.

%% @doc Returns the replicas of the given key.
-spec get_replica_keys(key()) -> [key()].
get_replica_keys(Key) ->
    [Key,
     Key bxor 16#40000000000000000000000000000000,
     Key bxor 16#80000000000000000000000000000000,
     Key bxor 16#C0000000000000000000000000000000
    ].

%% @doc Dumps the RT state for output in the web interface.
-spec dump(RT::rt()) -> KeyValueList::[{Index::non_neg_integer(), Node::string()}].
dump(RT) ->
    [{Index, lists:flatten(io_lib:format("~p", [Node]))} || {Index, Node} <- gb_trees:to_list(RT)].

%% userdevguide-begin rt_chord:stabilize
%% @doc Updates one entry in the routing table and triggers the next update.
-spec stabilize(MyId::key(), Succ::node:node_type(), OldRT::rt(),
                Index::index(), Node::node:node_type()) -> NewRT::rt().
stabilize(Id, Succ, RT, Index, Node) ->
    case (node:id(Succ) =/= node:id(Node))   % reached succ?
        andalso (not intervals:in(           % there should be nothing shorter
                   node:id(Node),            %   than succ
                   node:mk_interval_between_ids(Id, node:id(Succ)))) of
        true ->
            NewRT = gb_trees:enter(Index, Node, RT),
            Key = calculateKey(Id, next_index(Index)),
            lookup:unreliable_lookup(Key, {rt_get_node, comm:this(),
                                           next_index(Index)}),
            NewRT;
        _ -> RT
    end.
%% userdevguide-end rt_chord:stabilize

%% userdevguide-begin rt_chord:update
%% @doc Updates the routing table due to a changed node ID, pred and/or succ.
-spec update(Id::key(), Pred::node:node_type(), Succ::node:node_type(),
             OldRT::rt(), OldId::key(), OldPred::node:node_type(),
             OldSucc::node:node_type()) -> {trigger_rebuild, rt()}.
update(_Id, _Pred, Succ, _OldRT, _OldId, _OldPred, _OldSucc) ->
    % to be on the safe side ...
    {trigger_rebuild, empty(Succ)}.
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
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(chord_base) and
        config:is_greater_than_equal(chord_base, 2) and
        config:is_integer(rt_size_use_neighbors) and
        config:is_greater_than_equal(rt_size_use_neighbors, 0) and
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

-include("rt_generic.hrl").

%% userdevguide-begin rt_chord:handle_custom_message
%% @doc Chord reacts on 'rt_get_node_response' messages in response to its
%%      'rt_get_node' messages.
-spec handle_custom_message
        (custom_message(), rt_loop:state_init()) -> rt_loop:state_init();
        (any(), rt_loop:state_init()) -> unknown_event.
handle_custom_message({rt_get_node_response, Index, Node}, State) ->
    OldRT = rt_loop:get_rt(State),
    Id = rt_loop:get_id(State),
    Succ = rt_loop:get_succ(State),
    Pred = rt_loop:get_pred(State),
    NewRT = stabilize(Id, Succ, OldRT, Index, Node),
    check(OldRT, NewRT, Id, Pred, Succ),
    rt_loop:set_rt(State, NewRT);
handle_custom_message(_Message, _State) ->
    unknown_event.
%% userdevguide-end rt_chord:handle_custom_message

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:empty_ext
-spec empty_ext(node:node_type()) -> external_rt().
empty_ext(_Succ) -> gb_trees:empty().
%% userdevguide-end rt_chord:empty_ext

%% @doc Returns whether NId is between MyId and Id and not equal to Id.
-spec less_than_id(NId::?RT:key(), Id::?RT:key(), MyId::?RT:key()) -> boolean().
less_than_id(NId, Id, MyId) ->
    % note: succ_ord_id = less than or equal
    not nodelist:succ_ord_id(Id, NId, MyId).

%% @doc Look-up largest node in the NodeList that has an ID smaller than Id.
%%      NodeList must be sorted with the largest key first (reverse order of
%%      a neighborhood's successor list).
%%      Note: this implementation does not use intervals because comparing keys
%%      with succ_ord is (slightly) faster. Also this is faster than a
%%      lists:fold*/3.
-spec best_node_maxfirst(MyId::?RT:key(), Id::?RT:key(), NodeList::nodelist:snodelist(), LastFound::node:node_type()) -> Result::node:node_type().
best_node_maxfirst(_MyId, _Id, [], LastFound) -> LastFound;
best_node_maxfirst(MyId, Id, [H | T], LastFound) ->
    % note: succ_ord_id = less than or equal
%%     ct:pal("~w~n", [H]),
    HId = node:id(H),
    LTId = less_than_id(HId, Id, MyId),
    case LTId andalso nodelist:succ_ord_id(node:id(LastFound), HId, MyId) of
        true        -> H;
        _ when LTId -> best_node_maxfirst2(MyId, T, LastFound);
        _           -> best_node_maxfirst(MyId, Id, T, LastFound)
    end.
%% @doc Helper for best_node_maxfirst/4 which assumes that all nodes in
%%      NodeList are in a valid range, i.e. between MyId and the target Id.
-spec best_node_maxfirst2(MyId::?RT:key(), NodeList::nodelist:snodelist(), LastFound::node:node_type()) -> Result::node:node_type().
best_node_maxfirst2(_MyId, [], LastFound) -> LastFound;
best_node_maxfirst2(MyId, [H | T], LastFound) ->
    % note: succ_ord_id = less than or equal
%%     ct:pal("~w~n", [H]),
    case nodelist:succ_ord_id(node:id(LastFound), node:id(H), MyId) of
        true        -> H;
        _           -> best_node_maxfirst2(MyId, T, LastFound)
    end.

%% @doc Similar to best_node_maxfirst/4 but with a NodeList that must be sorted
%%      with the smallest key first (reverse order of a neighborhood's
%%      predecessor list).
-spec best_node_minfirst(MyId::?RT:key(), Id::?RT:key(), NodeList::nodelist:snodelist(), LastFound::node:node_type()) -> Result::node:node_type().
best_node_minfirst(_MyId, _Id, [], LastFound) -> LastFound;
best_node_minfirst(MyId, Id, [H | T], LastFound) ->
    % note: succ_ord_id = less than or equal
%%     ct:pal("~w~n", [H]),
    HId = node:id(H),
    LTId = less_than_id(HId, Id, MyId),
    case LTId andalso
             nodelist:succ_ord_id(node:id(LastFound), HId, MyId) of
        true        -> best_node_minfirst(MyId, Id, T, H);
        _ when LTId -> best_node_minfirst(MyId, Id, T, LastFound);
        _           -> LastFound
    end.

%% userdevguide-begin rt_chord:next_hop
%% @doc Returns the next hop to contact for a lookup.
%%      If the routing table has less entries than the rt_size_use_neighbors
%%      config parameter, the neighborhood is also searched in order to find a
%%      proper next hop.
%%      Note, that this code will be called from the dht_node process and
%%      it will thus have an external_rt!
-spec next_hop(dht_node_state:state(), key()) -> comm:mypid().
next_hop(State, Id) ->
    case intervals:in(Id, dht_node_state:get(State, succ_range)) of
        true -> dht_node_state:get(State, succ_pid);
        _ ->
            % check routing table:
            RT = dht_node_state:get(State, rt),
            RTSize = get_size(RT),
            NodeRT = case util:gb_trees_largest_smaller_than(Id, RT) of
                         {value, _Key, N} ->
                             N;
                         nil when RTSize =:= 0 ->
                             dht_node_state:get(State, succ);
                         nil -> % forward to largest finger
                             {_Key, N} = gb_trees:largest(RT),
                             N
                     end,
%%             ct:pal("rt: ~w~n", [NodeRT]),
            FinalNode =
                case RTSize < config:read(rt_size_use_neighbors) of
                    false -> NodeRT;
                    _     ->
                        % check neighborhood:
                        MyId = dht_node_state:get(State, node_id),
                        NodeNeigh1 =
                            best_node_maxfirst(MyId, Id,
                                               lists:reverse(tl(dht_node_state:get(State, succlist))),
                                               NodeRT),
                        best_node_minfirst(MyId, Id,
                                           lists:reverse(dht_node_state:get(State, predlist)),
                                           NodeNeigh1)
                end,
%%             ct:pal("final: ~w~n", [FinalNode]),
            node:pidX(FinalNode)
    end.
%% userdevguide-end rt_chord:next_hop

%% userdevguide-begin rt_chord:export_rt_to_dht_node
-spec export_rt_to_dht_node(rt(), key(), node:node_type(), node:node_type())
        -> external_rt().
export_rt_to_dht_node(RT, Id, Pred, Succ) ->
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
-spec to_list(dht_node_state:state()) -> nodelist:snodelist().
to_list(State) ->
    RT = dht_node_state:get(State, rt),
    Succ = dht_node_state:get(State, succ),
    nodelist:mk_nodelist([Succ | gb_trees:values(RT)],
                         dht_node_state:get(State, node)).
