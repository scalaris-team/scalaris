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
         init_stabilize/3, update/6,
         filter_dead_node/2, to_pid_list/1, get_size/1, get_keys_for_replicas/1,
         n/0, dump/1, to_list/1, export_rt_to_dht_node/4,
         handle_custom_message/2,
         check/6, check/5, check_fd/2,
         check_config/0]).

-ifdef(with_export_type_support).
-export_type([key/0, rt/0, custom_message/0, external_rt/0]).
-endif.

%% userdevguide-begin rt_chord:types
-opaque(key()::non_neg_integer()).
-opaque(rt()::gb_tree()).
-type(external_rt()::gb_tree()).        %% @todo: make opaque
-type(index() :: {pos_integer(), pos_integer()}).
-opaque(custom_message() ::
       {rt_get_node_response, Index::pos_integer(), Node::node:node_type()}).
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

%% @doc Generates a random node id.
%%      In this case it is a random 128-bit string.
-spec get_random_node_id() -> key().
get_random_node_id() ->
    hash_key(randoms:getRandomId()).

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
                            node:equals(Node, DeadPid)],
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
-spec get_keys_for_replicas(iodata() | integer()) -> [key()].
get_keys_for_replicas(Key) ->
    HashedKey = hash_key_(Key),
    [HashedKey,
     HashedKey bxor 16#40000000000000000000000000000000,
     HashedKey bxor 16#80000000000000000000000000000000,
     HashedKey bxor 16#C0000000000000000000000000000000
    ].

%% @doc Dumps the RT state for output in the web interface.
-spec dump(RT::rt()) -> KeyValueList::[{Index::non_neg_integer(), Node::string()}].
dump(RT) ->
    [{Index, lists:flatten(io_lib:format("~p", [Node]))} || {Index, Node} <- gb_trees:to_list(RT)].

%% userdevguide-begin rt_chord:stabilize
%% @doc Updates one entry in the routing table and triggers the next update.
-spec stabilize(key(), node:node_type(), rt(), pos_integer(), node:node_type())
        -> rt().
stabilize(Id, Succ, RT, Index, Node) ->
    case node:is_valid(Node)                        % do not add null nodes
        andalso (node:id(Succ) =/= node:id(Node))   % reached succ?
        andalso (not intervals:in(                  % there should be nothing shorter
                   node:id(Node),                   %   than succ
                   intervals:mk_from_node_ids(Id, node:id(Succ)))) of
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
             OldRT::rt(), OldId::key(), OldSucc::node:node_type())
        -> rt().
update(Id, _Pred, Succ, OldRT, OldId, _OldSucc) ->
    case Id == OldId of
        true -> % Succ or Pred changed
            % OldRT is still valid, but it could be inefficient
            OldRT;
        false -> % Id changed
            % to be on the safe side ...
            empty(Succ)
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

-spec first_index() -> index().
first_index() ->
   {1, config:read(chord_base) - 2}.

-spec next_index(index()) -> index().
next_index({I, 1}) ->
    {I + 1, config:read(chord_base) - 2};
next_index({I, J}) ->
    {I, J - 1}.

%% @doc Checks whether config parameters of the rt_chord process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(chord_base) and
        config:is_greater_than_equal(chord_base, 2).

-include("rt_generic.hrl").

%% userdevguide-begin rt_chord:handle_custom_message
%% @doc Chord reacts on 'rt_get_node_response' messages in response to its
%%      'rt_get_node' messages.
-spec handle_custom_message(custom_message(), rt_loop:state_init()) -> rt_loop:state_init().
handle_custom_message({rt_get_node_response, Index, Node}, State) ->
    OldRT = rt_loop:get_rt(State),
    Id = rt_loop:get_id(State),
    Succ = rt_loop:get_succ(State),
    Pred = rt_loop:get_pred(State),
    NewRT = stabilize(Id, Succ, OldRT, Index, Node),
    check(OldRT, NewRT, Id, Pred, Succ),
    rt_loop:set_rt(State, NewRT).
%% userdevguide-end rt_chord:handle_custom_message

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:empty_ext
-spec empty_ext(node:node_type()) -> external_rt().
empty_ext(_Succ) -> gb_trees:empty().
%% userdevguide-end rt_chord:empty_ext

%% userdevguide-begin rt_chord:next_hop
%% @doc Returns the next hop to contact for a lookup.
%%      Note, that this code will be called from the dht_node process and
%%      it will thus have an external_rt!
-spec next_hop(dht_node_state:state(), key()) -> comm:mypid().
next_hop(State, Id) ->
    RT = dht_node_state:get(State, rt),
    case get_size(RT) =:= 0 orelse
             intervals:in(Id, dht_node_state:get(State, succ_range)) of
        true -> dht_node_state:get(State, succ_pid); % -> succ
        _ -> % check routing table:
            case util:gb_trees_largest_smaller_than(Id, RT) of
                {value, _Key, Node} -> node:pidX(Node);
                nil -> % forward to largest finger
                    {_Key, Node} = gb_trees:largest(RT),
                    node:pidX(Node)
            end
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
