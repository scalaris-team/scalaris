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
         hash_key/1, getRandomNodeId/0, next_hop/2, init_stabilize/3,
         filterDeadNode/2, to_pid_list/1, get_size/1, get_keys_for_replicas/1,
         dump/1, to_list/1, export_rt_to_dht_node/4,
         update_pred_succ_in_dht_node/3, handle_custom_message/2,
         check/6, check/5, check_fd/2,
         check_config/0]).

% stabilize for Chord
-export([stabilize/5]).

%% userdevguide-begin rt_chord:types
-type(key()::non_neg_integer()).
-type(rt()::gb_tree()).
-type(external_rt()::gb_tree()).
-type(index() :: {pos_integer(), pos_integer()}).
-type(custom_message() ::
       {rt_get_node_response, Index::pos_integer(), Node::node:node_type()}).
%% userdevguide-end rt_chord:types

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Key Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:empty
%% @doc Creates an empty routing table.
-spec empty(node:node_type()) -> rt().
empty(_Succ) ->
    gb_trees:empty().
%% userdevguide-end rt_chord:empty

%% @doc Hashes the key to the identifier space.
-spec hash_key(iodata() | integer()) -> key().
hash_key(Key) ->
    rt_simple:hash_key(Key).

%% @doc Generates a random node id.
%%      In this case it is a random 128-bit string.
-spec getRandomNodeId() -> key().
getRandomNodeId() ->
    rt_simple:getRandomNodeId().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RT Management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% userdevguide-begin rt_chord:init_stab
%% @doc Starts the stabilization routine.
-spec init_stabilize(key(), node:node_type(), rt()) -> rt().
init_stabilize(Id, _Succ, RT) ->
    % calculate the longest finger
    Key = calculateKey(Id, first_index()),
    % trigger a lookup for Key
    lookup:unreliable_lookup(Key, {rt_get_node, comm:this(), first_index()}),
    RT.
%% userdevguide-end rt_chord:init_stab

%% userdevguide-begin rt_chord:filterDeadNode
%% @doc Removes dead nodes from the routing table.
-spec filterDeadNode(rt(), comm:mypid()) -> rt().
filterDeadNode(RT, DeadPid) ->
    DeadIndices = [Index|| {Index, Node}  <- gb_trees:to_list(RT),
                           node:equals(Node, DeadPid)],
    lists:foldl(fun(Index, Tree) -> gb_trees:delete(Index, Tree) end,
                RT, DeadIndices).
%% userdevguide-end rt_chord:filterDeadNode

%% @doc Returns the pids of the routing table entries.
-spec to_pid_list(rt()) -> [comm:mypid()].
to_pid_list(RT) ->
    lists:map(fun({_Idx, Node}) -> node:pidX(Node) end, gb_trees:to_list(RT)).

%% @doc Returns the size of the routing table.
-spec get_size(rt() | external_rt()) -> non_neg_integer().
get_size(RT) ->
    gb_trees:size(RT).

%% @doc Returns the replicas of the given key.
-spec get_keys_for_replicas(key()) -> [key()].
get_keys_for_replicas(Key) ->
    rt_simple:get_keys_for_replicas(Key).

%% @doc Dumps the RT state for output in the web interface.
-spec dump(RT::rt()) -> KeyValueList::[{Index::non_neg_integer(), Node::string()}].
dump(RT) ->
    [{Index, lists:flatten(io_lib:format("~p", [Node]))} || {Index, Node} <- gb_trees:to_list(RT)].

%% userdevguide-begin rt_chord:stab
%% @doc Updates one entry in the routing table and triggers the next update.
-spec stabilize(key(), node:node_type(), rt(), pos_integer(), node:node_type())
      -> rt().
stabilize(Id, Succ, RT, Index, Node) ->
    case node:is_valid(Node)                           % do not add null nodes
        andalso (node:id(Succ) =/= node:id(Node))      % there is nothing shorter than succ
        andalso (not intervals:in(node:id(Node), intervals:mk_from_node_ids(Id, node:id(Succ)))) of % there should not be anything shorter than succ
        true ->
            NewRT = gb_trees:enter(Index, Node, RT),
            Key = calculateKey(Id, next_index(Index)),
            lookup:unreliable_lookup(Key, {rt_get_node, comm:this(),
                                              next_index(Index)}),
            NewRT;
        false ->
            RT
    end.
%% userdevguide-end rt_chord:stab

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Finger calculation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @private
-spec calculateKey(key(), index()) -> key().
calculateKey(Id, {I, J}) ->
    % N / K^I * (J + 1)
    Offset = (rt_simple:n() div util:pow(config:read(chord_base), I)) * (J + 1),
    %io:format("~p: ~p + ~p~n", [{I, J}, Id, Offset]),
    rt_simple:normalize(Id + Offset).

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

%% @doc Chord reacts on 'rt_get_node_response' messages in response to its
%%      'rt_get_node' messages.
-spec handle_custom_message(custom_message(), rt_loop:state_init()) -> unknown_event.
handle_custom_message({rt_get_node_response, Index, Node}, {Id, Pred, Succ, RTState, TriggerState}) ->
    NewRTState = stabilize(Id, Succ, RTState, Index, Node),
    check(RTState, NewRTState, Id, Pred, Succ),
    {Id, Pred, Succ, NewRTState, TriggerState};

handle_custom_message(_Message, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Communication with dht_node
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec empty_ext(node:node_type()) -> external_rt().
empty_ext(_Succ) ->
    gb_trees:empty().

%% userdevguide-begin rt_chord:next_hop1
%% @doc Returns the next hop to contact for a lookup.
%%      Note, that this code will be called from the dht_node process and
%%      it will thus have an external_rt!
-spec next_hop(dht_node_state:state(), key()) -> comm:mypid().
next_hop(State, Id) ->
    case intervals:in(Id, dht_node_state:get(State, succ_range)) of
        %succ is responsible for the key
        true ->
            dht_node_state:get(State, succ_pid);
        % check routing table
        false ->
            case util:gb_trees_largest_smaller_than(Id, dht_node_state:get(State, rt)) of
                nil ->
                    dht_node_state:get(State, succ_pid);
                {value, _Key, Node} ->
                    node:pidX(Node)
            end
    end.
%% userdevguide-end rt_chord:next_hop1

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

-spec update_pred_succ_in_dht_node(node:node_type(), node:node_type(), external_rt())
      -> external_rt().
update_pred_succ_in_dht_node(Pred, Succ, RT) ->
    gb_trees:enter(node:id(Succ), Succ, gb_trees:enter(node:id(Pred), Pred, RT)).

%% @doc Converts the (external) representation of the routing table to a list
%%      in the order of the fingers, i.e. first=succ, second=shortest finger,
%%      third=next longer finger,...
-spec to_list(dht_node_state:state()) -> nodelist:nodelist().
to_list(State) ->
    RT = dht_node_state:get(State, rt),
    Succ = dht_node_state:get(State, succ),
    nodelist:mk_nodelist([Succ | gb_trees:values(RT)],
                         dht_node_state:get(State, node)).
