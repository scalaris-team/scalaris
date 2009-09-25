%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : rt_chord.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Routing Table
%%%
%%% Created : 10 Apr 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(rt_chord).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(routingtable).

% routingtable behaviour
-export([empty/1, hash_key/1, getRandomNodeId/0, next_hop/2, init_stabilize/3,
         filterDeadNode/2, to_pid_list/1, get_size/1, get_keys_for_replicas/1,
         dump/1, to_dict/1, export_rt_to_cs_node/4, update_pred_succ_in_cs_node/3]).

% stabilize for Chord
-export([stabilize/5]).

%% userdevguide-begin rt_chord:types
-type(key()::pos_integer()).
-ifdef(types_are_builtin).
-type(rt()::gb_tree()).
-type(external_rt()::gb_tree()).
-else.
-type(rt()::gb_trees:gb_tree()).
-type(external_rt()::gb_trees:gb_tree()).
-endif.
-type(index() :: {pos_integer(), pos_integer()}).
%% userdevguide-end rt_chord:types

%%====================================================================
%% Key Handling
%%====================================================================

%% userdevguide-begin rt_chord:empty
%% @doc creates an empty routing table.
-spec(empty/1 :: (node:node_type()) -> rt()).
empty(_Succ) ->
    gb_trees:empty().
%% userdevguide-end rt_chord:empty

%% @doc hashes the key to the identifier space.
-spec(hash_key/1 :: (any()) -> key()).
hash_key(Key) ->
    rt_simple:hash_key(Key).

%% @doc generates a random node id
%%      In this case it is a random 128-bit string.
-spec(getRandomNodeId/0 :: () -> key()).
getRandomNodeId() ->
    rt_simple:getRandomNodeId().

%%====================================================================
%% RT Management
%%====================================================================

%% userdevguide-begin rt_chord:init_stab
%% @doc starts the stabilization routine
-spec(init_stabilize/3 :: (key(), node:node_type(), rt()) -> rt()).
init_stabilize(Id, _Succ, RT) ->
    % calculate the longest finger
    Key = calculateKey(Id, first_index()),
    % trigger a lookup for Key
    cs_lookup:unreliable_lookup(Key, {rt_get_node, cs_send:this(), first_index()}),
    RT.
%% userdevguide-end rt_chord:init_stab

%% userdevguide-begin rt_chord:filterDeadNode
%% @doc remove all entries
-spec(filterDeadNode/2 :: (rt(), cs_send:mypid()) -> rt()).
filterDeadNode(RT, DeadPid) ->
    DeadIndices = [Index|| {Index, Node}  <- gb_trees:to_list(RT),
                           node:pidX(Node) == DeadPid],
    lists:foldl(fun (Index, Tree) -> gb_trees:delete(Index, Tree) end,
                RT, DeadIndices).
%% userdevguide-end rt_chord:filterDeadNode

%% @doc returns the pids of the routing table entries .
-spec(to_pid_list/1 :: (rt()) -> list(cs_send:mypid())).
to_pid_list(RT) ->
    lists:map(fun ({_Idx, Node}) -> node:pidX(Node) end, gb_trees:to_list(RT)).

%% @doc returns the size of the routing table.
%%      inefficient standard implementation
-spec(get_size/1 :: (rt()) -> pos_integer()).
get_size(RT) ->
    gb_trees:size(RT).

%% @doc returns the replicas of the given key
-spec(get_keys_for_replicas/1 :: (key()) -> list(key())).
get_keys_for_replicas(Key) ->
    rt_simple:get_keys_for_replicas(Key).

%% @doc
-spec(dump/1 :: (rt()) -> any()).
dump(RT) ->
    lists:flatten(io_lib:format("~p", [gb_trees:to_list(RT)])).

%% userdevguide-begin rt_chord:stab
%% @doc updates one entry in the routing table
%%      and triggers the next update
-spec(stabilize/5 :: (key(), node:node_type(), rt(), pos_integer(),
                      node:node_type()) -> rt()).
stabilize(Id, Succ, RT, Index, Node) ->
    case node:is_null(Node) orelse (node:id(Succ) == node:id(Node)) of
        true ->
            RT;
        false ->
            NewRT = gb_trees:enter(Index, Node, RT),
            Key = calculateKey(Id, next_index(Index)),
            cs_lookup:unreliable_lookup(Key, {rt_get_node, cs_send:this(),
                                              next_index(Index)}),
            NewRT
    end.
%% userdevguide-end rt_chord:stab

%%====================================================================
%% Finger calculation
%%====================================================================
% @private
-spec(calculateKey/2 :: (key(), index()) -> key()).
calculateKey(Id, {I, J}) ->
    % N / K^I * (J + 1)
    Offset = (rt_simple:n() div util:pow(config:read(chord_base), I)) * (J + 1),
    %io:format("~p: ~p + ~p~n", [{I, J}, Id, Offset]),
    rt_simple:normalize(Id + Offset).

first_index() ->
   {1, config:read(chord_base) - 2}.

next_index({I, 1}) ->
    {I + 1, config:read(chord_base) - 2};
next_index({I, J}) ->
    {I, J - 1}.

%%====================================================================
%%
%%====================================================================
% 0 -> succ
% 1 -> shortest finger
% 2 -> next longer finger
% 3 -> ...
% n -> me
-spec(to_dict/1 :: (cs_state:state()) -> dict:dictionary()).
to_dict(State) ->
    RT = cs_state:rt(State),
    Succ = cs_state:succ(State),
    Fingers = util:uniq(flatten(RT, 127)),
    {Dict, Next} = lists:foldl(fun(Finger, {Dict, Index}) ->
                                       {dict:store(Index, Finger, Dict), Index + 1}
                               end,
                               {dict:store(0, Succ, dict:new()), 1}, lists:reverse(Fingers)),
    dict:store(Next, cs_state:me(State), Dict).

% @private
flatten(RT, Index) ->
    case gb_trees:lookup(Index, RT) of
        {value, Entry} ->
            [Entry | flatten(RT, Index - 1)];
        none ->
            []
    end.

%%====================================================================
%% Communication with cs_node
%%====================================================================

%% userdevguide-begin rt_chord:next_hop1
%% @doc returns the next hop to contact for a lookup
%%      Note, that this code will be called from the cs_node process and
%%      it will have an external_rt!
-spec(next_hop/2 :: (cs_state:state(), key()) -> cs_send:mypid()).
next_hop(State, Id) ->
    case util:is_between(cs_state:id(State), Id, cs_state:succ_id(State)) of
        %succ is responsible for the key
        true ->
            cs_state:succ_pid(State);
        % check routing table
        false ->
            case util:gb_trees_largest_smaller_than(Id, cs_state:rt(State)) of
                nil ->
                    cs_state:succ_pid(State);
                {value, _Key, Value} ->
                    Value
            end
    end.
%% userdevguide-end rt_chord:next_hop1

-spec(export_rt_to_cs_node/4 :: (rt(), key(), node:node_type(), node:node_type())
      -> external_rt()).
export_rt_to_cs_node(RT, Id, Pred, Succ) ->
    Tree = gb_trees:enter(node:id(Succ), node:pidX(Succ),
                          gb_trees:enter(node:id(Pred), node:pidX(Pred),
                                         gb_trees:empty())),
    util:gb_trees_foldl(fun (_K, V, Acc) ->
                                % only store the ring id and pid
                                case node:id(V) == Id of
                                    true ->
                                        Acc;
                                    false ->
                                        gb_trees:enter(node:id(V), node:pidX(V), Acc)
                                end
                        end,
                        Tree,
                        RT).

-spec(update_pred_succ_in_cs_node/3 :: (node:node_type(), node:node_type(), external_rt())
      -> external_rt()).
update_pred_succ_in_cs_node(Pred, Succ, RT) ->
    gb_trees:enter(node:id(Succ), node:pidX(Succ),
                   gb_trees:enter(node:id(Pred), node:pidX(Pred),
                                  RT)).
