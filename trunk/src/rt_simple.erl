%  Copyright 2007-2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : rt_simple.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : sample routing table
%%%
%%% Created :  14 Apr 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(rt_simple).

-author('schuett@zib.de').
-vsn('$Id$ ').
% routingtable behaviour
-export([empty/1, hash_key/1, getRandomNodeId/0, next_hop/2, init_stabilize/3,
         filterDeadNode/2, to_pid_list/1, get_size/1, get_keys_for_replicas/1,
         dump/1, to_dict/1, export_rt_to_cs_node/4, n/0, to_html/1,
         update_pred_succ_in_cs_node/3]).

-export([normalize/1]).

-behaviour(routingtable).

%% userdevguide-begin rt_simple:types
% @type key(). Identifier.
-type(key()::pos_integer()).
% @type rt(). Routing Table.
-ifdef(types_are_builtin).
-type(rt()::{node:node_type(), gb_tree()}).
-type(external_rt()::{node:node_type(), gb_tree()}).
-else.
-type(rt()::{node:node_type(), gb_trees:gb_tree()}).
-type(external_rt()::{node:node_type(), gb_trees:gb_tree()}).
-endif.
%% userdevguide-end rt_simple:types

%% userdevguide-begin rt_simple:empty
%% @doc creates an empty routing table.
%%      per default the empty routing should already include
%%      the successor
-spec(empty/1 :: (node:node_type()) -> rt()).
empty(Succ) ->
    {Succ, gb_trees:empty()}.
%% userdevguide-end rt_simple:empty

%% userdevguide-begin rt_simple:hash_key
%% @doc hashes the key to the identifier space.
-spec(hash_key/1 :: (any()) -> key()).
hash_key(Key) when is_integer(Key) ->
    <<N:128>> = erlang:md5(erlang:term_to_binary(Key)),
    N;
hash_key(Key) ->
    <<N:128>> = erlang:md5(Key),
    N.
%% userdevguide-end rt_simple:hash_key

%% @doc generates a random node id
%%      In this case it is a random 128-bit string.
-spec(getRandomNodeId/0 :: () -> key()).
getRandomNodeId() ->
    % generates 128 bits of randomness
    hash_key(randoms:getRandomId()).

%% userdevguide-begin rt_simple:next_hop
%% @doc returns the next hop to contact for a lookup
-spec(next_hop/2 :: (cs_state:state(), key()) -> cs_send:mypid()).
next_hop(State, _Key) ->
    cs_state:succ_pid(State).
%% userdevguide-end rt_simple:next_hop

%% userdevguide-begin rt_simple:init_stabilize
%% @doc triggered by a new stabilization round
-spec(init_stabilize/3 :: (key(), node:node_type(), rt()) -> rt()).
init_stabilize(_Id, Succ, _RT) ->
    % renew routing table
    empty(Succ).
%% userdevguide-end rt_simple:init_stabilize

%% userdevguide-begin rt_simple:filterDeadNode
%% @doc removes dead nodes from the routing table
-spec(filterDeadNode/2 :: (rt(), cs_send:mypid()) -> rt()).
filterDeadNode(RT, _DeadPid) ->
    RT.
%% userdevguide-end rt_simple:filterDeadNode

%% userdevguide-begin rt_simple:to_pid_list
%% @doc returns the pids of the routing table entries .
-spec(to_pid_list/1 :: (rt()) -> [cs_send:mypid()]).
to_pid_list({Succ, _RoutingTable} = _RT) ->
    [node:pidX(Succ)].
%% userdevguide-end rt_simple:to_pid_list

%% @doc returns the size of the routing table.
-spec(get_size/1 :: (rt()) -> pos_integer()).
get_size(_RT) ->
    1.

%% userdevguide-begin rt_simple:get_keys_for_replicas
normalize(Key) ->
    Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.

n() ->
    16#100000000000000000000000000000000.

%% @doc returns the replicas of the given key
-spec(get_keys_for_replicas/1 :: (key() | string()) -> [key()]).
get_keys_for_replicas(Key) ->
    HashedKey = hash_key(Key),
    [HashedKey,
     HashedKey bxor 16#40000000000000000000000000000000,
     HashedKey bxor 16#80000000000000000000000000000000,
     HashedKey bxor 16#C0000000000000000000000000000000
    ].
%% userdevguide-end rt_simple:get_keys_for_replicas


%% userdevguide-begin rt_simple:dump
%% @doc
-spec(dump/1 :: (rt()) -> ok).
dump(_State) ->
    ok.
%% userdevguide-end rt_simple:dump

% 0 -> succ
% 1 -> shortest finger
% 2 -> next longer finger
% 3 -> ...
% n -> me
% @spec to_dict(cs_state:state()) -> dict:dictionary()
to_dict(State) ->
    Succ = cs_state:succ(State),
    dict:store(0, Succ, dict:store(1, cs_state:me(State), dict:new())).

-spec(export_rt_to_cs_node/4 :: (rt(), key(), node:node_type(), node:node_type()) -> external_rt()).
export_rt_to_cs_node(RT, _Id, _Pred, _Succ) ->
    RT.

%% @doc prepare routing table for printing in web interface
-spec(to_html/1 :: (rt()) -> list()).
to_html({Succ, _}) ->
    io_lib:format("succ: ~p", [Succ]).

-spec(update_pred_succ_in_cs_node/3 :: (node:node_type(), node:node_type(), external_rt())
      -> external_rt()).
update_pred_succ_in_cs_node(_Pred, Succ, {_Succ, Tree} = _RT) ->
    {Succ, Tree}.
