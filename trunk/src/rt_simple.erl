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
%%% File    : rt_simple.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : sample routing table
%%%
%%% Created :  14 Apr 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(rt_simple).

-author('schuett@zib.de').
-vsn('$Id$ ').

% routingtable behaviour
-export([empty/1, hash_key/1, getRandomNodeId/0, next_hop/2, stabilize/3, filterDeadNode/2,
	 to_pid_list/1, to_node_list/1, get_size/1, get_keys_for_replicas/1, is_equal_key/2, 
	 get_standard_key/1, get_other_replicas_for_key/1, dump/1, to_dict/1]).

-export([normalize/1]).

-behaviour(routingtable).

% @type key(). Identifier.
-type(key()::pos_integer()).
% @type rt(). Routing Table.
-ifdef(types_are_builtin).
-type(rt()::{node:node_type(), gb_tree()}).
-else.
-type(rt()::{node:node_type(), gb_trees:gb_tree()}).
-endif.

%% @doc creates an empty routing table.
%%      per default the empty routing should already include 
%%      the successor
-spec(empty/1 :: (node:node_type()) -> rt()).
empty(Succ) ->
    {Succ, gb_trees:empty()}.

%% @doc hashes the key to the identifier space.
-spec(hash_key/1 :: (any()) -> key()).
hash_key(Key) ->
    BitString = binary_to_list(crypto:md5(Key)),
    % binary to integer
    lists:foldl(fun(El, Total) -> (Total bsl 8) bor El end, 0, BitString).

%% @doc generates a random node id
%%      In this case it is a random 128-bit string.
-spec(getRandomNodeId/0 :: () -> key()).
getRandomNodeId() ->
    % generates 128 bits of randomness
    hash_key(integer_to_list(crypto:rand_uniform(1, 65536 * 65536))).

%% @doc returns the next hop to contact for a lookup
%% @spec next_hop(cs_state:state(), key()) -> pid()
next_hop(State, _Key) ->
    cs_state:succ_pid(State).

%% @doc triggers a new stabilization round
-spec(stabilize/3 :: (key(), node:node_type(), rt()) -> rt()).
stabilize(_Id, Succ, _RT) ->
    % renew routing table
    empty(Succ).

%% @doc removes dead nodes from the routing table
-spec(filterDeadNode/2 :: (rt(), cs_send:mypid()) -> rt()).
filterDeadNode(RT, _DeadPid) ->
    RT.

%% @doc returns the pids of the routing table entries .
%% @spec to_pid_list(rt()) -> [pid()]
to_pid_list({Succ, _RoutingTable} = _RT) ->
    [node:pidX(Succ)].

%% @doc returns the pids of the routing table entries .
%% @spec to_node_list(rt()) -> [node:node()]
to_node_list({Succ, _RoutingTable} = _RT) ->
    [Succ].

%% @doc returns the size of the routing table.
%%      inefficient standard implementation
%% @spec get_size(rt()) -> int()
-spec(get_size/1 :: (rt()) -> pos_integer()).
get_size(RT) ->
    length(to_pid_list(RT)).
    
%% @doc returns the replicas of the given key
%% @spec get_keys_for_replicas(key() | string()) -> [key()]
get_keys_for_replicas(Key) when is_integer(Key) ->
    [Key, 
     normalize(Key + 16#40000000000000000000000000000000),
     normalize(Key + 16#80000000000000000000000000000000),
     normalize(Key + 16#C0000000000000000000000000000000)
    ];
get_keys_for_replicas(Key) when is_list(Key) ->
    get_keys_for_replicas(hash_key(Key)).

%% @doc returns true if both keys describe the same item under replication
-spec(is_equal_key/2 :: (key(), key()) -> bool()).
is_equal_key(Key1, Key2) ->
    Key1 == Key2 
	orelse Key1 == normalize(Key2 + 16#40000000000000000000000000000000)
	orelse Key1 == normalize(Key2 + 16#80000000000000000000000000000000)
	orelse Key1 == normalize(Key2 + 16#C0000000000000000000000000000000).

%% @doc perform "undo" on replication
-spec(get_standard_key/1 :: (key()) -> key()).
get_standard_key(Key) ->    
    if
	Key >= 16#C0000000000000000000000000000000 ->
	    Key - 16#C0000000000000000000000000000000;
	Key >= 16#80000000000000000000000000000000 ->
	    Key - 16#80000000000000000000000000000000;
	Key >= 16#40000000000000000000000000000000 ->
	    Key - 16#40000000000000000000000000000000;
	true ->
	    Key
    end.

%% @doc get other replicas of the given replicated tree
-spec(get_other_replicas_for_key/1 :: (key()) -> list(key())).
get_other_replicas_for_key(Key) ->
    [
     normalize(Key + 16#40000000000000000000000000000000),
     normalize(Key + 16#80000000000000000000000000000000),
     normalize(Key + 16#C0000000000000000000000000000000)
    ].
    
%% @doc 
-spec(dump/1 :: (rt()) -> ok).
dump(_State) ->
    ok.

normalize(Key) ->
    Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.


% 0 -> succ
% 1 -> shortest finger
% 2 -> next longer finger
% 3 -> ...
% n -> me
% @spec to_dict(cs_state:state()) -> dict:dictionary()
to_dict(State) ->
    Succ = cs_state:succ(State),
    dict:store(0, Succ, dict:store(1, cs_state:me(State), dict:new())).
