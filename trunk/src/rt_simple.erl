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
%% @version $Id: rt_simple.erl 463 2008-05-05 11:14:22Z schuett $
-module(rt_simple).

-author('schuett@zib.de').
-vsn('$Id: rt_simple.erl 463 2008-05-05 11:14:22Z schuett $ ').

% routingtable behaviour
-export([empty/1, hash_key/1, getRandomNodeId/0, next_hop/2, stabilize/1, filterDeadNodes/2,
	to_pid_list/1, to_node_list/1, get_size/1, get_keys_for_replicas/1, dump/1]).

-export([normalize/1]).

-behaviour(routingtable).

%% @type rt() = {node:node(), gb_trees:gb_tree()}. Sample routing table
%% @type key() = int(). Identifier

%% @doc creates an empty routing table.
%%      per default the empty routing should already include 
%%      the successor
%% @spec empty(node:node()) -> rt()
empty(Succ) ->
    {Succ, gb_trees:empty()}.

%% @doc hashes the key to the identifier space.
%% @spec hash_key(string()) -> key()
hash_key(Key) ->
    BitString = binary_to_list(crypto:md5(Key)),
    % binary to integer
    lists:foldl(fun(El, Total) -> (Total bsl 8) bor El end, 0, BitString).

%% @doc generates a random node id
%%      In this case it is a random 128-bit string.
%% @spec getRandomNodeId() -> key()
getRandomNodeId() ->
    % generates 128 bits of randomness
    hash_key(integer_to_list(random:uniform(65536 * 65536))).

%% @doc returns the next hop to contact for a lookup
%% @spec next_hop(cs_state:state(), key()) -> pid()
next_hop(State, _Key) ->
    cs_state:succ_pid(State).

%% @doc triggers a new stabilization round
%% @spec stabilize(cs_state:state()) -> cs_state:state()
stabilize(State) ->
    % trigger the next stabilization round
    timer:send_after(config:pointerStabilizationInterval(), self(), {stabilize_pointers}),
    % renew routing table
    cs_state:set_rt(State, empty(cs_state:succ(State))).

%% @doc removes dead nodes from the routing table
%% @spec filterDeadNodes(rt(), [node:node()]) -> rt()
filterDeadNodes(RT, _DeadNodes) ->
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
get_size(RT) ->
    length(to_pid_list(RT)).
    
%% @doc returns the replicas of the given key
%% @spec get_keys_for_replicas(key() | string()) -> [key()]
get_keys_for_replicas(Key) when is_integer(Key) ->
    [Key, 
     normalize(Key + 16#40000000000000000000000000000000),
     normalize(Key + 16#80000000000000000000000000000000),
     normalize(Key + 16#C000000000000000000000000000000)
    ];
get_keys_for_replicas(Key) when is_list(Key) ->
    get_keys_for_replicas(hash_key(Key)).

%% @doc 
%% @spec dump(cs_state:state()) -> term()
dump(_State) ->
    ok.

normalize(Key) ->
    Key band 16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.
