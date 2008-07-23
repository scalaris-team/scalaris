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
%% @version $Id: rt_chord.erl 463 2008-05-05 11:14:22Z schuett $
-module(rt_chord).

-author('schuett@zib.de').
-vsn('$Id: rt_chord.erl 463 2008-05-05 11:14:22Z schuett $ ').

-behaviour(routingtable).

% routingtable behaviour
-export([empty/1, 
	 hash_key/1, getRandomNodeId/0, 
	 next_hop/2, 
	 stabilize/1, 
	 filterDeadNodes/2,
	 to_pid_list/1, to_node_list/1, get_size/1, 
	 get_keys_for_replicas/1, dump/1]).

% stabilize for Chord
-export([stabilize/3]).


%% @type rt() = gb_trees:gb_tree(). Chord routing table
%% @type key() = int(). Identifier

%% @doc creates an empty routing table.
%% @spec empty(node:node()) -> rt()
empty(_Succ) ->
    gb_trees:empty().

%% @doc hashes the key to the identifier space.
%% @spec hash_key(string()) -> key()
hash_key(Key) ->
    rt_simple:hash_key(Key).

%% @doc generates a random node id
%%      In this case it is a random 128-bit string.
%% @spec getRandomNodeId() -> key()
getRandomNodeId() ->
    rt_simple:getRandomNodeId().

%% @doc returns the next hop to contact for a lookup
%% @spec next_hop(cs_state:state(), key()) -> node()
next_hop(State, Id) -> 
    case util:is_between(cs_state:id(State), Id, cs_state:succ_id(State)) of
	%succ is responsible for the key
	true ->
	    cs_state:succ_pid(State);
	% check routing table
	false ->
	    RT = cs_state:rt(State),
	    next_hop(cs_state:id(State), RT, Id, 127, cs_state:succ_pid(State))
    end.

% @private
next_hop(N, RT, Id, Index, Candidate) ->
    case gb_trees:is_defined(Index, RT) of
	true ->
	    case gb_trees:get(Index, RT) of
		null ->
		    Candidate;
		Entry ->
		    case util:is_between_closed(N, node:id(Entry), Id) of
			true ->
			    node:pidX(Entry);
			false ->
			    next_hop(N, RT, Id, Index - 1, Candidate)
		    end
		end;
	false ->
	    Candidate
    end.

%% @doc starts the stabilization routine
%% @spec stabilize(cs_state:state()) -> cs_state:state()
stabilize(State) ->
    % trigger the next stabilization round
    timer:send_after(config:pointerStabilizationInterval(), self(), {stabilize_pointers}),
    % calculate the longest finger
    Key = calculateKey(State, 127),
    % trigger a lookup for Key
    cs_lookup:unreliable_lookup(Key, {rt_get_node, cs_send:this(), 127}),
    State.

%% @doc remove all entries with the given ids
%% @spec filterDeadNodes(gb_trees:gb_tree(), term()) -> gb_trees:gb_tree()
filterDeadNodes(RT, DeadNodes) ->
    gb_sets:fold(fun (Id, RT2) -> filter_intern(gb_trees:iterator(RT2), RT2, Id) end, RT, DeadNodes).

% @private
filter_intern(nil, RT, _) ->
    RT;
filter_intern([], RT, _) ->
    RT;
filter_intern(Iterator, RT, Id) ->
    {Index, Value, Next} = gb_trees:next(Iterator),
    case node:uniqueId(Value) == Id of
	true ->
	    filter_intern(Next, gb_trees:delete(Index, RT), Id);
	false ->
	    filter_intern(Next, RT, Id)
    end.

%% @doc returns the pids of the routing table entries .
%% @spec to_pid_list(rt()) -> [pid()]
to_pid_list(RT) ->
    lists:map(fun ({_Idx, Node}) -> node:pidX(Node) end, gb_trees:to_list(RT)).

%% @doc returns the pids of the routing table entries .
%% @spec to_node_list(rt()) -> [node:node()]
to_node_list(RT) ->
    lists:map(fun ({_Idx, Node}) -> Node end, gb_trees:to_list(RT)).

%% @doc returns the size of the routing table.
%%      inefficient standard implementation
%% @spec get_size(rt()) -> int()
get_size(RT) ->
    length(to_pid_list(RT)).
    
%% @doc returns the replicas of the given key
%% @spec get_keys_for_replicas(key() | string()) -> [key()]
get_keys_for_replicas(Key) ->
    rt_simple:get_keys_for_replicas(Key).

%% @doc 
%% @spec dump(cs_state:state()) -> term()
dump(_State) ->
    ok.

%% @doc updates one entry in the routing table
%%      and triggers the next update
%% @spec stabilize(cs_state:state(), int(), node:node()) -> cs_state:state()
stabilize(State, Index, Node) ->
    if
	Index == 1 ->
	    io:format("~p ~p ~p ~p~n", [Index, cs_state:me(State), cs_state:succ(State), Node]);
	true ->
	    ok
    end,
    RT = cs_state:rt(State),
    case node:is_null(Node) of
	true ->
	    State;
	false ->
	    case cs_state:succ(State) == Node of
		true ->
		    State;
		false ->
		    NewRT = gb_trees:enter(Index, Node, RT),
		    Key = calculateKey(State, Index - 1),
		    self() ! {lookup_aux, Key, {rt_get_node, cs_send:this(), Index - 1}},
		    cs_state:set_rt(State, NewRT)
	    end
    end.

% @private
calculateKey(State, Idx) ->
    % n + 2^Idx
    rt_simple:normalize(cs_state:id(State) + (1 bsl Idx)).
