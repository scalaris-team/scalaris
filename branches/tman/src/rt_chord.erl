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
-export([empty/1, 
	 hash_key/1, getRandomNodeId/0, 
	 next_hop/2, 
	 stabilize/3, 
	 filterDeadNode/2,
	 to_pid_list/1, to_node_list/1, get_size/1, 
	 get_keys_for_replicas/1, dump/1, to_dict/1]).

% stabilize for Chord
-export([stabilize/5]).

-type(key()::pos_integer()).
-type(rt()::gb_trees:gb_tree()).

%% @doc creates an empty routing table.
-spec(empty/1 :: (node:node_type()) -> rt()).
empty(_Succ) ->
    gb_trees:empty().

%% @doc hashes the key to the identifier space.
-spec(hash_key/1 :: (any()) -> key()).
hash_key(Key) ->
    rt_simple:hash_key(Key).

%% @doc generates a random node id
%%      In this case it is a random 128-bit string.
-spec(getRandomNodeId/0 :: () -> key()).
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
next_hop(_N, _RT, _Id, 0, Candidate) -> Candidate;
next_hop(N, RT, Id, Index, Candidate) ->
    case gb_trees:is_defined(Index, RT) of
	true ->
	    case gb_trees:get(Index, RT) of
		null ->
		    next_hop(N, RT, Id, Index - 1, Candidate);
		Entry ->
		    case util:is_between_closed(N, node:id(Entry), Id) of
			true ->
			    node:pidX(Entry);
			false ->
			    next_hop(N, RT, Id, Index - 1, Candidate)
		    end
		end;
	false ->
	    next_hop(N, RT, Id, Index - 1, Candidate)
    end.

%% @doc starts the stabilization routine
-spec(stabilize/3 :: (key(), node:node_type(), rt()) -> rt()).
stabilize(Id, Succ, RT) ->
    % calculate the longest finger
    Key = calculateKey(Id, 127),
    % trigger a lookup for Key
    cs_lookup:unreliable_lookup(Key, {rt_get_node, cs_send:this(), 127}),
    cleanup(gb_trees:iterator(RT), RT, Succ).

%% @doc remove all entries with the given ids
-spec(filterDeadNode/2 :: (rt(), cs_send:mypid()) -> rt()).
filterDeadNode(RT, DeadPid) ->
    DeadIndices = [Index|| {Index, Node}  <- gb_trees:to_list(RT),
                           node:pidX(Node) == DeadPid],
    lists:foldl(fun (Index, Tree) -> gb_trees:delete(Index, Tree) end,
                RT, DeadIndices).


%% @doc returns the pids of the routing table entries .
-spec(to_pid_list/1 :: (rt()) -> list(cs_send:mypid())).
to_pid_list(RT) ->
    lists:map(fun ({_Idx, Node}) -> node:pidX(Node) end, gb_trees:to_list(RT)).

%% @doc returns the pids of the routing table entries .
-spec(to_node_list/1 :: (rt()) -> list(node:node_type())).
to_node_list(RT) ->
    lists:map(fun ({_Idx, Node}) -> Node end, gb_trees:to_list(RT)).

%% @doc returns the size of the routing table.
%%      inefficient standard implementation
-spec(get_size/1 :: (rt()) -> pos_integer()).
get_size(RT) ->
    length(to_pid_list(RT)).
    
%% @doc returns the replicas of the given key
-spec(get_keys_for_replicas/1 :: (key()) -> list(key())).
get_keys_for_replicas(Key) ->
    rt_simple:get_keys_for_replicas(Key).

%% @doc 
-spec(dump/1 :: (rt()) -> any()).
dump(RT) ->
    lists:flatten(io_lib:format("~p", [gb_trees:to_list(RT)])).

%% @doc updates one entry in the routing table
%%      and triggers the next update
-spec(stabilize/5 :: (key(), node:node_type(), rt(), pos_integer(), node:node_type()) -> rt()).
stabilize(Id, Succ, RT, Index, Node) ->
    case node:is_null(Node) of
	true ->
	    RT;
	false ->
	    case (node:id(Succ) == node:id(Node)) or (Id == node:id(Node)) or (Index == -1) of
		true ->
		    RT;
		false ->
		    NewRT = gb_trees:enter(Index, Node, RT),
		    failuredetector2:subscribe(node:pidX(Node)),
		    Key = calculateKey(Id, Index - 1),
		    cs_lookup:unreliable_lookup(Key, {rt_get_node, cs_send:this(), Index - 1}),
		    NewRT
	    end
    end.

% @private
-spec(calculateKey/2 :: (key(), pos_integer()) -> key()).
calculateKey(Id, Idx) ->
    % n + 2^Idx
    rt_simple:normalize(Id + (1 bsl Idx)).

% 0 -> succ
% 1 -> shortest finger
% 2 -> next longer finger
% 3 -> ...
% n -> me
% @spec to_dict(cs_state:state()) -> dict:dictionary()
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

% @private
% @doc check whether the successor is stored by accident in the RT
cleanup(It, RT, Succ) ->
    case gb_trees:next(It) of
	{Index, Node, Iter2} ->
	    case node:id(Node) == node:id(Succ) of
		true ->
		    cleanup(Iter2, gb_trees:delete(Index, RT), Succ);
		false ->
		    cleanup(Iter2, RT, Succ)
	    end;
	none ->
	    RT
    end.
