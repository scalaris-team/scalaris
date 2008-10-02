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
%%% File    : cs_join.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : join procedure
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_join.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_join).

-author('schuett@zib.de').
-vsn('$Id: cs_join.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([join/1, join_request/4]).

-include("chordsharp.hrl").

%% @doc handle the join request of a new node
%% @spec join_request(state:state(), pid(), Id, UniqueId) -> state:state()
%%   Id = term()
%%   UniqueId = term()
join_request(State, Source_PID, Id, UniqueId) ->
    Pred = node:new(Source_PID, Id, UniqueId),
    HisData = ?DB:split_data(cs_state:id(State), Id),
    SuccList = cs_state:succ_list(State),
    cs_send:send(Source_PID, {join_response, cs_state:pred(State), HisData, SuccList}),
    failuredetector:add_node(UniqueId, Id, Source_PID),
    cs_state:update_pred(State, Pred).

%%%------------------------------Join---------------------------------

%% @doc join an empty ring
join_first(Id) -> 
    io:format("[ I | Node   | ~w ] join as first ~w ~n",[self(), Id]),
    Me = node:make(cs_send:this(), Id),
    cs_state:new(?RT:empty(Me), [Me], Me, Me, {Id, Id}, cs_lb:new()).

%% @doc join a ring
join_ring(Id, Succ) ->
    io:format("[ I | Node   | ~w ] join_ring ~w ~n",[self(), Id]),
    Me = node:make(cs_send:this(), Id),
    UniqueId = node:uniqueId(Me),
    cs_send:send(node:pidX(Succ), {join, cs_send:this(), Id, UniqueId}),
    receive
	{join_response, Pred, Data, SuccList} -> 
	    io:format("[ I | Node   | ~w ] got pred ~w~n",[self(), Pred]),
	    case node:is_null(Pred) of
		true ->
		    failuredetector:add_node(node:uniqueId(Succ), node:id(Succ), node:pidX(Succ)),
		    ?DB:add_data(Data),
		    cs_state:new(?RT:empty(Succ), [Succ | SuccList], Pred, Me, {Id, Id}, cs_lb:new());
		false ->
		    failuredetector:add_nodes([{node:uniqueId(Pred), node:id(Pred), node:pidX(Pred)}, 
					       {node:uniqueId(Succ), node:id(Succ), node:pidX(Succ)}]),
		    cs_send:send(node:pidX(Pred), {update_succ, Me}),
		    ?DB:add_data(Data),
		    cs_state:new(?RT:empty(Succ), [Succ | SuccList], Pred, Me, {node:id(Pred), Id}, cs_lb:new())
	    end
    end.

%join_ring(_, error) ->
%    cs_join:join(randoms:getRandomNodeId()).

%% @doc join a ring and return initial state
%%      the boolean indicates whether it was the first 
%%      node in the ring or not
%% @spec join(Id) -> {true|false, state:state()}
%%   Id = term()
join(Id) -> 
    io:format("[ I | Node   | ~w ] joining ~p ~n",[self(), Id]),
    Ringsize = boot_server:number_of_nodes(),
    if
	Ringsize == 0 ->
	    State = join_first(Id),
	    cs_reregister:reregister(cs_state:uniqueId(State)),
	    {true, State};
	true ->
	    case cs_lookup:reliable_get_node(erlang:get(instance_id), 
					     Id, 60000) of
		error ->
		    join(Id);
		{ok, Succ} ->
		    State = join_ring(Id, Succ),
		    cs_reregister:reregister(cs_state:uniqueId(State)),
		    {false, State}
	    end
    end.

