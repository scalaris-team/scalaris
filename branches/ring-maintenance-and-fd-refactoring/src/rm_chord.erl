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
%%% File    : rm_chord.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Chord-like ring maintenance
%%%
%%% Created :  27 Nov 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(rm_chord).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([start/2]).

-behavior(ring_maintenance).

-export([start_link/1, initialize/4, 
	 get_successorlist/0, succ_left/1, pred_left/1, 
	 notify/1, update_succ/1, update_pred/1, 
	 get_as_list/0]).

% unit testing
-export([merge/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a chord-like ring maintenance process
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    Link = spawn_link(?MODULE, start, [InstanceId, self()]),
    receive
	start_done ->
	    ok
    end,
    {ok, Link}.

%% @doc called once by the cs_node when joining the ring
initialize(Id, Me, Pred, Succ) ->
    get_pid() ! {init, Id, Me, Pred, [Succ], self()},
    receive
	{init_done} ->
	    ok
    end.

get_successorlist() ->
    get_pid() ! {get_successorlist, self()},
    receive
	{get_successorlist_response, SuccList} ->
	    SuccList
    end.

%% @doc notification that my succ left
%%      parameter is his current succ list
succ_left(_SuccsSuccList) ->
    %% @TODO
    ok.

%% @doc notification that my pred left
%%      parameter is his current pred
pred_left(_PredsPred) ->
    %% @TODO
    ok.

%% @doc notification that my succ changed
%%      parameter is potential new succ
update_succ(_Succ) ->
    %% @TODO
    ok.

%% @doc notification that my pred changed
%%      parameter is potential new pred
update_pred(_Pred) ->
    %% @TODO
    ok.

notify(Pred) ->
    get_pid() ! {notify, Pred}.

get_as_list() ->
    get_successorlist().
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
    receive
	{init, NewId, NewMe, NewPred, NewSuccList, CSNode} -> %set info for cs_node
	    ring_maintenance:update_succ_and_pred(NewPred, hd(NewSuccList)),
	    cs_send:send(node:pidX(hd(NewSuccList)), {get_succ_list, cs_send:this()}),
	    failuredetector2:subscribe([node:pidX(Node) || Node <- [NewPred | NewSuccList]]),
	    CSNode ! {init_done},
	    loop(NewId, NewMe, NewPred, NewSuccList)
    end.

loop(Id, Me, Pred, Succs) ->
    receive
	{init, NewId, NewMe, NewPred, NewSuccList, CSNode} -> %set info for cs_node
	    ring_maintenance:update_succ_and_pred(NewPred, hd(NewSuccList)),
	    cs_send:send(node:pidX(hd(NewSuccList)), {get_succ_list, cs_send:this()}),
	    failuredetector2:subscribe([node:pidX(Node) || Node <- [NewPred | NewSuccList]]),
	    CSNode ! {init_done},
	    loop(NewId, NewMe, NewPred, NewSuccList);
	{get_successorlist, Pid} ->
	    Pid ! {get_successorlist_response, Succs},
	    loop(Id, Me, Pred, Succs);
	{stabilize} -> % new stabilization interval
	    cs_send:send(node:pidX(hd(Succs)), {get_pred, cs_send:this()}),
	    loop(Id, Me, Pred, Succs);
	{get_pred_response, SuccsPred} ->
	    case node:is_null(SuccsPred) of
		false ->
		    case util:is_between_stab(Id, node:id(SuccsPred), node:id(hd(Succs))) of
			true ->
			    cs_send:send(node:pidX(SuccsPred), {get_succ_list, cs_send:this()}),
			    ring_maintenance:update_succ_and_pred(Pred, SuccsPred),
			    failuredetector2:subscribe(node:pidX(SuccsPred)),
			    loop(Id, Me, Pred, [SuccsPred | Succs]);
			false ->
			    cs_send:send(node:pidX(hd(Succs)), {get_succ_list, cs_send:this()}),
			    loop(Id, Me, Pred, Succs)
		    end;
		true ->
		    cs_send:send(node:pidX(hd(Succs)), {get_succ_list, cs_send:this()}),
		    loop(Id, Me, Pred, Succs)
	    end;
	{get_succ_list_response, Succ, SuccsSuccList} ->
	    NewSuccs = util:trunc(merge([Succ | SuccsSuccList], Succs, Id), config:succListLength()),
	    %% @TODO if(length(NewSuccs) < succListLength() / 2) do something right now
	    cs_send:send(node:pidX(hd(NewSuccs)), {notify, Me}),
	    ring_maintenance:update_succ_and_pred(Pred, hd(NewSuccs)), 
	    failuredetector2:subscribe([node:pidX(Node) || Node <- NewSuccs]),
	    loop(Id, Me, Pred, NewSuccs);
	{notify, NewPred} ->
	    case node:is_null(Pred) of
		true ->
		    ring_maintenance:update_succ_and_pred(NewPred, hd(Succs)),
		    failuredetector2:subscribe(node:pidX(NewPred)),
		    loop(Id, Me, NewPred, Succs);
		false ->
		    case util:is_between_stab(node:id(Pred), node:id(NewPred), Id) of
			true ->
			    ring_maintenance:update_succ_and_pred(NewPred, hd(Succs)),
			    failuredetector2:subscribe(node:pidX(NewPred)),
			    loop(Id, Me, NewPred, Succs);
			false ->
			    loop(Id, Me, Pred, Succs)
		    end
	    end;
	{crash, DeadPid} ->
	    case node:is_null(Pred) orelse DeadPid == node:pidX(Pred) of
		true ->
		    loop(Id, Me, node:null(), filter(DeadPid, Succs));
		false ->
		    loop(Id, Me, Pred, filter(DeadPid, Succs))
	    end;
	{'$gen_cast', {debug_info, Requestor}} ->
	    Requestor ! {debug_info_response, [{"pred", lists:flatten(io_lib:format("~p", [Pred]))}, 
					       {"succs", lists:flatten(io_lib:format("~p", [Succs]))}]},
	    loop(Id, Me, Pred, Succs);
	X ->
	    io:format("@rm_chord unknown message ~p~n", [X]),
	    loop(Id, Me, Pred, Succs)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc merge two successor lists into one
%%      and sort by identifier
merge(L1, L2, Id) ->
    MergedList = lists:append(L1, L2),
    Order = fun(A, B) ->
		    node:id(A) =< node:id(B)
	    end,
    Larger  = util:uniq(lists:sort(Order, [X || X <- MergedList, node:id(X) >  Id])),
    Equal   = util:uniq(lists:sort(Order, [X || X <- MergedList, node:id(X) == Id])),
    Smaller = util:uniq(lists:sort(Order, [X || X <- MergedList, node:id(X) <  Id])),
    lists:append([Larger, Smaller, Equal]).

filter(_Pid, []) ->
    [];
filter(Pid, [Succ | Rest]) ->
    case Pid == node:pidX(Succ) of
	true ->
	    filter(Pid, Rest);
	false ->
	    [Succ | filter(Pid, Rest)]
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc starts ring maintenance
start(InstanceId, Sup) ->
    process_dictionary:register_process(InstanceId, ring_maintenance, self()),
    io:format("[ I | RM     | ~p ] starting ring maintainer~n", [self()]),
    timer:send_interval(config:stabilizationInterval(), self(), {stabilize}),
    Sup ! start_done,
    start().

% @private
get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), ring_maintenance).
