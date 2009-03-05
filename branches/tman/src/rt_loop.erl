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
%%% File    : rt_loop.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : routing table process
%%%
%%% Created :  5 Dec 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(rt_loop).

-author('schuett@zib.de').
-vsn('$Id$ ').

% for routing table implementation
-export([start_link/1, start/2]).

-export([dump/0]).

-include("chordsharp.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Routing Table maintenance process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a routing table maintenance process
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    Link = spawn_link(?MODULE, start, [InstanceId, self()]),
    receive
        start_done ->
            ok
    end,
    {ok, Link}.

start(InstanceId, Sup) ->
    process_dictionary:register_process(InstanceId, routing_table, self()),
    log:log(info,"[ RT ~p ] starting routingtable", [self()]),
    timer:send_interval(config:pointerStabilizationInterval(), self(), {stabilize}),
    Sup ! start_done,
    loop().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Private Code
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
loop() ->
    receive
        {init, Id, Pred, Succ} ->
            loop(Id, Pred, Succ, ?RT:empty(Succ))
    end.

loop(Id, Pred, Succ, RTState) ->
    receive
	% can happen after load-balancing
        {init, Id, NewPred, NewSucc} ->
            check(RTState, ?RT:empty(NewSucc)),
            loop(Id, NewPred, NewSucc, ?RT:empty(NewSucc));
        % regular stabilize operation
        {stabilize} ->
            Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
            Pid ! {get_pred_succ, cs_send:this()},
            NewRTState = ?RT:stabilize(Id, Succ, RTState),
            check(RTState, NewRTState),
            loop(Id, Pred, Succ, NewRTState);
        % got new successor
        {get_pred_succ_response, NewPred, NewSucc} ->
            loop(Id, NewPred, NewSucc, RTState);
        {rt_get_node_response, Index, Node} ->
            NewRTState = ?RT:stabilize(Id, Succ, RTState, Index, Node),
            check(RTState, NewRTState),
            loop(Id, Pred, Succ, NewRTState);
        {lookup_pointer_response, Index, Node} ->
            NewRTState = ?RT:stabilize_pointer(Id, RTState, Index, Node),
            check(RTState, NewRTState),
            loop(Id, Pred, Succ, NewRTState);
        {'$gen_cast', {debug_info, Requestor}} ->
            Requestor ! {debug_info_response, [{"rt_debug", ?RT:dump(RTState)}, {"rt_size", ?RT:get_size(RTState)}]},
            loop(Id, Pred, Succ, RTState);
        {check,Port} ->
            Y = [ {Index,node:pidX(Node)} || {Index,Node} <-gb_trees:to_list(RTState) ],
            log:log(info,"[ RT ]: Wrongitems: ~p",[[Index || {Index,{_IP,PORT,_PID}} <-Y, PORT/=Port]]),
            loop(Id, Pred, Succ, RTState);
        {crash, DeadPid} ->
            NewRT = ?RT:filterDeadNode(RTState, DeadPid),
	    %io:format("~p~n",[{DeadPid,gb_trees:to_list(RTState),gb_trees:to_list(NewRT)}]),
            check(RTState, NewRT, false),
            loop(Id, Pred, Succ, NewRT );
        {dump} ->
            log:log(info,"[ RT ] ~p:~p", [Id, ?RT:dump(RTState)]),
            loop(Id, Pred, Succ, RTState);
        X ->
            log:log(warn,"[ RT ]: unknown message ~p", [X]),
            loop(Id, Pred, Succ, RTState)
    end.

check(Old, New) ->
    check(Old, New, true).

% OldRT, NewRT, CheckFD
check(X, X, _) ->
    ok;
check(OldRT, NewRT, true) ->
    Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
    Pid ! {rt_update, NewRT},
    check_fd(NewRT, OldRT);
check(_OldRT, NewRT, false) ->
    Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
    Pid ! {rt_update, NewRT}.

-spec(check_fd/2 :: (list(cs_send:mypid()), list(cs_send:mypid())) -> any()).
check_fd(X, X) ->
    ok;
check_fd(NewRT, OldRT) ->
    NewView = lists:usort(?RT:to_pid_list(NewRT)),
    OldView = lists:usort(?RT:to_pid_list(OldRT)),
    NewNodes = util:minus(NewView,OldView),
    OldNodes = util:minus(OldView,NewView),
    failuredetector2:unsubscribe(OldNodes),             
    failuredetector2:subscribe(NewNodes).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Debug
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dump() ->
    Pids = process_dictionary:find_all_processes(routing_table),
    [Pid ! {dump} || Pid <- Pids],
    ok.
