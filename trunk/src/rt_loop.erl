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

-behaviour(gen_component).

% for routing table implementation
-export([start_link/1]).
-export([init/1, on/2]).

-include("../include/scalaris.hrl").

% state of the routing table loop
-type(state() :: {Id::?RT:key(), 
		  Pred::node:node_type(), 
		  Succ::node:node_type(), 
		  RTState::?RT:rt()}).

% accepted messages of rt_loop processes
-type(message() :: 
      {init, Id::?RT:key(), Pred::node:node_type(), Succ::node:node_type()}
     | {stabilize}
     | {get_pred_succ_response, NewPred::node:node_type(), NewSucc::node:node_type()}
     | {rt_get_node_response, Index::pos_integer(), Node::node:node_type()}
     | {lookup_pointer_response, Index::pos_integer(), Node::node:node_type()} 
     | {crash, DeadPid::cs_send:mypid()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Routing Table maintenance process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a routing table maintenance process
%% @spec start_link(term()) -> {ok, pid()}
-spec(start_link/1 :: (any()) -> {ok, pid()}).
start_link(InstanceId) ->
    gen_component:start_link(?MODULE, [InstanceId], []).

-spec(init/1 :: ([any()]) -> state()).
init([InstanceId]) ->
    process_dictionary:register_process(InstanceId, routing_table, self()),
    log:log(info,"[ RT ~p ] starting routingtable", [self()]),
    cs_send:send_after(config:pointerStabilizationInterval(), self(), {stabilize}),
    {uninit}.
    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Private Code
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc message handler
-spec(on/2 :: (message(), state()) -> state()).

on({init, Id, Pred, Succ},{uninit}) ->
    {Id, Pred, Succ, ?RT:empty(Succ)};
on(Message,{uninit}) ->
    cs_send:send_local(self() , Message),
    {uninit};

% re-initialize routing table
on({init, Id2, NewPred, NewSucc}, {_, _, _, RTState}) ->
    check(RTState, ?RT:empty(NewSucc), Id2, NewPred, NewSucc),
    {Id2, NewPred, NewSucc, ?RT:empty(NewSucc)};

% start new periodic stabilization
on({stabilize}, {Id, Pred, Succ, RTState}) ->
    % trigger next stabilization
    cs_send:send_after(config:pointerStabilizationInterval(), self(), {stabilize}),
    Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
    % get new pred and succ from cs_node
    cs_send:send_local(Pid , {get_pred_succ, cs_send:this()}),
    % start periodic stabilization
    NewRTState = ?RT:init_stabilize(Id, Succ, RTState),
    check(RTState, NewRTState, Id, Pred, Succ),
    {Id, Pred, Succ, NewRTState};

% got new predecessor/successor
on({get_pred_succ_response, NewPred, NewSucc}, {Id, _, _, RTState}) ->
    {Id, NewPred, NewSucc, RTState};

%
on({rt_get_node_response, Index, Node}, {Id, Pred, Succ, RTState}) ->
    NewRTState = ?RT:stabilize(Id, Succ, RTState, Index, Node),
    check(RTState, NewRTState, Id, Pred, Succ),
    {Id, Pred, Succ, NewRTState};

%
on({lookup_pointer_response, Index, Node}, {Id, Pred, Succ, RTState}) ->
    NewRTState = ?RT:stabilize_pointer(Id, RTState, Index, Node),
    check(RTState, NewRTState, Id, Pred, Succ),
    {Id, Pred, Succ, NewRTState};

% failure detector reported dead node
on({crash, DeadPid}, {Id, Pred, Succ, RTState}) ->
    NewRT = ?RT:filterDeadNode(RTState, DeadPid),
    check(RTState, NewRT, Id, Pred, Succ, false),
    {Id, Pred, Succ, NewRT};

% debug_info for web interface
on({'$gen_cast', {debug_info, Requestor}}, {Id, Pred, Succ, RTState}) ->
    cs_send:send_local(Requestor , {debug_info_response, [{"rt_debug", ?RT:dump(RTState)}, 
				       {"rt_size", ?RT:get_size(RTState)}]}),
    {Id, Pred, Succ, RTState};

% unknown message
on(_UnknownMessage, _State) ->
    unknown_event.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
-spec(check/5 :: (Old::?RT:state(), New::?RT:state(), ?RT:key(), node:node_type(),
                  node:node_type()) -> any()).
check(Old, New, Id, Pred, Succ) ->
    check(Old, New, Id, Pred, Succ, true).

% OldRT, NewRT, CheckFD
-spec(check/6 :: (Old::?RT:state(), New::?RT:state(), ?RT:key(), node:node_type(),
                  node:node_type(), ReportFD::bool()) -> any()).
check(X, X, Id, _Pred, _Succ, _) ->
    ok;
check(OldRT, NewRT, Id, Pred, Succ, true) ->
    Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
    cs_send:send_local(Pid ,  {rt_update, ?RT:export_rt_to_cs_node(NewRT, Id, Pred, Succ)}),
    check_fd(NewRT, OldRT);
check(_OldRT, NewRT, Id, Pred, Succ, false) ->
    Pid = process_dictionary:lookup_process(erlang:get(instance_id), cs_node),
    cs_send:send_local(Pid ,  {rt_update, ?RT:export_rt_to_cs_node(NewRT, Id, Pred, Succ)}).

check_fd(X, X) ->
    ok;
check_fd(NewRT, OldRT) ->
    NewView = ?RT:to_pid_list(NewRT),
    OldView = ?RT:to_pid_list(OldRT),
    NewNodes = util:minus(NewView,OldView),
    OldNodes = util:minus(OldView,NewView),
    failuredetector2:unsubscribe(OldNodes),
    failuredetector2:subscribe(NewNodes).
