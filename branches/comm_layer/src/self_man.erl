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
%%% File    : self_man.erl
%%% Author  : Chrsitian Hennig <hennig@zib.de>
%%% Description : self management Service, in this stadium only for logging
%%%
%%% Created : 3. Sep 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Chrsitian Hennig <hennig@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(self_man).
-include("../include/scalaris.hrl").
-author('hennig@zib.de').
-vsn('$Id$ ').
-behaviour(gen_component).
-export([start_link/1,on/2,init/1,get_pid/0]).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     %
     % handle message

    ];
behaviour_info(_Other) ->
    undefined.



start_link(InstanceID) ->
   gen_component:start_link(?MODULE, [InstanceID,[]], [{register,InstanceID, self_man}]).

init(_Args) ->
    Start = erlang:now(),
    log:log(info,"[ SM ~p ] starting self_man", [self()]),
    cs_send:send_after(config:read(self_man_timer),self() ,{trigger}),
    {gb_trees:empty(),Start}.

on({request_trigger,Pid,U},{State,Start}) ->
    io:format("[ SM ] ~p~n ",[{request_trigger,Pid}]),
    {gb_trees:enter(Pid,U,State),Start};

on({no_churn},{State,Start}) ->
    %%cs_send:send_local(get_pid_rt(),{no_churn}),
    {State,Start};
on({trigger},{State,Start}) ->
    cs_send:send_after(config:read(self_man_timer),self() ,{trigger}),
    List = gb_trees:to_list(State),
    S = lists:sort(fun help/2, List),
    %io:format("S: ~p~n",[S]),
    R = trigger_send(S),
    {R,Start};
on(_, _State) ->
    unknown_event.

trigger_send([]) ->
    gb_trees:empty();
trigger_send([{Pid,_}|T]) ->
    io:format("[ SM ] ~p~n ",[{trigger,Pid}]),
    cs_send:send_local(Pid,{trigger}),
    gb_trees:from_orddict(T).

help({_,A},{_,B}) ->
    A(0,0) > B(0,0).


get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), self_man).
