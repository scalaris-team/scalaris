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
    {gb_trees:empty(),Start}.

on({subscribe,Pid,Module,ValueName,Min,Max,Value,Mesg},{State,Start}) ->
    io:format("[ SM ] ~p~n ",[{subscribe,Pid,Module,ValueName,Min,Max,Value,Mesg}]),
    {gb_trees:enter({Module,ValueName,Pid},{Min,Max,Value,Mesg},State),Start};
on({unsubscribe,Pid,Module,ValueName},{State,Start}) ->
    %io:format("[ SM ] ~p~n ",[{unsubscribe,Pid,Module,ValueName}]),
    {gb_trees:delete({Module,ValueName,Pid},State),Start};
on({update,Module,ValueName,Pid,Value},{State,Start})->
    %io:format("[ SM ] ~p~n ",[{update,Module,ValueName,Pid,Value}]),
    {update(Module,ValueName,Pid,Value,State),Start};

on({no_churn},{State,Start}) ->
    %%cs_send:send_local(get_pid_rt(),{no_churn}),
    {State,Start}.



update(Module,ValueName,Pid,NewValue,State) ->
    {Min,Max,_Value,Mesg} = gb_trees:get({Module,ValueName,Pid},State),
    %log_to_file(Module,ValueName,Min,Max,NewValue,Start),
    gb_trees:enter({Module,ValueName,Pid},{Min,Max,NewValue,Mesg},State).


log_to_file(Module,ValueName,_Min,_Max,NewValue,Start) ->
    Now = erlang:now(),
    N = io_lib:format("~p_~p.log",[Module,ValueName]),
    {ok,S} = file:open(N,[append]),
    %io:format(S,"~p ~p~n",[time_diff(Start,Now),NewValue]),
    file:close(S).

get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), self_man).

get_pid_rt() ->
    process_dictionary:lookup_process(erlang:get(instance_id), routing_table).

time_diff({SMe,SSe,SMi},{EMe,ESe,EMi}) ->
    (EMe*1000000+ESe+EMi/1000000)-(SMe*1000000+SSe+SMi/1000000).