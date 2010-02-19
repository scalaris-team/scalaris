%  Copyright 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : trigger.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : trigger 
%%%
%%% Created :  2 Oct 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$

-module(trigger_periodic).

-author('hennig@zib.de').
-vsn('$Id$ ').

-behaviour(trigger).

-export([init/1, trigger_first/2, trigger_next/2]).



init(Module) ->
    %io:format("[ TR ~p ] ~p init ~n", [self(),Module]),
    {Module, ok}.

trigger_first({Module, ok}, _U) ->
    %io:format("[ TR ~p ] ~p first ~n", [self(),Module]),
    TimerRef = cs_send:send_after(0,self(), {trigger}),
    {Module, TimerRef}.

trigger_next({Module,ok},_U) ->
    NewTimerRef = cs_send:send_after(Module:get_base_interval(),self(), {trigger}),
    {Module,NewTimerRef};



trigger_next({Module, TimerRef}, _U) ->

    % test ob noch einer Timer laeuft
    case erlang:read_timer(TimerRef) of
        false ->
            %io:format("[ TR ~p ] ~p next ~n", [self(),Module]),
            NewTimerRef = cs_send:send_after(Module:get_base_interval(),self(), {trigger});
        _T ->
            %io:format("[ TR ~p ] ~p call next befor Timer Release ~p ms ~n", [self(),Module,T]),
            NewTimerRef = TimerRef
    end,
    {Module, NewTimerRef}.

