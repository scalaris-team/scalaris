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

-module(trigger_dynamic).

-author('hennig@zib.de').
-vsn('$Id$ ').

-behaviour(trigger).

-export([init/1, trigger_first/2, trigger_next/2]).



init(Module) ->
    {Module, ok}.

trigger_first({Module, ok}, _U) ->

    TimerRef = cs_send:send_after(0,self(), {trigger}),
    {Module, TimerRef}.

trigger_next({Module,ok},_U) ->
    NewTimerRef = cs_send:send_after(Module:get_base_interval(),self(), {trigger}),
    {Module,NewTimerRef};

% 0 - > max
% 1 - > base
% 2 - > min
% 3 - > now,min
trigger_next({Module, TimerRef}, U) ->
    % test ob noch einer Timer laeuft
    case erlang:read_timer(TimerRef) of
        false ->
            ok;
        _ ->
            erlang:cancel_timer(TimerRef)
    end,
    %io:format("[ TD ] ~p U(0,0) ~p~n",[self(),U(0,0)]),
    NewTimerRef = case U(0,0) of
                0 ->
                    cs_send:send_after(Module:get_max_interval(),self(), {trigger});
                1 ->
                    cs_send:send_after(Module:get_base_interval(),self(), {trigger});
                2 ->
                    cs_send:send_after(Module:get_min_interval(),self(), {trigger});
                3 ->
                    cs_send:send_local(self(), {trigger}),
                    cs_send:send_after(Module:get_min_interval(),self(), {trigger});
                _ ->
                    cs_send:send_after(Module:get_base_interval(),self(), {trigger})

            
     end,
    {Module, NewTimerRef}.