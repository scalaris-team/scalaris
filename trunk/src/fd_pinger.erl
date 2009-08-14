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
%%% File    : fd_pinger.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : 
%% Author: christian
%% Created: Jun 16, 2009
%% Description: TODO: Add description to fd_pinger
%%%
%%% Created :  12 Jan 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin

-module(fd_pinger).
-author('hennig@zib.de').

-behavior(gen_component).
-export([init/1,on/2,start_link/1]).

-export([]).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a fd_pinger instance
%% @spec start_link(term()) -> {ok, pid()}

start_link([Module,Pid]) ->
   gen_component:start_link(?MODULE, [Module,Pid], []).

init([Module,Pid]) ->
    log:log(info,"[ fd_pinger ~p ] starting Node", [self()]),
    cs_send:send(Pid, {ping, cs_send:this(), 0}),
    cs_send:send_after(config:failureDetectorInterval(), self(), {timeout,0}), 
    {Module,Pid,0}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


on({stop},{_Module,_Pid,_Count}) ->
    kill;
on({pong,_},{Module,Pid,Count}) ->
    {Module,Pid,Count+1};
on({timeout,OldCount},{Module,Pid,Count}) ->
    case OldCount < Count of 
        true -> 
            cs_send:send(Pid, {ping, cs_send:this(), Count}),
            cs_send:send_after(config:failureDetectorInterval(), self(), {timeout,Count}),
            {Module,Pid,Count};
        false ->    
           report_crash(Pid,Module),
           kill
    end;
 on(_, _State) ->
    unknown_event.
% @private

%-spec(report_crash/1 :: (cs_send:mypid()) -> ok).
report_crash(Pid,Module) ->
    log:log(warn,"[ FD ] ~p crashed",[Pid]),
    cs_send:send_local(Module , {crash, Pid}).


