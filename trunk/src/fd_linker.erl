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
%%% File    : fd_linker.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : 
%%%
%%% Created :  12 Jan 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum fÃ¼r Informationstechnik Berlin

-module(fd_linker).

-author('hennig@zib.de').



-behavior(gen_component).
-export([init/1,on/2,start_link/1]).

-export([]).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a fd_linker instance
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    start_link(InstanceId, []).

start_link(InstanceId,[Module,Param]) ->
   gen_component:start_link(?MODULE,Module, [{Param},{register, InstanceId,fd_linker}]).

init(Module) ->
    
    log:log(info,"[ fd_linker ~p ] starting Node", [self()]),
    {Module}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


on({'EXIT', Pid, _Reason},{Module}) ->
        remove_subscriber(Pid,Module),
        {nostate};
on({link, Pid},{Module}) ->
        link(Pid),
        {nostate};
on({unlink, Pid},{Module}) ->
        unlink(Pid),
        {nostate};
on(_, _State) ->
    unknown_event.
% @private

get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id),fd_linker).


remove_subscriber(Pid,Module) ->
 cs_send:send_local(Module , {remove_subscriber, Pid}).


