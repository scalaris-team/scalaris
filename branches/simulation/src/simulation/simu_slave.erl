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
%%% File    : simu_slave.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : 
%%%
%%% Created :  12 Jan 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin

-module(simu_slave).

-author('hennig@zib.de').



-behavior(gen_component).
-export([init/1,on/2,start/0]).





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


start() ->
   gen_component:start_link(?MODULE, [], [{register_native, simu_slave}]).

init(_ARG) ->
    cs_send:send_after(5000, self(), {addnodes}),
    cs_send:send_after(1000*60*60, self(), {simu_stop}),
    {initState}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


on({addnodes},{initState}) ->
    admin:add_nodes(100),
    {state_1}; 
on({simu_stop},{state_1}) ->
    
    halt(0),
    {kill};
on(_, _State) ->
    unknown_event.
% @private

get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id),simu_slave).
