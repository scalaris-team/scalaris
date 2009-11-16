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
%%% File    : dn_cache.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : Dead node Cache
%%%
%%% Created :  12 Jan 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum f�r Informationstechnik Berlin
%% @version $Id$
-module(dn_cache,[Trigger]).

-author('hennig@zib.de').
-vsn('$Id$ ').

-export([init/1,on/2, get_base_interval/0]).
-behavior(gen_component).

-export([start_link/1]).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a Dead Node Cache
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    start_link(InstanceId, []).

start_link(InstanceId,Options) ->
   gen_component:start_link(?MODULE:new(Trigger), [InstanceId, Options], [{register, InstanceId, dn_cache}]).




add_zombie_candidate(Node) ->
    cs_send:send_local(get_pid() , {add_zombie_candidate, Node}).

subscribe() ->
    cs_send:send_local(get_pid() , {subscribe, self()}).

unsubscribe() ->
    cs_send:send_local(get_pid() , {unsubscribe, self()}).

      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_ARG) ->
    TriggerState = Trigger:init(?MODULE:new(Trigger)),
    TriggerState2 = Trigger:trigger_first(TriggerState,1),
    log:log(info,"[ DNC ~p ] starting Dead Node Cache", [self()]),
	{fix_queue:new(config:read(zombieDetectorSize)),gb_sets:new(),TriggerState2}.

% @doc the Token takes care, that there is only one timermessage for stabilize 

on({trigger},{Queue,Subscriber,TriggerState}) ->
        fix_queue:map(fun (X) -> cs_send:send(node:pidX(X),{ping,cs_send:this(),X}) end,Queue), 
        NewTriggerState = Trigger:trigger_next(TriggerState,1),
        {Queue,Subscriber,NewTriggerState};
on({pong,Zombie},{Queue,Subscriber,TriggerState}) ->
        gb_sets:fold(fun (X,_) -> cs_send:send_local(X , {zombie,Zombie}) end,0, Subscriber),
        {Queue,Subscriber,TriggerState};
on({add_zombie_candidate, Node},{Queue,Subscriber,TriggerState}) ->
		{fix_queue:add(Node,Queue),Subscriber,TriggerState};
on({subscribe,Node},{Queue,Subscriber,TriggerState}) ->
		{Queue,gb_sets:insert(Node,Subscriber),TriggerState};
on({unsubscribe,Node},{Queue,Subscriber,TriggerState}) ->
		{Queue,gb_sets:del_element(Node,Subscriber),TriggerState};
on(_, _State) ->
    unknown_event.
% @private
get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), dn_cache).


get_base_interval() ->
    config:read(zombieDetectorInterval).