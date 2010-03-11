%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : cs_reregister.erl
%%% Author  : Thorsten Schuett <schuett@csr-pc11.zib.de>
%%% Description : reregister with boot nodes
%%%
%%% Created : 11 Oct 2007 by Thorsten Schuett <schuett@csr-pc11.zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(cs_reregister,[Trigger]).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([start_link/1,init/1,on/2, get_base_interval/0]).
-behavior(gen_component).



start_link(InstanceId) ->
   gen_component:start_link(?MODULE:new(Trigger), [], [{register, InstanceId, cs_reregister}]).

init(_Args_) ->
    uninit.

on({go},uninit) ->
    TriggerState = Trigger:init(?MODULE:new(Trigger)),
    TriggerState2 = Trigger:trigger_first(TriggerState,1),
    TriggerState2;
on(_,uninit) ->
    uninit;
on({trigger},TriggerState) ->
    trigger_reregister(),
    Trigger:trigger_next(TriggerState,1);
on({go},TriggerState) ->
    trigger_reregister(),
    Trigger:trigger_next(TriggerState,1);
on(_, _State) ->
    unknown_event.

trigger_reregister() ->
    RegisterMessage = {register,get_cs_node_this()},
    reregister(config:register_hosts(), RegisterMessage).

reregister(failed, Message)->
    cs_send:send(config:bootPid(), Message);
reregister(Hosts, Message) ->
    lists:foreach(fun (Host) -> 
			  cs_send:send(Host, Message)
		  end, 
		  Hosts).


get_base_interval() ->
    config:read(reregister_interval).


get_cs_node_this() ->
    cs_send:make_global(process_dictionary:get_group_member(cs_node)).
