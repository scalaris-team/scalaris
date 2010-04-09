%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%% File    dht_node_reregister.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Re-register with boot nodes
%%% @end
%%% Created : 11 Oct 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(dht_node_reregister).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behavior(gen_component).

-include("../include/scalaris.hrl").

-export([start_link/1]).
-export([init/1, on/2, get_base_interval/0]).

-type(message() ::
    {go} |
    {trigger}).

-type(state() :: {init | uninit, trigger:state()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a Dead Node Cache process, registers it with the process
%%      dictionary and returns its pid for use by a supervisor.
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    Trigger = config:read(dht_node_reregister_trigger),
    gen_component:start_link(?MODULE, Trigger, [{register, InstanceId, dht_node_reregister}]).

%% @doc Initialises the module with an uninitialized state.
-spec init(module()) -> {uninit, trigger:state()}.
init(Trigger) ->
    log:log(info,"[ DNC ~p ] starting Dead Node Cache", [cs_send:this()]),
    TriggerState = trigger:init(Trigger, ?MODULE),
    {uninit, TriggerState}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state() | unknown_event.
on({go}, {uninit, TriggerState}) ->
    NewTriggerState = trigger:first(TriggerState),
    {init, NewTriggerState};

on(_, {uninit, _TriggerState} = State) ->
    State;

on({trigger}, {init, TriggerState}) ->
    trigger_reregister(),
    NewTriggerState = trigger:next(TriggerState),
    {init, NewTriggerState};

on({go}, {init, TriggerState}) ->
    trigger_reregister(),
    NewTriggerState = trigger:next(TriggerState),
    {init, NewTriggerState};

on(_, _State) ->
    unknown_event.

trigger_reregister() ->
    RegisterMessage = {register, get_dht_node_this()},
    reregister(config:read(register_hosts), RegisterMessage).

reregister(failed, Message)->
    cs_send:send(bootPid(), Message);
reregister(Hosts, Message) ->
    lists:foreach(
      fun(Host) -> cs_send:send(Host, Message) end,
      Hosts).

%% @doc Gets the zombie detector interval set in scalaris.cfg.
get_base_interval() ->
    config:read(reregister_interval).

%% @doc Gets the pid of the dht_node process in the same group as the calling
%%      process.
-spec get_dht_node_this() -> cs_send:mypid().
get_dht_node_this() ->
    cs_send:make_global(process_dictionary:get_group_member(dht_node)).

%% @doc pid of the boot daemon
-spec bootPid() -> cs_send:mypid().
bootPid() ->
    config:read(boot_host).
