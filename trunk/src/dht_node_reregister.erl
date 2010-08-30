%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Re-register with boot nodes
%% @end
%% @version $Id$
-module(dht_node_reregister).

-author('schuett@zib.de').
-vsn('$Id$').

-behavior(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).
-export([init/1, on/2, get_base_interval/0]).

-type(message() :: {register}).

-type(state() :: {init | uninit, trigger:state()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a Dead Node Cache process, registers it with the process
%%      dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = config:read(dht_node_reregister_trigger),
    gen_component:start_link(?MODULE, Trigger,
                             [{pid_groups_join_as, DHTNodeGroup, dht_node_reregister}]).

%% @doc Initialises the module with an uninitialized state.
-spec init(module()) -> {uninit, trigger:state()}.
init(Trigger) ->
    TriggerState = trigger:init(Trigger, fun get_base_interval/0, register),
    {uninit, TriggerState}.
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(message(), state()) -> state().
on({register}, {uninit, TriggerState}) ->
    NewTriggerState = trigger:now(TriggerState),
    {init, NewTriggerState};

on(_, {uninit, _TriggerState} = State) ->
    State;

on({register}, {init, TriggerState}) ->
    trigger_reregister(),
    NewTriggerState = trigger:next(TriggerState),
    {init, NewTriggerState}.

-spec trigger_reregister() -> ok.
trigger_reregister() ->
    RegisterMessage = {register, get_dht_node_this()},
    reregister(config:read(register_hosts), RegisterMessage).

-spec reregister(failed | [comm:mypid()],
                 Message::{register, ThisNode::comm:mypid()}) -> ok.
reregister(failed, Message)->
    comm:send(bootPid(), Message);
reregister(Hosts, Message) ->
    lists:foreach(fun(Host) -> comm:send(Host, Message) end, Hosts),
    ok.

%% @doc Gets the interval to trigger re-registering the node set in scalaris.cfg.
-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(reregister_interval).

%% @doc Gets the pid of the dht_node process in the same group as the calling
%%      process.
-spec get_dht_node_this() -> comm:mypid().
get_dht_node_this() ->
    comm:make_global(pid_groups:get_my(dht_node)).

%% @doc pid of the boot daemon
-spec bootPid() -> comm:mypid().
bootPid() ->
    config:read(boot_host).
