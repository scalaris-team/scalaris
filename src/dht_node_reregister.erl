%  @copyright 2007-2011 Zuse Institute Berlin

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
-export([init/1, on_active/2, on_inactive/2,
         activate/0, deactivate/0,
         get_base_interval/0]).

-type(message() ::
    {register} |
    {web_debug_info, Requestor::comm:erl_local_pid()}).

-type state_active() :: trigger:state().
-type state_inactive() :: {inactive, trigger:state()} .

%% @doc Activates the re-register process. If not activated, it will
%%      queue most messages without processing them.
-spec activate() -> ok.
activate() ->
    Pid = pid_groups:get_my(dht_node_reregister),
    comm:send_local(Pid, {activate_reregister}).

%% @doc Deactivates the re-register process.
-spec deactivate() -> ok.
deactivate() ->
    Pid = pid_groups:get_my(dht_node_reregister),
    comm:send_local(Pid, {deactivate_reregister}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a re-register process, registers it with the process
%%      dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = config:read(dht_node_reregister_trigger),
    gen_component:start_link(?MODULE, Trigger,
                             [{pid_groups_join_as, DHTNodeGroup, dht_node_reregister}]).

%% @doc Initialises the module with an uninitialized state.
-spec init(module()) -> {'$gen_component', [{on_handler, Handler::on_inactive}], State::state_inactive()}.
init(Trigger) ->
    TriggerState = trigger:init(Trigger, fun get_base_interval/0, register),
    gen_component:change_handler({inactive, TriggerState}, on_inactive).
      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on_inactive(message(), state_inactive()) -> state_inactive();
                 ({activate_reregister}, state_inactive()) -> {'$gen_component', [{on_handler, Handler::on_active}], State::state_active()}.
on_inactive({activate_reregister}, {inactive, TriggerState}) ->
    log:log(info, "[ Reregister ~.0p ] activating...~n", [comm:this()]),
    NewTriggerState = trigger:now(TriggerState),
    gen_component:change_handler(NewTriggerState, on_active);

on_inactive({web_debug_info, Requestor}, State) ->
    KeyValueList = [{"", ""}, {"inactive re-register process", ""}],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

on_inactive(_Msg, {inactive, _TriggerState} = State) ->
    State.

-spec on_active(message(), state_active()) -> state_active();
         ({deactivate_reregister}, state_active()) -> {'$gen_component', [{on_handler, Handler::on_inactive}], State::state_inactive()}.
on_active({deactivate_reregister}, TriggerState)  ->
    log:log(info, "[ Reregister ~.0p ] deactivating...~n", [comm:this()]),
    gen_component:change_handler({inactive, TriggerState}, on_inactive);

on_active({register}, TriggerState) ->
    RegisterMessage = {register, get_dht_node_this()},
    _ = case config:read(register_hosts) of
            failed -> comm:send(mgmtServer(), RegisterMessage);
            Hosts  -> [comm:send(Host, RegisterMessage) || Host <- Hosts]
        end,
    NewTriggerState = trigger:next(TriggerState),
    NewTriggerState;

on_active({web_debug_info, Requestor}, TriggerState) ->
    KeyValueList =
        case config:read(register_hosts) of
            failed -> [{"Hosts (boot):", lists:flatten(io_lib:format("~.0p", [mgmtServer()]))}];
            Hosts  -> [{"Hosts:", ""} |
                           [{"", lists:flatten(io_lib:format("~.0p", [Host]))} || Host <- Hosts]]
        end,
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    TriggerState.

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
-spec mgmtServer() -> comm:mypid().
mgmtServer() ->
    config:read(mgmt_server).
