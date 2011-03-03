%  @copyright 2011 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    A dht_node mockup that can ignore some messages or process them
%%         differently compared to dht_node.
%% @end
%% @version $Id$
-module(mockup_dht_node).
-author('kruber@zib.de').
-vsn('$Id$ ').

-include("scalaris.hrl").

-behaviour(gen_component).

-export([start_link/2, on/2, init/1,
         is_alive/1]).

-type message() ::
        comm:message() |
        {mockup_dht_node, add_match_specs, DropSpecs::[mockup:match_spec()]} |
        {mockup_dht_node, clear_match_specs}.
-type module_state() ::
        dht_node_join:join_state() | dht_node_state:state() |
        {'$gen_component', [{on_handler, Handler::on}], dht_node_state:state()} | 
        {'$gen_component', [{on_handler, Handler::on_join}], dht_node_join:join_state()}.
% note: must have first element 'state' to work for dht_node:is_alive/1
-type state() :: {state, Module::module(), Handler::atom(), ModuleState::module_state(), MsgDropSpecs::[mockup:match_spec()]}.

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p~n", [self(), Msg, State])).

-spec on(message(), state()) -> state().
on(_Msg = {mockup_dht_node, add_match_specs, DropSpecs},
    _State = {state, Module, Handler, ModuleState, MsgDropSpecs}) ->
    ?TRACE1(_Msg, _State),
    {state, Module, Handler, ModuleState, lists:append(MsgDropSpecs, DropSpecs)};
on(_Msg = {mockup_dht_node, clear_match_specs},
    _State = {state, Module, Handler, ModuleState, _MsgDropSpecs}) ->
    ?TRACE1(_Msg, _State),
    {state, Module, Handler, ModuleState, []};
on(Msg, State = {state, Module, Handler, ModuleState, MsgDropSpecs}) ->
%%     ?TRACE1(Msg, State),
    case mockup:match_any(Msg, MsgDropSpecs) of
        {true, {_Head, _Conditions, _Count, drop_msg}, NewMatchSpecs} ->
            ?TRACE("[ ~.0p ] ignoring ~.0p~n", [self(), Msg]),
            {state, Module, Handler, ModuleState, NewMatchSpecs};
        {true, {_Head, _Conditions, _Count, ActionFun}, NewMatchSpecs}
          when is_function(ActionFun) ->
            ?TRACE("[ ~.0p ] calling ~.0p for message ~.0p~n", [self(), ActionFun, Msg]),
            NewModuleState = ActionFun(Msg, ModuleState),
            module_state_to_my_state(NewModuleState, {state, Module, Handler, ModuleState, NewMatchSpecs});
        false ->
            NewModuleState = Module:Handler(Msg, ModuleState),
            module_state_to_my_state(NewModuleState, State)
    end.

-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(?MODULE, {DHTNodeGroup, Options},
                             [{pid_groups_join_as, DHTNodeGroup, dht_node}, wait_for_init]).

-spec init({DHTNodeGroup::pid_groups:groupname(), Options::[tuple()]}) -> state().
init({DHTNodeGroup, Options}) ->
    % at first, join pid_groups - allow dht_node to overwrite my_pid (it will join as dht_node!):
    pid_groups:join_as(DHTNodeGroup, mockup_dht_node),
    ModuleState = dht_node:init(Options),
    module_state_to_my_state(ModuleState, {state, dht_node, on, ModuleState, []}).

-spec module_state_to_my_state(module_state(), state()) -> state().
module_state_to_my_state(ModuleState, {state, Module, OldHandler, _, NewMatchSpecs}) ->
    Handler = case ModuleState of
                  {'$gen_component', Config, ModuleRealState} ->
                      {on_handler, NewHandler} =
                          lists:keyfind(on_handler, 1, Config),
                      NewHandler;
                  ModuleRealState ->
                      OldHandler
              end,
    {state, Module, Handler, ModuleRealState, NewMatchSpecs}.

-spec is_alive(Pid::pid()) -> boolean().
is_alive(Pid) ->
    case gen_component:get_state(Pid) of
        {state, _Module, _Handler, ModuleState, _MsgDropSpecs}
          when element(1, ModuleState) =:= state -> true;
        _ -> false
    end.
