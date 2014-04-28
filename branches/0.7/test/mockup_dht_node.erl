%  @copyright 2011-2014 Zuse Institute Berlin

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
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(gen_component).

-export([start_link/2, on/2, init/1,
         is_alive/1, is_alive_no_slide/1, is_alive_fully_joined/1]).

-type message() ::
        comm:message() |
        {mockup_dht_node, add_match_specs, DropSpecs::[mockup:match_spec()]} |
        {mockup_dht_node, clear_match_specs}.
-type module_state() ::
        dht_node_join:join_state() | dht_node_state:state() |
        {'$gen_component', [{on_handler, Handler::gen_component:handler()}], dht_node_state:state() | dht_node_join:join_state()}.
-type state() :: {Module::module(), Handler::gen_component:handler(), ModuleState::module_state(), MsgDropSpecs::[mockup:match_spec()]}.

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p~n", [self(), Msg, State])).

-spec on(message(), state()) -> state().
on(_Msg = {mockup_dht_node, add_match_specs, DropSpecs},
    _State = {Module, Handler, ModuleState, MsgDropSpecs}) ->
    ?TRACE1(_Msg, _State),
    {Module, Handler, ModuleState, lists:append(MsgDropSpecs, DropSpecs)};
on(_Msg = {mockup_dht_node, clear_match_specs},
    _State = {Module, Handler, ModuleState, _MsgDropSpecs}) ->
    ?TRACE1(_Msg, _State),
    {Module, Handler, ModuleState, []};
on(Msg, State = {Module, Handler, ModuleState, MsgDropSpecs}) ->
%%     ?TRACE1(Msg, State),
    case mockup:match_any(Msg, MsgDropSpecs) of
        {true, {_Head, _Conditions, _Count, drop_msg}, NewMatchSpecs} ->
            ?TRACE("[ ~.0p ] ignoring ~.0p~n", [self(), Msg]),
            {Module, Handler, ModuleState, NewMatchSpecs};
        {true, {_Head, _Conditions, _Count, ActionFun}, NewMatchSpecs}
          when is_function(ActionFun) ->
            ?TRACE("[ ~.0p ] calling ~.0p for message ~.0p~n", [self(), ActionFun, Msg]),
            NewModuleState = ActionFun(Msg, ModuleState),
            module_state_to_my_state(NewModuleState, {Module, Handler, ModuleState, NewMatchSpecs});
        false ->
            NewModuleState = Handler(Msg, ModuleState),
            module_state_to_my_state(NewModuleState, State)
    end.

-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, Options,
                             [{pid_groups_join_as, DHTNodeGroup, dht_node}, {wait_for_init}]).

-spec init(Options::[tuple()]) -> state().
init(Options) ->
    case lists:keytake(match_specs, 1, Options) of
        {value, {match_specs, MatchSpecs}, RestOptions} ->
            ok;
        false -> RestOptions = Options,
                 MatchSpecs = [],
                 ok
    end,
    ModuleState = dht_node:init(RestOptions),
    module_state_to_my_state(ModuleState, {dht_node, fun dht_node:on/2, ModuleState, MatchSpecs}).

-spec module_state_to_my_state(module_state(), state()) -> state().
module_state_to_my_state(ModuleState, {Module, OldHandler, _, NewMatchSpecs}) ->
    case ModuleState of
        {'$gen_component', Commands, ModuleRealState} ->
            case lists:keyfind(on_handler, 1, Commands) of
                {on_handler, NewHandler} ->
                    {Module, NewHandler, ModuleRealState, NewMatchSpecs};
                false ->
                    {'$gen_component', Commands,
                     {Module, OldHandler, ModuleRealState, NewMatchSpecs}}
            end;
        _ -> {Module, OldHandler, ModuleState, NewMatchSpecs}
    end.

-spec is_alive(State::state() | term()) -> boolean().
is_alive({_Module, _Handler, ModuleState, _MsgDropSpecs}) ->
    dht_node:is_alive(ModuleState);
is_alive(_) -> false.

-spec is_alive_no_slide(State::state() | term()) -> boolean().
is_alive_no_slide({_Module, _Handler, ModuleState, _MsgDropSpecs}) ->
    dht_node:is_alive_no_slide(ModuleState);
is_alive_no_slide(_) -> false.

-spec is_alive_fully_joined(State::state() | term()) -> boolean().
is_alive_fully_joined({_Module, _Handler, ModuleState, _MsgDropSpecs}) ->
    dht_node:is_alive_fully_joined(ModuleState);
is_alive_fully_joined(_) -> false.
