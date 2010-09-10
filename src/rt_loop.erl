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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    routing table process
%%% @end
%% @version $Id$
-module(rt_loop).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

% for routing table implementation
-export([start_link/1]).
-export([init/1, on_startup/2, on/2, get_base_interval/0, check_config/0,
         get_id/1, get_pred/1, get_succ/1, get_rt/1, set_rt/2,
         activate/3]).

-ifdef(with_export_type_support).
-export_type([state_init/0]).
-endif.

% state of the routing table loop
%% userdevguide-begin rt_loop:state
-opaque(state_init() :: {Id           :: ?RT:key(),
                         Pred         :: node:node_type(),
                         Succ         :: node:node_type(),
                         RTState      :: ?RT:rt(),
                         TriggerState :: trigger:state()}).
-type(state_uninit() :: {uninit, MessageQueue::msg_queue:msg_queue(),
                         TriggerState::trigger:state()}).
%% -type(state() :: state_init() | state_uninit()).
%% userdevguide-end rt_loop:state

% accepted messages of rt_loop processes
-type(message() ::
    {update, Id::?RT:key(), Pred::node:node_type(), Succ::node:node_type()} |
    {stabilize} |
    {crash, DeadPid::comm:mypid()} |
    ?RT:custom_message()).

%% @doc Sends an initialization message to the node's routing table.
-spec activate(Id::?RT:key(), Pred::node:node_type(), Succ::node:node_type()) -> ok.
activate(Id, Pred, Succ) ->
    Pid = pid_groups:get_my(routing_table),
    comm:send_local(Pid, {init_rt, Id, Pred, Succ}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the routing tabe maintenance process, registers it with the
%%      process dictionary and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    Trigger = config:read(routingtable_trigger),
    gen_component:start_link(?MODULE, Trigger, [{pid_groups_join_as, DHTNodeGroup, routing_table}]).

%% @doc Initialises the module with an empty state.
-spec init(module()) -> {'$gen_component', [{on_handler, Handler::on_startup}], State::state_uninit()}.
init(Trigger) ->
    TriggerState = trigger:init(Trigger, ?MODULE),
    gen_component:change_handler({uninit, msg_queue:new(), TriggerState},
                                 on_startup).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler during start up phase (will change to on/2 when a
%%      'init_rt' message is received).
-spec on_startup(message(), state_uninit()) -> state_uninit();
                ({init_rt, Id::?RT:key(), Pred::node:node_type(),
                  Succ::node:node_type()}, state_uninit()) -> {'$gen_component', [{on_handler, Handler::on}], State::state_init()}.
on_startup({init_rt, Id, Pred, Succ}, {uninit, QueuedMessages, TriggerState}) ->
    TriggerState2 = trigger:next(TriggerState),
    rm_loop:subscribe(self(), fun rm_loop:subscribe_dneighbor_change_filter/2,
                      fun rm_send_update/3),
    msg_queue:send(QueuedMessages),
    gen_component:change_handler(
      {Id, Pred, Succ, ?RT:empty(Succ), TriggerState2}, on);

on_startup(Message, {uninit, QueuedMessages, TriggerState}) ->
    {uninit, msg_queue:add(QueuedMessages, Message), TriggerState}.

%% @doc Message handler when the module is fully initialized.
-spec on(message(), state_init()) -> state_init() | unknown_event.
%% userdevguide-begin rt_loop:update_rt
% update routing table with changed ID, pred and/or succ
on({update_rt, NewId, NewPred, NewSucc},
   {OldId, OldPred, OldSucc, OldRT, TriggerState}) ->
    case ?RT:update(NewId, NewPred, NewSucc, OldRT, OldId, OldPred, OldSucc) of
        {trigger_rebuild, NewRT} ->
            % trigger immediate rebuild
            NewTriggerState = trigger:now(TriggerState),
            ?RT:check(OldRT, NewRT, NewId, OldPred, NewPred, OldSucc, NewSucc),
            new_state(NewId, NewPred, NewSucc, NewRT, NewTriggerState);
        {ok, NewRT} ->
            ?RT:check(OldRT, NewRT, NewId, OldPred, NewPred, OldSucc, NewSucc),
            new_state(NewId, NewPred, NewSucc, NewRT, TriggerState)
    end;
%% userdevguide-end rt_loop:update_rt

%% userdevguide-begin rt_loop:trigger
on({trigger}, {Id, Pred, Succ, RTState, TriggerState}) ->
    % start periodic stabilization
    % log:log(debug, "[ RT ] stabilize"),
    NewRTState = ?RT:init_stabilize(Id, Succ, RTState),
    ?RT:check(RTState, NewRTState, Id, Pred, Succ),
    % trigger next stabilization
    NewTriggerState = trigger:next(TriggerState),
    new_state(Id, Pred, Succ, NewRTState, NewTriggerState);
%% userdevguide-end rt_loop:trigger

% failure detector reported dead node
on({crash, DeadPid}, {Id, Pred, Succ, OldRT, TriggerState}) ->
    NewRT = ?RT:filter_dead_node(OldRT, DeadPid),
    ?RT:check(OldRT, NewRT, Id, Pred, Succ, false),
    new_state(Id, Pred, Succ, NewRT, TriggerState);

% debug_info for web interface
on({web_debug_info, Requestor},
   {_Id, _Pred, _Succ, RTState, _TriggerState} = State) ->
    KeyValueList =
        [{"rt_size", ?RT:get_size(RTState)},
         {"rt (index, node):", ""} | ?RT:dump(RTState)],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

on({dump, Pid}, {_Id, _Pred, _Succ, RTState, _TriggerState} = State) ->
    comm:send_local(Pid, {dump_response, RTState}),
    State;

% unknown message
on(Message, State) ->
    ?RT:handle_custom_message(Message, State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% rt_loop:state_init() handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% handling rt_loop's (opaque) state - these handlers should at least be used
% outside this module:

-spec new_state(Id::?RT:key(), Pred::node:node_type(), Succ::node:node_type(),
                 RTState::?RT:rt(), TriggerState::trigger:state()) -> state_init().
new_state(Id, Pred, Succ, RTState, TriggerState) ->
    {Id, Pred, Succ, RTState, TriggerState}.

-spec get_id(State::state_init()) -> ?RT:key().
get_id(State) -> element(1, State).

-spec get_pred(State::state_init()) -> node:node_type().
get_pred(State) -> element(2, State).

-spec get_succ(State::state_init()) -> node:node_type().
get_succ(State) -> element(3, State).

-spec get_rt(State::state_init()) -> ?RT:rt().
get_rt(State) -> element(4, State).

-spec set_rt(State::state_init(), RT::?RT:rt()) -> NewState::state_init().
set_rt(State, RT) -> setelement(4, State, RT).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Misc.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Notifies the node's routing table of a changed node ID, predecessor
%%      and/or successor. Used to subscribe to the ring maintenance. 
-spec rm_send_update(Subscriber::comm:erl_local_pid(),
                     OldNeighbors::nodelist:neighborhood(),
                     NewNeighbors::nodelist:neighborhood()) -> ok.
rm_send_update(Pid, _OldNeighbors, NewNeighbors) ->
    NewId = nodelist:nodeid(NewNeighbors),
    NewPred = nodelist:pred(NewNeighbors),
    NewSucc = nodelist:succ(NewNeighbors),
    comm:send_local(Pid, {update_rt, NewId, NewPred, NewSucc}).

-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(pointer_base_stabilization_interval).

%% @doc Checks whether config parameters of the rt_loop process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(pointer_base_stabilization_interval) and
        config:is_greater_than_equal(pointer_base_stabilization_interval, 1000).
