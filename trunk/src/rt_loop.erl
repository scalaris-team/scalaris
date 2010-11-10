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
-export([init/1, on_inactive/2, on_active/2,
         activate/1, deactivate/0,
         get_base_interval/0, check_config/0,
         get_id/1, get_pred/1, get_succ/1, get_neighb/1, get_rt/1, set_rt/2,
         rm_send_update/4]).

-ifdef(with_export_type_support).
-export_type([state_active/0]).
-endif.

% state of the routing table loop
%% userdevguide-begin rt_loop:state
-opaque(state_active() :: {NeighbTable  :: tid(),
                           RTState      :: ?RT:rt(),
                           TriggerState :: trigger:state()}).
-type(state_inactive() :: {inactive,
                           MessageQueue::msg_queue:msg_queue(),
                           TriggerState::trigger:state()}).
%% -type(state() :: state_active() | state_inactive()).
%% userdevguide-end rt_loop:state

% accepted messages of rt_loop processes
-type(message() ::
    {trigger_rt} |
    {update_rt, OldNeighbors::nodelist:neighborhood()} |
    {crash, DeadPid::comm:mypid()} |
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    {dump, Pid::comm:erl_local_pid()} |
    ?RT:custom_message()).

%% @doc Activates the routing table process. If not activated, it will
%%      queue most messages without processing them.
%%      Pre: ring_maintenance must be up and running
-spec activate(NeighbTable::tid()) -> ok.
activate(NeighbTable) ->
    Pid = pid_groups:get_my(routing_table),
    comm:send_local(Pid, {activate_rt, NeighbTable}).

%% @doc Deactivates the re-register process.
-spec deactivate() -> ok.
deactivate() ->
    Pid = pid_groups:get_my(routing_table),
    comm:send_local(Pid, {deactivate_rt}).

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
-spec init(module()) -> {'$gen_component', [{on_handler, Handler::on_inactive}], State::state_inactive()}.
init(Trigger) ->
    TriggerState = trigger:init(Trigger, fun get_base_interval/0, trigger_rt),
    gen_component:change_handler({inactive, msg_queue:new(), TriggerState},
                                 on_inactive).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler during start up phase (will change to on_active/2 when a
%%      'activate_rt' message is received).
-spec on_inactive(message(), state_inactive()) -> state_inactive();
                 ({activate_rt, NeighbTable::tid()}, state_inactive())
                    -> {'$gen_component', [{on_handler, Handler::on_active}], State::state_active()}.
on_inactive({activate_rt, NeighbTable}, {inactive, QueuedMessages, TriggerState}) ->
    log:log(info, "[ RT ~.0p ] activating...~n", [comm:this()]),
    TriggerState2 = trigger:next(TriggerState),
    rm_loop:subscribe(self(), rt_loop,
                      fun rm_loop:subscribe_dneighbor_change_filter/2,
                      fun rt_loop:rm_send_update/4),
    msg_queue:send(QueuedMessages),
    gen_component:change_handler(
      {NeighbTable, ?RT:empty(rm_loop:get_neighbors(NeighbTable)), TriggerState2}, on_active);

on_inactive({web_debug_info, Requestor}, {inactive, QueuedMessages, _TriggerState} = State) ->
    % get a list of up to 50 queued messages to display:
    MessageListTmp = [{"", lists:flatten(io_lib:format("~p", [Message]))}
                  || Message <- lists:sublist(QueuedMessages, 50)],
    MessageList = case length(QueuedMessages) > 50 of
                      true -> lists:append(MessageListTmp, [{"...", ""}]);
                      _    -> MessageListTmp
                  end,
    KeyValueList = [{"", ""}, {"inactive RT process", ""}, {"queued messages:", ""} | MessageList],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

on_inactive(_Msg, State) ->
    State.

%% @doc Message handler when the module is fully initialized.
-spec on_active(message(), state_active()) -> state_active() | unknown_event;
               ({deactivate_rt}, state_active()) -> {'$gen_component', [{on_handler, Handler::on_inactive}], State::state_inactive()}.
on_active({deactivate_rt}, {NeighbTable, _OldRT, TriggerState})  ->
    log:log(info, "[ RT ~.0p ] deactivating...~n", [comm:this()]),
    rm_loop:unsubscribe(self(), rt_loop),
    % send new empty RT to the dht_node so that all routing messages
    % must be passed to the successor: 
    comm:send_local(pid_groups:get_my(dht_node),
                    {rt_update, ?RT:empty_ext(rm_loop:get_neighbors(NeighbTable))}),
    gen_component:change_handler({inactive, msg_queue:new(), TriggerState},
                                 on_inactive);

%% userdevguide-begin rt_loop:update_rt
% update routing table with changed ID, pred and/or succ
on_active({update_rt, OldNeighbors}, {NeighbTable, OldRT, TriggerState}) ->
    NewNeighbors = rm_loop:get_neighbors(NeighbTable),
    case ?RT:update(OldRT, OldNeighbors, NewNeighbors) of
        {trigger_rebuild, NewRT} ->
            % trigger immediate rebuild
            NewTriggerState = trigger:now(TriggerState),
            ?RT:check(OldRT, NewRT, OldNeighbors, NewNeighbors, true),
            new_state(NeighbTable, NewRT, NewTriggerState);
        {ok, NewRT} ->
            ?RT:check(OldRT, NewRT, OldNeighbors, NewNeighbors, true),
            new_state(NeighbTable, NewRT, TriggerState)
    end;
%% userdevguide-end rt_loop:update_rt

%% userdevguide-begin rt_loop:trigger
on_active({trigger_rt}, {NeighbTable, OldRT, TriggerState}) ->
    % start periodic stabilization
    % log:log(debug, "[ RT ] stabilize"),
    Neighbors = rm_loop:get_neighbors(NeighbTable),
    NewRT = ?RT:init_stabilize(Neighbors, OldRT),
    ?RT:check(OldRT, NewRT, Neighbors, true),
    % trigger next stabilization
    NewTriggerState = trigger:next(TriggerState),
    new_state(NeighbTable, NewRT, NewTriggerState);
%% userdevguide-end rt_loop:trigger

% failure detector reported dead node
on_active({crash, DeadPid}, {NeighbTable, OldRT, TriggerState}) ->
    NewRT = ?RT:filter_dead_node(OldRT, DeadPid),
    ?RT:check(OldRT, NewRT, rm_loop:get_neighbors(NeighbTable), false),
    new_state(NeighbTable, NewRT, TriggerState);

% debug_info for web interface
on_active({web_debug_info, Requestor},
   {_Id, _Pred, _Succ, RTState, _TriggerState} = State) ->
    KeyValueList =
        [{"rt_size", ?RT:get_size(RTState)},
         {"rt (index, node):", ""} | ?RT:dump(RTState)],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

on_active({dump, Pid}, {_NeighbTable, RTState, _TriggerState} = State) ->
    comm:send_local(Pid, {dump_response, RTState}),
    State;

% unknown message
on_active(Message, State) ->
    ?RT:handle_custom_message(Message, State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% rt_loop:state_active() handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% handling rt_loop's (opaque) state - these handlers should at least be used
% outside this module:

-spec new_state(NeighbTable::tid(), RTState::?RT:rt(),
                TriggerState::trigger:state()) -> state_active().
new_state(NeighbTable, RT, TriggerState) ->
    {NeighbTable, RT, TriggerState}.

-spec get_id(State::state_active()) -> ?RT:key().
get_id({NeighbTable, _RT, _TriggerState}) ->
    nodelist:nodeid(rm_loop:get_neighbors(NeighbTable)).

-spec get_pred(State::state_active()) -> node:node_type().
get_pred({NeighbTable, _RT, _TriggerState}) ->
    nodelist:pred(rm_loop:get_neighbors(NeighbTable)).

-spec get_succ(State::state_active()) -> node:node_type().
get_succ({NeighbTable, _RT, _TriggerState}) ->
    nodelist:succ(rm_loop:get_neighbors(NeighbTable)).

-spec get_neighb(State::state_active()) -> nodelist:neighborhood().
get_neighb({NeighbTable, _RT, _TriggerState}) ->
    rm_loop:get_neighbors(NeighbTable).

-spec get_rt(State::state_active()) -> ?RT:rt().
get_rt({_NeighbTable, RT, _TriggerState}) -> RT.

-spec set_rt(State::state_active(), RT::?RT:rt()) -> NewState::state_active().
set_rt({NeighbTable, _OldRT, TriggerState}, NewRT) ->
    {NeighbTable, NewRT, TriggerState}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Misc.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Notifies the node's routing table of a changed node ID, predecessor
%%      and/or successor. Used to subscribe to the ring maintenance. 
-spec rm_send_update(Subscriber::pid(), Tag::rt_loop,
                     OldNeighbors::nodelist:neighborhood(),
                     NewNeighbors::nodelist:neighborhood()) -> ok.
rm_send_update(Pid, rt_loop, OldNeighbors, _NewNeighbors) ->
    comm:send_local(Pid, {update_rt, OldNeighbors}).

-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(pointer_base_stabilization_interval).

%% @doc Checks whether config parameters of the rt_loop process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(pointer_base_stabilization_interval) and
        config:is_greater_than_equal(pointer_base_stabilization_interval, 1000).
