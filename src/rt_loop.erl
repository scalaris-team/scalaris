%  @copyright 2007-2014 Zuse Institute Berlin

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
%% @doc    routing table process
%% @end
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
         check_config/0,
         get_neighb/1, get_rt/1, set_rt/2, get_ert/1, set_ert/2,
         rm_neighbor_change/3, rm_send_update/5]).

-export_type([state_active/0]).

-ifdef(enable_debug).
% add source information to debug routing damaged messages
-define(HOPS_TO_DATA(Hops), {comm:this(), Hops}).
-define(HOPS_FROM_DATA(Data), element(2, Data)).
-type data() :: {Source::comm:mypid(), Hops::non_neg_integer()}.
-else.
-define(HOPS_TO_DATA(Hops), Hops).
-define(HOPS_FROM_DATA(Data), Data).
-type data() :: Hops::non_neg_integer().
-endif.

% state of the routing table loop
%% userdevguide-begin rt_loop:state
-opaque(state_active() :: {Neighbors    :: nodelist:neighborhood(),
                           RT           :: ?RT:rt(),
                           ERT          :: ?RT:external_rt()}).
-type(state_inactive() :: {inactive,
                           MessageQueue::msg_queue:msg_queue()}).
%% -type(state() :: state_active() | state_inactive()).
%% userdevguide-end rt_loop:state

% accepted messages of rt_loop processes
-type(message() ::
    {trigger_rt} | %% just for periodic wake up
    {periodic_rt_rebuild} | %% actually initiate a rt rebuild
    {update_rt, OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood()} |
    {fd_notify, fd:event(), DeadPid::comm:mypid(), Reason::fd:reason()} |
    {web_debug_info, Requestor::comm:erl_local_pid()} |
    {dump, Pid::comm:erl_local_pid()} |
    ?RT:custom_message()).

%% @doc Activates the routing table process. If not activated, it will
%%      queue most messages without processing them.
%%      Pre: dht_node must be up and running
-spec activate(Neighbors::nodelist:neighborhood()) -> ok.
activate(Neighbors) ->
    Pid = pid_groups:get_my(routing_table),
    comm:send_local(Pid, {activate_rt, Neighbors}).

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
    gen_component:start_link(?MODULE, fun ?MODULE:on_inactive/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, routing_table}]).

%% @doc Initialises the module with an empty state.
-spec init([]) -> state_inactive().
init([]) ->
    %% generate trigger msg only once and then keep it repeating
    msg_delay:send_trigger(get_base_interval(), {trigger_rt}),
    {inactive, msg_queue:new()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Message handler during start up phase (will change to on_active/2 when a
%%      'activate_rt' message is received).
-spec on_inactive(message(), state_inactive()) -> state_inactive();
                 ({activate_rt, Neighbors::nodelist:neighborhood()}, state_inactive())
                    -> {'$gen_component', [{on_handler, Handler::gen_component:handler()},...], State::state_active()}.
on_inactive({activate_rt, Neighbors}, {inactive, QueuedMessages}) ->
    log:log(info, "[ RT ~.0p ] activating...~n", [comm:this()]),
    comm:send_local(self(), {periodic_rt_rebuild}),
    rm_loop:subscribe(self(), ?MODULE,
                      fun ?MODULE:rm_neighbor_change/3,
                      fun ?MODULE:rm_send_update/5, inf),
    msg_queue:send(QueuedMessages),
    gen_component:change_handler(
      {Neighbors, ?RT:init(Neighbors), ?RT:empty_ext(Neighbors)},
      fun ?MODULE:on_active/2);

on_inactive({trigger_rt}, State) ->
    %% keep trigger active to avoid generating new triggers when
    %% frequently jumping between inactive and active state.
    msg_delay:send_trigger(get_base_interval(), {trigger_rt}),
    State;

on_inactive({web_debug_info, Requestor}, {inactive, QueuedMessages} = State) ->
    % get a list of up to 50 queued messages to display:
    MessageListTmp = [{"", webhelpers:safe_html_string("~p", [Message])}
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
-spec on_active(message(), state_active())
                  -> state_active() |
                         {'$gen_component', [{post_op, Msg::{periodic_rt_rebuild}},...], state_active()} |
                         unknown_event;
               ({deactivate_rt}, state_active())
                  -> {'$gen_component', [{on_handler, Handler::gen_component:handler()},...], State::state_inactive()}.
on_active({deactivate_rt}, {Neighbors, _OldRT, _ERT})  ->
    log:log(info, "[ RT ~.0p ] deactivating...~n", [comm:this()]),
    rm_loop:unsubscribe(self(), ?MODULE),
    % send new empty RT to the dht_node so that all routing messages
    % must be passed to the successor:
    comm:send_local(pid_groups:get_my(dht_node),
                    {rt_update, ?RT:empty_ext(Neighbors)}),
    gen_component:change_handler({inactive, msg_queue:new()},
                                 fun ?MODULE:on_inactive/2);

%% userdevguide-begin rt_loop:update_rt
% Update routing table with changed neighborhood from the rm
on_active({update_rt, OldNeighbors, NewNeighbors}, {_Neighbors, OldRT, OldERT}) ->
    case ?RT:update(OldRT, OldNeighbors, NewNeighbors) of
        {trigger_rebuild, NewRT} ->
            %% ?RT:check(OldRT, NewRT, OldNeighbors, NewNeighbors, true),
            NewERT = rt_chord:check_tmp(OldRT, NewRT, OldERT, OldNeighbors, NewNeighbors, true), % TODO replace _tmp and rt_chord
            % trigger immediate rebuild
            gen_component:post_op({periodic_rt_rebuild}, {NewNeighbors, NewRT, NewERT})
        ;
        {ok, NewRT} ->
            %% ?RT:check(OldRT, NewRT, OldNeighbors, NewNeighbors, true),
            NewERT = rt_chord:check_tmp(OldRT, NewRT, OldERT, OldNeighbors, NewNeighbors, true), % TODO replace _tmp and rt_chord
            {NewNeighbors, NewRT, NewERT}
    end;
%% userdevguide-end rt_loop:update_rt

%% userdevguide-begin rt_loop:trigger
% Message handler to manage the trigger
on_active({trigger_rt}, State) ->
    msg_delay:send_trigger(get_base_interval(), {trigger_rt}),
    gen_component:post_op({periodic_rt_rebuild}, State);

% Actual periodic rebuilding of the RT
on_active({periodic_rt_rebuild}, {Neighbors, OldRT, OldERT}) ->
    % start periodic stabilization
    % log:log(debug, "[ RT ] stabilize"),
    NewRT = ?RT:init_stabilize(Neighbors, OldRT),
    %% ?RT:check(OldRT, NewRT, Neighbors, true),
    NewERT = rt_chord:check_tmp(OldRT, NewRT, OldERT, Neighbors, true), % TODO replace _tmp and rt_chord
    {Neighbors, NewRT, NewERT};
%% userdevguide-end rt_loop:trigger

% failure detector reported dead node
on_active({fd_notify, crash, DeadPid, Reason}, {Neighbors, OldRT, OldERT}) ->
    NewRT = ?RT:filter_dead_node(OldRT, DeadPid, Reason),
    %% ?RT:check(OldRT, NewRT, Neighbors, false),
    NewERT = rt_chord:check_tmp(OldRT, NewRT, OldERT, Neighbors, false), % TODO replace _tmp and rt_chord
    {Neighbors, NewRT, NewERT};
on_active({fd_notify, _Event, _DeadPid, _Reason}, State) ->
    State;

% debug_info for web interface
on_active({web_debug_info, Requestor},
   {_Neighbors, RT, _ERT} = State) ->
    KeyValueList =
        [{"rt_size", ?RT:get_size(RT)},
         {"rt (index, node):", ""} | ?RT:dump(RT)],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;

on_active({dump, Pid}, {_Neighbors, RT, _ERT} = State) ->
    comm:send_local(Pid, {dump_response, RT}),
    State;

on_active({?lookup_aux, Key, Hops, Msg}, {Neighbors, _RT, ERT} = State) ->
    case config:read(leases) of
        true ->
            lookup_aux_leases(Neighbors, ERT, Key, Hops, Msg);
        _ ->
            lookup_aux_chord(Neighbors, ERT, Key, Hops, Msg)
    end,
    State;

on_active({send_error, _Target, {?send_to_group_member, routing_table, {?lookup_aux, Key, Hops, Msg}} = _Message, _Reason}, State) ->
    log:log(warn, "[routing_table] lookup_aux failed 1. Target: ~p. Msg: ~p.", [_Target, _Message]),
    _ = comm:send_local_after(100, self(), {?lookup_aux, Key, Hops + 1, Msg}),
    State;

on_active({send_error, _Target, {?lookup_aux, Key, Hops, Msg} = _Message, _Reason}, State) ->
    log:log(warn, "[routing_table] lookup_aux failed 2. Target: ~p~nMsg: ~.2p.", [_Target, _Message]),
    _ = comm:send_local_after(100, self(), {?lookup_aux, Key, Hops + 1, Msg}),
    State;

on_active({send_error, _Target, {?lookup_fin, Key, Data, Msg} = _Message, _Reason}, State) ->
    _ = comm:send_local_after(100, self(), {?lookup_aux, Key, ?HOPS_FROM_DATA(Data) + 1, Msg}),
    State;

% unknown message
on_active(Message, State) ->
    ?RT:handle_custom_message(Message, State).

-spec lookup_aux_chord(Neighbors::nodelist:neighborhood(), RT::?RT:external_rt(), Key::intervals:key(),
                       Hops::non_neg_integer(), Msg::comm:message()) -> ok.
lookup_aux_chord(Neighbors, ERT, Key, Hops, Msg) ->
    %% TODO : wrap_message expects a dht_node_state
    %% Noop in chord, simple
    %% frt_common: Neighbours, node_id, external_rt,
    %% WrappedMsg = ?RT:wrap_message(Key, Msg, State, Hops),
    % ==> change wrap_message/4: instead of State, use Neighbors and RT-State!
    WrappedMsg = Msg,
    % NOTE: chord-like routing requires routing through predecessor -> only decide at pred:
    case rt_chord:next_hop(Neighbors, ERT, Key) of
        succ ->
            %% TODO: do I need a WrappedMsg here ??!
            % TODO: check efficients of pid_groups:get_my/1 vs. caching the PID or retrieving from Neighbors - caching is probably the best
            comm:send_local(pid_groups:get_my(dht_node), {lookup_decision, Key, Hops, WrappedMsg});
        NextHop ->
            NewMsg = {?lookup_aux, Key, Hops + 1, WrappedMsg},
            comm:send(NextHop, NewMsg, [{shepherd, self()}])
    end.

-spec lookup_aux_leases(Neighbors::nodelist:neighborhood(), ERT::?RT:external_rt(),
                        Key::intervals:key(), Hops::non_neg_integer(),
                        Msg::comm:message()) -> ok.
lookup_aux_leases(Neighbors, ERT, Key, Hops, Msg) ->
    %% TODO implement WrappedMsg
    WrappedMsg = ?RT:wrap_message(Key, Msg, no_dht_node_state, Hops),
    % NOTE: leases do not require routing through predecessor -> let the own node decide:
    case intervals:in(Key, nodelist:node_range(Neighbors)) of
        true ->
            % TODO: use lookup_fin
            comm:send_local(pid_groups:get_my(dht_node),
                            {lookup_decision, Key, Hops, WrappedMsg});
        false ->
            %% next_hop and nodelist:succ(Neighbors) return different nodes if
            %% key is in the interval of the succ.
            NextHop = case ?RT:next_hop(Neighbors, ERT, Key) of
                          succ -> node:pidX(nodelist:succ(Neighbors));
                          Pid -> Pid
                      end,
            NewMsg = {?lookup_aux, Key, Hops + 1, WrappedMsg},
            comm:send(NextHop, NewMsg, [{shepherd, self()}])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% rt_loop:state_active() handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% handling rt_loop's (opaque) state - these handlers should at least be used
% outside this module:

-spec get_neighb(State::state_active()) -> nodelist:neighborhood().
get_neighb({Neighbors, _RT, _ERT}) ->
    Neighbors.

-spec get_rt(State::state_active()) -> ?RT:rt().
get_rt({_Neighbors, RT, _ERT}) -> RT.

-spec set_rt(State::state_active(), RT::?RT:rt()) -> NewState::state_active().
set_rt({Neighbors, _OldRT, ERT}, NewRT) ->
    {Neighbors, NewRT, ERT}.

-spec get_ert(State::state_active()) -> ?RT:external_rt().
get_ert({_Neighbors, _RT, ERT}) -> ERT.

-spec set_ert(State::state_active(), ERT::?RT:external_rt()) -> NewState::state_active().
set_ert({Neighbors, RT, _OldERT}, ERT) ->
    {Neighbors, RT, ERT}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Misc.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Filter function for subscriptions that returns true if a
%%      any neighbor changes.
-spec rm_neighbor_change(
        OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood(),
        Reason::rm_loop:reason()) -> boolean().
rm_neighbor_change(OldNeighbors, NewNeighbors, _Reason) ->
    OldNeighbors =/= NewNeighbors.

%% @doc Notifies the node's routing table of a changed neighborhood.
%%      Used to subscribe to the ring maintenance.
-spec rm_send_update(Subscriber::pid(), Tag::?MODULE,
                     OldNeighbors::nodelist:neighborhood(),
                     NewNeighbors::nodelist:neighborhood(),
                     Reason::rm_loop:reason()) -> ok.
rm_send_update(Pid, ?MODULE, OldNeighbors, NewNeighbors, _Reason) ->
    comm:send_local(Pid, {update_rt, OldNeighbors, NewNeighbors}).

-spec get_base_interval() -> pos_integer().
get_base_interval() ->
    config:read(pointer_base_stabilization_interval) div 1000.

%% @doc Checks whether config parameters of the rt_loop process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(pointer_base_stabilization_interval) and
        config:cfg_is_greater_than_equal(pointer_base_stabilization_interval, 1000).
