%  @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Jens V. Fischer <jensvfischer@gmail.com>
%% @doc    Behaviour modul for gossip_beh.erl. Implements the generic code
%%         of the gossiping framework.
%%         Used abbreviations:
%%         <ul>
%%            <li> cb: callback module (a module implementing the
%%                     gossip_beh.erl behaviour)
%%            </li>
%%         </ul>
%%
%% @version $Id$
-module(gossip).
-author('jensvfischer@gmail.com').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

% interaction with gen_component
-export([init/1, on_inactive/2, on_active/2]).

%API
-export([start_link/1, activate/1, deactivate/0, start_gossip_task/2, stop_gossip_task/1, remove_all_tombstones/0, check_config/0]).

% interaction with the ring maintenance:
-export([rm_filter_slide_msg/3, rm_send_activation_msg/4, rm_my_range_changed/3, rm_send_new_range/4]).

% testing
-export([tester_create_state/9, is_state/1]).

-define(PDB, pdb_ets).
-define(PDB_OPTIONS, [set, protected]).

% prevent warnings in the log
% (node availability is not that important to gossip)
-define(SEND_TO_GROUP_MEMBER(Pid, Process, Msg),
        comm:send(Pid, Msg, [{group_member, Process}, {shepherd, self()}])).

%% -define(SHOW, config:read(log_level)).
-define(SHOW, debug).

-define(CBMODULES, [{gossip_load, default}]). % callback modules as list
-define(CBMODULES_TYPE, {gossip_load, default}). % callback modules as union of atoms


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type state() :: ets:tab().
-type cb_module() :: ?CBMODULES_TYPE.
-type state_key_cb() :: cb_state | cb_status | cycles | trigger_lock |
                        exch_data | round.
-type state_key() :: cb_modules | msg_queue | range | status |
                     {reply_peer, pos_integer()} |
                     {trigger_group, pos_integer()} |
                     {state_key_cb(), cb_module()} .
-type cb_fun_name() :: get_values_all | get_values_best | handle_msg |
                       integrate_data | notify_change | round_has_converged |
                       select_data | select_node | select_reply_data |
                       web_debug_info | shutdown.

% accepted messages of gossip behaviour module

-ifdef(forward_or_recursive_types_are_not_allowed).
-type send_error() :: {send_error, _Pid::comm:mypid(), Msg::comm:message(), Reason::atom()}.
-else.
-type send_error() :: {send_error, _Pid::comm:mypid(), Msg::message(), Reason::atom()}.
-endif.

-type bh_message() ::
    {activate_gossip, Range::intervals:interval()} |
    {start_gossip_task, CBModule::cb_module(), Args::list()} |
    {gossip_trigger, TriggerInterval::pos_integer()} |
    {update_range, NewRange::intervals:interval()} |
    {web_debug_info, SourcePid::comm:mypid()} |
    send_error() |
    {bulkowner, deliver, Id::uid:global_uid(), Range::intervals:interval(),
        Msg::comm:message(), Parents::[comm:mypid(),...]} |
    {remove_all_tombstones}
.

-type cb_message() ::
    {selected_data, CBModule::cb_module(), PData::gossip_beh:exch_data()} |
    {selected_peer, CBModule::cb_module(), CyclonMsg::{cy_cache,
            RandomNodes::[node:node_type()]} } |
    {p2p_exch, CBModule::cb_module(), SourcePid::comm:mypid(),
        PData::gossip_beh:exch_data(), OtherRound::non_neg_integer()} |
    {selected_reply_data, CBModule::cb_module(), QData::gossip_beh:exch_data(),
        Ref::pos_integer(), Round::non_neg_integer()} |
    {p2p_exch_reply, CBModule::cb_module(), SourcePid::comm:mypid(),
        QData::gossip_beh:exch_data(), OtherRound::non_neg_integer()} |
    {integrated_data, CBModule::cb_module(), current_round} |
    {new_round, CBModule::cb_module(), NewRound::non_neg_integer()} |
    {cb_reply, CBModule::cb_module(), Msg::comm:message()} |
    {get_values_best, CBModule::cb_module(), SourcePid::comm:mypid()} |
    {get_values_all, CBModule::cb_module(), SourcePid::comm:mypid()} |
    {stop_gossip_task, CBModule::cb_module()} |
    no_msg
.

-type message() :: bh_message() | cb_message().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% called by sup_dht_node
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on_inactive/2, [],
                             [{pid_groups_join_as, DHTNodeGroup, gossip}]).


% called by gen_component, results in on_inactive
-spec init([]) -> state().
init([]) ->
    TabName = ?PDB:new(state, ?PDB_OPTIONS),
    state_set(status, uninit, TabName),
    state_set(cb_modules, [], TabName),
    state_set(msg_queue, msg_queue:new(), TabName),
    TabName.


% called by dht_node_join, results in on_active
-spec activate(Range::intervals:interval()) -> ok.
activate(MyRange) ->
    case MyRange =:= intervals:all() of
        true ->
            % We're the first node covering the whole ring range.
            % Start gossip right away because it's needed for passive
            % load balancing when new nodes join the ring.
            comm:send_local(pid_groups:get_my(gossip), {activate_gossip, MyRange});
        _    ->
            % subscribe to ring maintenance (rm) for {slide_finished, succ} or {slide_finished, pred}
            rm_loop:subscribe(self(), ?MODULE,
                              fun gossip:rm_filter_slide_msg/3,
                              fun gossip:rm_send_activation_msg/4, 1)
    end.

%% @doc Deactivates all gossip processes.
-spec deactivate() -> ok.
deactivate() ->
    Msg = {?send_to_group_member, gossip, {deactivate_gossip}},
    bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(), Msg).


-spec start_gossip_task(CBModule, Args) -> ok when
    is_subtype(CBModule, atom() | cb_module() | {cb_module(), uid:global_uid()}),
    is_subtype(Args, list()).
start_gossip_task(ModuleName, Args) when is_atom(ModuleName) ->
    Id = uid:get_global_uid(),
    start_gossip_task({ModuleName, Id}, Args);

start_gossip_task({ModuleName, Id}, Args) when is_atom(ModuleName) ->
    Msg = {?send_to_group_member, gossip,
                {start_gossip_task, {ModuleName, Id}, Args}},
    bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(), Msg).


-spec stop_gossip_task(CBModule::cb_module()) -> ok.
stop_gossip_task(CBModule) ->
    Msg = {?send_to_group_member, gossip, {stop_gossip_task, CBModule}},
    bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(), Msg).


-spec remove_all_tombstones() -> ok.
remove_all_tombstones() ->
    Msg = {?send_to_group_member, gossip, {remove_all_tombstones}},
    bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(), Msg).


%% @doc Checks whether the received notification is a {slide_finished, succ} or
%%      {slide_finished, pred} msg. Used as filter function for the ring maintanance.
-spec rm_filter_slide_msg(Neighbors, Neighbors, Reason) -> boolean() when
                          is_subtype(Neighbors, nodelist:neighborhood()),
                          is_subtype(Reason, rm_loop:reason()).
rm_filter_slide_msg(_OldNeighbors, _NewNeighbors, Reason) ->
        Reason =:= {slide_finished, pred} orelse Reason =:= {slide_finished, succ}.

%% @doc Sends the activation message to the behaviour module (this module)
%%      Used to subscribe to the ring maintenance for {slide_finished, succ} or
%%      {slide_finished, pred} msg.
-spec rm_send_activation_msg(Subscriber, ?MODULE, Neighbours, Neighbours) -> ok when
                             is_subtype(Subscriber, pid()),
                             is_subtype(Neighbours, nodelist:neighborhood()).
rm_send_activation_msg(_Pid, ?MODULE, _OldNeighbours, NewNeighbours) ->
    %% io:format("Pid: ~w. Self: ~w. PidGossip: ~w~n", [Pid, self(), Pid2]),
    MyRange = nodelist:node_range(NewNeighbours),
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {activate_gossip, MyRange}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Main Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%-------------------------- on_inactive ---------------------------%%

-spec on_inactive(Msg::message(), State::state()) -> state().
on_inactive({activate_gossip, MyRange}=Msg, State) ->
    ?PDB:set({status, init}, State),

    % subscribe to ring maintenance (rm)
    rm_loop:subscribe(self(), ?MODULE,
                      fun gossip:rm_my_range_changed/3,
                      fun gossip:rm_send_new_range/4, inf),

    init_gossip_tasks(State),

    % set range and notify cb modules about leader state
    state_set(range, MyRange, State),
    Msg1 = case is_leader(MyRange) of
        true -> {is_leader, MyRange};
        false -> {no_leader, MyRange}
    end,
    List = [leader, Msg1],
    Fun = fun (CBModule) -> cb_call(notify_change, List, Msg, CBModule, State) end,
    CBModules = state_get(cb_modules, State),
    lists:foreach(Fun, CBModules),

    % change handler to on_active
    gen_component:change_handler(State, fun ?MODULE:on_active/2);


on_inactive({p2p_exch, _CBModule, SourcePid, _PData, _Round}=Msg, State) ->
    comm:send(SourcePid, {send_error, comm:this(), Msg, on_inactive}),
    State;


on_inactive({p2p_exch_reply, _CBModule, SourcePid, _QData, _Round}=Msg, State) ->
    comm:send(SourcePid, {send_error, comm:this(), Msg, on_inactive}),
    State;


on_inactive({get_values_best, _CBModule, _SourcePid}=Msg, State) ->
    msg_queue_add(Msg, State), State;


on_inactive({get_values_all, _CBModule, _SourcePid}=Msg, State) ->
   msg_queue_add(Msg, State), State;


on_inactive({web_debug_info, _Requestor}=Msg, State) ->
    msg_queue_add(Msg, State), State;


on_inactive({stop_gossip_task, _CBModule}=Msg, State) ->
    msg_queue_add(Msg, State), State;


on_inactive({start_gossip_task, _CBModule, _Args}=Msg, State) ->
    msg_queue_add(Msg, State), State;


on_inactive({remove_all_tombstones}=Msg, State) ->
    msg_queue_add(Msg, State), State;


on_inactive(_Msg, State) ->
    State.


%%--------------------------- on_active ----------------------------%%

-spec on_active(Msg::message(), State::state()) -> state().
on_active({start_gossip_task, CBModule, Args}, State) ->
    CBModules = state_get(cb_modules, State),
    case contains(CBModule, CBModules) of
        true ->
            log:log(warn, "[ Gossip ] Trying to start an already existing Module: ~w ."
                ++ "Request will be ignored.", [CBModule]);
        false -> init_gossip_task(CBModule, Args, State)
    end,
    State;


on_active({gossip_trigger, TriggerInterval}=Msg, State) ->
    msg_queue_send(State),
    log:log(debug, "[ Gossip ] Triggered: ~w", [Msg]),
    case state_get_raw({trigger_group, TriggerInterval}, State) of
        undefined ->
            ok; %% trigger group does no longer exist, forget about this trigger
        {CBModules} ->
            _ = [
                 begin
                     case state_get(trigger_lock, CBModule, State) of
                         free ->
                             log:log(debug, "[ Gossip ] Module ~w got triggered", [CBModule]),
                             log:log(?SHOW, "[ Gossip ] Cycle: ~w, Round: ~w",
                                     [state_get(cycles, CBModule, State), state_get(round, CBModule, State)]),

                             %% set cycle status to active
                             state_set(trigger_lock, locked, CBModule, State),

                             %% reset exch_data
                             state_set(exch_data, {undefined, undefined}, CBModule, State),

                             %% request node (by the cb module or the bh module)
                             case cb_call(select_node, [], Msg, CBModule, State) of
                                 true -> ok;
                                 false -> request_random_node(CBModule)
                             end,

                             %% request data
                             cb_call(select_data, [], Msg, CBModule, State);
                         locked -> do_nothing % ignore trigger when within prepare-request phase
                     end
                 end || CBModule <- CBModules
                ],

            %% trigger next
            msg_delay:send_trigger(TriggerInterval, {gossip_trigger, TriggerInterval}),
            state_set({trigger_group, TriggerInterval}, {CBModules}, State)
    end,
    State;


on_active({update_range, NewRange}=FullMsg, State) ->
    state_set(range, NewRange, State),
    Msg = case is_leader(NewRange) of
        true -> {is_leader, NewRange};
        false -> {no_leader, NewRange}
    end,
    Fun = fun (CBModule) -> cb_call(notify_change, [leader, Msg], FullMsg, CBModule, State) end,
    CBModules = state_get(cb_modules, State),
    lists:foreach(Fun, CBModules),
    State;


on_active({web_debug_info, Requestor}=Msg, State) ->
    CBModules = lists:reverse(state_get(cb_modules, State)),
    Fun = fun (CBModule, Acc) -> Acc ++ [{"",""}] ++
            cb_call(web_debug_info, [], Msg, CBModule, State) end,
    KeyValueList = [{"",""}] ++ web_debug_info(State) ++ lists:foldl(Fun, [], CBModules),
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State;


% received from shepherd, from on_inactive on from rejected messages
on_active({send_error, _Pid, Msg, Reason}=ErrorMsg, State) ->
    % unpack msg if necessary
    MsgUnpacked = case Msg of
        % msg from shepherd
        {_, ?MODULE, OriginalMsg} -> OriginalMsg;
        % other send_error msgs, e.g. from on_inactive
        _Msg -> _Msg
    end,
    CBStatus = state_get(cb_status, element(2, MsgUnpacked), State),
    case MsgUnpacked of
        _ when CBStatus =:= tombstone ->
            log:log(warn(), "[ Gossip ] Got ~w msg for tombstoned module ~w. Reason: ~w. Original Msg: ~w",
                [element(1, ErrorMsg), element(2, MsgUnpacked), Reason, element(1, Msg)]);
        {p2p_exch, CBModule, _SourcePid, PData, Round} ->
            log:log(warn(), "[ Gossip ] p2p_exch failed because of ~w", [Reason]),
            _ = cb_call(notify_change, [exch_failure, {p2p_exch, PData, Round}], ErrorMsg, CBModule, State);
        {p2p_exch_reply, CBModule, QData, Round} ->
            log:log(warn(), "[ Gossip ] p2p_exch_reply failed because of ~w", [Reason]),
            _ = cb_call(notify_change, [exch_failure, {p2p_exch_reply, QData, Round}], ErrorMsg, CBModule, State);
        _ ->
            log:log(?SHOW, "[ Gossip ] Failed to deliever the Msg ~w because ~w", [Msg, Reason])
    end,
    State;


% unpack bulkowner msg
on_active({bulkowner, deliver, _Id, _Range, Msg, _Parents}, State) ->
    comm:send_local(self(), Msg),
    State;


on_active({remove_all_tombstones}, State) ->
    TombstoneKeys = get_tombstones(State),
    lists:foreach(fun (Key) -> ?PDB:delete(Key, State) end, TombstoneKeys),
    State;


on_active({deactivate_gossip}, State) ->
    log:log(warn, "[ Gossip ] deactivating gossip framwork"),
    rm_loop:unsubscribe(self(), ?MODULE),

    % stop all gossip tasks
    lists:foreach(fun (CBModule) -> handle_msg({stop_gossip_task, CBModule}, State) end,
        state_get(cb_modules, State)),

    % cleanup state
    state_set(status, uninit, State),
    state_set(cb_modules, [], State),
    lists:foreach(fun (Key) -> ?PDB:delete(Key, State) end,
        [msg_queue, range]),

    gen_component:change_handler(State, fun ?MODULE:on_inactive/2);


% messages expected reaching this on_active clause have the form:
%   {MsgTag, CBModule, ...}
%   element(1, Msg) = MsgTag
%   element(2, Msg) = CBModule
on_active(Msg, State) ->
    try state_get(cb_status, element(2, Msg), State) of
        tombstone ->
            log:log(warn(), "[ Gossip ] Got ~w msg for tombstoned module ~w",
                [element(1, Msg), element(2, Msg)]);
        unstarted ->
            log:log(?SHOW, "[ Gossip ] Got ~w msg in cbstatus 'unstarted' for ~w",
                [element(1, Msg), element(2, Msg)]),
            msg_queue_add(Msg, State);
        started ->
            handle_msg(Msg, State)
    catch
        _:_ -> log:log(warn(), "[ Gossip ] Unknown msg: ~w", [Msg])
    end,
    State.


-spec handle_msg(Msg::cb_message(), State::state()) -> state().
% re-request node if node list is empty
handle_msg({selected_peer, CBModule, _Msg={cy_cache, []}}, State) ->
    Delay = cb_call(trigger_interval, CBModule),
    request_random_node_delayed(Delay, CBModule),
    State;


handle_msg({selected_peer, CBModule, _Msg={cy_cache, [Node]}}, State) ->
    % This message is received as a response to a get_subset message to the
    % cyclon process and should contain a random node.
    %% io:format("gossip: got random node from Cyclon: ~p~n",[node:pidX(Node)]),
    {_Node, PData} = state_get(exch_data, CBModule, State),
    case PData of
        undefined -> state_set(exch_data, {Node, undefined}, CBModule, State);
        _ -> start_p2p_exchange(Node, PData, CBModule, State)
    end,
    State;


handle_msg({selected_data, CBModule, PData}, State) ->
    % check if a peer has been received already
    {Peer, _PData} = state_get(exch_data, CBModule, State),
    case Peer of
        undefined -> state_set(exch_data, {undefined, PData}, CBModule, State);
        _ -> start_p2p_exchange(Peer, PData, CBModule, State)
    end,
    State;


handle_msg({p2p_exch, CBModule, SourcePid, PData, OtherRound}=Msg, State) ->
    log:log(debug, "[ Gossip ] p2p_exch msg received from ~w. PData: ~w",[SourcePid, PData]),
    state_set({reply_peer, Ref=uid:get_pids_uid()}, SourcePid, State),
    case check_round(OtherRound, CBModule, State) of
        ok ->
            select_reply_data(PData, Ref, current_round, OtherRound, Msg, CBModule, State);
        start_new_round -> % self is leader
            log:log(?SHOW, "[ Gossip ] Starting a new round in p2p_exch"),
            _ = cb_call(notify_change, [new_round, state_get(round, CBModule, State)], Msg, CBModule, State),
            select_reply_data(PData, Ref, old_round, OtherRound, Msg, CBModule, State),
            comm:send(SourcePid, {new_round, CBModule, state_get(round, CBModule, State)});
        enter_new_round ->
            log:log(?SHOW, "[ Gossip ] Entering a new round in p2p_exch"),
            _ = cb_call(notify_change, [new_round, state_get(round, CBModule, State)], Msg, CBModule, State),
            select_reply_data(PData, Ref, current_round, OtherRound, Msg, CBModule, State);
        propagate_new_round -> % i.e. MyRound > OtherRound
            log:log(debug, "[ Gossip ] propagate round in p2p_exch"),
            select_reply_data(PData, Ref, old_round, OtherRound, Msg, CBModule, State),
            comm:send(SourcePid, {new_round, CBModule, state_get(round, CBModule, State)})
    end,
    State;


handle_msg({selected_reply_data, CBModule, QData, Ref, Round}, State)->
    Peer = state_take({reply_peer, Ref}, State),
    log:log(debug, "[ Gossip ] selected_reply_data. CBModule: ~w, QData ~w, Peer: ~w",
        [CBModule, QData, Peer]),
    comm:send(Peer, {p2p_exch_reply, CBModule, comm:this(), QData, Round}, [{shepherd, self()}]),
    State;


handle_msg({p2p_exch_reply, CBModule, SourcePid, QData, OtherRound}=Msg, State) ->
    log:log(debug, "[ Gossip ] p2p_exch_reply, CBModule: ~w, QData ~w", [CBModule, QData]),
    _ = case check_round(OtherRound, CBModule, State) of
        ok ->
            _ = cb_call(integrate_data, [QData, current_round, OtherRound], Msg, CBModule, State);
        start_new_round -> % self is leader
            log:log(?SHOW, "[ Gossip ] Starting a new round p2p_exch_reply"),
            _ = cb_call(notify_change, [new_round, state_get(round, CBModule, State)], Msg, CBModule, State),
            _ = cb_call(integrate_data, [QData, old_round, OtherRound], Msg, CBModule, State),
            comm:send(SourcePid, {new_round, CBModule, state_get(round, CBModule, State)});
        enter_new_round ->
            log:log(?SHOW, "[ Gossip ] Entering a new round p2p_exch_reply"),
            _ = cb_call(notify_change, [new_round, state_get(round, CBModule, State)], Msg, CBModule, State),
            _ = cb_call(integrate_data, [QData, current_round, OtherRound], Msg, CBModule, State);
        propagate_new_round -> % i.e. MyRound > OtherRound
            log:log(debug, "[ Gossip ] propagate round in p2p_exch_reply"),
            comm:send(SourcePid, {new_round, CBModule, state_get(round, CBModule, State)}),
            _ = cb_call(integrate_data, [QData, old_round, OtherRound], Msg, CBModule, State)
    end,
    State;


handle_msg({integrated_data, CBModule, current_round}, State) ->
    state_update(cycles, fun (X) -> X+1 end, CBModule, State),
    State;


% finishing an old round should not affect cycle counter of current round
handle_msg({integrated_data, _CBModule, old_round}, State) ->
    State;


handle_msg({cb_reply, CBModule, Msg}=FullMsg, State) ->
    _ = cb_call(handle_msg, [Msg], FullMsg, CBModule, State),
    State;

% round propagation message
handle_msg({new_round, CBModule, NewRound}=Msg, State) ->
    MyRound = state_get(round, CBModule, State),
    if
        MyRound < NewRound ->
            log:log(?SHOW, "[ Gossip ] Entering new round via round propagation message"),
            _ = cb_call(notify_change, [new_round, NewRound], Msg, CBModule, State),
            state_set(round, NewRound, CBModule, State),
            state_set(cycles, 0, CBModule, State);
        MyRound =:= NewRound -> % i.e. the round propagation msg was already received
            log:log(?SHOW, "[ Gossip ] Received propagation msg for round i'm already in"),
            do_nothing;
        MyRound > NewRound ->
            log:log(?SHOW, "[ Gossip ] MyRound > OtherRound")
    end,
    State;


handle_msg({get_values_best, CBModule, SourcePid}=Msg, State) ->
    BestValues = cb_call(get_values_best, [], Msg, CBModule, State),
    comm:send_local(SourcePid, {gossip_get_values_best_response, BestValues}),
    State;


handle_msg({get_values_all, CBModule, SourcePid}=Msg, State) ->
    {Prev, Current, Best} = cb_call(get_values_all, [], Msg, CBModule, State),
    comm:send_local(SourcePid,
        {gossip_get_values_all_response, Prev, Current, Best}),
    State;


handle_msg({stop_gossip_task, CBModule}=Msg, State) ->
    log:log(?SHOW, "[ Gossip ] Stopping ~w", [CBModule]),
    % shutdown callback module
    _ = cb_call(shutdown, [], Msg, CBModule, State),

    % delete callback module dependet entries from state
    Fun = fun (Key) -> ?PDB:delete({Key, CBModule}, State) end,
    lists:foreach(Fun, [cb_state, cb_status, cycles, trigger_lock, exch_data, round]),

    % remove from list of modules
    Fun1 = fun (ListOfModules) -> lists:delete(CBModule, ListOfModules) end,
    state_update(cb_modules, Fun1, State),

    % remove from trigger group
    Interval = cb_call(trigger_interval, CBModule) div 1000,
    {CBModules} = state_get({trigger_group, Interval}, State),
    NewCBModules = lists:delete(CBModule, CBModules),
    case NewCBModules of
        [] ->
            ?PDB:delete({trigger_group, Interval}, State);
        _ ->
            state_set({trigger_group, Interval}, {NewCBModules}, State)
    end,

    % set tombstone
    state_set(cb_status, tombstone, CBModule, State),
    State.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Msg Exchange with Peer
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% called by either on({selected_data,...}) or on({selected_peer, ...}),
% depending on which finished first
-spec start_p2p_exchange(Peer::node:node_type(), PData::gossip_beh:exch_data(),
    CBModule::cb_module(), State::state()) -> ok.
start_p2p_exchange(Peer, PData, CBModule, State)  ->
    case node:is_me(Peer) of
        false ->
            %% io:format("starting p2p exchange. Peer: ~w, Ref: ~w~n",[Peer, Ref]),
            ?SEND_TO_GROUP_MEMBER(
                    node:pidX(Peer), gossip,
                    {p2p_exch, CBModule, comm:this(), PData, state_get(round, CBModule, State)}),
            state_set(trigger_lock, free, CBModule, State);
        true  ->
            %% todo does this really happen??? cyclon should not have itself in the cache
            log:log(?SHOW, "[ Gossip ] Node was ME, requesting new node"),
            request_random_node(CBModule),
            {Peer, Data} = state_get(exch_data, CBModule, State),
            state_set(exch_data, {undefined, Data}, CBModule, State)
    end,
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Interacting with the Callback Modules
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init_gossip_tasks(State::state()) -> ok.
init_gossip_tasks(State) ->
    Fun = fun (CBModule) ->
            state_set(cb_status, unstarted, CBModule, State),
            comm:send_local(self(), {start_gossip_task, CBModule, []})
          end,
    lists:foreach(Fun, ?CBMODULES).


-spec init_gossip_task(CBModule::cb_module(), Args::list(), State::state()) -> ok.
init_gossip_task(CBModule, Args, State) ->

    % initialize CBModule
    {ok, CBState} = cb_call(init, [CBModule|Args], CBModule),

    % add state ob CBModule to state
    state_set(cb_state, CBState, CBModule, State),

    % set cb_status to init
    state_set(cb_status, started, CBModule, State),

    % notify cb module about leader state
    MyRange = state_get(range, State),
    LeaderMsg = case is_leader(MyRange) of
        true -> {is_leader, MyRange};
        false -> {no_leader, MyRange}
    end,

    % TODO no_msg is no solution
    _ = cb_call(notify_change, [leader, LeaderMsg], no_msg, CBModule, State),

    % configure and add trigger
    TriggerInterval = cb_call(trigger_interval, CBModule) div 1000,
    {TriggerGroup} =
    case state_get_raw({trigger_group, TriggerInterval}, State) of
        undefined ->
            % create and init new trigger group
            msg_delay:send_trigger(0,  {gossip_trigger, TriggerInterval}),
            {[CBModule]};
        {OldTriggerGroup} ->
            % add CBModule to existing trigger group
            {[CBModule|OldTriggerGroup]}
    end,
    state_set({trigger_group, TriggerInterval}, {TriggerGroup}, State),

    % add CBModule to list of cbmodules
    CBModules = state_get(cb_modules, State),
    state_set(cb_modules, [CBModule|CBModules], State),

    % initialize exch_data table with empty entry
    state_set(exch_data, {undefined, undefined}, CBModule, State),

    % set cycles to 0
    state_set(cycles, 0, CBModule, State),

    % set rounds to 0
    state_set(round, 0, CBModule, State),

    % set cycle status to inactive (gets activated by trigger)
    state_set(trigger_lock, free, CBModule, State),

    ok.


-spec cb_call(FunName, CBModule) -> non_neg_integer() | pos_integer() when
    is_subtype(FunName, min_cycles_per_round | max_cycles_per_round | trigger_interval),
    is_subtype(CBModule, cb_module()).
cb_call(FunName, CBModule) ->
    cb_call(FunName, [], CBModule).

-spec cb_call(FunName, Args, CBModule) -> Return when
    is_subtype(FunName, init | min_cycles_per_round | max_cycles_per_round | trigger_interval),
    is_subtype(Args, list()),
    is_subtype(CBModule, cb_module()),
    is_subtype(Return, non_neg_integer() | pos_integer() | {ok, any()}).
cb_call(FunName, Args, CBModule) ->
    {CBModuleName, _Id} = CBModule,
    apply(CBModuleName, FunName, Args).


-spec cb_call(FunName, Arguments, Msg, CBModule, State) -> Return when
    is_subtype(FunName, cb_fun_name()),
    is_subtype(Arguments, list()),
    is_subtype(Msg, message()),
    is_subtype(CBModule, cb_module()),
    is_subtype(State, state()),
    is_subtype(Return, ok | discard_msg
        | send_back | boolean() | {any(), any(), any()} | list({list(), list()})).
cb_call(FunName, Args, Msg, CBModule, State) ->
    {ModuleName, _InstanceId} = CBModule,
    CBState = state_get(cb_state, CBModule, State),
    Args1 = Args ++ [CBState],
    ReturnTuple = apply(ModuleName, FunName, Args1),
    case ReturnTuple of
        {ok, ReturnedCBState} ->
            log:log(debug, "[ Gossip ] cb_call: ReturnTuple: ~w, ReturendCBState ~w", [ReturnTuple, ReturnedCBState]),
            state_set(cb_state, ReturnedCBState, CBModule, State), ok;
        {retry, ReturnedCBState} ->
            msg_queue_add(Msg, State),
            state_set(cb_state, ReturnedCBState, CBModule, State),
            discard_msg;
        {discard_msg, ReturnedCBState} ->
            state_set(cb_state, ReturnedCBState, CBModule, State),
            discard_msg;
        {send_back, ReturnedCBState} ->
            case Msg of
                {p2p_exch,_,SourcePid,_,_} ->
                    comm:send(SourcePid, {send_error, comm:this(), Msg, message_rejected});
                {p2p_exch_reply,_,SourcePid,_,_} ->
                    comm:send(SourcePid, {send_error, comm:this(), Msg, message_rejected});
                _Other ->
                    log:log(error, "send_back on non backsendable msg")
            end,
            state_set(cb_state, ReturnedCBState, CBModule, State),
            send_back;
        {ReturnValue, ReturnedCBState} ->
            log:log(debug, "[ Gossip ] cb_call: ReturnTuple: ~w, ReturnValue: ~w ReturendCBState: ~w",
                [ReturnTuple, ReturnValue, ReturnedCBState]),
            state_set(cb_state, ReturnedCBState, CBModule, State),
            ReturnValue
    end.


-spec select_reply_data(PData::gossip_beh:exch_data(), Ref::pos_integer(),
    RoundStatus::gossip_beh:round_status(), Round::non_neg_integer(),
    Msg::message(), CBModule::cb_module(), State::state()) -> ok.
select_reply_data(PData, Ref, RoundStatus, Round, Msg, CBModule, State) ->
    case cb_call(select_reply_data, [PData, Ref, RoundStatus, Round], Msg, CBModule, State) of
        ok -> ok;
        discard_msg ->
            state_take({reply_peer, Ref}, State), ok;
        send_back ->
            state_take({reply_peer, Ref}, State), ok
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Requesting Peers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends the local node's cyclon process an enveloped request for a random node.
%%      on_active({selected_peer, CBModule, {cy_cache, Cache}}, State) will handle the response
-spec request_random_node(CBModule::cb_module()) -> ok.
request_random_node(CBModule) ->
    CyclonPid = pid_groups:get_my(cyclon),
    EnvPid = comm:reply_as(self(), 3, {selected_peer, CBModule, '_'}),
    comm:send_local(CyclonPid, {get_subset_rand, 1, EnvPid}).


-spec request_random_node_delayed(Delay::non_neg_integer(), CBModule::cb_module()) ->
    reference().
request_random_node_delayed(Delay, CBModule) ->
    CyclonPid = pid_groups:get_my(cyclon),
    EnvPid = comm:reply_as(self(), 3, {selected_peer, CBModule, '_'}),
    comm:send_local_after(Delay, CyclonPid, {get_subset_rand, 1, EnvPid}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Round Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec check_round(OtherRound::non_neg_integer(), CBModule::cb_module(), State::state())
    -> ok | start_new_round | enter_new_round | propagate_new_round.
check_round(OtherRound, CBModule, State) ->
    MyRound = state_get(round, CBModule, State),
    Leader = is_leader(state_get(range, State)),
    case MyRound =:= OtherRound of
        true when Leader ->
            case is_end_of_round(CBModule, State) of
                true ->
                    state_update(round, fun (X) -> X+1 end, CBModule, State),
                    state_set(cycles, 0, CBModule, State),
                    start_new_round;
                false -> ok
            end;
        true -> ok;
        false when MyRound < OtherRound ->
            state_set(round, OtherRound, CBModule, State),
            state_set(cycles, 0, CBModule, State),
            enter_new_round;
        false when MyRound > OtherRound ->
            propagate_new_round
    end.


-spec is_end_of_round(CBModule::cb_module(), State::state()) -> boolean().
is_end_of_round(CBModule, State) ->
    Cycles = state_get(cycles, CBModule, State),
    log:log(debug, "[ Gossip ] check_end_of_round. Cycles: ~w", [Cycles]),
    Cycles >= cb_call(min_cycles_per_round, CBModule) andalso
    (   ( Cycles >= cb_call(max_cycles_per_round, CBModule)) orelse
        ( cb_call(round_has_converged, [], no_msg, CBModule, State) ) ) .


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Range/Leader Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether the node is the current leader.
-spec is_leader(MyRange::intervals:interval()) -> boolean().
is_leader(MyRange) ->
    intervals:in(?RT:hash_key("0"), MyRange).


%% @doc Checks whether the node's range has changed, i.e. either the node
%%      itself or its pred changed.
-spec rm_my_range_changed(OldNeighbors::nodelist:neighborhood(),
                          NewNeighbors::nodelist:neighborhood(),
                          IsSlide::rm_loop:reason()) -> boolean().
rm_my_range_changed(OldNeighbors, NewNeighbors, _IsSlide) ->
    nodelist:node(OldNeighbors) =/= nodelist:node(NewNeighbors) orelse
        nodelist:pred(OldNeighbors) =/= nodelist:pred(NewNeighbors).


%% @doc Notifies the node's gossip process of a changed range.
%%      Used to subscribe to the ring maintenance.
-spec rm_send_new_range(Subscriber::pid(), Tag::?MODULE,
                        OldNeighbors::nodelist:neighborhood(),
                        NewNeighbors::nodelist:neighborhood()) -> ok.
rm_send_new_range(Pid, ?MODULE, _OldNeighbors, NewNeighbors) ->
    NewRange = nodelist:node_range(NewNeighbors),
    comm:send_local(Pid, {update_range, NewRange}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% State: Getters and Setters
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the given key from the given state.
%%      Allowed keys:
%%      <ul>
%%        <li>`cb_modules', a list of registered callback modules ,</li>
%%        <li>`msg_queue', the message queue of the behaviour module, </li>
%%        <li>`range', the key range of the node, </li>
%%        <li>`{reply_peer, Ref}', the peer to send the p2p_exch_reply to, </li>
%%        <li>`{trigger_group, TriggerInterval}', trigger group, </li>
%%      </ul>
-spec state_get(Key::state_key(), State::state()) -> any().
state_get(Key, State) ->
    case ?PDB:get(Key, State) of
        {Key, Value} -> Value;
        undefined ->
            log:log(error(), "[ gossip ] Lookup of ~w in ~w failed", [Key, State]),
            erlang:error(lookup_failed, [Key, State])
    end.

-spec state_get_raw(Key::state_key(), State::state()) -> any().
state_get_raw(Key, State) ->
    case ?PDB:get(Key, State) of
        {Key, Value} -> Value;
        undefined -> undefined
    end.


-spec state_take(Key::state_key(), State::state()) -> any().
state_take(Key, State) ->
    case ?PDB:take(Key, State) of
        {Key, Value} -> Value;
        undefined ->
            log:log(error, "[ gossip ] Take of ~w in ~w failed", [Key, State]),
            erlang:error(lookup_failed, [Key, State])
    end.

-spec state_set(Key::state_key(), Value::any(), State::state()) -> ok.
state_set(Key, Value, State) ->
    ?PDB:set({Key, Value}, State).


-spec state_update(Key::state_key(), UpdateFun::fun(), State::state()) -> ok.
state_update(Key, Fun, State) ->
    NewValue = apply(Fun, [state_get(Key, State)]),
    state_set(Key, NewValue, State).

%%---------------- Callback Module Specific State ------------------%%

%% @doc Gets the given key from the given state.
%%      Allowed keys:
%%      <ul>
%%        <li>`cb_state', the state of the given callback module </li>
%%        <li>`cb_status', indicates, if `init()' was called on callback module
%%                  (allowed values: unstarted, started) </li>
%%        <li>`exch_data', a tuple of the data to exchange and the peer to
%%                  exchange the data with. Can be one of the following: </li>
%%          <ul>
%%            <li>`{undefined, undefined}'</li>
%%            <li>`{undefined, Peer::comm:mypid()}'</li>
%%            <li>`{ExchData::any(), undefined}'</li>
%%            <li>`{ExchData::any(), Peer::comm:mypid()}'</li>
%%          </ul>
%%        <li>`round', the round of the given callback </li>
%%        <li>`trigger_lock', locks triggering while within prepare-request phase
%%              (allowed values: free, locked) </li>
%%        <li>`cycles', cycle counter, </li>
%%      </ul>
-spec state_get(Key::state_key_cb(), CBModule::cb_module(), State::state()) -> any().
state_get(Key, CBModule, State) ->
    state_get({Key, CBModule}, State).


%% state_get_raw(Key, CBModule, State) ->
%%     state_get_raw({Key, CBModule}, State).
%%

%% @doc Sets the given value for the given key in the given state.
%%      Allowed keys see state_get/3
-spec state_set(Key::state_key_cb(), Value::any(), CBModule::cb_module(), State::state()) -> ok.
state_set(Key, Value, CBModule, State) ->
    state_set({Key, CBModule}, Value, State).


-spec state_update(Key::state_key_cb(), UpdateFun::fun(), CBModule::cb_module(), State::state()) -> ok.
state_update(Key, Fun, CBModule, State) ->
    Value = apply(Fun, [state_get(Key, CBModule, State)]),
    state_set(Key, Value, CBModule, State).


%%------------------------- Message Queue --------------------------%%

-spec msg_queue_add(Msg::message(), State::state()) -> ok.
msg_queue_add(Msg, State) ->
    MsgQueue = case state_get_raw(msg_queue, State) of
        undefined -> msg_queue:new();
        CurrentMsgQueue -> CurrentMsgQueue
    end,
    NewMsgQueue = msg_queue:add(MsgQueue, Msg),
    state_set(msg_queue, NewMsgQueue, State).


-spec msg_queue_send(State::state()) -> ok.
msg_queue_send(State) ->
    NewMsgQueue = case state_get_raw(msg_queue, State) of
        undefined -> msg_queue:new();
        MsgQueue ->
            msg_queue:send(MsgQueue),
            msg_queue:new()
    end,
    state_set(msg_queue, NewMsgQueue, State).

-spec get_tombstones(State::state()) -> list({cb_status, cb_module()}).
get_tombstones(State) ->
    StateList = ?PDB:tab2list(State),
    Fun = fun ({{cb_status, CBModule}, Status}, Acc) ->
            if Status =:= tombstone -> [{cb_status, CBModule}|Acc];
               Status =/= tombstone -> Acc
            end;
        (_Entry, Acc) -> Acc end,
    lists:foldl(Fun, [], StateList).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Misc
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec web_debug_info(State::state()) -> [{_,_}, ...].
web_debug_info(State) ->
    CBModules = state_get(cb_modules, State),
    Tombstones = lists:map(fun ({cb_status, CBModule}) -> CBModule end, get_tombstones(State)),
    _KeyValueList =
        [{"behaviour module",   ""},
         {"msg_queue_len",      length(state_get(msg_queue, State))},
         {"status",             state_get(status, State)},
         {"registered modules", to_string(CBModules)},
         {"tombstones",         to_string(Tombstones)}
     ].


-spec contains(Element::any(), List::list()) -> boolean().
contains(_Element, []) -> false;

contains(Element, [H|List]) ->
    if H =:= Element -> true;
       H =/= Element -> contains(Element, List)
    end.

-spec to_string(list()) -> string().
to_string(List) when is_list(List) ->
    lists:flatten(io_lib:format("~w", [List])).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% For Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec tester_create_state(Status, Range, Interval,
    CBState, CBStatus, ExchData, Round, TriggerLock, Cycles) -> state()
    when    is_subtype(Status, init | uninit),
            is_subtype(Range, intervals:interval()),
            is_subtype(Interval, pos_integer()),
            is_subtype(CBState, any()),
            is_subtype(CBStatus, unstarted | started | tombstone),
            is_subtype(ExchData, any()),
            is_subtype(Round, non_neg_integer()),
            is_subtype(TriggerLock, free | locked),
            is_subtype(Cycles, non_neg_integer()).
tester_create_state(Status, Range, Interval, CBState, CBStatus,
        ExchData, Round, TriggerLock, Cycles) ->
    State = ?PDB:new(state, ?PDB_OPTIONS),
    state_set(status, Status, State),
    state_set(cb_modules, ?CBMODULES, State),
    state_set(msg_queue, msg_queue:new(), State),
    state_set(range, Range, State),
    state_set({reply_peer, uid:get_pids_uid()}, comm:this(), State),
    state_set({trigger_group, Interval}, {?CBMODULES}, State),
    Fun = fun (CBModule) ->
            state_set(cb_state, CBState, CBModule, State),
            state_set(cb_status, CBStatus, CBModule, State),
            state_set(exch_data, {ExchData, comm:this()}, CBModule, State),
            state_set(round, Round, CBModule, State),
            state_set(trigger_lock, TriggerLock, CBModule, State),
            state_set(cycles, Cycles, CBModule, State)
    end,
    lists:foreach(Fun, ?CBMODULES),
    State.

%%% @doc Checks if a given state is a valid state.
%%%      Used as type_checker in tester.erl (property testing).
-spec is_state(State::state()) -> boolean().
is_state(State) ->
    try
        StateAsList = ?PDB:tab2list(State),
        SimpleKeys = [cb_modules, msg_queue, range],
        Fun1 = fun (Key, AccIn) ->
                case lists:keyfind(Key, 1, StateAsList) of
                    false -> AccIn andalso false;
                    _ -> AccIn andalso true
                end
        end,
        HasKeys1 = lists:foldl(Fun1, true, SimpleKeys),
        % reply_peer exlcuded
        TupleKeys = [trigger_group, cb_state, cycles, trigger_lock, exch_data, round],
        Fun2 = fun (Key, AccIn) -> AccIn andalso tuplekeyfind(Key, StateAsList) =/= false end,
        HasKeys2 = lists:foldl(Fun2, true, TupleKeys),
        HasKeys1 andalso HasKeys2
    catch
        % if ets table does not exist
        error:badarg -> false
    end.

-spec tuplekeyfind(atom(), list()) -> {{atom(), any()}, any()} | false.
tuplekeyfind(_Key, []) -> false;

tuplekeyfind(Key, [H|List]) ->
    case H of
        Tuple = {{TupleKey, _}, _} ->
            if  Key =:= TupleKey -> Tuple;
                Key =/= TupleKey -> tuplekeyfind(Key, List)
            end;
        _ -> tuplekeyfind(Key, List)
    end.

-compile({nowarn_unused_function, {init_gossip_task_feeder, 3}}).
-spec init_gossip_task_feeder(cb_module(), [1..50], state()) -> {cb_module(), list(), state()}.
init_gossip_task_feeder(CBModule, Args, State) ->
    Args1 = if length(Args)>1 -> [hd(Args)];
               true -> Args
            end,
    {CBModule, Args1, State}.

-compile({nowarn_unused_function, {request_random_node_delayed_feeder, 2}}).
-spec request_random_node_delayed_feeder(Delay::0..1000, CBModule::cb_module()) ->
    {non_neg_integer(), cb_module()}.
request_random_node_delayed_feeder(Delay, CBModule) ->
    {Delay, CBModule}.

-compile({nowarn_unused_function, {state_get_feeder, 2}}).
-spec state_get_feeder(Key::state_key(), State::state()) -> {state_key(), state()}.
state_get_feeder(Key, State) ->
    state_feeder_helper(Key, State).

-compile({nowarn_unused_function, {state_take_feeder, 2}}).
-spec state_take_feeder(Key::state_key(), State::state()) -> {state_key(), state()}.
state_take_feeder(Key, State) ->
    state_feeder_helper(Key, State).

-compile({nowarn_unused_function, {state_feeder_helper, 2}}).
-spec state_feeder_helper(state_key(), state()) -> {state_key(), state()}.
state_feeder_helper(Key, State) ->
    case Key of
        {reply_peer, _} ->
            {KeyTuple, _Value} = tuplekeyfind(reply_peer, ?PDB:tab2list(State)),
            {KeyTuple, State};
        {trigger_group, _} ->
            {KeyTuple, _Value} = tuplekeyfind(reply_peer, ?PDB:tab2list(State)),
            {KeyTuple, State};
        _ -> {Key, State}
    end.

% hack to be able to suppress warnings when testing via config:write()
-spec warn() -> log:log_level().
warn() ->
    case config:read(gossip_log_level_warn) of
        failed -> warn;
        Level -> Level
    end.

% hack to be able to suppress warnings when testing via config:write()
-spec error() -> log:log_level().
error() ->
    case config:read(gossip_log_level_error) of
        failed -> warn;
        Level -> Level
    end.

-spec check_config() -> boolean().
check_config() ->
    lists:foldl(fun({Module, _Args}, Acc) -> Acc andalso Module:check_config() end, true, ?CBMODULES).
