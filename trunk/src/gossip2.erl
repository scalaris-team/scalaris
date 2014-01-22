%  @copyright 2010-2011, 2014 Zuse Institute Berlin

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
%% @doc    Behaviour modul for gossip_beh.erl. Implements the generic code of the
%%         gossiping framework.
%%         Used abbreviations:
%%         <ul>
%%            <li> cb: callback module </li>
%%         </ul>
%%
%% @version $Id$
-module(gossip2).
-author('jensvfischer@gmail.com').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1]).
-export([init/1, activate/1, on_inactive/2, on_active/2]).

% interaction with the ring maintenance:
-export([rm_my_range_changed/3, rm_send_new_range/4]).

% testing
-export([tester_create_state/10, is_state/1]).

-define(PDB, pdb_ets).
-define(PDB_OPTIONS, [set, protected]).

% prevent warnings in the log
% (node availability is not that important to gossip)
-define(SEND_TO_GROUP_MEMBER(Pid, Process, Msg), comm:send(Pid, Msg, [{group_member, Process}, {shepherd, self()}])).

%% -define(SHOW, config:read(log_level)).
-define(SHOW, debug).

-define(CBMODULES, [gossip_load]).
-define(CBMODULES_TYPE, gossip_load).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type state() :: ets:tab().
-type state_key() :: cb_modules | msg_queue | range | status | {reply_peer, pos_integer()} |
    {trigger_group, pos_integer()} | {state_key_cb(), cb_module()} .
-type state_key_cb() :: cb_state | cb_status | cycles | trigger_lock | exch_data | round .
-type cb_fun_name() :: get_values_all | get_values_best | handle_msg |
    integrate_data | notify_change | round_has_converged | select_data |
    select_node | select_reply_data | web_debug_info.
-type cb_module() :: ?CBMODULES_TYPE.

% accepted messages of gossip behaviour module
-type(message() ::
    {activate_gossip, Range::intervals:interval()} |
    {init_gossip_task, CBModule::cb_module()} |
    {gossip2_trigger, TriggerInterval::pos_integer(), {gossip2_trigger}} |
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
    {update_range, NewRange::intervals:interval()} |
    {get_values_best, CBModule::cb_module(), SourcePid::comm:mypid()} |
    {get_values_all, CBModule::cb_module(), SourcePid::comm:mypid()} |
    {web_debug_info, SourcePid::comm:mypid()} |
    {send_error, _Pid::comm:mypid(), Msg::message(), Reason::atom()}
).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


-spec activate(Range::intervals:interval()) -> ok.
activate(MyRange) ->
    Pid = pid_groups:get_my(gossip2),
    comm:send_local(Pid, {activate_gossip, MyRange}).

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on_inactive/2, [], [{pid_groups_join_as, DHTNodeGroup, gossip2}]).

-spec init([]) -> state().
init([]) ->
    TabName = ?PDB:new(state, ?PDB_OPTIONS),
    state_set(status, uninit, TabName),
    state_set(cb_modules, [], TabName),
    TabName.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Main Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on_inactive(Msg::message(), State::state()) -> state().
on_inactive({activate_gossip, MyRange}=Msg, State) ->
    ?PDB:set({status, init}, State),

    % subscribe to ring maintenance (rm)
    rm_loop:subscribe(self(), ?MODULE,
                      fun gossip2:rm_my_range_changed/3,
                      fun gossip2:rm_send_new_range/4, inf),

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
    msg_queue_add(Msg, State),
    State;


on_inactive({get_values_all, _CBModule, _SourcePid}=Msg, State) ->
    msg_queue_add(Msg, State),
    State;


on_inactive({web_debug_info, _Requestor}=Msg, State) ->
    msg_queue_add(Msg, State),
    State;


on_inactive(_Msg, State) ->
    State.

-spec on_active(Msg::message(), State::state()) -> state().
on_active({init_gossip_task, CBModule}, State) ->
    init_gossip_task(CBModule, State),
    State;

on_active({gossip2_trigger, TriggerInterval, {gossip2_trigger}}=Msg, State) ->
    msg_queue_send(State),
    log:log(debug, "[ Gossip ] Triggered: ~w", [Msg]),
    {TriggerState, CBModules} = state_get({trigger_group, TriggerInterval}, State),
    _ = [
        begin
                case state_get(trigger_lock, CBModule, State) of
                    free ->
                        log:log(debug, "[ Gossip ] Module ~w got triggered", [CBModule]),
                        log:log(?SHOW, "[ Gossip ] Cycle: ~w, Round: ~w",
                            [state_get(cycles, CBModule, State), state_get(round, CBModule, State)]),

                        % set cycle status to active
                        state_set(trigger_lock, locked, CBModule, State),

                        % reset exch_data
                        state_set(exch_data, {undefined, undefined}, CBModule, State),

                        % request node (by the cb module or the bh module)
                        case cb_call(select_node, [], Msg, CBModule, State) of
                            true -> ok;
                            false -> request_random_node(CBModule)
                        end,

                        % request data
                        cb_call(select_data, [], Msg, CBModule, State);
                    locked -> do_nothing % ignore trigger when within prepare-request phase
                end
        end || CBModule <- CBModules
    ],

    % trigger next
    EnvPid = comm:reply_as(self(), 3, {gossip2_trigger, TriggerInterval, '_'}),
    NewTriggerState = trigger:next(TriggerState, base_interval, EnvPid),
    state_set({trigger_group, TriggerInterval}, {NewTriggerState, CBModules}, State),
    State;


on_active({selected_data, CBModule, PData}, State) ->
    %% io:format("Received selected_data msg, PData: ~w~n", [PData]),
    % check if a peer has been received already
    {Peer, _PData} = state_get(exch_data, CBModule, State),
    case Peer of
        undefined -> state_set(exch_data, {undefined, PData}, CBModule, State);
        _ -> start_p2p_exchange(Peer, PData, CBModule, State)
    end,
    State;


% re-request node if node list is empty
on_active({selected_peer, CBModule, _Msg={cy_cache, []}}, State) ->
    %% io:format("Node cache empty~n"),
    Delay = CBModule:trigger_interval(),
    request_random_node_delayed(Delay, CBModule),
    State;


on_active({selected_peer, CBModule, _Msg={cy_cache, [Node]}}, State) ->
    % This message is received as a response to a get_subset message to the
    % cyclon process and should contain a random node.
    %% io:format("gossip: got random node from Cyclon: ~p~n",[node:pidX(Node)]),
    {_Node, PData} = state_get(exch_data, CBModule, State),
    case PData of
        undefined -> state_set(exch_data, {Node, undefined}, CBModule, State);
        _ -> start_p2p_exchange(Node, PData, CBModule, State)
    end,
    State;


on_active({p2p_exch, CBModule, SourcePid, PData, OtherRound}=Msg, State) ->
    case state_get(cb_status, CBModule, State) of
        unstarted -> msg_queue_add(Msg, State);
        started ->
            log:log(debug, "[ Gossip ] State: ~w", [State]),
            log:log(debug, "[ Gossip ] p2p_exch msg received from ~w. PData: ~w",
                [SourcePid, PData]),
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
            end
    end,
    State;


on_active({selected_reply_data, CBModule, QData, Ref, Round}, State)->
    Peer = state_take({reply_peer, Ref}, State),
    log:log(debug, "[ Gossip ] selected_reply_data. CBModule: ~w, QData ~w, Peer: ~w",
        [CBModule, QData, Peer]),
    comm:send(Peer, {p2p_exch_reply, CBModule, comm:this(), QData, Round}, [{shepherd, self()}]),
    State;


on_active({p2p_exch_reply, CBModule, SourcePid, QData, OtherRound}=Msg, State) ->
    _ = case state_get(cb_status, CBModule, State) of
        unstarted -> msg_queue_add(Msg, State);
        started ->
            log:log(debug, "[ Gossip ] p2p_exch_reply, CBModule: ~w, QData ~w",
                [CBModule, QData]),
            case check_round(OtherRound, CBModule, State) of
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
            end
    end,
    State;


on_active({integrated_data, CBModule, current_round}, State) ->
    state_update(cycles, fun (X) -> X+1 end, CBModule, State),
    State;


% finishing an old round should not affect cycle counter of current round
on_active({integrated_data, _CBModule, old_round}, State) ->
    State;


% round propagation message
on_active({new_round, CBModule, NewRound}=Msg, State) ->
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


on_active({cb_reply, CBModule, Msg}=FullMsg, State) ->
    _ = cb_call(handle_msg, [Msg], FullMsg, CBModule, State),
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


on_active({get_values_best, CBModule, SourcePid}=Msg, State) ->
    BestValues = cb_call(get_values_best, [], Msg, CBModule, State),
    comm:send_local(SourcePid, {gossip_get_values_best_response, BestValues}),
    State;


on_active({get_values_all, CBModule, SourcePid}=Msg, State) ->
    {Prev, Current, Best} = cb_call(get_values_all, [], Msg, CBModule, State),
    comm:send_local(SourcePid,
        {gossip_get_values_all_response, Prev, Current, Best}),
    State;


on_active({web_debug_info, Requestor}=Msg, State) ->
    CBModules = state_get(cb_modules, State),
    Fun = fun (CBModule, Acc) -> Acc ++ [{"",""}] ++
            cb_call(web_debug_info, [], Msg, CBModule, State) end,
    KeyValueList = lists:foldl(Fun, [], CBModules),
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
    case MsgUnpacked of
        {p2p_exch, CBModule, _SourcePid, PData, Round} ->
            log:log(?SHOW, "[ Gossip ] p2p_exch failed because of ~w", [Reason]),
            _ = cb_call(notify_change, [exch_failure, {p2p_exch, PData, Round}], ErrorMsg, CBModule, State);
        {p2p_exch_reply, CBModule, QData, Round} ->
            log:log(?SHOW, "[ Gossip ] p2p_exch_reply failed because of ~w", [Reason]),
            _ = cb_call(notify_change, [exch_failure, {p2p_exch_reply, QData, Round}], ErrorMsg, CBModule, State);
        _ ->
            log:log(?SHOW, "[ Gossip ] Failed to deliever the Msg ~w because ~w", [Msg, Reason])
    end,
    State.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Msg Exchange with Peer
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


% called by either on({selected_data,...}) or on({selected_peer, ...}),
% depending on which finished first
-spec start_p2p_exchange(Peer::node:node_type(), PData::gossip_beh:exch_data(),
    CBModule::cb_module(), State::state()) -> ok.
start_p2p_exchange(Peer, PData, CBModule, State)  ->
    case node:is_me(Peer) of
        false ->
            %% io:format("starting p2p exchange. Peer: ~w, Ref: ~w~n",[Peer, Ref]),
            ?SEND_TO_GROUP_MEMBER(
                    node:pidX(Peer), gossip2,
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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Interacting with the Callback Modules
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init_gossip_tasks(State::state()) -> ok.
init_gossip_tasks(State) ->
    Fun = fun (CBModule) ->
            state_set(cb_status, unstarted, CBModule, State),
            InitDelay = CBModule:init_delay(),
            comm:send_local_after(InitDelay, self(), {init_gossip_task, CBModule})
          end,
    lists:foreach(Fun, ?CBMODULES).


-spec init_gossip_task(CBModule::cb_module(), State::state()) -> ok.
init_gossip_task(CBModule, State) ->
    % initialize CBModule
    {ok, CBState} = CBModule:init(),

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
    % todo no_msg is no solution
    _ = cb_call(notify_change, [leader, LeaderMsg], no_msg, CBModule, State),

    % configure and add trigger
    TriggerInterval = CBModule:trigger_interval(),
    {NewTriggerState, TriggerGroup} =
    case state_get_raw({trigger_group, TriggerInterval}, State) of
        undefined ->
            % create and init new trigger group
            TriggerStateInit = trigger:init(trigger_periodic, TriggerInterval, gossip2_trigger),
            EnvPid = comm:reply_as(self(), 3, {gossip2_trigger, TriggerInterval, '_'}),
            TriggerState = trigger:now(TriggerStateInit, EnvPid),
            {TriggerState, [CBModule]};
        {TriggerState, OldTriggerGroup} ->
            % add CBModule to existing trigger group
            {TriggerState, [CBModule|OldTriggerGroup]}
    end,
    state_set({trigger_group, TriggerInterval}, {NewTriggerState, TriggerGroup}, State),

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
    state_set(trigger_lock, free, CBModule, State).



-spec cb_call(FunName::cb_fun_name(), Arguments::list(), Msg::message(),
    CBModule::cb_module(), State::state()) ->
    ok | discard_msg | send_back | boolean() | {any(), any(), any()} | list({list(), list()}).
cb_call(FunName, Args, Msg, CBModule, State) ->
    CBState = state_get(cb_state, CBModule, State),
    Args1 = Args ++ [CBState],
    ReturnTuple = apply(CBModule, FunName, Args1),
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
            log:log(debug, "[ Gossip ] cb_call: ReturnTuple: ~w, ReturnValue: ~w ReturendCBState: ~w", [ReturnTuple, ReturnValue, ReturnedCBState]),
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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Requesting Peers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Round Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
    Cycles >= CBModule:min_cycles_per_round() andalso
    (   ( Cycles >= CBModule:max_cycles_per_round() ) orelse
        ( cb_call(round_has_converged, [], no_msg, CBModule, State) ) ) .


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Range/Leader Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Getters and Setters
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




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
            log:log(error, "[ gossip2 ] Lookup of ~w in ~w failed", [Key, State]),
            error(lookup_failed, [Key, State])
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
            log:log(error, "[ gossip2 ] Take of ~w in ~w failed", [Key, State]),
            error(lookup_failed, [Key, State])
    end.

-spec state_set(Key::state_key(), Value::any(), State::state()) -> ok.
state_set(Key, Value, State) ->
    ?PDB:set({Key, Value}, State).


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

%% @doc Sets the given value for the given key in the given state.
%%      Allowed keys see state_get/3
-spec state_set(Key::state_key_cb(), Value::any(), CBModule::cb_module(), State::state()) -> ok.
state_set(Key, Value, CBModule, State) ->
    state_set({Key, CBModule}, Value, State).


-spec state_update(Key::state_key_cb(), UpdateFun::fun(), CBModule::cb_module(), State::state()) -> ok.
state_update(Key, Fun, CBModule, State) ->
    Value = apply(Fun, [state_get(Key, CBModule, State)]),
    state_set(Key, Value, CBModule, State).


%% Message Queue %%

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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% For Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec tester_create_state(Status, Range, Interval, TriggerState,
    CBState, CBStatus, ExchData, Round, TriggerLock, Cycles) -> state()
    when    Status :: init | uninit,
            Range :: intervals:interval(),
            Interval :: pos_integer(),
            TriggerState :: trigger:state(),
            CBState :: any(),
            CBStatus :: unstarted | started,
            ExchData :: any(),
            Round :: non_neg_integer(),
            TriggerLock :: free | locked,
            Cycles :: non_neg_integer().
tester_create_state(Status, Range, Interval, TriggerState, CBState, CBStatus,
        ExchData, Round, TriggerLock, Cycles) ->
    State = ?PDB:new(state, ?PDB_OPTIONS),
    state_set(status, Status, State),
    state_set(cb_modules, ?CBMODULES, State),
    state_set(msg_queue, msg_queue:new(), State),
    state_set(range, Range, State),
    state_set({reply_peer, uid:get_pids_uid()}, comm:this(), State),
    state_set({trigger_group, Interval}, {TriggerState, ?CBMODULES}, State),
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


-spec state_get_feeder(Key::state_key(), State::state()) -> {state_key(), state()}.
state_get_feeder(Key, State) ->
    state_feeder_helper(Key, State).


-spec state_take_feeder(Key::state_key(), State::state()) -> {state_key(), state()}.
state_take_feeder(Key, State) ->
    state_feeder_helper(Key, State).


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
