%  @copyright 2010-2015 Zuse Institute Berlin

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
%%
%% @doc The behaviour modul (gossip_beh.erl) of the gossiping framework.
%%
%%      The framework is designed to allow the implementation of gossip based
%%      dissemination and gossip based aggregation protocols. Anti-entropy
%%      gossiping was not considered. The communication scheme used by the
%%      framework is push-pull gossiping as this offers the best speed of
%%      convergence. The membership protocol used for the peer selection is
%%      Cyclon.
%%
%%      The gossiping framework comprises three kinds of components:
%%      <ol>
%%          <li> The gossiping behaviour (interface) gossip_beh.erl. The
%%               behaviour defines the contract that allows the callback module
%%               to be used by the behaviour module. The behaviour defines the
%%               contract by specifying functions the callback module has to
%%               implement. </li>
%%          <li> The callback modules. A callback module implements a concrete
%%               gossiping protocol by implementing the gossip_beh.erl, i.e. by
%%               implementing the functions specified in the gossip_beh.erl.
%%               The callback module provides the protocol specific code.
%%               For an example callback module see gossip_load.erl.</li>
%%          <li> The behaviour module gossip.erl (this module). The behaviour
%%               module provides the generic code of the gossiping  framework.
%%               It calls the callback functions of the callback modules defined
%%               in gossip_beh.erl.</li>
%%      </ol>
%%
%%      The relation between behaviour and callback modules is modelled as a
%%      one-to-many relation. That is to say, the behaviour module is implemented
%%      as single process (per node) and all the callback module run in the
%%      context of this single process. This has the advantage of reducing the
%%      number of spawned processes and allowing for a better grouping of messages.
%%
%%      The framework is started as part of the startup procedure of a dht_node.
%%      The framework maintains a list of callback modules in the CBMODULES macro
%%      which are started together with the framework. It is also possible to
%%      individually start and stop callback modules later.
%%
%%      The pattern for communication between the behaviour module and a callback
%%      module is the following: From the behaviour module to a callback module
%%      communication occurs as a call to a function of the callback module.
%%      These calls have to return quickly, no long-lasting operations, especially
%%      no receiving of messages, are allowed. Therefore, the answers to these
%%      function calls are mainly realised as messages from the respective
%%      callback module to the behaviour module, not as return values of the
%%      function calls.
%%
%%      == Phases of a Gossiping Operation ==
%%
%%      === Prepare-Request Phase ===
%%
%%      The  prepare-request phase consists of peer and data selection. The
%%      selection of the peer is usually managed by the framework. At the beginning
%%      of every cycle the behaviour module requests a peer from the Cyclon
%%      module of Scalaris, which is then used for the data exchange. The peer
%%      selection is governed by the select_node() function: returning
%%      false causes the behaviour module to handle the peer selection as described.
%%      Returning true causes the behaviour module to expect a selected_peer
%%      message with a peer to be used by for the exchange. How many peers are
%%      contracted for data exchanges every cycle depends on the fanout() config
%%      function.
%%
%%      The selection of the exchange data is dependent on the specific gossiping
%%      task and therefore done by a callback module. It is initiated by a call
%%      to select_data(). When called with select_data(), the respective callback
%%      module has to initiate a selected_data message to the behaviour module,
%%      containing the selected exchange data. Both peer and data selection are
%%      initiated in immediate succession through periodical trigger messages,
%%      so they can run concurrently. When both data and peer are received by
%%      the behaviour module, a p2p_exch message with the exchange data is sent
%%      to the peer, that is to say to the gossip behaviour module of the peer.
%%
%%      === Prepare-Reply Phase ===
%%
%%      Upon receiving a p2p_exch message, a node enters the prepare-reply
%%      phase and is now in its passive role as responder. This phase is about
%%      the integration of the received data and the preparation of the reply data.
%%      Both of these tasks need to be handled by the callback module. The
%%      behaviour module passes the received data with a call to select_reply_data(QData)
%%      to the correspondent callback module, which merges the data with its own
%%      local data and prepares the reply data. The reply data is sent back to
%%      the behaviour module with a selected_reply_data message. The behaviour
%%      module then sends the reply data as a  p2p_exch_reply message back to
%%      the original requester.
%%
%%      === Integrate-Reply Phase ===
%%
%%      The integrate-reply phase is triggered by a p2p_exch_reply message.
%%      Every p2p_exch_reply is the response to an earlier p2p_exch (although
%%      not necessarily to the last p2p_exch request. The p2p_exch_reply contains
%%      the reply data from the peer, which is passed to the correspondent
%%      callback module with a call to integrate_data(QData). The callback module
%%      processes the received data and signals to the behaviour module the
%%      completion with an integrated_data message. On a conceptual level, a full
%%      cycle is finished at this point and the behaviour module counts cycles
%%      by counting the integrated_data messages. Due to the uncertainties
%%      of message delays and local clock drift it should be clear however, that
%%      this can only be an approximation. For instance, a new cycle could have
%%      been started before the reply to the current request has been received
%%      (phase interleaving) and, respectively, replies from the other cycle could
%%      be "wrongly" counted as finishing the current cycle (cycle interleaving).
%%
%%      == Instantiation ==
%%
%%      Many of the interactions conducted by the behaviour module are specific
%%      to a certain callback module. Therefore, all messages and function
%%      concerning a certain callback module need to identify with which callback
%%      module the message or call is associated. This is achieved by adding a
%%      tuple of the module name and an instance id to all those messages and
%%      calls. While the name would be enough to identify the module, adding the
%%      instance id allows for multiple instantiation of the same callback module
%%      by one behaviour module. This tuple of callback module and instance id
%%      is also used to store information specific to a certain callback module
%%      in the behaviour module's state.
%%
%%
%%      == Messages to the Callback Modules (cb_msg) ==
%%
%%      Messages which shall be passed directly to a callback module need to have
%%      the form {cb_msg, CModule, Msg}, where CBModule is of type cb_module() and
%%      Msg is any message the respective callback module handles.
%%
%%      Messages in this form are unpacked by the gossip module and only the Msg
%%      is send to the given CMModule.
%%
%%      If a callback module wants to receive a response from another process, it
%%      should pack its Pid with an envelope of the form
%%          {PidOfRequestor, e, 3, {cb_msg, CBModule, '_'}}
%%      with a call to
%%          EnvPid = comm:reply_as(PidOfRequestor, 3, {cb_msg, CBModule, '_'})
%%      and use the EnvPid as SourcePid when sending the request, e.g.
%%          comm:send(Pid, {get_dht_nodes, EnvPid}, [{?quiet}])
%%
%%
%%      == Used abbreviations ==
%%
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
-include("record_helpers.hrl").

% interaction with gen_component
-export([init/1, on_inactive/2, on_active/2]).

%API
-export([start_link/1, activate/1, remove_all_tombstones/0, check_config/0]).

% interaction with the ring maintenance:
-export([rm_filter_slide_msg/3, rm_send_activation_msg/5, rm_my_range_changed/3, rm_send_new_range/5]).

% testing and debugging
-export([start_gossip_task/2, stop_gossip_task/1, tester_create_cb_module_names/1]).

-include("gen_component.hrl").

%% -define(PDB, pdb_ets). % easier debugging because state accesible from outside the process
-define(PDB_OPTIONS, [set]).
-define(PDB, pdb). % better performance

% prevent warnings in the log
-define(SEND_TO_GROUP_MEMBER(Pid, Process, Msg),
        comm:send(Pid, Msg, [{group_member, Process}, {shepherd, self()}])).

%% -define(SHOW, config:read(log_level)).
-define(SHOW, debug).

%-define(TRACE(X,Y), log:pal(X,Y)).
%% -define(TRACE(X,Y), ok).
-define(TRACE_TRIGGER(FormatString, Data), ok).
%% -define(TRACE_TRIGGER(FormatString, Data), log:pal(FormatString, Data)).
-define(TRACE_ROUND(FormatString, Data), ok).
%% -define(TRACE_ROUND(FormatString, Data), log:pal(FormatString, Data)).

%% list of callback modules to be activated on startup
-define(CBMODULES, [{gossip_load, default}, {gossip_cyclon, default}, {gossip_vivaldi, default}]).

% for developement, should be disabled for production
-define(FIRST_TRIGGER_DELAY, 0). % delay in s for first trigger


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Type Definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type status() :: init | uninit.

-type cb_module_name() :: module().
-type cb_module_id() :: Id :: atom() | uid:global_uid().
-type cb_module() :: {Module::cb_module_name(), Id::cb_module_id()}.
-type cb_status() :: unstarted | started | tombstone.
-type exch_data() :: { ExchData :: undefined | any(), Peer :: undefined | comm:mypid()}.

-type cb_fun_name() :: handle_msg | integrate_data | notify_change | round_has_converged |
                       select_data | select_node | select_reply_data | web_debug_info | shutdown.

% state record
-record(state, {
        cb_modules = [] :: [cb_module()] ,
        msg_queue = msg_queue:new() :: msg_queue:msg_queue(),
        range = intervals:all() :: intervals:interval(),
        status = uninit  :: init | uninit,
        trigger_add = [] :: [pos_integer()],
        trigger_groups = [] :: [{TriggerInterval::pos_integer(), CBModules::[cb_module()]}],
        trigger_locks = [] :: [{cb_module(), locked | free}],
        trigger_remove = [] :: [pos_integer()],
        cb_states = [] :: [{cb_module(), any()}],
        cb_stati = [] :: [{cb_module(), cb_status()}],
        cycles = [] :: [{cb_module(), non_neg_integer()}],
        exch_datas = [] :: [{cb_module(), exch_data()}],
        reply_peers = [] :: [{Ref::pos_integer(), Pid::comm:mypid()}],
        rounds= [] :: [{cb_module(), non_neg_integer()}]
}).

-type state() :: #state{}.

% accepted messages of gossip behaviour module
-type send_error() :: {send_error, _Pid::comm:mypid(), Msg::message(), Reason::atom()}.

-type bh_message() ::
    {activate_gossip, Neighbors::nodelist:neighborhood()} |
    {start_gossip_task, CBModule::cb_module(), Args::list()} |
    {gossip_trigger, TriggerInterval::pos_integer()} |
    {trigger_action, TriggerInterval::pos_integer()} |
    {update_range, NewRange::intervals:interval()} |
    {web_debug_info, SourcePid::comm:mypid()} |
    send_error() |
    {bulkowner, deliver, Id::uid:global_uid(), Range::intervals:interval(),
        Msg::comm:message(), Parents::[comm:mypid(),...]} |
    {remove_all_tombstones}.

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
    {cb_msg, CBModule::cb_module(), Msg::comm:message()} |
    {stop_gossip_task, CBModule::cb_module()} |
    {no_msg}.

-type message() :: bh_message() | cb_message().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start the process of the gossip module. <br/>
%%      Called by sup_dht_node, calls gen_component:start_link to start the process.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on_inactive/2, [],
                             [{wait_for_init},
                              {pid_groups_join_as, DHTNodeGroup, gossip}]).


%% @doc Initialises the state of the gossip module. <br/>
%%      Called by gen_component, results in on_inactive handler.
-spec init([]) -> state().
init([]) ->
    State = #state{},

    % initialise a base trigger
    msg_delay:send_trigger(?FIRST_TRIGGER_DELAY,  {gossip_trigger, 1}),
    State.


%% @doc Activate the gossip module. <br/>
%%      Called by dht_node_join. Activates process (when only node of the system)
%%      or subscribes to the rm to activate on slide_finished messages. <br/>
%%      Result of the activation is to switch to the on_active handler.
-spec activate(Neighbors::nodelist:neighborhood()) -> ok.
activate(Neighbors) ->
    case nodelist:node_range(Neighbors) =:= intervals:all() of
        true ->
            % We're the first node covering the whole ring range.
            % Start gossip right away because it's needed for passive
            % load balancing when new nodes join the ring.
            comm:send_local(pid_groups:get_my(gossip), {activate_gossip, Neighbors});
        _    ->
            % subscribe to ring maintenance (rm) for {slide_finished, succ} or {slide_finished, pred}
            rm_loop:subscribe(self(), ?MODULE,
                              fun gossip:rm_filter_slide_msg/3,
                              fun gossip:rm_send_activation_msg/5, 1)
    end.


%% @doc Globally removes all tombstones from previously stopped callback modules.
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
-spec rm_send_activation_msg(Subscriber, ?MODULE, Neighbours, Neighbours, Reason) -> ok when
                             is_subtype(Subscriber, pid()),
                             is_subtype(Neighbours, nodelist:neighborhood()),
                             is_subtype(Reason, rm_loop:reason()).
rm_send_activation_msg(_Pid, ?MODULE, _OldNeighbours, NewNeighbours, _Reason) ->
    %% io:format("Pid: ~w. Self: ~w. PidGossip: ~w~n", [Pid, self(), Pid2]),
    Pid = pid_groups:get_my(gossip),
    comm:send_local(Pid, {activate_gossip, NewNeighbours}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Main Message Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%-------------------------- on_inactive ---------------------------%%


%% @doc Message handler during the startup of the gossip module.
-spec on_inactive(Msg::message(), State::state()) -> state().
on_inactive({activate_gossip, Neighbors}, State) ->
    MyRange = nodelist:node_range(Neighbors),
    State1 = state_set(status, init, State),

    % subscribe to ring maintenance (rm)
    rm_loop:subscribe(self(), ?MODULE,
                      fun gossip:rm_my_range_changed/3,
                      fun gossip:rm_send_new_range/5, inf),

    % set range and notify cb modules about leader state
    State2 = state_set(range, MyRange, State1),
    Msg1 = case is_leader(MyRange) of
        true -> {is_leader, MyRange};
        false -> {no_leader, MyRange}
    end,
    State3 = lists:foldl(fun(CBModule, StateIn) ->
                                 NewState = cb_notify_change(leader, Msg1, CBModule, StateIn),
                                 NewState
                         end, State2, state_get(cb_modules, State2)),

    State4 = init_gossip_tasks(Neighbors, State3),
    State5 = msg_queue_send(State4),

    % change handler to on_active
    gen_component:change_handler(State5, fun ?MODULE:on_active/2);


on_inactive({gossip_trigger, TriggerInterval}=_Msg, State) ->
    ?TRACE_TRIGGER("[ Gossip ] on_inactive: ~w", [_Msg]),
    handle_trigger(TriggerInterval, State);


on_inactive({p2p_exch, _CBModule, SourcePid, _PData, _Round}=Msg, State) ->
    comm:send(SourcePid, {send_error, comm:this(), Msg, on_inactive}),
    State;


on_inactive({p2p_exch_reply, _CBModule, SourcePid, _QData, _Round}=Msg, State) ->
    comm:send(SourcePid, {send_error, comm:this(), Msg, on_inactive}),
    State;

%% for debugging
on_inactive(print_state, State) ->
    log:log(warn, "~s", [to_string(State)]),
    State;


on_inactive({web_debug_info, _Requestor}=Msg, State) ->
    msg_queue_add(Msg, State);


on_inactive({stop_gossip_task, _CBModule}=Msg, State) ->
    msg_queue_add(Msg, State);


on_inactive({start_gossip_task, _CBModule, _Args}=Msg, State) ->
    msg_queue_add(Msg, State);


on_inactive({remove_all_tombstones}=Msg, State) ->
    msg_queue_add(Msg, State);


on_inactive({cb_msg, _CBModule, _msg}=Msg, State) ->
    msg_queue_add(Msg, State);

%% consume all other message (inluding: trigger messages)
on_inactive(_Msg, State) ->
    State.


%%--------------------------- on_active ----------------------------%%

%% @doc Message handler during the normal operation of the gossip module.
%% @end

-spec on_active(Msg::message(), State::state()) -> state().
on_active({deactivate_gossip}, _State) ->
    rm_loop:unsubscribe(self(), ?MODULE),
    gen_component:change_handler(#state{}, fun ?MODULE:on_inactive/2);


%% This message is received from self() from init_gossip_task or through
%% start_gossip_task()/bulkowner
on_active({start_gossip_task, CBModule, Args}, State) ->
    CBModules = state_get(cb_modules, State),
    State1 = case lists:member(CBModule, CBModules) of
        true ->
            log:log(warn, "[ Gossip ] Trying to start an already existing Module: ~w ."
                ++ "Request will be ignored.", [CBModule]),
            State;
        false -> init_gossip_task(CBModule, Args, State)
    end,
    State1;


%% Trigger message starting a new cycle.
on_active({gossip_trigger, TriggerInterval}=_Msg, State) ->
    ?TRACE_TRIGGER("[ Gossip ]:~w", [_Msg]),
    State1 = handle_trigger(TriggerInterval, State),
    case state_get({trigger_group, TriggerInterval}, State) of
        false ->
            State1; %% trigger group does not exist, do nothing (e.g. during startup)
        CBModules ->
            [ comm:send_local(self(), {trigger_action, CBModule}) || CBModule <- CBModules ],
            State1
    end;


on_active({trigger_action, CBModule}=_Msg, State) ->
    State1 = msg_queue_send(State),

    case state_get({trigger_lock, CBModule}, State1) of
        free ->
            log:log(debug, "[ Gossip ] Module ~w got triggered", [CBModule]),
            log:log(?SHOW, "[ Gossip ] Cycle: ~w, Round: ~w",
                    [state_get({cycle, CBModule}, State1), state_get({round, CBModule}, State1)]),

            %% set cycle status to active
            State2 = state_set({trigger_lock, CBModule}, locked, State1),

            %% reset exch_data
            State3 = state_set({exch_data, CBModule}, {undefined, undefined}, State2),

            %% request node (by the cb module or the bh module)
            State4 = case cb_select_node(CBModule, State3) of
                {true, NewState} -> NewState;
                {false, NewState} -> request_random_node(CBModule), NewState
            end,

            %% request data
            State5 = cb_select_data(CBModule, State4),
            State5;
        locked -> State1 % ignore trigger when within prepare-request phase
    end;


%% received through the rm on key range changes
on_active({update_range, NewRange}, State) ->
    State1 = state_set(range, NewRange, State),
    Msg = case is_leader(NewRange) of
        true -> {is_leader, NewRange};
        false -> {no_leader, NewRange}
    end,
    Fun = fun (CBModule, StateIn) ->
                StateOut = cb_notify_change(leader, Msg, CBModule, StateIn),
                StateOut
          end,
    CBModules = state_get(cb_modules, State1),
    lists:foldl(Fun, State1, CBModules);


%% request for debug info
on_active({web_debug_info, Requestor}, State) ->
    CBModules = state_get(cb_modules, State),
    Fun = fun(CBModule, {KVListIn, StateIn}) ->
            {KVListCBModule, NewState} = cb_web_debug_info(CBModule, StateIn),
            {KVListIn ++ [{"",""}] ++ KVListCBModule, NewState}
          end,
    {KVListCBModule, State1} = lists:foldr(Fun, {[], State}, CBModules),
    KVListAll = [{"",""}] ++ web_debug_info(State) ++ KVListCBModule,
    comm:send_local(Requestor, {web_debug_info_reply, KVListAll}),
    State1;


%% received from shepherd, from on_inactive or from rejected messages
on_active({send_error, _Pid, Msg, Reason}=ErrorMsg, State) ->
    % unpack msg if necessary
    MsgUnpacked = case Msg of
        % msg from shepherd
        {_, ?MODULE, OriginalMsg} -> OriginalMsg;
        % other send_error msgs, e.g. from on_inactive
        _Msg -> _Msg
    end,
    CBStatus = state_get({cb_status, element(2, MsgUnpacked)}, State),
    case MsgUnpacked of
        _ when CBStatus =:= tombstone ->
            log:log(warn(), "[ Gossip ] Got ~w msg for tombstoned module ~w. Reason: ~w. Original Msg: ~w",
                [element(1, ErrorMsg), element(2, MsgUnpacked), Reason, element(1, Msg)]),
            State;
        {p2p_exch, CBModule, _SourcePid, PData, Round} ->
            log:log(warn(), "[ Gossip ] p2p_exch from ~w (gossip) to ~w (dht_node)" ++
                    " failed because of ~w", [_SourcePid, _Pid, Reason]),
            NewState1 = cb_notify_change(exch_failure, {p2p_exch, PData, Round}, CBModule, State),
            NewState1;
        {p2p_exch_reply, CBModule, _SourcePid, QData, Round} ->
            log:log(warn(), "[ Gossip ] p2p_exch_reply failed because of ~w", [Reason]),
            NewState1 = cb_notify_change(exch_failure, {p2p_exch_reply, QData, Round}, CBModule, State),
            NewState1;
        _ ->
            log:log(?SHOW, "[ Gossip ] Failed to deliever the Msg ~w because ~w", [Msg, Reason]),
            State
    end;


%% unpack bulkowner msg
on_active({bulkowner, deliver, _Id, _Range, Msg, _Parents}, State) ->
    comm:send_local(self(), Msg),
    State;


%% received through remove_all_tombstones()/bulkowner
on_active({remove_all_tombstones}, State) ->
    lists:foldl(fun(CBModule, StateIn) -> state_remove({cb_status, CBModule}, StateIn) end,
                State, get_tombstones(State));


%% for debugging
on_active(print_state, State) ->
    log:log(warn, "~s", [to_string(State)]),
    State;

%% for debugging
on_active({get_state, SourcePid}, State) ->
    comm:send(SourcePid, State),
    %% log:log(warn, "~s", [to_string(State)]),
    State;

%% Only messages for callback modules are expected to reach this on_active clause.
%% they have the form:
%%   {MsgTag, CBModule, ...}
%%   element(1, Msg) = MsgTag
%%   element(2, Msg) = CBModule
on_active(Msg, State) ->
    State1 = try state_get({cb_status, element(2, Msg)}, State) of
        tombstone ->
            log:log(warn(), "[ Gossip ] Got ~w msg for tombstoned module ~w",
                [element(1, Msg), element(2, Msg)]),
            State;
        started ->
            handle_msg(Msg, State);
        false ->
            log:log(warn, "[ Gossip ] Unknown Callback Module: ~w", [element(2, Msg)]),
            State
    catch
        _:_ -> log:log(warn, "[ Gossip ] Unknown msg: ~w", [Msg]),
               State
    end,
    State1.


%% This message is received as a response to a get_subset message to the
%% cyclon process and should contain a list of random nodes.
-spec handle_msg(Msg::cb_message(), State::state()) -> state().
% re-request node if node list is empty
handle_msg({selected_peer, CBModule, _Msg={cy_cache, []}}, State) ->
    Delay = cb_config(trigger_interval, CBModule),
    request_random_node_delayed(Delay, CBModule),
    State;


handle_msg({selected_peer, CBModule, _Msg={cy_cache, Nodes}}, State) ->
    log:log(info, "selected_peer: ~w, ~w", [CBModule, _Msg]),
    {_Node, PData} = state_get({exch_data, CBModule}, State),
    case PData of
        undefined -> state_set({exch_data, CBModule}, {Nodes, undefined}, State);
        _ -> start_p2p_exchange(Nodes, PData, CBModule, State)
    end;


%% This message is a reply from a callback module to CBModule:select_data()
handle_msg({selected_data, CBModule, PData}, State) ->
    % check if a peer has been received already
    {Peer, _PData} = state_get({exch_data, CBModule}, State),
    case Peer of
        undefined -> state_set({exch_data, CBModule}, {undefined, PData}, State);
        _ -> start_p2p_exchange(Peer, PData, CBModule, State)
    end;


%% This message is a request from another peer (i.e. another gossip module) to
%% exchange data, usually results in CBModule:select_reply_data()
handle_msg({p2p_exch, CBModule, SourcePid, PData, OtherRound}=Msg, State) ->
    log:log(debug, "[ Gossip ] p2p_exch msg received from ~w. PData: ~w",[SourcePid, PData]),
    State1 = state_set({reply_peer, Ref=uid:get_pids_uid()}, SourcePid, State),
    case check_round(OtherRound, CBModule, State1) of
        {ok, State2} ->
            cb_select_reply_data(PData, Ref, OtherRound, Msg, CBModule, State2);
        {start_new_round, State2} -> % self is leader
            ?TRACE_ROUND("[ Gossip ] Starting a new round in p2p_exch", []),
            State3 = cb_notify_change(new_round, state_get({round, CBModule}, State2), CBModule, State2),
            State4 = cb_select_reply_data(PData, Ref, OtherRound, Msg, CBModule, State3),
            comm:send(SourcePid, {new_round, CBModule, state_get({round, CBModule}, State4)}),
            State4;
        {enter_new_round, State2} ->
            ?TRACE_ROUND("[ Gossip ] Entering a new round in p2p_exch", []),
            State3 = cb_notify_change(new_round, state_get({round, CBModule}, State2), CBModule, State2),
            State4 = cb_select_reply_data(PData, Ref, OtherRound, Msg, CBModule, State3),
            State4;
        {propagate_new_round, State2} -> % i.e. MyRound > OtherRound
            ?TRACE_ROUND("[ Gossip ] propagate round in p2p_exch", []),
            State3 = cb_select_reply_data(PData, Ref, OtherRound, Msg, CBModule, State2),
            comm:send(SourcePid, {new_round, CBModule, state_get({round, CBModule}, State3)}),
            State3
    end;


%% This message is a reply from a callback module to CBModule:select_reply_data()
handle_msg({selected_reply_data, CBModule, QData, Ref, Round}, State)->
    case take_reply_peer(Ref, State) of
        {none, State1} ->
            log:log(warn, "[ Gossip ] Got 'selected_reply_data', but no matching reply peer stored in State.");
        {Peer, State1} ->
            comm:send(Peer, {p2p_exch_reply, CBModule, comm:this(), QData, Round}, [{shepherd, self()}])
    end,
    log:log(debug, "[ Gossip ] selected_reply_data. CBModule: ~w, QData ~w",
        [CBModule, QData]),
    State1;


%% This message is a reply from another peer (i.e. another gossip module) to
%% a p2p_exch request, usually results in CBModule:integrate_data()
handle_msg({p2p_exch_reply, CBModule, SourcePid, QData, OtherRound}=Msg, State) ->
    log:log(debug, "[ Gossip ] p2p_exch_reply, CBModule: ~w, QData ~w", [CBModule, QData]),
    case check_round(OtherRound, CBModule, State) of
        {ok, State1} ->
            cb_integrate_data(QData, OtherRound, Msg, CBModule, State1);
        {start_new_round, State1} -> % self is leader
            ?TRACE_ROUND("[ Gossip ] Starting a new round p2p_exch_reply", []),
            State2 = cb_notify_change(new_round, state_get({round, CBModule}, State1), CBModule, State1),
            State3 = cb_integrate_data(QData, OtherRound, Msg, CBModule, State2),
            comm:send(SourcePid, {new_round, CBModule, state_get({round, CBModule}, State3)}),
            State3;
        {enter_new_round, State1} ->
            ?TRACE_ROUND("[ Gossip ] Entering a new round p2p_exch_reply", []),
            State2 = cb_notify_change(new_round, state_get({round, CBModule}, State1), CBModule, State1),
            cb_integrate_data(QData, OtherRound, Msg, CBModule, State2);
        {propagate_new_round, State1} -> % i.e. MyRound > OtherRound
            ?TRACE_ROUND("[ Gossip ] propagate round in p2p_exch_reply", []),
            comm:send(SourcePid, {new_round, CBModule, state_get({round, CBModule}, State1)}),
            cb_integrate_data(QData, OtherRound, Msg, CBModule, State1)
    end;


%% This message is a reply from a callback module to CBModule:integrate_data()
%% Markes the end of a cycle
handle_msg({integrated_data, CBModule, cur_round}, State) ->
    state_update({cycle, CBModule}, fun (X) -> X+1 end, State);


% finishing an old round should not affect cycle counter of current round
handle_msg({integrated_data, _CBModule, prev_round}, State) ->
    State;


%% pass messages for callback modules to the respective callback module
%% messages to callback modules need to have the form {cb_msg, CBModule, Msg}.
%% Use envelopes if necessary.
handle_msg({cb_msg, CBModule, Msg}, State) ->
    cb_handle_msg(Msg, CBModule, State);


% round propagation message
handle_msg({new_round, CBModule, NewRound}, State) ->
    MyRound = state_get({round, CBModule}, State),
    if
        MyRound < NewRound ->
            ?TRACE_ROUND("[ Gossip ] Entering new round via round propagation message", []),
            State1 = cb_notify_change(new_round, NewRound, CBModule, State),
            State2 = state_set({round, CBModule}, NewRound, State1),
            state_set({cycle, CBModule}, 0, State2);
        MyRound =:= NewRound -> % i.e. the round propagation msg was already received
            ?TRACE_ROUND("[ Gossip ] Received propagation msg for round i'm already in", []),
            State;
        MyRound > NewRound ->
            ?TRACE_ROUND("[ Gossip ] MyRound > OtherRound", []),
            State
    end;


%% Received through stop_gossip_task/bulkowner
%% Stops gossip tasks and cleans state of all garbage
%% sets tombstone to handle possible subsequent request for already stopped tasks
handle_msg({stop_gossip_task, CBModule}, State) ->
    log:log(?SHOW, "[ Gossip ] Stopping ~w", [CBModule]),
    % shutdown callback module
    State1 = cb_shutdown(CBModule, State),

    % delete callback module dependent entries from state
    State2 = state_remove_cb(CBModule, State1),

    % remove from list of modules
    State3 = state_update(cb_modules, fun(Modules) -> lists:delete(CBModule, Modules) end, State2),

    % remove from trigger group
    Interval = cb_config(trigger_interval, CBModule) div 1000,
    CBModules = state_get({trigger_group, Interval}, State),
    NewCBModules = lists:delete(CBModule, CBModules),
    State4 = case NewCBModules of
        [] ->
            NewState = state_set({trigger_group, Interval}, NewCBModules, State3),
            state_update(trigger_remove, fun (Intervals) -> [Interval|Intervals] end, NewState);
        _ ->
            state_set({trigger_group, Interval}, NewCBModules, State3)
    end,

    % set tombstone
    State5 = state_set({cb_status, CBModule}, tombstone, State4),
    State5.


%% @doc Renew trigger message and handle adding/removal of triggers.
%%      To avoid infecting mpath traces with infinite triggers, a basetrigger with
%%      interval 1s is started during the startup of the process. This trigger is
%%      uninfected, even if it belongs to a node which was added during infection.
%%
%%      To add and remove callback module specific triggers without infecting the triggers,
%%      the request for the addition/removel of a trigger is saved to the state and
%%      processed during the handling of the base trigger.
-spec handle_trigger(TriggerInterval::non_neg_integer(), state()) -> state().
handle_trigger(TriggerInterval, State) ->
    % check for trigger removal
    State1 = case lists:member(TriggerInterval, state_get(trigger_remove, State)) of
        true -> % remove trigger by not renewing the trigger
            ?TRACE_TRIGGER("Remove trigger: ~w", [TriggerInterval]),

            % unconditionally renew basetrigger, even if the last trigger was removed
            if TriggerInterval =:= 1 ->
                msg_delay:send_trigger(TriggerInterval, {gossip_trigger, TriggerInterval});
               TriggerInterval =/= 1 -> ok
            end,

            state_update(trigger_remove,
                fun (Triggers) -> lists:delete(TriggerInterval, Triggers) end, State);

        false -> % renew trigger
            msg_delay:send_trigger(TriggerInterval, {gossip_trigger, TriggerInterval}),
            State
    end,

    % check for new new triggers to add
    case state_get(trigger_add, State1) of
        [] -> State1;
        NewTriggerIntervals ->
            ?TRACE_TRIGGER("Add triggers: ~w", [NewTriggerIntervals]),
            lists:foreach(fun (NewTriggerInterval) ->
                                msg_delay:send_trigger(NewTriggerInterval, {gossip_trigger, NewTriggerInterval})
                          end, NewTriggerIntervals),
            state_set(trigger_add, [], State1)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Msg Exchange with Peer
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% called by either on({selected_data,...}) or on({selected_peer, ...}),
% depending on which finished first
-spec start_p2p_exchange(Peers::[node:node_type(),...], PData::gossip_beh:exch_data(),
    CBModule::cb_module(), State::state()) -> state().
start_p2p_exchange(Peers, PData, CBModule, State)  ->
    SendToPeer = fun(Peer, StateIn) ->
        case node:is_me(Peer) of
            false ->
                %% log:log(warn, "starting p2p exchange. Peer: ~w~n",[Peer]),
                ?SEND_TO_GROUP_MEMBER(
                        node:pidX(Peer), gossip,
                        {p2p_exch, CBModule, comm:this(), PData, state_get({round, CBModule}, StateIn)}),
                state_set({trigger_lock, CBModule}, free, StateIn);
            true  ->
                %% todo does this really happen??? cyclon should not have itself in the cache
                log:log(?SHOW, "[ Gossip ] Node was ME, requesting new node"),
                request_random_node(CBModule),
                {Peer, Data} = state_get({exch_data, CBModule}, StateIn),
                state_set({exch_data, CBModule}, {undefined, Data}, StateIn)
        end
    end,
    lists:foldl(SendToPeer, State, Peers).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Interacting with the Callback Modules
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% called when activating gossip module
-spec init_gossip_tasks(nodelist:neighborhood(), state()) -> state().
init_gossip_tasks(Neighbors, State) ->
    Init = fun (CBModule, StateIn) ->
            _StateOut = init_gossip_task(CBModule, [{neighbors, Neighbors}], StateIn)
          end,
    lists:foldl(Init, State, ?CBMODULES).


%% initialises a gossip task / callback mdoule
%% called on activation of gossip module or on start_gossip_task message
-spec init_gossip_task(CBModule::cb_module(), Args::list(), State::state()) -> state().
init_gossip_task(CBModule, Args, State) ->

    % initialize CBModule
    CBState = cb_init([{instance, CBModule} | Args], CBModule),
    State1 = state_set({cb_state, CBModule}, CBState, State),
    State2 = lists:foldl(fun({Key, Value}, StateIn) -> state_set({Key, CBModule}, Value, StateIn) end,
                         State1,
                         [{cb_status, started}, {cycle, 0}, {exch_data,
                          {undefined, undefined}}, {round, 0}, {trigger_lock, free}]
                        ),

    % notify cb module about leader state
    MyRange = state_get(range, State2),
    LeaderMsg = case is_leader(MyRange) of
        true -> {is_leader, MyRange};
        false -> {no_leader, MyRange}
    end,

    State3 = cb_notify_change(leader, LeaderMsg, CBModule, State2),

    % configure and add trigger
    TriggerInterval = cb_config(trigger_interval, CBModule) div 1000,
    ?TRACE_TRIGGER("Initiating Trigger for ~w. Interval: ~w", [CBModule, TriggerInterval]),
    {TriggerGroup, State4}  =
    case state_get({trigger_group, TriggerInterval}, State3) of
        false ->
            % create and init new trigger group and request the new trigger
            NewState1 = state_update(trigger_add, fun (List) -> [TriggerInterval|List] end, State3),
            {[CBModule], NewState1};
        OldTriggerGroup ->
            % add CBModule to existing trigger group
            {[CBModule|OldTriggerGroup], State3}
    end,
    State5 = state_set({trigger_group, TriggerInterval}, TriggerGroup, State4),

    % add CBModule to list of cbmodules
    state_update(cb_modules, fun(CBModules) -> [CBModule|CBModules] end, State5).


%% @doc Calls the config function FunName of the callback module.
%%      Allowed config functions are:
%%      <ul>
%%          <li> fanout: the number of peers contacted per cycle </li>
%%          <li> min_cycles_per_round: The minimum number of cycles per round </li>
%%          <li> man_cycles_per_round: The maximum number of cycles per round </li>
%%          <li> trigger_interval: The time interval in ms after which a new cycle
%%                  is triggered </li>
%%      </ul>
-spec cb_config(FunName, CBModule) -> non_neg_integer() when
    is_subtype(FunName, fanout | min_cycles_per_round | max_cycles_per_round | trigger_interval),
    is_subtype(CBModule, cb_module()).
cb_config(FunName, {ModuleName, _Id}) ->
    apply(ModuleName, FunName, []).


%% @doc Called upon startup and calls CBModule:init().
%%      It calls the init function with the given arguments. It is usually used
%%      to initialise the state of the callback module.
-spec cb_init(Args::list(proplists:property()), cb_module()) -> CBState::any().
cb_init(Args, {ModuleName, _Id}) ->
    {ok, CBState} = apply(ModuleName, init, [Args]), CBState.


%% @doc Called at the beginning of every cycle and calls CBModule:select_node().
%%      Should return true, if the peer selection is to be done by behaviour module,
%%      false otherwise. If false is returned, the behaviour module expects a
%%      selected_peer message.
-spec cb_select_node(cb_module(), state()) -> {boolean(), state()}.
cb_select_node(CBModule, State) ->
    cb_call(select_node, [], CBModule, State).


%% @doc Called at the beginning of a cycle and calls CBModule:select_data().
%%      The callback module has to select the exchange data to be sent to the
%%      peer. The exchange data has to be sent back to the gossip module as a
%%      message of the form {selected_data, Instance, ExchangeData}.
%%      If 'discard_msg' is returned, the current trigger is ignored.
%%      (Note: Storing the trigger in the message queue would lead to self-accelerating
%%      recursion of storing and triggering)
-spec cb_select_data(cb_module(), state()) -> state().
cb_select_data(CBModule, State) ->
    case cb_call(select_data, [], CBModule, State) of
        {ok, State1} ->
            State1;
        {discard_msg, State1} ->
            state_set({trigger_lock, CBModule}, free, State1)
    end.



%% @doc Called upon a p2p_exch message and calls CBModule::select_reply_data().
%%      Passes the PData from a p2p_exch request to the callback module. The callback
%%      module has to select the exchange data to be sent to the peer. The Ref is
%%      used by the behaviour module to identify the request.
%%      The RoundStatus and Round information can be used for special handling
%%      of messages from previous rounds.
%%      The selected reply data is to be sent back to the behaviour module as a
%%      message of the form {selected_reply_data, Instance, QData, Ref, Round}.
%%      On certain return values, the reply_peer is removed from the state of the
%%      gossip module. This is necessary if the callback module will not send an
%%      selected_reply_data message (because the message is dscarded or sent back directly).
-spec cb_select_reply_data(PData::gossip_beh:exch_data(), Ref::pos_integer(),
    Round::non_neg_integer(), Msg::message(), CBModule::cb_module(), State::state()) -> state().
cb_select_reply_data(PData, Ref, Round, Msg, CBModule, State) ->
    case cb_call(select_reply_data, [PData, Ref, Round], CBModule, State) of
        {ok, State1} -> State1;
        {discard_msg, State1} ->
            {_Peer, State2} = take_reply_peer(Ref, State1),
            State2;
        {retry, State1} ->
            {_Peer, State2} = take_reply_peer(Ref, State1),
            msg_queue_add(Msg, State2);
        {send_back, State1} ->
            {p2p_exch, _, SourcePid, _, _} = Msg,
            comm:send(SourcePid, {send_error, comm:this(), Msg, message_rejected}),
            {_Peer, State2} = take_reply_peer(Ref, State1),
            State2
    end.


%% @doc Called by the upon a p2p_exch message and calls CBModule:integrate_data().
%%      Passes the QData from a p2p_exch_reply to the callback module. Upon finishing
%%      the processing of the data, a message of the form
%%      {integrated_ data, Instance, RoundStatus} is to be sent to the gossip module.
-spec cb_integrate_data(QData::gossip_beh:exch_data(), OtherRound::non_neg_integer(),
                        message(), cb_module(), state()) -> state().
cb_integrate_data(QData, OtherRound, Msg, CBModule, State) ->
    {RetVal, State1} = cb_call(integrate_data, [QData, OtherRound], CBModule, State),
    if RetVal =:= retry ->
           msg_queue_add(Msg, State1);
       RetVal =:= send_back ->
            {p2p_exch_reply,_,SourcePid,_,_} = Msg,
            comm:send(SourcePid, {send_error, comm:this(), Msg, message_rejected}),
            State1;
       RetVal =:= ok orelse RetVal =:= discard_msg ->
            State1
    end.


%% @doc Called upon messages of the form {cb_msg, CBModule, Msg} and calls
%%      CBModule:handle_msg().
%%      Passes the message Msg to the callback module, used to handle messages
%%      for the callback module.
-spec cb_handle_msg(comm:message(), cb_module(), state()) -> state().
cb_handle_msg(Msg, CBModule, State) ->
    {ok, State1} = cb_call(handle_msg, [Msg], CBModule, State), State1.


%% @doc Called upon {web_debug_info} messages and calls CBModule:web_debug_info().
%%      The callback module has to return debugging infos, to be displayed in the
%%      Scalaris Web Debug Interface.
-spec cb_web_debug_info(cb_module(), state()) ->
    {[{Key::string(), Value::any()}], state()}.
cb_web_debug_info(CBModule, State) ->
    cb_call(web_debug_info, [], CBModule, State).


%% @doc Called upon every p2p_exch/p2p_exch_reply message and calls
%%      CBmodule:round_has_converged().
%%      The callback module should return true if the current round has converged
%%      to a stable value, false otherwise (refers to gossip based aggregation
%%      protocols implementing a convergence criterion).
-spec cb_round_has_converged(cb_module(), state()) -> {boolean(), state()}.
cb_round_has_converged(CBModule, State) ->
    cb_call(round_has_converged, [], CBModule, State).


%% @doc Called to notify the callback module about certain state changes indepent
%%      of the standard message loop.
%%      Used to notify a callback module about
%%      <ul>
%%          <li> 'new_round': the starting of a new round </li>
%%          <li> 'leader': changes in the key range of the node. The MsgTag indicates
%%                  whether the node is a leader or not, the NewRange is the new
%%                  key range of the node </li>
%%          <li> 'exch_failure': a failed message delivery, including exchange
%%                  Data and Round from the original message </li>
%%      </ul>
-spec cb_notify_change(Tag::new_round, Round::non_neg_integer(), cb_module(), state()) -> state();
    (Tag::leader, {is_leader|no_leader, intervals:interval()}, cb_module(), state()) -> state();
    (Tag::exch_failure, {MsgTag::atom(), Data::any(), Round::non_neg_integer()}, cb_module(), state()) -> state().
cb_notify_change(Tag, Notification, CBModule, State) ->
    {ok, State1} = cb_call(notify_change, [Tag,Notification], CBModule, State),
    State1.


%% @doc Called upon stop_gossip_task(CBModule) and calls CBModule:shutdown().
%%      It should be the opposite of init() and do any necessary clean up.
-spec cb_shutdown(cb_module(), state()) -> state().
cb_shutdown(CBModule, State) ->
    {ok, State1} = cb_call(shutdown, [], CBModule, State), State1.


%% @doc Helper function for the cb_* functions.
%%      Adds the state of the respective callback module to the Args, calls the
%%      given function and repacks the returned client state.
-spec cb_call(cb_fun_name(), list(), cb_module(), state()) -> {ReturnValue::any(), state()}.
cb_call(FunName, Args, CBModule={ModuleName, _InstanceId}, State) when is_atom(ModuleName) ->
    Args1 = Args ++ [state_get({cb_state, CBModule}, State)],
    {ReturnValue, ReturnedCBState} = apply(ModuleName, FunName, Args1),
    {ReturnValue, state_set({cb_state, CBModule}, ReturnedCBState, State)}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Requesting Peers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sends the local node's cyclon process an enveloped request for a random node.
%%      on_active({selected_peer, CBModule, {cy_cache, Cache}}, State) will handle the response
-spec request_random_node(CBModule::cb_module()) -> ok.
request_random_node(CBModule) ->
    EnvPid = comm:reply_as(self(), 3, {selected_peer, CBModule, '_'}),
    Fanout = cb_config(fanout, CBModule),
    comm:send_local(self(), {cb_msg, {gossip_cyclon, default},
                             {get_subset_rand, Fanout, EnvPid}}).


%% Used for rerequesting peers from cyclon when cyclon returned an empty list,
%% which is usually the case during startup.
%% The delay prohibits bombarding the cyclon process with requests.
-spec request_random_node_delayed(Delay::0..4294967295, CBModule::cb_module()) ->
    ok.
request_random_node_delayed(Delay, CBModule) ->
    EnvPid = comm:reply_as(self(), 3, {selected_peer, CBModule, '_'}),
    Fanout = cb_config(fanout, CBModule),
    msg_delay:send_local(Delay div 1000, self(), {cb_msg, {gossip_cyclon, default},
                                          {get_subset_rand, Fanout, EnvPid}}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Round Handling
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% called at every p2p_exch and p2p_exch_reply message
-spec check_round(OtherRound::non_neg_integer(), CBModule::cb_module(), State::state())
    -> {ok | start_new_round | enter_new_round | propagate_new_round, state()}.
check_round(OtherRound, CBModule, State) ->
    MyRound = state_get({round, CBModule}, State),
    ?TRACE_ROUND("[ Gossip ] check_round. CBModule: ~w. MyRound: ~w. OtherRound: ~w",
                 [CBModule, MyRound, OtherRound]),
    Leader = is_leader(state_get(range, State)),
    case MyRound =:= OtherRound of
        true when Leader ->
            case is_end_of_round(CBModule, State) of
                {true, State1} ->
                    State2 = state_update({round, CBModule}, fun (X) -> X+1 end, State1),
                    State3 = state_set({cycle, CBModule}, 0, State2),
                    {start_new_round, State3};
                {false, State1} -> {ok, State1}
            end;
        true ->
            {ok, State};
        false when MyRound < OtherRound ->
            State1 = state_set({round, CBModule}, OtherRound, State),
            State2 = state_set({cycle, CBModule}, 0, State1),
            {enter_new_round, State2};
        false when MyRound > OtherRound ->
            {propagate_new_round, State}
    end.


%% checks the convergence of the current round (only called at leader)
-spec is_end_of_round(CBModule::cb_module(), State::state()) -> {boolean(), state()}.
is_end_of_round(CBModule, State) ->
    Cycles = state_get({cycle, CBModule}, State),
    ?TRACE_ROUND("[ Gossip ] check_end_of_round. Cycles: ~w", [Cycles]),
    {RoundHasConverged, State1} = cb_round_has_converged(CBModule, State),
    IsEndOfRound = Cycles >= cb_config(min_cycles_per_round, CBModule) andalso
        (   ( Cycles >= cb_config(max_cycles_per_round, CBModule)) orelse
        ( RoundHasConverged ) ),
    {IsEndOfRound, State1}.


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
                        NewNeighbors::nodelist:neighborhood(),
                        Reason::rm_loop:reason()) -> ok.
rm_send_new_range(Pid, ?MODULE, _OldNeighbors, NewNeighbors, _Reason) ->
    NewRange = nodelist:node_range(NewNeighbors),
    comm:send_local(Pid, {update_range, NewRange}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% State: Getters and Setters
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the value for the given key from the given state.
%%      Allowed keys:
%%      <ul>
%%          <li>`cb_modules', all active callback modules,</li>
%%          <li>`cb_states', the states of all callback modules ,</li>
%%          <li>`cb_stati', stati of all callback modules ,</li>
%%          <li>`cycles', the cycles of all callback modules ,</li>
%%          <li>`exch_datas', the exch_data of all callback modules ,</li>
%%          <li>`msg_queue', the message queue of the gossip module, </li>
%%          <li>`range', the key range of the node, </li>
%%          <li>`reply_peers', all reply_peers, </li>
%%          <li>`rounds', rounds of all callback modules ,</li>
%%          <li>`status', the status of the gossip module, </li>
%%          <li>`trigger_add', triggers to be added on next trigger, </li>
%%          <li>`trigger_groups', a list of all trigger groups, </li>
%%          <li>`trigger_groups', a list of all trigger locks, </li>
%%          <li>`trigger_remove', triggers to be removed on next trigger, </li>
%%      </ul>
%%      The entries of the following keys are specified by a {Key, SecondaryKey}
%%      Tuple, allowing to have more than one entry per key. These are implemented
%%      as Tuplelists of [{SecondaryKey, Value}] and accessed by the singular of
%%      the key of the according record field (e.g. "groups" -> "group").
%%      <ul>
%%          <li>`{cb_state, CBModule}', the state of the given callback module, </li>
%%          <li>`{cb_status, CBModule}', indicates, if `init()' was called on given
%%              callback module, </li>
%           <li>`{cycle, CBModule}', the cycle counter for the given callback module, </li>
%%          <li>`{exch_data, CBModule}', a tuple of the data to exchange and the peer to
%%                  exchange the data with. </li>
%%          <li>`{round, CBModule}', the round of the given callback module, </li>
%%          <li>`{reply_peer, Ref}', the peer to send the g2p_exch_reply to, </li>
%%          <li>`{trigger_group, TriggerInterval}', the trigger group (i.e. a list
%%              of callback modules) to the given TriggerInterval, </li>
%%          <li>`{trigger_lock, CBModule}', locks triggering while within prepare-request
%%              phase for the given callback module, </li>
%%      </ul>
-spec state_get(cb_modules, state()) -> [cb_module()];
               (cb_states, state()) -> [{cb_module(), any()}];
               (cb_stati, state()) -> [{cb_module(), cb_status()}];
               (cycles, state()) -> [{cb_module(), non_neg_integer()}];
               (exch_datas, state()) -> [{cb_module(), exch_data()}];
               (msg_queue, state()) -> msg_queue:msg_queue();
               (range, state()) -> intervals:interval();
               (reply_peers, state()) -> [{Ref::pos_integer(), Pid::comm:mypid()}];
               (rounds, state()) -> [{cb_module(), non_neg_integer()}];
               (status, state()) -> status();
               (trigger_add, state()) -> [pos_integer()];
               (trigger_groups, state()) -> [{TriggerInterval::pos_integer(), CBModules::[cb_module()]}];
               (trigger_locks, state()) -> [{cb_module(), locked | free}];
               (trigger_remove, state()) -> [pos_integer()];
               ({cb_state, cb_module()}, state()) -> any();
               ({cb_status, cb_module()}, state()) -> cb_status() | false;
               ({cycle, cb_module()}, state()) -> non_neg_integer() | false;
               ({exch_data, cb_module()}, state()) -> exch_data() | false;
               ({round, cb_module()}, state()) -> non_neg_integer() | false;
               ({trigger_group, TriggerInterval::pos_integer()}, state()) -> [cb_module()] | false;
               ({trigger_lock, cb_module()}, state()) -> locked | free | false.
state_get(cb_modules, State=#state{cb_modules=CBModules}) when is_record(State, state) ->
    CBModules;
state_get(cb_states, State=#state{cb_states=CBStates}) when is_record(State, state) ->
    CBStates;
state_get(cb_stati, State=#state{cb_stati=CBStati}) when is_record(State, state) ->
    CBStati;
state_get(cycles, State=#state{cycles=Cycles}) when is_record(State, state) ->
    Cycles;
state_get(exch_datas, State=#state{exch_datas=ExchDatas}) when is_record(State, state) ->
    ExchDatas;
state_get(msg_queue, State=#state{msg_queue=MsgQueue}) when is_record(State, state) ->
    MsgQueue;
state_get(range, State=#state{range=Range}) when is_record(State, state) ->
    Range;
state_get(reply_peers, State=#state{reply_peers=ReplyPeers}) when is_record(State, state) ->
    ReplyPeers;
state_get(rounds, State=#state{rounds=Round}) when is_record(State, state) ->
    Round;
state_get(status, State=#state{status=Status}) when is_record(State, state) ->
    Status;
state_get(trigger_add, State=#state{trigger_add=Triggers}) when is_record(State, state) ->
    Triggers;
state_get(trigger_groups, State=#state{trigger_groups=TriggerGroups}) when is_record(State, state) ->
    TriggerGroups;
state_get(trigger_locks, State=#state{trigger_locks=TriggerLocks}) when is_record(State, state) ->
    TriggerLocks;
state_get(trigger_remove, State=#state{trigger_remove=Triggers}) when is_record(State, state) ->
    Triggers;
state_get({cb_state, CBModule}, State=#state{cb_states=CBStates}) when is_record(State, state) ->
    state_get_helper(CBModule, CBStates);
state_get({cb_status, CBModule}, State=#state{cb_stati=CBStati}) when is_record(State, state) ->
    state_get_helper(CBModule, CBStati);
state_get({cycle, CBModule}, State=#state{cycles=Cycles}) when is_record(State, state) ->
    state_get_helper(CBModule, Cycles);
state_get({exch_data, CBModule}, State=#state{exch_datas=ExchDatas}) when is_record(State, state) ->
    state_get_helper(CBModule, ExchDatas);
state_get({round, CBModule}, State=#state{rounds=Round}) when is_record(State, state) ->
    state_get_helper(CBModule, Round);
state_get({trigger_group, Interval}, State=#state{trigger_groups=TriggerGroups}) when is_record(State, state) ->
    state_get_helper(Interval, TriggerGroups);
state_get({trigger_lock, CBModule}, State=#state{trigger_locks=TriggerLocks}) when is_record(State, state) ->
    state_get_helper(CBModule, TriggerLocks).


%% @doc Helper for state_get, extracts a value to the given (secondary) key from the given Tuplelist
-spec state_get_helper(pos_integer() | cb_module() , [{cb_module()|pos_integer(), ValueType::any()}]) -> ValueType::any().
state_get_helper(Key, TupleList) when is_list(TupleList) ->
    case lists:keyfind(Key, 1, TupleList) of
        {Key, Value} -> Value;
        false -> false
    end.


%% @doc Sets the given value for the given key in the given state.
%%      For a description of the keys see state_get/2.
-spec state_set(cb_modules, [cb_module()], state()) -> state();
               (msg_queue, msg_queue:msg_queue(), state()) -> state();
               (range, intervals:interval(), state()) -> state();
               (status, status(), state()) -> state();
               (trigger_add, [pos_integer()], state()) -> state();
               (trigger_remove, [pos_integer()], state()) -> state();
               ({cb_state, cb_module()}, any(), state()) -> state();
               ({cb_status, cb_module()}, cb_status(), state()) -> state();
               ({cycle, cb_module()}, non_neg_integer(), state()) -> state();
               ({exch_data, cb_module()}, exch_data(), state()) -> state();
               ({reply_peer, Ref::pos_integer()}, comm:mypid(), state()) -> state();
               ({round, cb_module()}, non_neg_integer(), state()) -> state();
               ({trigger_group, TriggerInterval::pos_integer()}, [cb_module()], state()) -> state();
               ({trigger_lock, cb_module()}, locked | free, state()) -> state().
state_set({Key, SecondaryKey}, Value, State) when is_record(State, state) ->
    List = case Key of
        cb_state -> State#state.cb_states;
        cb_status -> State#state.cb_stati;
        cycle -> State#state.cycles;
        exch_data -> State#state.exch_datas;
        reply_peer -> State#state.reply_peers;
        round -> State#state.rounds;
        trigger_group -> State#state.trigger_groups;
        trigger_lock -> State#state.trigger_locks
    end,
    List1 = lists:keystore(SecondaryKey, 1, List, {SecondaryKey, Value}),
    case Key of
        cb_state -> State#state{cb_states=List1};
        cb_status -> State#state{cb_stati=List1};
        cycle -> State#state{cycles=List1};
        exch_data -> State#state{exch_datas=List1};
        reply_peer -> State#state{reply_peers=List1};
        round -> State#state{rounds=List1};
        trigger_group -> State#state{trigger_groups=List1};
        trigger_lock -> State#state{trigger_locks=List1}
    end;

state_set(Key, Value, State) when is_record(State, state) ->
    case Key of
        cb_modules -> State#state{cb_modules = Value};
        msg_queue -> State#state{msg_queue = Value};
        range -> State#state{range = Value};
        status -> State#state{status = Value};
        trigger_add -> State#state{trigger_add = Value};
        trigger_remove -> State#state{trigger_remove = Value}
    end.

%% @doc Remove callback module dependent entries from state.
-spec state_remove_cb(cb_module(), state()) -> state().
state_remove_cb(CBModule, State) when is_record(State, state) ->
    State1 = State#state{cb_states=lists:keydelete(CBModule, 1, State#state.cb_states)},
    State2 = State1#state{cb_stati=lists:keydelete(CBModule, 1, State1#state.cb_stati)},
    State3 = State2#state{cycles=lists:keydelete(CBModule, 1, State2#state.cycles)},
    State4 = State3#state{exch_datas=lists:keydelete(CBModule, 1, State3#state.exch_datas)},
    State5 = State4#state{rounds=lists:keydelete(CBModule, 1, State4#state.rounds)},
    State5#state{trigger_locks=lists:keydelete(CBModule, 1, State5#state.trigger_locks)}.


%% @doc Remove the entry of the given key.
-spec state_remove(Key::{cb_status, SecondaryKey::cb_module()}, state()) -> state().
state_remove({cb_status, CBModule}, #state{cb_stati=CBStati}=State) when is_record(State, state) ->
    CBStati1 = lists:keydelete(CBModule, 1, CBStati),
    State#state{cb_stati=CBStati1}.


%% @doc Updates an entry with the given update function
%%      For a description of the keys see state_get/2.
-spec state_update(cb_modules, UpdateFun::fun(), state()) -> state();
                  (msg_queue, UpdateFun::fun(), state()) -> state();
                  (range, UpdateFun::fun(), state()) -> state();
                  (status, UpdateFun::fun(), state()) -> state();
                  (trigger_add, UpdateFun::fun(), state()) -> state();
                  (trigger_remove, UpdateFun::fun(), state()) -> state();
                  ({cycle, cb_module()}, UpdateFun::fun(), state()) -> state();
                  ({reply_peer, Ref::pos_integer()}, UpdateFun::fun(), state()) -> state();
                  ({round, cb_module()}, UpdateFun::fun(), state()) -> state();
                  ({trigger_group, TriggerInterval::pos_integer()}, UpdateFun::fun(), state()) -> state();
                  ({cb_state, cb_module()}, UpdateFun::fun(), state()) -> state().
state_update(Key, Fun, State) when is_record(State, state) ->
    NewValue = apply(Fun, [state_get(Key, State)]),
    state_set(Key, NewValue, State).


%% @doc Retrieve and remove the reply peer given by the Ref from the state.
-spec take_reply_peer(Ref::pos_integer(), State::state()) -> {comm:mypid()|none, state()}.
take_reply_peer(Ref, #state{reply_peers=Peers}=State) when is_record(State, state) ->
    case lists:keytake(Ref, 1, Peers) of
        false -> {none, State};
        {value, {Ref, Peer}, Rest} -> {Peer, State#state{reply_peers=Rest}}
    end.


%% @doc Gets all the tombstones from the state of the gossip module.
-spec get_tombstones(State::state()) -> [cb_module()].
get_tombstones(State) ->
    [CBModule || {CBModule, tombstone} <- state_get(cb_stati, State)].



%%------------------------- Message Queue --------------------------%%

%% add to message queue and create message queue if necessary
-spec msg_queue_add(Msg::message(), State::state()) -> state().
msg_queue_add(Msg, State) ->
    MsgQueue1 = state_get(msg_queue, State),
    MsgQueue2 = msg_queue:add(MsgQueue1, Msg),
    state_set(msg_queue, MsgQueue2, State).


%% send the messages from the current message queue and create a new message queue
-spec msg_queue_send(State::state()) -> state().
msg_queue_send(State) ->
    msg_queue:send(state_get(msg_queue, State)),
    state_set(msg_queue, msg_queue:new(), State).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Misc
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% provide some debug information for the gossip moudle (to be added to the
%% information of a the callback modules)
-spec web_debug_info(State::state()) -> [{any(), any()}, ...].
web_debug_info(State) ->
    CBModules = state_get(cb_modules, State),
    Tombstones = get_tombstones(State),
    _KeyValueList =
        [{"behaviour module",   ""},
         {"msg_queue_len",      length(state_get(msg_queue, State))},
         {"status",             state_get(status, State)},
         {"registered modules", to_string(CBModules)},
         {"trigger groups", to_string(state_get(trigger_groups, State))},
         {"cycles", to_string(state_get(cycles, State))},
         {"tombstones",         to_string(Tombstones)}
     ].


%% @doc String conversion for sane outputs and debugging
-spec to_string(list()|state()) -> string().
%% Returns a list as string
to_string(List) when is_list(List) ->
    lists:flatten(io_lib:format("~w", [List]));

%% returns the state as string (comment/uncomment as necessary to control output)
to_string(State) when is_record(State, state) ->
    _CBModules = state_get(cb_modules, State),
    _CBStates = state_get(cb_states, State),
    RawMsgQueue = state_get(msg_queue, State),
    _Range = state_get(range, State),
    _Status = state_get(status, State),
    _TriggerAdd = state_get(trigger_add, State),
    _TriggerGroups = state_get(trigger_groups, State),
    _TriggerLocks = state_get(trigger_locks, State),
    _TriggerRemove = state_get(trigger_remove, State),
    _CBStati = state_get(cb_stati, State),
    _Cycles = state_get(cycles, State),
    _ExchDatas = state_get(exch_datas, State),
    _ReplyPeers = state_get(reply_peers, State),
    _Rounds = state_get(rounds, State),
    %% _MsgQueue = RawMsgQueue,
    _MsgQueue = lists:map(fun(Tuple) -> {element(1, Tuple), '...'} end, RawMsgQueue),
    Str =
        io_lib:format("State: ~n", []) ++
        io_lib:format("\tCBModules: ~w~n", [_CBModules]) ++
        io_lib:format("\tMsgQueue: ~w~n", [_MsgQueue]) ++
        io_lib:format("\tRange: ~w~n", [_Range]) ++
        io_lib:format("\tStatus: ~w~n", [_Status]) ++
        io_lib:format("\tTriggerAdd: ~w~n", [_TriggerAdd]) ++
        io_lib:format("\tTriggerRemove: ~w~n", [_TriggerRemove]) ++
        io_lib:format("\tReplyPeers: ~w~n", [_ReplyPeers]) ++
        io_lib:format("\tTriggerGroups: ~w~n", [_TriggerGroups]) ++
        io_lib:format("\tTriggerLocks: ~w~n", [_TriggerLocks]) ++
        io_lib:format("\tCBStati: ~w~n", [_CBStati]) ++
        io_lib:format("\tCycles: ~w~n", [_Cycles]) ++
        io_lib:format("\tRounds: ~w~n", [_Rounds]) ++
        %% io_lib:format("\tExchDatas: ~w~n", [_ExchDatas]) ++
        %% io_lib:format("\tCBStates: ~w~n", [_CBStates]) ++
        io_lib:format("", []),
    lists:flatten(Str).


%% @doc Check the config of the gossip module. <br/>
%%      Calls the check_config functions of all callback modules.
-spec check_config() -> boolean().
check_config() ->
    lists:foldl(fun({Module, _Args}, Acc) -> Acc andalso Module:check_config() end, true, ?CBMODULES).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Testing and Debugging
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @doc Globally starts a gossip task identified by CBModule. <br/>
%%      Args is passed to the init function of the callback module. <br/>
%%      CBModule is either the name of a callback module or an name-instance_id
%%      tuple.
-spec start_gossip_task(GossipTask, Args) -> ok when
    is_subtype(GossipTask, cb_module_name() | cb_module()),
    is_subtype(Args, list(proplists:property())).
start_gossip_task(ModuleName, Args) when is_atom(ModuleName) ->
    Id = uid:get_global_uid(),
    start_gossip_task({ModuleName, Id}, Args);

start_gossip_task({ModuleName, Id}, Args) when is_atom(ModuleName) ->
    Msg = {?send_to_group_member, gossip,
                {start_gossip_task, {ModuleName, Id}, Args}},
    bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(), Msg).


%% @doc Globally stop a gossip task.
-spec stop_gossip_task(CBModule::cb_module()) -> ok.
stop_gossip_task(CBModule) ->
    Msg = {?send_to_group_member, gossip, {stop_gossip_task, CBModule}},
    bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(), Msg).


%% hack to be able to suppress warnings when testing via config:write()
-spec warn() -> log:log_level().
warn() ->
    case config:read(gossip_log_level_warn) of
        failed -> info;
        Level -> Level
    end.


%% hack to be able to suppress warnings when testing via config:write()
-compile({nowarn_unused_function, {error, 0}}).
-spec error() -> log:log_level().
error() ->
    case config:read(gossip_log_level_error) of
        failed -> warn;
        Level -> Level
    end.


%% @doc Value creater for type_check_SUITE.
-spec tester_create_cb_module_names(1) -> cb_module_name().
tester_create_cb_module_names(1) ->
    gossip_load.


-compile({nowarn_unused_function, {init_gossip_task_feeder, 3}}).
-spec init_gossip_task_feeder(cb_module(), NoOfBuckets::gossip_load:histogram_size(), state())
        -> {cb_module(), list(), state()}.
init_gossip_task_feeder(CBModule, NoOfBuckets, State) ->
    %% note: gossip_load (the only supported cb_module for now) requires an
    %%       integer NoOfBuckets parameter
    %%       (other modules may have different parameters)
    {CBModule, [NoOfBuckets], State}.
