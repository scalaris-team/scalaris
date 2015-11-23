% @copyright 2009-2015 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc Part of a generic implementation of transactions using Paxos Commit -
%%      the roles of the (replicated) transaction manager TM and RTM.
%% @end
%% @version $Id$
-module(tx_tm_rtm).
-author('schintke@zib.de').
-vsn('$Id$').

-compile({inline, [get_item_entry/2]}).

%%-define(TRACE_RTM_MGMT(X,Y), io:format(X,Y)).
%%-define(TRACE_RTM_MGMT(X,Y), log:pal(X,Y)).
-define(TRACE_RTM_MGMT(X,Y), ok).
%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(_X,_Y), ok).
-behaviour(gen_component).
-include("scalaris.hrl").
-include("client_types.hrl").

%% public interface for transaction validation using Paxos-Commit.
-export([commit/4]).
-export([msg_commit_reply/3]).
-export([rm_send_update/5]).

%% functions for gen_component module, supervisor callbacks and config
-export([start_link/2]).
-export([on/2, init/1]).
-export([on_init/2]).
-export([check_config/0]).

%% functions for dht_node
-export([get_my/2]).

-export_type([rtms/0]).
-export_type([tx_item_id/0, paxos_id/0]).
-export_type([tx_id/0]).

-type tx_id() :: {?tx_id, uid:global_uid()}.

-type rtm() :: {?RT:key(),
                {comm:mypid()} | unknown,
                Role :: pos_integer(),
                {Acceptor :: comm:mypid()} | unknown}.

-type rtms() :: [rtm()].

-include("gen_component.hrl").
-include("tx_item_state.hrl").
-include("tx_state.hrl").

-type state() ::
    {RTMs           :: rtms(),
     TableName      :: pdb:tableid(),
     Role           :: pid_groups:pidname(),
     LocalAcceptor  :: pid(),
     GLocalLearner  :: comm:mypid(),
     OpenTxNum      :: non_neg_integer(),
     LocalSnapNo    :: non_neg_integer()}.

%% messages a client has to expect when using this module
-spec msg_commit_reply(comm:mypid(), any(), any()) -> ok.
msg_commit_reply(Client, ClientsID, Result) ->
    comm:send(Client, {tx_tm_rtm_commit_reply, ClientsID, Result}).

-spec msg_tp_do_commit_abort(comm:mypid(), any(), ?commit | ?abort, non_neg_integer()) -> ok.
msg_tp_do_commit_abort(TP, Id, Result, LocalSnapNumber) ->
    comm:send(TP, {?tp_do_commit_abort, Id, Result, LocalSnapNumber}).

%% public interface for transaction validation using Paxos-Commit.
%% ClientsID may be nil, its not used by tx_tm. It will be repeated in
%% replies to allow to map replies to the right requests in the
%% client.
-spec commit(comm:erl_local_pid(), comm:mypid(), any(), tx_tlog:tlog()) -> ok.
commit(TM, Client, ClientsID, TLog) ->
    Msg = {tx_tm_rtm_commit, Client, ClientsID, TLog},
    comm:send_local(TM, Msg).

%% @doc Notifies the tx_tm_rtm of a changed node ID.
-spec rm_send_update(Subscriber::pid(), Tag::?MODULE,
                     OldNeighbors::nodelist:neighborhood(),
                     NewNeighbors::nodelist:neighborhood(),
                     Reason::rm_loop:reason()) -> ok.
rm_send_update(Pid, ?MODULE, OldNeighbors, NewNeighbors, _Reason) ->
    OldId = node:id(nodelist:node(OldNeighbors)),
    NewId = node:id(nodelist:node(NewNeighbors)),
    case OldId =/= NewId of
        true  -> comm:send_local(Pid, {new_node_id, NewId});
        false -> ok
    end.

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname(), any()) -> {ok, pid()}.
start_link(DHTNodeGroup, Role) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [],
                             [{pid_groups_join_as, DHTNodeGroup, Role},
                              {spawn_opts, [{fullsweep_after, 0},
                                            {min_heap_size, 16383}]}]).

%% initialize: return initial state.
-spec init([]) -> state() | {'$gen_component', [{on_handler, gen_component:handler()},...],
                             state()}.
init([]) ->
    Role = pid_groups:my_pidname(),
    ?TRACE("tx_tm_rtm:init for instance: ~p ~p~n",
           [pid_groups:my_groupname(), Role]),
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = erlang:list_to_atom(pid_groups:group_to_filename(pid_groups:my_groupname()) ++ "_tx_tm_rtm_" ++ atom_to_list(Role)),
    %%Table = pdb:new(TableName, [set, protected, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    Table = pdb:new(?MODULE, [set]),
    LAcceptor = get_my(Role, acceptor),
    GLLearner = comm:make_global(get_my(Role, learner)),
    %% start getting rtms and maintain them.
    InitialState = {_RTMs = [], Table, Role, LAcceptor, GLLearner,
                    _OpenTx = 0, _LocalSnapNo = 0},
    case Role of
        tx_tm ->
            comm:send_local(self(), {get_node_details}),
            rtm_update_trigger(state_get_RTMs(InitialState)),
            msg_delay:send_trigger(1, {update_RTMs_on_init}),
            %% subscribe to id changes
            rm_loop:subscribe(self(), ?MODULE,
                              fun rm_loop:subscribe_dneighbor_change_filter/3,
                              fun ?MODULE:rm_send_update/5, inf),
            gen_component:change_handler(InitialState, fun ?MODULE:on_init/2);
        _ -> InitialState
    end.

-spec on(comm:message(), state()) -> state().
%% a TP comitted/roled back the item as requested
on({?tp_committed, ItemId} = _Msg, State) ->
    ?TRACE("tx_tm_rtm:on(~p)~n", [_Msg]),

    %% retrieve the item and handle the commit ack therein
    {OrigItemResult, ItemState} = get_item_entry(ItemId, State),
    case OrigItemResult of
        new -> State;
        _ ->
            {ItemResult, NewItemState} = tx_item_add_commit_ack(ItemState),
            _ = set_entry(NewItemState, State),
            %% retrieve the tx entry and increment the number of commit acks
            TxId = tx_item_get_txid(NewItemState),
            {_, TxState} = get_tx_entry(TxId, State),
            _ = case ItemResult of
                    state_updated ->
                        _ = set_entry(TxState, State);
                        % trigger_delete_if_done(TxState, State);
                    item_newly_safe ->
                        {TxResult, TmpTxState} = tx_state_add_item_acked(TxState),
                        NewTxState =
                            case TxResult of
                                open -> TmpTxState;
                                all_items_acked ->
                                    %% to inform, we do not need to know the new state
                                    Result = tx_state_is_decided(TmpTxState),
                                    inform_client(TmpTxState, State, Result),
                                    inform_rtms(TxId, TmpTxState, Result),
                                    TmpTxState
                            end,
                        _ = set_entry(NewTxState, State),
                        trigger_delete_if_done(NewTxState, State)
                end,
            State
    end;

on({delete_if_still_uninitialized, ItemId}, State) ->
    {NewUninitializedOK, _Item} = get_item_entry(ItemId, State),
    case NewUninitializedOK of
        uninitialized ->
            TableName = state_get_tablename(State),
            pdb:delete(ItemId, TableName);
        _ -> ok
    end,
    State;

%% a paxos consensus is decided (msg generated by learner.erl)
on({learner_decide, ItemId, _PaxosID, _Value} = Msg, State) ->
    ?TRACE("tx_tm_rtm:on(~p)~n", [Msg]),

    %% retrieve the item and handle the learner decide therein
    {NewUnitializedOK, ItemState} = get_item_entry(ItemId, State),
    case NewUnitializedOK of
        new ->
            %% we are the first or the tx is already done...?
            %% (delete_if_still_uninitialized is only triggered when the
            %% delayed trigger_delete_if_done does not catch the fourth
            %% learner_decide - see below)
            msg_delay:send_local(30, self(),
                                 {delete_if_still_uninitialized, ItemId});
        _ -> ok
    end,
    {ItemResult, NewItemState} = tx_item_add_learner_decide(ItemState, Msg),
    _ = set_entry(NewItemState, State),

    %% retrieve the tx entry and increment the number of paxos decided
    %% The TxState may be dropped, when hold_back (then it was newly created...).
    TxId = tx_item_get_txid(NewItemState),
    {_, TxState} = get_tx_entry(TxId, State),

    _ = case ItemResult of
            hold_back -> ok;
            state_updated ->
                _ = set_entry(TxState, State);
            {item_newly_decided, Decision} ->
                {TxResult, TmpTxState} = tx_state_add_item_decision(TxState, Decision),
                NewTxState =
                    case TxResult of
                        ?undecided -> TmpTxState;
                        false -> TmpTxState;
                        {tx_newly_decided, Result} ->
                            T1TxState = inform_tps(TmpTxState, State, Result),
                            %% to inform, we do not need to know the new state

                            %% client and rtms are informed, when
                            %% maj. of tps comitted
                            %% inform_client(T1TxState, State, Result),
                            %% inform_rtms(TxId, TmpTxState, Result),
                            T1TxState
                    end,
                set_entry(NewTxState, State)
        end,
    State;

on({tx_tm_rtm_commit, Client, ClientsID, TransLog}, State) ->
    ?TRACE("tx_tm_rtm:on({commit, ...}) for TLog ~p as ~p~n",
           [TransLog, state_get_role(State)]),
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    Maj = quorum:majority_for_accept(config:read(replication_factor)),
    GLLearner = state_get_gllearner(State),
    TLogUid = uid:get_global_uid(),
    NewTid = {?tx_id, TLogUid},
    This = comm:this(),
    ItemStates =
        util:map_with_nr(
          fun(TLogEntry, NrX) ->
                  ItemId = {?tx_item_id, TLogUid, NrX},
                  TItemState = tx_item_new(ItemId, NewTid, TLogEntry),
                  ItemState = tx_item_set_status(TItemState, ok),
                  %% initialize local learner
                  _ = [ learner:start_paxosid(GLLearner, element(1, X),
                                              Maj, This, ItemId)
                          || X <- tx_item_get_paxosids_rtlogs_tps(ItemState) ],
                  ItemState
          end, TransLog, 0),

    RTMs = state_get_RTMs(State),
    TmpTxState = tx_state_new(NewTid, Client, ClientsID, This, RTMs,
                              ItemStates, [GLLearner]),
    TxState = tx_state_set_status(TmpTxState, ok),

    init_TPs(TxState, ItemStates, state_get_local_snapno(State)),
    init_RTMs(TxState, ItemStates),

    _ = [ set_entry(ItemState, State) || ItemState <- ItemStates ],
    _ = set_entry(TxState, State),

    %% send a weak timeout to ourselves to take further care
    %% if the tx is slow (or a majority of tps failed)
    msg_delay:send_local((config:read(tx_timeout) * 2) div 1000, self(),
                         {tx_tm_rtm_tid_isdone, NewTid}),
    state_inc_opentxnum(State);

%% is the txid done?
on({tx_tm_rtm_tid_isdone, TxId}, State) ->
    ?TRACE("tx_tm_rtm_tid_isdone ~p as ~p~n",
           [TxId, state_get_role(State)]),
    {ErrCode, TxState} = get_tx_entry(TxId, State),
    case ErrCode of
        new -> ok; %% already finished, nothing to be done
        uninitialized ->
            %% can happen when already cleaned up at tm
            ok;
        ok ->
            Maj = quorum:majority_for_accept(config:read(replication_factor)),
            WhatToDo =
                case tx_state_is_decided(TxState)
                of
                    ?abort ->
                        case Maj > tx_state_get_numcommitack(TxState) of
                            true -> delay;
                            false -> {delete, ?abort}
                        end;
                    ?commit ->
                        case Maj > tx_state_get_numcommitack(TxState) of
                            true -> delay;
                            false -> {delete, ?commit}
                        end;
                    ?undecided ->
                        %% This tx is a bit slow. Start fds on the participants
                        %% and then take over on crash messages.  When not enough
                        %% tps have registered? propose yourself.

                        %% TODO: instead of direct takeover, fd:subscribe to the
                        %% participants. And then takeover when a crash is
                        %% actually reported.
                        QLen = element(2, erlang:process_info(self(), message_queue_len)),

                        %% TODO: check if any messages in the queue contribute to this TxId,
                        %%       then, delay, otherwise trigger take over
                        case QLen > state_get_opentxnum(State) of
                            true ->
                                                % we have replies other than 'isdone' for some
                                                % of the tx in the queue
                                delay;
                            false ->
                                takeover
                        end
                end,
            case WhatToDo of
                {delete, Decision} ->
                    %% we delete the local state. As we have to maintain opentxnum
                    %% we send a message to ourselves:
                    %% log:log("Trigger tx_tm_rtm_delete in ~p~n", [state_get_role(State)]),
                    comm:send_local(self(), {?tx_tm_rtm_delete, TxId, Decision});
                delay ->
                    msg_delay:send_local((config:read(tx_timeout) * 2)
                                             div 1000,
                                         self(), {tx_tm_rtm_tid_isdone, TxId});
                takeover ->
                    %% FDSubscribe = enough_tps_registered(TxState, State),
                    ValidRTMs = [ X || X <- tx_state_get_rtms(TxState),
                                       unknown =/= get_rtmpid(X) ],
                    send_to_rtms(ValidRTMs,
                                 fun(_X) -> {tx_tm_rtm_propose_yourself,
                                            TxId} end)
            end,
            ok
    end,
    State;

%% this tx is finished and enough TPs were informed, delete the state
on({?tx_tm_rtm_delete, TxId, Decision}, State) ->
    ?TRACE("tx_tm_rtm:on({delete, ~p, ~p}) in ~p ~n",
           [TxId, Decision, state_get_role(State)]),
    {ErrCode, TxState} = get_tx_entry(TxId, State),
    %% inform RTMs on delete
    Role = state_get_role(State),
    {DeleteIt, NewState} =
        case ErrCode of
            ok when Role =:= tx_tm ->
                %% deletes to RTMS are already send in inform_RTMs.
                %% inform used learner to delete paxosids.
                AllPaxIds = get_paxos_ids(State, TxState),
                %% We could delete immediately, but we still miss the
                %% minority of learner_decides, which would re-create the
                %% id in the learner, which then would have to be deleted
                %% separately, so we give the minority a second to arrive
                %% and then send the delete request.
                %% learner:stop_paxosids(GLLearner, AllPaxIds),
                GLLearner = state_get_gllearner(State),
                msg_delay:send_local(1, comm:make_local(GLLearner),
                                     {learner_deleteids, AllPaxIds}),
                {_DeleteIt = true, State};
            ok ->
                %% the test trigger_delete was passed, at least by the TM
                %% RTMs only wait for all tp register messages, to not miss them
                %% record, that every TP was informed and all paxids decided
                TmpTxState = tx_state_set_numinformed(
                               TxState, tx_state_get_numids(TxState) *
                                   config:read(replication_factor)),
                Tmp2TxState = tx_state_set_decided(TmpTxState, Decision),
                _ = set_entry(Tmp2TxState, State),
                Delete = tx_state_all_tps_registered(TxState),
                TmpState =
                    case Delete of
                        true ->
                            state_unsubscribe(State, tx_state_get_tm(TxState));
                        false -> State
                    end,
                %% inform used acceptors to delete paxosids.
                AllPaxIds = get_paxos_ids(State, TxState),
                LAcceptor = state_get_lacceptor(State),
                %% msg_delay:send_local((config:read(tx_timeout) * 2) div 1000, LAcceptor,
                %%                      {acceptor_deleteids, AllPaxIds});
                comm:send_local(LAcceptor, {acceptor_deleteids, AllPaxIds}),
                {Delete, TmpState};
            new -> {false, State}; %% already deleted
            uninitialized ->
                {false, State} %% will be deleted when msg_delay triggers it
        end,
    case DeleteIt of
        false ->
            %% @TODO if we are a rtm, we still wait for register TPs
            %% trigger same delete later on, as we do not get a new
            %% request to delete from the tm
            NewState;
        true ->
            TableName = state_get_tablename(NewState),
            %% delete locally
            _ = [ pdb:delete(ItemId, TableName)
              || ItemId <- tx_state_get_txitemids(TxState)],
            pdb:delete(TxId, TableName),
            state_dec_opentxnum(NewState)
            %% @TODO failure cases are not handled yet. If some
            %% participants do not respond, the state is not deleted.
            %% In the future, we will handle this using msg_delay for
            %% outstanding txids to trigger a delete of the items.
    end;

%% sent by init_RTMs
on({?tx_tm_rtm_init_RTM, TxState, ItemStates, _InRole} = _Msg, State) ->
   ?TRACE("tx_tm_rtm:on({init_RTM, ...}) ~n", []),

    %% lookup transaction id locally and merge with given TxState
    Tid = tx_state_get_tid(TxState),
    {LocalTxStatus, LocalTxEntry} = get_tx_entry(Tid, State),
    {TmpEntry, NewEntryHoldBackQ, NewState} =
        case LocalTxStatus of
            new ->
                %% nothing known locally
                %% rtms subscribe to tm crashes for running tx.
                TmpState = state_subscribe(State, tx_state_get_tm(TxState)),
                {TxState, [], TmpState};
            uninitialized ->
                %%io:format("initRTM takes over hold back queue for id ~p in ~p~n", [Tid, Role]),
                %% take hold back from existing entry
                {TxState, tx_state_get_hold_back(LocalTxEntry), State};
            ok ->
                log:log(error, "Duplicate init_RTM", []),
                %% hold back queue should be empty!
                {LocalTxEntry, [], State}
        end,
    % note: do not need to set the holdback queue for the entry
    %       -> we will process it below
    NewEntry = tx_state_set_status(TmpEntry, ok),
    _ = set_entry(NewEntry, NewState),

    %% lookup items locally, merge with given ItemStates,
    %% initiate local paxos acceptors (with received paxos_ids)
    Learners = tx_state_get_learners(TxState),
    LAcceptor = state_get_lacceptor(NewState),
    NewItemsHoldBackQ = lists:append(
                          merge_item_states(Tid, tx_state_get_txitemids(NewEntry),
                                            ItemStates, NewState, Learners, LAcceptor)),

    %% process hold back messages for tx_state
    %% io:format("Starting hold back queue processing~n"),
    NewState2 = lists:foldr(fun on/2, NewState, NewEntryHoldBackQ),
    %% process hold back messages for tx_items
    NewState3 = lists:foldr(fun on/2, NewState2, NewItemsHoldBackQ),
    %% io:format("Stopping hold back queue processing~n"),

    %% set timeout and remember timerid to cancel, if finished earlier?
    %%msg_delay:send_local(1 + InRole, self(), {tx_tm_rtm_propose_yourself, Tid}),
    %% after timeout take over and initiate new paxos round as proposer
    %% done in on({tx_tm_rtm_propose_yourself...}) handler
    NewState3;

% received by RTMs
on({?register_TP, {Tid, ItemId, PaxosID, TP}} = Msg, State) ->
    Role = state_get_role(State),
    %% TODO merge ?register_TP and accept messages to a single message
    ?TRACE("tx_tm_rtm:on(~p) as ~p~n", [Msg, Role]),
    {ErrCodeTx, TmpTxState} = get_tx_entry(Tid, State),
    _ = case ok =/= ErrCodeTx of
        true -> %% new / uninitialized
            %% hold back and handle when corresponding tx_state is
            %% created in init_RTM
            %% io:format("Holding back a registerTP for id ~p in ~p~n", [Tid, Role]),
            T2TxState = tx_state_hold_back(Msg, TmpTxState),
            NewTxState = tx_state_set_status(T2TxState, uninitialized),
            set_entry(NewTxState, State);
        false -> %% ok
            TxState = tx_state_inc_numtpsregistered(TmpTxState),
            _ = set_entry(TxState, State),
            {ok, ItemState} = get_item_entry(ItemId, State),

            case tx_state_is_decided(TxState) of
                ?undecided ->
                    %% store TP info to corresponding PaxosId
                    NewEntry =
                        tx_item_set_tp_for_paxosid(ItemState, TP, PaxosID),
                    trigger_delete_if_done(TxState, State),
                    set_entry(NewEntry, State);
                Decision when Role =:= tx_tm ->
                    %% if ?register_TP arrives after tx decision, inform the
                    %% slowly client directly
                    %% find matching RTLogEntry and send commit_reply
                    {PaxosID, RTLogEntry, _TP} =
                        lists:keyfind(PaxosID, 1,
                          tx_item_get_paxosids_rtlogs_tps(ItemState)),
                    msg_tp_do_commit_abort(TP, {PaxosID, RTLogEntry, comm:this(), ItemId}, Decision, state_get_local_snapno(State)),
                    %% record in txstate and try to delete entry?
                    NewTxState = tx_state_inc_numinformed(TxState),
                    trigger_delete_if_done(NewTxState, State),
                    set_entry(NewTxState, State);
                _ ->
                    %% RTMs check whether everything is done
                    trigger_delete_if_done(TxState, State)
            end
    end,
    State;

% timeout on Tid maybe a proposer crashed? Force proposals with abort.
on({tx_tm_rtm_propose_yourself, Tid}, State) ->
    ?TRACE("tx_tm_rtm:propose_yourself(~p) as ~p~n", [Tid, state_get_role(State)]),
    %% only on in rtm processes!
    ?DBG_ASSERT(tx_tm =/= state_get_role(State)),
    %% after timeout take over and initiate new paxos round as proposer
    {ErrCodeTx, TxState} = get_tx_entry(Tid, State),
%%    log:pal("propose yourself (~.0p/~.0p) for: ~.0p ~.0p~n",
%%           [self(),
%%            pid_groups:group_and_name_of(self()),
%%            Tid, {ErrCodeTx, TxState}]),
    _ =
    case ErrCodeTx of
        new -> ok; %% takeover is not necessary. Was finished successfully.
        _Any ->
            log:log(info, "Takeover by RTM was necessary."),
            Maj = quorum:majority_for_accept(config:read(replication_factor)),
            RTMs = tx_state_get_rtms(TxState),
            Role = state_get_role(State),
            ValidAccs = rtms_get_valid_accpids(RTMs),
            MaxProposers = length(ValidAccs) + 1,
            This = comm:this(),
            case comm:is_valid(This) of
                false ->
                    log:log(warn, "Cannot discover my comm:this().~n");
                true ->
                    {tx_rtm, ThisRTMsNumber} = Role,

            %% add ourselves as learner and
            %% trigger paxos proposers for new round with own proposal 'abort'
            GLLearner = state_get_gllearner(State),
            Proposer = comm:make_global(get_my(Role, proposer)),
            [ begin
                  {_, ItemState} = get_item_entry(ItemId, State),
                  case tx_item_get_decided(ItemState) of
                      false ->
%%                          log:pal("initiating proposer~n"),
                          [ begin
                                learner:start_paxosid(GLLearner, PaxId, Maj,
                                                      This, ItemId),
                                %% add learner to running paxos acceptors
                                _ = [ comm:send(X,
                                                {acceptor_add_learner,
                                                 PaxId, GLLearner})
                                      || X <- ValidAccs],
                                proposer:start_paxosid(
                                  Proposer, PaxId, _Acceptors = ValidAccs, ?abort,
                                  Maj, MaxProposers, _InitialRound = ThisRTMsNumber),
                                ok
                            end
                            || {PaxId, _RTLog, _TP}
                                   <- tx_item_get_paxosids_rtlogs_tps(ItemState) ];
                      _Decision -> % already decided to prepared / abort
                          log:pal("Already decided~n"),
                          ok
                  end
              end || ItemId <- tx_state_get_txitemids(TxState) ]
            end
        end,
    State;

%% sent by snapshot.erl to update tx_tm on new local snapshot numbers
on({update_snapno, SnapNo}, State) ->
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    state_set_local_snapno(State, SnapNo);

%% failure detector events
on({fd, Cookie, {fd_notify, crash, Pid, Reason}}, State) ->
    %% in tx_tm and rtm processes! (rtms subscribe to the tm for running tx)
    ?TRACE_RTM_MGMT("tx_tm_rtm:on({crash,...}) of Pid ~p~n", [Pid]),
    %% in tx_tm and rtm processes! (rtms subscribe to the tm for running tx)
    handle_crash(Pid, Reason, Cookie, State, on);
%% on({fd, _Cookie, {fd_notify, crash, _Pid, _Reason}}
%%    {_RTMs, _TableName, _Role, _LAcceptor, _GLLearner} = State) ->
%%     ?TRACE("tx_tm_rtm:on:crash of ~p in Transaction ~p~n", [_Pid, binary_to_term(_Cookie)]),
%%     %% @todo should we take over, if the TM failed?
%%     %% Takeover done by timeout (propose yourself). Doing it here could
%%     %% improve speed, but really necessary!?
%%     %%
%%     %% for all Tids make a fold with
%%     %% NewState = lists:foldr(fun(X, XState) ->
%%     %%   on({tx_tm_rtm_propose_yourself, Tid}, XState)
%%     %%                        end, State, listwithalltids),
%%     State;
on({fd, _Cookie, {fd_notify, _Event, _Pid, _Reason}}, State) ->
    State;

on({new_node_id, Id}, State) ->
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    RTMs = state_get_RTMs(State),
    IDs = ?RT:get_replica_keys(Id),
    NewRTMs = [ set_rtmkey(R, I) || {R, I} <- lists:zip(RTMs, IDs) ],
    state_set_RTMs(State, NewRTMs);
%% periodic RTM update
on({update_RTMs}, State) ->
    ?TRACE_RTM_MGMT("tx_tm_rtm:on:update_RTMs in Pid ~p ~n", [self()]),
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    rtm_update_trigger(state_get_RTMs(State)),
    State;
on({update_RTMs_on_init}, State) ->
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    %% keep trigger alive to avoid restarting it (possibly infected)
    msg_delay:send_trigger(1, {update_RTMs_on_init}),
    State;
%% accept RTM updates
on({get_rtm_reply, InKey, InPid, InAcceptor}, State) ->
    ?TRACE_RTM_MGMT("tx_tm_rtm:on:get_rtm_reply in Pid ~p for Pid ~p and State ~p~n", [self(), InPid, State]),
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    RTMs = state_get_RTMs(State),
    NewRTMs = rtms_upd_entry(RTMs, InKey, InPid, InAcceptor),
    rtms_of_same_dht_node(NewRTMs),
    state_set_RTMs(State, NewRTMs).


-spec on_init(comm:message(), state())
    -> state() |
       {'$gen_component', [{on_handler, Handler::gen_component:handler()}], State::state()}.
on_init({get_node_details}, State) ->
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    util:wait_for(fun() -> comm:is_valid(comm:this()) end),
    comm:send_local(pid_groups:get_my(dht_node),
                    {get_node_details, comm:this(), [node]}),
    % update gllearner with determined ip-address
    state_set_gllearner(State,
                        comm:make_global(get_my(state_get_role(State),
                                                learner)));

%% While initializing
on_init({get_node_details_response, NodeDetails}, State) ->
    ?TRACE("tx_tm_rtm:on_init:get_node_details_response State; ~p~n", [_State]),
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    IdSelf = node:id(node_details:get(NodeDetails, node)),
    %% provide ids for RTMs (sorted by increasing latency to them).
    %% first entry is the locally hosted replica of IdSelf
    RTM_ids = ?RT:get_replica_keys(IdSelf),
    {NewRTMs, _} = lists:foldr(
                fun(X, {Acc, I}) ->
                  {[rtm_entry_new(X, unknown, I, unknown) | Acc ], I - 1}
                end,
                {[], length(RTM_ids)}, RTM_ids),
    rtm_update_once(NewRTMs),
    state_set_RTMs(State, NewRTMs);

on_init({update_RTMs}, State) ->
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    rtm_update_trigger(state_get_RTMs(State)),
    State;
on_init({update_RTMs_on_init}, State) ->
    ?TRACE_RTM_MGMT("tx_tm_rtm:on_init:update_RTMs in Pid ~p ~n", [self()]),
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    rtm_update_once(state_get_RTMs(State)),
    msg_delay:send_trigger(1, {update_RTMs_on_init}),
    State;

on_init({get_rtm_reply, InKey, InPid, InAcceptor}, State) ->
    ?TRACE_RTM_MGMT("tx_tm_rtm:on_init:get_rtm_reply in Pid ~p for Pid ~p State ~p~n", [self(), InPid, State]),
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    RTMs = state_get_RTMs(State),
    NewRTMs = rtms_upd_entry(RTMs, InKey, InPid, InAcceptor),
    case lists:keymember(unknown, 2, NewRTMs) of %% filled all entries?
        false ->
            rtms_of_same_dht_node(NewRTMs),
            gen_component:change_handler(state_set_RTMs(State, NewRTMs), fun ?MODULE:on/2);
        _ -> state_set_RTMs(State, NewRTMs)
    end;

on_init({new_node_id, Id}, State) ->
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    case state_get_RTMs(State) of
        [] ->
            %% new_node_id before first get_node_details: delay this info.
            comm:send_local(self(), {new_node_id, Id}),
            State;
        [_|_] = RTMs ->
            %% tx_tm processes have always #replicas entries here,
            %% crashed rtms are listed as 'unknown' (so using
            %% lists:zip is safe here)
            IDs = ?RT:get_replica_keys(Id),
            NewRTMs = [ set_rtmkey(R, I) || {R, I} <- lists:zip(RTMs, IDs) ],
            state_set_RTMs(State, NewRTMs)
    end;

%% do not accept new commit requests when not enough rtms are valid
on_init({tx_tm_rtm_commit, _Client, _ClientsID, _TransLog} = Msg, State) ->
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    %% forward request to a node which is ready to serve requests
    %% there, redirect message to tx_tm
    RedirectMsg = {?send_to_group_member, tx_tm, Msg},
    comm:send_local(pid_groups:get_my(routing_table),
                    {?lookup_aux, ?RT:get_random_node_id(), 0, RedirectMsg}),
    State;

%% perform operations on already running transactions, should not be a problem
on_init({learner_decide, _ItemId, _PaxosID, _Value} = Msg, State) ->
    on(Msg, State);
on_init({tx_tm_rtm_tid_isdone, _TxId} = Msg, State) ->
    on(Msg, State);
on_init({?tp_committed, _ItemId} = Msg, State) ->
    on(Msg, State);
on_init({delete_if_still_uninitialized, _ItemId} = Msg, State) ->
    on(Msg, State);
on_init({?tx_tm_rtm_delete, _TxId, _Decision} = Msg, State) ->
    on(Msg, State);
on_init({?register_TP, {Tid, _ItemId, _PaxosID, _TP}} = Msg, State) ->
    {ErrCodeTx, _TxState} = get_tx_entry(Tid, State),
    case new =:= ErrCodeTx of
        true ->
            msg_delay:send_local(1, self(), Msg),
            State;
        false ->
            on(Msg, State)
    end;
on_init({update_snapno, SnapNo}, State) ->
    %% only in tx_tm not in rtm processes!
    ?DBG_ASSERT(tx_tm =:= state_get_role(State)),
    state_set_local_snapno(State, SnapNo);
on_init({fd, Cookie, {fd_notify, crash, Pid, Reason}}, State) ->
    %% only in tx_tm
    handle_crash(Pid, Reason, Cookie, State, on_init);
on_init({fd, _Cookie, {fd_notify, _Event, _Pid, _Reason}}, State) ->
    State.

%% functions for periodic RTM updates
-spec rtm_update_trigger(rtms()) -> ok.
rtm_update_trigger(RTMs) ->
    rtm_update_once(RTMs),
    msg_delay:send_trigger(config:read(tx_rtm_update_interval) div 1000,
                           {update_RTMs}),
    ok.

-spec rtm_update_once(rtms()) -> ok.
rtm_update_once(RTMs) ->
    This = comm:this(),
    _ = [ begin
              Name = get_nth_rtm_name(get_nth(RTM)),
              Key = get_rtmkey(RTM),
              api_dht_raw:unreliable_lookup(Key, {get_rtm, This, Key, Name})
          end
          || RTM <- RTMs],
    ok.

%% functions for tx processing
-spec init_RTMs(tx_state(), [tx_item_state()]) -> ok.
init_RTMs(TxState, ItemStates) ->
    ?TRACE("tx_tm_rtm:init_RTMs~n", []),
    ItemStatesForRTM =
        [ {tx_item_get_itemid(ItemState),
           tx_item_get_maj_for_prepared(ItemState),
           tx_item_get_maj_for_abort(ItemState),
           tx_item_get_tlog(ItemState)}
        || ItemState <- ItemStates],
    RTMs = tx_state_get_rtms(TxState),
    send_to_rtms(
      RTMs, fun(X) -> {?tx_tm_rtm_init_RTM, TxState, ItemStatesForRTM, get_nth(X)}
            end).

-spec init_TPs(tx_state(), [tx_item_state()], non_neg_integer()) -> ok.
init_TPs(TxState, ItemStates, LocalSnapNumber) ->
    ?TRACE("tx_tm_rtm:init_TPs~n", []),
    %% send to each TP its own record / request including the RTMs to
    %% be used
    Tid = tx_state_get_tid(TxState),
    RTMs = tx_state_get_rtms(TxState),
    CleanRTMs = rtms_get_valid_rtmpids(RTMs),
    Accs = rtms_get_valid_accpids(RTMs),
    TM = comm:this(),
    _ = [ begin
          %% ItemState = lists:keyfind(ItemId, 1, ItemStates),
          ItemId = tx_item_get_itemid(ItemState),
          [ begin
                Key = tx_tlog:get_entry_key(RTLog),
                Msg1 = {?init_TP, {Tid, CleanRTMs, Accs, TM,
                                   tx_tlog:drop_value(RTLog), ItemId, PaxId,
                                   LocalSnapNumber}},
                %% delivers message to a dht_node process, which has
                %% also the role of a TP
                api_dht_raw:unreliable_lookup(Key, Msg1)
            end
            || {PaxId, RTLog, _TP} <- tx_item_get_paxosids_rtlogs_tps(ItemState) ]
              %%      end || ItemId <- tx_state_get_txitemids(TxState) ],
      end || ItemState <- ItemStates ],
    ok.

-spec get_tx_entry(tx_id(), state())
                     -> {new | ok | uninitialized, tx_state()}.
get_tx_entry(Id, State) ->
    case pdb:get(Id, state_get_tablename(State)) of
        undefined -> {new, tx_state_new(Id)};
        Entry -> {tx_state_get_status(Entry), Entry}
    end.

-spec get_item_entry(tx_item_id(), state())
        -> {new | uninitialized | ok, tx_item_state()}.
get_item_entry(Id, State) ->
    case pdb:get(Id, state_get_tablename(State)) of
        undefined -> {new, tx_item_new(Id)};
        Entry -> {tx_item_get_status(Entry), Entry}
    end.

-spec set_entry(tx_state() | tx_item_state(), state()) -> ok.
set_entry(NewEntry, State) ->
    pdb:set(NewEntry, state_get_tablename(State)).

-spec get_paxos_ids(State::state(), TxState::tx_state()) -> [paxos_id()].
get_paxos_ids(State, TxState) ->
    lists:flatmap(
      fun(ItemId) ->
              {ok, ItemState} = get_item_entry(ItemId, State),
              [PaxId || {PaxId, _RTLog, _TP}
                            <- tx_item_get_paxosids_rtlogs_tps(ItemState)]
      end, tx_state_get_txitemids(TxState)).

-spec inform_client(tx_state(), state(), ?commit | ?abort) -> ok.
inform_client(TxState, State, Result) ->
    ?TRACE("tx_tm_rtm:inform client~n", []),
    Client = tx_state_get_client(TxState),
    ClientsId = tx_state_get_clientsid(TxState),
    ClientResult = case Result of
                       ?commit -> commit;
                       ?abort -> {abort, get_failed_keys(TxState, State)}
                   end,
    case Client of
        unknown -> ok;
        _       -> msg_commit_reply(Client, ClientsId, ClientResult)
    end,
    ok.

-spec inform_tps(tx_state(), state(), ?commit | ?abort) ->
                           tx_state().
inform_tps(TxState, State, Result) ->
    ?TRACE("tx_tm_rtm:inform tps~n", []),
    LocalSnapNumber = state_get_local_snapno(State),
    %% inform TPs
    This = comm:this(),
    Informed =
        lists:foldl(
          fun(ItemId, Sum) ->
                  {ok, ItemState} = get_item_entry(ItemId, State),
                  lists:foldl(
                    fun({PaxId, RTLogEntry, TP}, Sum2) ->
                            case comm:is_valid(TP) of
                                true ->
                                    msg_tp_do_commit_abort(
                                      TP,
                                      {PaxId, RTLogEntry, This, ItemId},
                                      Result, LocalSnapNumber),
                                    Sum2 + 1;
                                _ -> Sum2
                            end
                    end, Sum, tx_item_get_paxosids_rtlogs_tps(ItemState))
          end, 0, tx_state_get_txitemids(TxState)),
    tx_state_set_numinformed(TxState, Informed).

-spec inform_rtms(tx_id(), tx_state(), ?commit | ?abort) -> ok.
inform_rtms(TxId, TxState, Result) ->
    ?TRACE("tx_tm_rtm:inform rtms~n", []),
    %% inform the rtms stored in the txid:
    RTMs = tx_state_get_rtms(TxState),
    _ = [ begin
              case get_rtmpid(RTM) of
                  unknown -> ok;
                  {RTMPid} -> comm:send(RTMPid,
                                        {?tx_tm_rtm_delete, TxId, Result})
              end
          end
          || RTM <- RTMs ],
    ok.

-spec trigger_delete_if_done(tx_state(), state()) -> ok.
trigger_delete_if_done(TxState, State) ->
    ?TRACE("tx_tm_rtm:trigger delete?~n", []),
    case (tx_state_is_decided(TxState)) of
        ?undecided -> ok;
        false -> ok;
        Decision -> %% commit / abort
            %% @TODO majority informed is sufficient?!
            case tx_state_all_tps_informed(TxState)
                     andalso ((tx_state_get_numids(TxState) =:= tx_state_get_numcommitack(TxState))
                              orelse (tx_tm =/= state_get_role(State)))
                %%    andalso tx_state_all_tps_registered(TxState)
            of
                true ->
                    TxId = tx_state_get_tid(TxState),
                    %% wait a bit for slow minority of answers to arrive
                    %% (delete_if_still_uninitialized is only triggered when
                    %% this is not enough to catch the fourth learner_decide)
                    msg_delay:send_local(2, self(),
                                         {?tx_tm_rtm_delete, TxId, Decision});
                false -> ok
            end
    end, ok.

%% @doc Merges the item states transferred in a ?tx_tm_rtm_init_RTM message
%%      into the locally known state, initiates new paxos processes and
%%      returns the holdback (message) queue.
-spec merge_item_states(Tid::tx_id(), [tx_item_id()],
                        [{EntryId::tx_item_id(),
                          Maj_for_prepared::non_neg_integer(),
                          Maj_for_abort::non_neg_integer(), tx_tlog:tlog_entry()}],
                         State::state(), Learners::[comm:mypid()], LAcceptor::pid())
        -> HoldBackQ::[[comm:message()]].
merge_item_states(_Tid, [], [], _State, _Learners, _LAcceptor) -> [];
merge_item_states(Tid, [EntryId | RestLocal],
                  [{EntryId, Maj_for_prepared, Maj_for_abort, TLogEntry} | RestNew],
                  State, Learners, LAcceptor) ->
    {LocalItemStatus, LocalItem} = get_item_entry(EntryId, State),
    {TmpItem, TmpItemHoldBackQR} =
        case LocalItemStatus of
            new -> %% nothing known locally
                {tx_item_new(
                  EntryId, Tid, TLogEntry, Maj_for_prepared,
                  Maj_for_abort), []};
            uninitialized ->
                %% take over hold back from existing entry
                IHoldBQ = tx_item_get_hold_back(LocalItem),
                Entry = tx_item_new(
                          EntryId, Tid, TLogEntry, Maj_for_prepared,
                          Maj_for_abort),
                {Entry, lists:reverse(IHoldBQ)};
            ok ->
                log:log(error, "Duplicate init_RTM for an item", []),
                %% hold back queue should be empty!
                {LocalItem, []}
        end,
    NewItem = tx_item_set_status(TmpItem, ok),
    _ = set_entry(NewItem, State),
    _ = [ acceptor:start_paxosid_local(LAcceptor, PaxId, Learners)
            || {PaxId, _RTlog, _TP} <- tx_item_get_paxosids_rtlogs_tps(NewItem) ],
    [TmpItemHoldBackQR |
         merge_item_states(Tid, RestLocal, RestNew, State, Learners, LAcceptor)].

%% -spec count_messages_for_type(Type::term()) ->
%%                                      {TypeCount::non_neg_integer(),
%%                                       OtherCount::non_neg_integer()}.
%% count_messages_for_type(Type) ->
%%     {_, Msg} = erlang:process_info(self(), messages),
%%     lists:foldl(fun(X, {AccType, AccOther} = Acc) ->
%%                         if is_tuple(X) ->
%%                                Tag = element(1, X),
%%                                case Tag of
%%                                    Type -> {AccType + 1, AccOther};
%%                                    _    -> {AccType, AccOther + 1}
%%                                end;
%%                            true -> Acc
%%                         end
%%                 end, {0, 0}, Msg).

%% enough_tps_registered(TxState, State) ->
%%     BoolV =
%%         [ begin
%%               {ok, ItemState} = get_item_entry(X, State),
%%               {_,_,TPs} =
%%                   lists:unzip3(tx_item_get_paxosids_rtlogs_tps(ItemState)),
%%               % TODO: if activated, rather fold over the list of TPs to get the number of valid TPs for better performance(?)
%%               ValidTPs = [ Y || Y <- TPs, unknown =/= Y],
%%               length(ValidTPs) >= tx_item_get_maj_for_prepared(ItemState)
%%           end
%%           || X <- tx_state_get_txitemids(TxState)],
%%     lists:foldl(fun(X, Acc) -> Acc andalso X end, true, BoolV).

-spec rtms_of_same_dht_node(rtms()) -> boolean().
rtms_of_same_dht_node(InRTMs) ->
    %% group_of may return failed, don't include these
    Groups =
        lists:usort(
          [G || X <- InRTMs, unknown =/= (RTMPid = get_rtmpid(X)),
                failed =/= (G = pid_groups:group_of(
                                  comm:make_local(element(1, RTMPid))))]),
    case length(Groups) of
        4 -> false;
        _ ->
            log:log(info, "RTMs of same DHT node are used. Please start more Scalaris nodes.~n"),
            true
    end.

-compile({inline, [rtm_entry_new/4, get_rtmkey/1, set_rtmkey/2, get_rtmpid/1,
                   get_nth/1, %get_accpid/1,
                   rtms_get_valid_rtmpids/1, rtms_get_valid_accpids/1]}).

-spec rtm_entry_new(?RT:key(), {comm:mypid()} | unknown,
                    pos_integer(), {comm:mypid()} | unknown) -> rtm().
rtm_entry_new(Key, RTMPid, Nth, AccPid) -> {Key, RTMPid, Nth, AccPid}.
-spec get_rtmkey(rtm()) -> ?RT:key().
get_rtmkey(RTMEntry) -> element(1, RTMEntry).
-spec set_rtmkey(rtm(), ?RT:key()) -> rtm().
set_rtmkey(RTMEntry, Val) -> setelement(1, RTMEntry, Val).
-spec get_rtmpid(rtm()) -> {comm:mypid()} | unknown.
get_rtmpid(RTMEntry) -> element(2, RTMEntry).
-spec get_nth(rtm()) -> pos_integer().
get_nth(RTMEntry)    -> element(3, RTMEntry).
%% -spec get_accpid(rtm()) -> {comm:mypid()} | unknown.
%% get_accpid(RTMEntry) -> element(4, RTMEntry).

-spec rtms_get_valid_rtmpids(rtms()) -> [comm:mypid()].
rtms_get_valid_rtmpids(RTMs) ->
    [ RTMPid || {_Key, {RTMPid}, _Nth, _AccPid} <- RTMs ].
-spec rtms_get_valid_accpids(rtms()) -> [comm:mypid()].
rtms_get_valid_accpids(RTMs) ->
    [ AccPid || {_Key, _RTMPid, _Nth, {AccPid}} <- RTMs ].

-spec rtms_upd_entry(rtms(), ?RT:key(), comm:mypid(), comm:mypid()) -> rtms().
rtms_upd_entry(RTMs, InKey, InPid, InAccPid) ->
    [ case get_rtmkey(Entry) of
          InKey ->
              RTMPid = {InPid},
              case get_rtmpid(Entry) of
                  RTMPid  -> ok;
                  unknown -> fd:subscribe_refcount(
                               comm:reply_as(self(), 3, {fd, tx_tm_rtm, '_'}),
                               [InPid]);
                  {XPid}  -> Self = comm:reply_as(self(), 3, {fd, tx_tm_rtm, '_'}),
                             fd:unsubscribe_refcount(Self, [XPid]),
                             fd:subscribe_refcount(Self, [InPid])
              end,
              rtm_entry_new(InKey, RTMPid, get_nth(Entry), {InAccPid});
          _ -> Entry
      end || Entry <- RTMs ].

-spec send_to_rtms(rtms(), fun((rtm()) -> comm:message())) -> ok.
send_to_rtms(RTMs, MsgGen) ->
    _ = [ case get_rtmpid(RTM) of
              unknown  -> ok;
              {RTMPid} -> comm:send(RTMPid, MsgGen(RTM))
          end || RTM <- RTMs ],
    ok.

-spec get_nth_rtm_name(pos_integer()) -> pid_groups:pidname().
get_nth_rtm_name(N) -> {tx_rtm, N}.

-spec get_my(pid_groups:pidname(), atom()) -> pid() | failed.
get_my(Role, PaxosRole) ->
    pid_groups:get_my({Role, PaxosRole}).

-compile({inline, [state_get_RTMs/1, state_set_RTMs/2,
                   state_get_tablename/1, state_get_role/1, state_get_lacceptor/1,
                   state_get_gllearner/1, state_set_gllearner/2,
                   state_get_opentxnum/1, state_inc_opentxnum/1, state_dec_opentxnum/1,
                   state_get_local_snapno/1, state_set_local_snapno/2,
                   state_subscribe/2, state_unsubscribe/2]}).

-spec state_get_RTMs(state())      -> rtms().
state_get_RTMs(State)          -> element(1, State).
-spec state_set_RTMs(state(), rtms())
                    -> state().
state_set_RTMs(State, Val)     -> setelement(1, State, Val).
-spec state_get_tablename(state()) -> pdb:tableid().
state_get_tablename(State)     -> element(2, State).
-spec state_get_role(state())       -> pid_groups:pidname().
state_get_role(State)          -> element(3, State).
-spec state_get_lacceptor(state())  -> pid().
state_get_lacceptor(State)     -> element(4, State).
-spec state_get_gllearner(state()) -> comm:mypid().
state_get_gllearner(State) -> element(5, State).
-spec state_set_gllearner(state(), comm:mypid()) -> state().
state_set_gllearner(State, Pid) -> setelement(5, State, Pid).
-spec state_get_opentxnum(state()) -> non_neg_integer().
state_get_opentxnum(State) -> element(6, State).
-spec state_inc_opentxnum(state()) -> state().
state_inc_opentxnum(State) -> setelement(6, State, element(6, State) + 1).
-spec state_dec_opentxnum(state()) -> state().
state_dec_opentxnum(State) ->
    setelement(6, State, erlang:max(0, element(6, State) - 1)).
-spec state_get_local_snapno(state()) -> non_neg_integer().
state_get_local_snapno(State) -> element(7, State).
-spec state_set_local_snapno(state(), non_neg_integer()) -> state().
state_set_local_snapno(State, No) -> setelement(7, State, No).

-spec state_subscribe(state(), comm:mypid()) -> state().
state_subscribe(State, Pid) ->
    Self = comm:reply_as(self(), 3, {fd, state_get_role(State), '_'}),
    fd:subscribe_refcount(Self, [Pid]),
    State.

-spec state_unsubscribe(state(), comm:mypid()) -> state().
state_unsubscribe(State, Pid) ->
    Self = comm:reply_as(self(), 3, {fd, state_get_role(State), '_'}),
    fd:unsubscribe_refcount(Self, [Pid]),
    State.

-spec get_failed_keys(tx_state(), state()) -> [client_key()].
get_failed_keys(TxState, State) ->
    NumAbort = tx_state_get_numabort(TxState),
    Result =
    case NumAbort of
        0 -> [];
        _ ->
            [ tx_tlog:get_entry_key(tx_item_get_tlog(TxItem))
                || TxItemId <- tx_state_get_txitemids(TxState),
                   ?abort =:= tx_item_get_decided(
                     TxItem = element(2, get_item_entry(TxItemId, State)))]
    end,
    ?DBG_ASSERT2(length(Result) =:= NumAbort,
                 {'NumAbort counter differs from actual TxItem states',
                  length(Result), '=/=', NumAbort}),
    Result.

-spec handle_crash(comm:mypid(), Reason::fd:reason(), Cookie::tx_tm_rtm | pid_groups:pidname(),
                   state(), on | on_init)
                  -> state() |
                     {'$gen_component',
                      [{on_handler, fun((comm:message(), state()) -> state())}],
                      state()}.
handle_crash(Pid, _Reason, _Cookie, State, Handler) ->
    %% tm: update rtms
    %% rtm: take over tx

    %% in case an rtm crashed: (RTMs is an empty list for rtms)
    RTMs = state_get_RTMs(State),
    This = comm:this(),
    NewRTMs = [ case get_rtmpid(RTM) of
                    X when unknown =:= X orelse {Pid} =:= X ->
                        I = get_nth(RTM),
                        Name = get_nth_rtm_name(I),
                        Key = get_rtmkey(RTM),
                        api_dht_raw:unreliable_lookup(
                          Key, {get_rtm, This, Key, Name}),
                        rtm_entry_new(Key, unknown, I, unknown);
                    _ -> RTM
                end
                || RTM <- RTMs ],

    %% in case a tm crashed:
    %% scan over all running transactions and delete this Pid
    %% if necessary, takeover the tx and try deciding with abort
    NewState =
        case state_get_role(State) of
            tx_tm -> State;
            _ -> %% tx_rtm
                lists:foldl(
                  fun(X,StateIter) ->
                          case is_tx_state(X) of
                              true ->
                                  log:pal(
                                    "propose yourself (~.0p/~.0p) for: ~.0p~n",
                                    [self(),
                                     pid_groups:group_and_name_of(self()),
                                     tx_state_get_tid(X)]),
                                  on({tx_tm_rtm_propose_yourself, tx_state_get_tid(X)}, StateIter);
                              false -> StateIter
                          end
                  end, State, pdb:tab2list(state_get_tablename(State)))
        end,

    %% in case an rtm crashed: (RTMs is an empty list for rtms)
    %% no longer use this RTM
    ValidRTMs = [ X || X <- NewRTMs, unknown =/= get_rtmpid(X) ],
    case length(ValidRTMs) < 3
        andalso tx_tm =:= state_get_role(NewState)
        andalso on =:= Handler of
        true ->
            gen_component:change_handler(
              state_set_RTMs(NewState, NewRTMs),
              fun ?MODULE:on_init/2);
        false -> state_set_RTMs(NewState, NewRTMs)
    end.

-spec is_tx_state(tx_state() | tx_item_state()) -> boolean().
is_tx_state(X) when is_tuple(X) ->
    ?tx_state =:= element(2, X);
is_tx_state(_) -> false.

%% @doc Checks whether config parameters for tx_tm_rtm exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(replication_factor) and
    config:cfg_is_greater_than(replication_factor, 0) and

    config:cfg_is_integer(tx_timeout) and
    config:cfg_is_greater_than(tx_timeout, 0) and
    config:cfg_is_integer(tx_rtm_update_interval) and
    config:cfg_is_greater_than(tx_rtm_update_interval, 0) and

    config:cfg_is_greater_than_equal(tx_timeout, 1000 div 3)
%%     config:cfg_is_greater_than_equal(tx_timeout, 1000 div 2)
    .
