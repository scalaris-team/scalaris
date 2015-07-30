% @copyright 2009-2015 Zuse Institute Berlin,
%                      onScale solutions GmbH

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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc Part of generic transaction implementation -
%%      The state for a single request in a transaction of a TM and RTM.
%% @version $Id$

-compile({inline, [tx_item_get_itemid/1, %tx_item_set_itemid/2,
                   tx_item_get_txid/1,
                   tx_item_get_tlog/1,
                   tx_item_get_maj_for_prepared/1,
                   tx_item_get_maj_for_abort/1,
                   tx_item_get_decided/1, tx_item_set_decided/2,
                   tx_item_get_numprepared/1, tx_item_inc_numprepared/1,
                   tx_item_get_numabort/1, tx_item_inc_numabort/1,
                   tx_item_get_paxosids_rtlogs_tps/1,
                   tx_item_set_paxosids_rtlogs_tps/2,
                   tx_item_get_status/1, tx_item_set_status/2,
                   tx_item_get_hold_back/1, tx_item_hold_back/2,
                   %tx_item_set_hold_back/2,
                   tx_item_get_numcommitted/1, tx_item_inc_numcommitted/1
                  ]}).

-type tx_item_id() :: {?tx_item_id, TLogUid::uid:global_uid(), ItemId::non_neg_integer()}.
-opaque paxos_id() :: {?paxos_id, TLogUid::uid:global_uid(), ItemId::non_neg_integer(), PaxosId::non_neg_integer()}.
-type tx_item_state() ::
 {
   TxItemId::tx_item_id(), %%  1 id of the item
   ?tx_item_state,     %%  2 tx_item_state, data type tag for debugging
   TxId::tx_id() | undefined_tx_id,  %%  3 part of transaction with id TxId
   TLogEntry::tx_tlog:tlog_entry() | empty_tlog_entry, %%  4 corresponding transaction log entry
   Maj_for_prepared::non_neg_integer(), %%  5 prepare votes to decide prepared
   Maj_for_abort::non_neg_integer(), %%  6 abort votes to decide abort
   Decided::false | ?prepared | ?abort, %%  7 current decision status
   Numprepared::non_neg_integer(), %%  8 number of received prepare votes
   Numabort::non_neg_integer(), %%  9 number of received abort votes
   [{paxos_id(), RTLogEntry::tx_tlog:tlog_entry(), TP::comm:mypid() | unknown}], %% 10 involved PaxosIDs
   Status::new | uninitialized | ok, %% 11 item status
   HoldBackQueue::[comm:message()],         %% 12 when not initialized
   NumCommitted::non_neg_integer()  %% 13
 }.
%% @TODO maybe the following entries are also necessary?:
%%      tx_tm_helper_behaviour to use? needed? for what?,
%%      timeout before the first RTM takes over

-spec tx_item_new(tx_item_id()) -> tx_item_state().
tx_item_new(ItemId) ->
    ReplDeg = config:read(replication_factor),
    {ItemId, ?tx_item_state, undefined_tx_id, empty_tlog_entry,
     quorum:majority_for_accept(ReplDeg), quorum:majority_for_deny(ReplDeg),
     false, 0, 0, _no_paxIds = [], uninitialized, _HoldBack = [], 0}.

-spec tx_item_new(tx_item_id(), tx_id(), tx_tlog:tlog_entry())
         -> tx_item_state().
tx_item_new(ItemId, TxId, TLogEntry) ->
    ReplDeg = config:read(replication_factor),
    tx_item_new(ItemId, TxId, TLogEntry, quorum:majority_for_accept(ReplDeg),
        quorum:majority_for_deny(ReplDeg)).

%% pre: if paxos IDs are given, their order must match the order of the created RTLogEntries!
-spec tx_item_new(tx_item_id(), tx_id(), tx_tlog:tlog_entry(),
                  Maj_for_prepared::non_neg_integer(),
                  Maj_for_abort::non_neg_integer())
        -> tx_item_state().
tx_item_new({?tx_item_id, TLogUid, ItemNr} = ItemId, TxId, TLogEntry,
    Maj_for_prepared, Maj_for_abort) ->
    %% expand TransLogEntry to replicated translog entries
    RTLogEntries =
        case tx_tlog:get_entry_operation(TLogEntry) of
            ?read ->
                rdht_tx_read:validate_prefilter(TLogEntry);
            ?write ->
                rdht_tx_write:validate_prefilter(TLogEntry)
        end,
    PaxIDsRTLogsTPs =
        util:map_with_nr(
          fun(RTLogEntry, NrX) ->
                  {{?paxos_id, TLogUid, ItemNr, NrX}, RTLogEntry, unknown}
          end, RTLogEntries, 0),
    {ItemId, ?tx_item_state, TxId, TLogEntry,
     Maj_for_prepared, Maj_for_abort,
     false, 0, 0, PaxIDsRTLogsTPs,
     uninitialized, _HoldBack = [], 0}.

-spec tx_item_get_itemid(tx_item_state()) -> tx_item_id().
tx_item_get_itemid(State) ->         element(1, State).
%% -spec tx_item_set_itemid(tx_item_state(), tx_item_id()) -> tx_item_state().
%% tx_item_set_itemid(State, Val) ->    setelement(1, State, Val).
-spec tx_item_get_txid(tx_item_state()) -> tx_id() | undefined_tx_id.
tx_item_get_txid(State) ->           element(3, State).
-spec tx_item_get_tlog(tx_item_state()) -> tx_tlog:tlog_entry() | empty_tlog_entry.
tx_item_get_tlog(State) ->           element(4, State).
-spec tx_item_get_maj_for_prepared(tx_item_state()) -> non_neg_integer().
tx_item_get_maj_for_prepared(State) -> element(5, State).
-spec tx_item_get_maj_for_abort(tx_item_state()) -> non_neg_integer().
tx_item_get_maj_for_abort(State) ->  element(6, State).
-spec tx_item_get_decided(tx_item_state()) -> false | ?prepared | ?abort.
tx_item_get_decided(State) ->        element(7, State).
-spec tx_item_set_decided(tx_item_state(), false | ?prepared | ?abort) -> tx_item_state().
tx_item_set_decided(State, Val) ->   setelement(7, State, Val).
-spec tx_item_get_numprepared(tx_item_state()) -> non_neg_integer().
tx_item_get_numprepared(State) ->    element(8, State).
-spec tx_item_inc_numprepared(tx_item_state()) -> tx_item_state().
tx_item_inc_numprepared(State) ->    setelement(8, State, element(8,State) + 1).
-spec tx_item_get_numabort(tx_item_state()) -> non_neg_integer().
tx_item_get_numabort(State) ->       element(9, State).
-spec tx_item_inc_numabort(tx_item_state()) -> tx_item_state().
tx_item_inc_numabort(State) ->       setelement(9, State, element(9,State) + 1).

-spec tx_item_get_paxosids_rtlogs_tps(tx_item_state())
        -> [{paxos_id(), tx_tlog:tlog_entry(), comm:mypid() | unknown}].
tx_item_get_paxosids_rtlogs_tps(State) -> element(10, State).
-spec tx_item_set_paxosids_rtlogs_tps(tx_item_state(), [{paxos_id(), tx_tlog:tlog_entry(), comm:mypid()}]) -> tx_item_state().
tx_item_set_paxosids_rtlogs_tps(State, NewTPList) -> setelement(10, State, NewTPList).

-spec tx_item_get_status(tx_item_state()) -> new | uninitialized | ok.
tx_item_get_status(State) ->         element(11, State).
-spec tx_item_set_status(tx_item_state(), new | uninitialized | ok) -> tx_item_state().
tx_item_set_status(State, Status) -> setelement(11, State, Status).
-spec tx_item_get_hold_back(tx_item_state()) -> [comm:message()].
tx_item_get_hold_back(State) ->      element(12, State).
-spec tx_item_hold_back(comm:message(), tx_item_state()) -> tx_item_state().
tx_item_hold_back(Msg, State) ->     setelement(12, State, [Msg | element(12, State)]).
%% -spec tx_item_set_hold_back(tx_item_state(), [comm:message()]) -> tx_item_state().
%% tx_item_set_hold_back(State, Queue) -> setelement(12, State, Queue).

-spec tx_item_get_numcommitted(tx_item_state()) -> non_neg_integer().
tx_item_get_numcommitted(State) ->    element(13, State).
-spec tx_item_inc_numcommitted(tx_item_state()) -> tx_item_state().
tx_item_inc_numcommitted(State) ->       setelement(13, State, element(13,State) + 1).

-spec tx_item_newly_decided(tx_item_state()) -> false | ?prepared | ?abort.
tx_item_newly_decided(State) ->
    case tx_item_get_decided(State) of
        false ->
            Prepared = tx_item_get_numprepared(State) =:= tx_item_get_maj_for_prepared(State),
            Abort =    tx_item_get_numabort(State) =:= tx_item_get_maj_for_abort(State),
            Bad = quorum:majority_for_accept(config:read(replication_factor)) + 1,
            case tx_item_get_numprepared(State) + tx_item_get_numabort(State) of
                Bad -> log:log("Deciding at ~pth answer!!!", [Bad]);
                _ -> ok
            end,
            if Prepared andalso not Abort -> ?prepared;
               not Prepared andalso Abort -> ?abort;
               true -> false
            end;
        _Any -> false
    end.

-spec tx_item_set_tp_for_paxosid(tx_item_state(), comm:mypid(), paxos_id()) -> tx_item_state().
tx_item_set_tp_for_paxosid(State, TP, PaxosId) ->
    TPList = tx_item_get_paxosids_rtlogs_tps(State),
    NewTPList = util:lists_keystore2(PaxosId, 1, TPList, 3, TP),
    tx_item_set_paxosids_rtlogs_tps(State, NewTPList).

-spec tx_item_add_learner_decide(tx_item_state(),
                                 {learner_decide, tx_item_id(),
                                  paxos_id(), ?prepared | ?abort})
        -> {hold_back | state_updated | {item_newly_decided, ?prepared | ?abort},
            tx_item_state()}.
tx_item_add_learner_decide(State, {learner_decide, _ItemId, _PaxosID, Value} = Msg) ->
    case ok =/= tx_item_get_status(State) of
        true -> %% new | uninitialized
            TmpState = tx_item_hold_back(Msg, State),
            NewState = tx_item_set_status(TmpState, uninitialized),
            {hold_back, NewState};
        false -> %% ok
            TmpState =
                case Value of
                    ?prepared -> tx_item_inc_numprepared(State);
                    ?abort ->    tx_item_inc_numabort(State)
                end,
            case tx_item_newly_decided(TmpState) of
                false -> {state_updated, TmpState};
                Decision ->
                    NewState = tx_item_set_decided(TmpState, Decision),
                    {{item_newly_decided, Decision}, NewState}
            end
    end.

-spec tx_item_add_commit_ack(tx_item_state())
        -> {item_newly_safe | state_updated, tx_item_state()}.
tx_item_add_commit_ack(State) ->
    NewState = tx_item_inc_numcommitted(State),
    Maj = tx_item_get_maj_for_prepared(NewState),
    case tx_item_get_numcommitted(NewState) of
        Maj -> {item_newly_safe, NewState};
        _   -> {state_updated, NewState}
    end.
