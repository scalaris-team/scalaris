% @copyright 2009-2015 Zuse Institute Berlin.

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
%%      The state for a whole transaction in a TM and RTM.
%% @end
%% @version $Id$

-compile({inline, [tx_state_get_tid/1, %tx_state_set_tid/2,
                   tx_state_get_client/1, %tx_state_set_client/2,
                   tx_state_get_clientsid/1, %tx_state_set_clientsid/2,
                   tx_state_get_tm/1, %tx_state_set_tm/2,
                   tx_state_get_rtms/1, %tx_state_set_rtms/2,
                   tx_state_get_txitemids/1, %tx_state_set_txitemids/2,
                   tx_state_get_learners/1,
                   tx_state_is_decided/1, tx_state_set_decided/2,
                   tx_state_get_numids/1,
                   tx_state_get_numprepared/1, tx_state_inc_numprepared/1,
                   tx_state_get_numabort/1, tx_state_inc_numabort/1,
                   tx_state_get_numinformed/1, tx_state_inc_numinformed/1,
                   tx_state_set_numinformed/2,
                   tx_state_get_numcommitack/1, tx_state_inc_numcommitack/1,
                   tx_state_get_numtpsregistered/1, tx_state_inc_numtpsregistered/1,
                   tx_state_get_status/1, tx_state_set_status/2,
                   tx_state_get_hold_back/1, tx_state_hold_back/2,
                   %tx_state_set_hold_back/2,
                   tx_state_all_tps_informed/1, tx_state_all_tps_registered/1
                  ]}).

-type tx_state() ::
        {tx_id(),                  %% Tid
         ?tx_state,                 %% type-marker (unnecessary in principle)
         comm:mypid() | unknown,   %% Client
         any(),                    %% ClientsId,
         comm:mypid() | unknown,   %% TM
         rtms(), %% [{Key, RTM, Nth, Acceptor}]
         [tx_item_id()], %% _txitemids = [TxItemId],
         [comm:mypid()],           %% Learners,
         ?undecided | false | ?abort | ?commit, %% Status decided?
         non_neg_integer(),        %% NumIds,
         non_neg_integer(),        %% NumPrepared,
         non_neg_integer(),        %% NumAbort,
         non_neg_integer(),        %% NumTPsInformed
         non_neg_integer(),        %% Number of items committed
         non_neg_integer(),        %% NumTpsRegistered
         new | uninitialized | ok, %% status: new / uninitialized / ok
         [comm:message()]                   %% HoldBack
         }.
%% @TODO also necessary?
%%               Majority of RTMs,
%%               timeout before RTM takes over
%%               }

%% -spec is_tx_state(tx_state()) -> boolean().
%% is_tx_state(TxState) ->
%%     is_tuple(TxState)
%%         andalso erlang:tuple_size(TxState) >= 2
%%         andalso ?tx_state =:= element(2, TxState).

-spec tx_state_new(tx_id(), comm:mypid() | unknown,
                   comm:mypid() | unknown, comm:mypid() | unknown,
                   tx_tm_rtm:rtms(),
                   [tx_item_state()],
                   [comm:mypid()]) -> tx_state().
tx_state_new(Tid, Client, ClientsID, TM, RTMs, ItemStates, Learners) ->
    {Tid, ?tx_state, Client, ClientsID, TM, RTMs,
     [tx_item_get_itemid(ItemState) || ItemState <- ItemStates],
     Learners, ?undecided, length(ItemStates),
     _Prepared = 0, _Aborts = 0, _Informed = 0, _CommitsAcked = 0,
     _TpsRegistered = 0, _Status = uninitialized, _HoldBackQueue = []}.

-spec tx_state_new(tx_id()) -> tx_state().
tx_state_new(Tid) ->
    tx_state_new(Tid, unknown, unknown, unknown, _RTMs = [], _ItemStates = [], []).

-spec tx_state_get_tid(tx_state()) -> tx_id().
tx_state_get_tid(State) ->                 element(1, State).
%% -spec tx_state_set_tid(tx_state(), tx_id()) -> tx_state().
%% tx_state_set_tid(State, Val) ->            setelement(1, State, Val).
-spec tx_state_get_client(tx_state()) -> comm:mypid() | unknown.
tx_state_get_client(State) ->              element(3, State).
%% -spec tx_state_set_client(tx_state(), comm:mypid() | unknown) -> tx_state().
%% tx_state_set_client(State, Val) ->         setelement(3, State, Val).
-spec tx_state_get_clientsid(tx_state()) -> any().
tx_state_get_clientsid(State) ->           element(4, State).
%% -spec tx_state_set_clientsid(tx_state(), any()) -> tx_state().
%% tx_state_set_clientsid(State, Val) ->      setelement(4, State, Val).

-spec tx_state_get_tm(tx_state()) -> comm:mypid() | unknown.
tx_state_get_tm(State) ->                  element(5, State).
%% -spec tx_state_set_tm(tx_state(), comm:mypid() | unknown) -> tx_state().
%% tx_state_set_tm(State, Val) ->             setelement(5, State, Val).

-spec tx_state_get_rtms(tx_state()) -> tx_tm_rtm:rtms().
tx_state_get_rtms(State) ->                element(6, State).
%% -spec tx_state_set_rtms(tx_state(), tx_tm_rtm:rtms()) -> tx_state().
%% tx_state_set_rtms(State, Val) ->           setelement(6, State, Val).
-spec tx_state_get_txitemids(tx_state()) -> [tx_tm_rtm:tx_item_id()].
tx_state_get_txitemids(State) ->      element(7, State).
%% -spec tx_state_set_txitemids(tx_state(), [tx_tm_rtm:tx_item_id()]) -> tx_state().
%% tx_state_set_txitemids(State, Val) -> setelement(7, State, Val).
-spec tx_state_get_learners(tx_state()) -> [comm:mypid()].
tx_state_get_learners(State) ->            element(8, State).
-spec tx_state_is_decided(tx_state()) -> ?undecided | false | ?abort | ?commit.
tx_state_is_decided(State) ->              element(9, State).
-spec tx_state_set_decided(tx_state(), ?undecided | false | ?abort | ?commit) -> tx_state().
tx_state_set_decided(State, Val) ->        setelement(9, State, Val).
-spec tx_state_get_numids(tx_state()) -> non_neg_integer().
tx_state_get_numids(State) ->              element(10, State).
-spec tx_state_get_numprepared(tx_state()) -> non_neg_integer().
tx_state_get_numprepared(State) ->         element(11, State).
-spec tx_state_inc_numprepared(tx_state()) -> tx_state().
tx_state_inc_numprepared(State) ->         setelement(11, State, 1 + element(11, State)).
-spec tx_state_get_numabort(tx_state()) -> non_neg_integer().
tx_state_get_numabort(State) ->            element(12, State).
-spec tx_state_inc_numabort(tx_state()) -> tx_state().
tx_state_inc_numabort(State) ->            setelement(12, State, 1 + element(12, State)).
-spec tx_state_get_numinformed(tx_state()) -> non_neg_integer().
tx_state_get_numinformed(State) ->         element(13, State).
-spec tx_state_inc_numinformed(tx_state()) -> tx_state().
tx_state_inc_numinformed(State) ->         setelement(13, State, 1 + element(13, State)).
-spec tx_state_set_numinformed(tx_state(), non_neg_integer()) -> tx_state().
tx_state_set_numinformed(State, Val) ->    setelement(13, State, Val).
-spec tx_state_get_numcommitack(tx_state()) -> non_neg_integer().
tx_state_get_numcommitack(State) ->        element(14, State).
-spec tx_state_inc_numcommitack(tx_state()) -> tx_state().
tx_state_inc_numcommitack(State) ->        setelement(14, State, 1 + element(14, State)).
-spec tx_state_get_numtpsregistered(tx_state()) -> non_neg_integer().
tx_state_get_numtpsregistered(State) ->    element(15, State).
-spec tx_state_inc_numtpsregistered(tx_state()) -> tx_state().
tx_state_inc_numtpsregistered(State) ->    setelement(15, State, 1 + element(15, State)).

-spec tx_state_get_status(tx_state()) -> new | uninitialized | ok.
tx_state_get_status(State) -> element(16, State).
-spec tx_state_set_status(tx_state(), new | uninitialized | ok) -> tx_state().
tx_state_set_status(State, Status) -> setelement(16, State, Status).
-spec tx_state_get_hold_back(tx_state()) -> [comm:message()].
tx_state_get_hold_back(State) -> element(17, State).
-spec tx_state_hold_back(comm:message(), tx_state()) -> tx_state().
tx_state_hold_back(Msg, State) -> setelement(17, State, [Msg | element(17, State)]).
%% -spec tx_state_set_hold_back(tx_state(), [comm:message()]) -> tx_state().
%% tx_state_set_hold_back(State, Queue) -> setelement(17, State, Queue).


-spec tx_state_newly_decided(tx_state()) -> ?undecided | false | ?abort | ?commit.
tx_state_newly_decided(State) ->
    case (?undecided =:= tx_state_is_decided(State)) of
        false -> false; %% was already decided
        true ->
            %% maybe: calculate decision:
            case tx_state_get_numabort(State) > 0 of
                true -> ?abort;
                _ ->
                    case tx_state_get_numprepared(State) =:= tx_state_get_numids(State) of
                        true -> ?commit;
                        _ -> ?undecided
                    end
            end
    end.

-spec tx_state_all_tps_informed(tx_state()) -> boolean().
tx_state_all_tps_informed(State) ->
    tx_state_get_numinformed(State)
        =:= (tx_state_get_numids(State)
             * config:read(replication_factor)).

-spec tx_state_all_tps_registered(tx_state()) -> boolean().
tx_state_all_tps_registered(State) ->
    tx_state_get_numtpsregistered(State)
        =:= (tx_state_get_numids(State)
             * config:read(replication_factor)).

-spec tx_state_add_item_decision(tx_state(), ?prepared | ?abort)
        -> {?undecided | false | {tx_newly_decided, ?abort | ?commit},
            tx_state()}.
tx_state_add_item_decision(State, Decision) ->
    T1 =
        case Decision of
            ?prepared -> tx_state_inc_numprepared(State);
            ?abort    -> tx_state_inc_numabort(State)
        end,
    case tx_state_newly_decided(T1) of
        ?undecided -> {?undecided, T1};
        false -> {false, T1};
        Result -> %% commit or abort
            T2 = tx_state_set_decided(T1, Result),
            {{tx_newly_decided, Result}, T2}
    end.

-spec tx_state_add_item_acked(tx_state())
        -> {all_items_acked | open, tx_state()}.
tx_state_add_item_acked(State) ->
    NewState = tx_state_inc_numcommitack(State),
    NumAll = tx_state_get_numids(NewState),
    case tx_state_get_numcommitack(NewState) of
        NumAll -> {all_items_acked, NewState};
        _      -> {open, NewState}
    end.
