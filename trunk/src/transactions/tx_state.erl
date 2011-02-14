% @copyright 2009-2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%            2009 onScale solutions GmbH

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
-module(tx_state).
-author('schintke@onscale.de').
-vsn('$Id$').

-include("scalaris.hrl").

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

%% Operations on tx_state
-export([new/6, new/1]).
-export([get_tid/1, set_tid/2]).
-export([get_client/1, set_client/2]).
-export([get_clientsid/1, set_clientsid/2]).
-export([get_rtms/1, set_rtms/2]).
-export([get_tlog_txitemids/1, set_tlog_txitemids/2]).
-export([get_learners/1]).
-export([is_decided/1, set_decided/2]).
-export([get_numids/1]).
-export([get_numprepared/1, inc_numprepared/1]).
-export([get_numabort/1, inc_numabort/1]).
-export([get_numinformed/1, set_numinformed/2, inc_numinformed/1]).
-export([get_numpaxdecided/1, set_numpaxdecided/2, inc_numpaxdecided/1]).
-export([get_numtpsregistered/1, inc_numtpsregistered/1]).
-export([get_status/1, set_status/2]).
-export([hold_back/2, get_hold_back/1, set_hold_back/2]).

-export([newly_decided/1]).
-export([all_tps_informed/1]).
-export([all_pax_decided/1]).
-export([all_tps_registered/1]).

-ifdef(with_export_type_support).
-export_type([tx_id/0, tx_state/0]).
-endif.

-type tx_id() :: {tx_id, util:global_uid()}.
-type tx_state() ::
        {tx_id(),                  %% Tid
         tx_state,                 %% type-marker (unnecessary in principle)
         comm:mypid() | unknown,   %% Client
         any(),                    %% ClientsId,
         [tx_tm_rtm:rtms()], %% [{Key, RTM, Nth}]
         [{tx_tlog:tlog_entry(),
           tx_item_state:tx_item_id()}], %% _tlogtxitemids = [{TLogEntry, TxItemId}],
         [comm:mypid()],           %% Learners,
         undecided | false | abort | commit, %% Status decided?
         non_neg_integer(),        %% NumIds,
         non_neg_integer(),        %% NumPrepared,
         non_neg_integer(),        %% NumAbort,
         non_neg_integer(),        %% NumTPsInformed
         non_neg_integer(),        %% NumPaxDecided
         non_neg_integer(),        %% NumTpsRegistered
         new | uninitialized | ok, %% status: new / uninitialized / ok
         [any()]                   %% HoldBack
         }.
%% @TODO also necessary?
%%               Majority of RTMs,
%%               timeout before RTM takes over
%%               }

-spec new(tx_id(), _, _, _, _, _) -> tx_state().
new(Tid, Client, ClientsID, RTMs, TLogTxItemIds, Learners) ->
    {Tid, tx_state, Client, ClientsID, RTMs, TLogTxItemIds, Learners,
     undecided, length(TLogTxItemIds),
     _Prepared = 0, _Aborts = 0, _Informed = 0, _PaxDecided = 0,
     _TpsRegistered = 0, _Status = uninitialized, _HoldBackQueue = []}.
-spec new(tx_id()) -> tx_state().
new(Tid) ->
    new(Tid, unknown, unknown, _RTMs = [], _TLogTxItemIds = [], []).

-spec get_tid(tx_state()) -> tx_id().
get_tid(State) ->                 element(1, State).
-spec set_tid(tx_state(), tx_id()) -> tx_state().
set_tid(State, Val) ->            setelement(1, State, Val).
-spec get_client(tx_state()) -> comm:mypid() | unknown.
get_client(State) ->              element(3, State).
-spec set_client(tx_state(), comm:mypid() | unknown) -> tx_state().
set_client(State, Val) ->         setelement(3, State, Val).
-spec get_clientsid(tx_state()) -> any().
get_clientsid(State) ->           element(4, State).
-spec set_clientsid(tx_state(), any()) -> tx_state().
set_clientsid(State, Val) ->      setelement(4, State, Val).
-spec get_rtms(tx_state()) -> tx_tm_rtm:rtms().
get_rtms(State) ->                element(5, State).
-spec set_rtms(tx_state(), tx_tm_rtm:rtms()) -> tx_state().
set_rtms(State, Val) ->           setelement(5, State, Val).
-spec get_tlog_txitemids(tx_state()) -> [{tx_tlog:tlog_entry(),
                                          tx_item_state:tx_item_id()}].
get_tlog_txitemids(State) ->      element(6, State).
-spec set_tlog_txitemids(tx_state(),
                         [{tx_tlog:tlog_entry(),
                           tx_item_state:tx_item_id()}]) -> tx_state().
set_tlog_txitemids(State, Val) -> setelement(6, State, Val).
-spec get_learners(tx_state()) -> [comm:mypid()].
get_learners(State) ->            element(7, State).
-spec is_decided(tx_state()) -> undecided | false | abort | commit.
is_decided(State) ->              element(8, State).
-spec set_decided(tx_state(), undecided | false | abort | commit) -> tx_state().
set_decided(State, Val) ->        setelement(8, State, Val).
-spec get_numids(tx_state()) -> non_neg_integer().
get_numids(State) ->              element(9, State).
-spec get_numprepared(tx_state()) -> non_neg_integer().
get_numprepared(State) ->         element(10, State).
-spec inc_numprepared(tx_state()) -> tx_state().
inc_numprepared(State) ->         setelement(10, State, 1 + element(10, State)).
-spec get_numabort(tx_state()) -> non_neg_integer().
get_numabort(State) ->            element(11, State).
-spec inc_numabort(tx_state()) -> tx_state().
inc_numabort(State) ->            setelement(11, State, 1 + element(11, State)).
-spec get_numinformed(tx_state()) -> non_neg_integer().
get_numinformed(State) ->         element(12, State).
-spec inc_numinformed(tx_state()) -> tx_state().
inc_numinformed(State) ->         setelement(12, State, 1 + element(12, State)).
-spec set_numinformed(tx_state(), non_neg_integer()) -> tx_state().
set_numinformed(State, Val) ->    setelement(12, State, Val).
-spec get_numpaxdecided(tx_state()) -> non_neg_integer().
get_numpaxdecided(State) ->       element(13, State).
-spec inc_numpaxdecided(tx_state()) -> tx_state().
inc_numpaxdecided(State) ->       setelement(13, State, 1 + element(13, State)).
-spec set_numpaxdecided(tx_state(), non_neg_integer()) -> tx_state().
set_numpaxdecided(State, Val) ->  setelement(13, State, Val).
-spec get_numtpsregistered(tx_state()) -> non_neg_integer().
get_numtpsregistered(State) ->    element(14, State).
-spec inc_numtpsregistered(tx_state()) -> tx_state().
inc_numtpsregistered(State) ->    setelement(14, State, 1 + element(14, State)).

-spec get_status(tx_state()) -> new | uninitialized | ok.
get_status(State) -> element(15, State).
-spec set_status(tx_state(), new | uninitialized | ok) -> tx_state().
set_status(State, Status) -> setelement(15, State, Status).
-spec hold_back(comm:message(), tx_state()) -> tx_state().
hold_back(Msg, State) -> setelement(16, State, [Msg | element(16, State)]).
-spec get_hold_back(tx_state()) -> [comm:message()].
get_hold_back(State) -> element(16, State).
-spec set_hold_back(tx_state(), [comm:message()]) -> tx_state().
set_hold_back(State, Queue) -> setelement(16, State, Queue).

-spec newly_decided(tx_state()) -> undecided | false | abort | commit.
newly_decided(State) ->
    case (undecided =:= is_decided(State)) of
        false -> false; %% was already decided
        true ->
            %% maybe: calculate decision:
            case {get_numabort(State) > 0,
                  get_numprepared(State) =:= get_numids(State)} of
                {true, _} -> abort;
                {_, true} -> commit;
                {_, _} -> undecided
            end
    end.

-spec all_tps_informed(tx_state()) -> boolean().
all_tps_informed(State) ->
    %% @TODO use repl. degree
    get_numinformed(State) =:= get_numids(State) * 4.

-spec all_pax_decided(tx_state()) -> boolean().
all_pax_decided(State) ->
    %% @TODO use repl. degree
    get_numpaxdecided(State) =:= get_numids(State) * 4.

-spec all_tps_registered(tx_state()) -> boolean().
all_tps_registered(State) ->
    %% @TODO use repl. degree
    get_numtpsregistered(State) =:= get_numids(State) * 4.
