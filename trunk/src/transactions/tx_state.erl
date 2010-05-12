% @copyright 2009, 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%    and onScale solutions GmbH

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
%% tx_state: {TId,
%%            tx_state,
%%            Client,
%%            ClientsId,
%%            RTMs,
%%            _tlogtxitemids = [{TLogEntry, TxItemId}],
%%            Learner,
%%            NumIds,
%%            NumPrepared,
%%            NumAbort,
%%            NumTPsInformed

%% @TODO also necessary?
%%               Majority of RTMs,
%%               timeout before RTM takes over
%%               }

new(Tid, Client, ClientsID, RTMs, TLogTxItemIds, Learners) ->
    {Tid, tx_state, Client, ClientsID, RTMs, TLogTxItemIds, Learners,
     undecided, length(TLogTxItemIds),
     _Prepared = 0, _Aborts = 0, _Informed =0, _PaxDecided = 0,
     _TpsRegistered = 0, _Status = uninitialized, _HoldBackQueue = []}.
new(Tid) ->
    new(Tid, unknown, unknown, _RTMs = [], _TLogTxItemIds = [], []).

get_tid(State) ->                 element(1, State).
set_tid(State, Val) ->            setelement(1, State, Val).
get_client(State) ->              element(3, State).
set_client(State, Val) ->         setelement(3, State, Val).
get_clientsid(State) ->           element(4, State).
set_clientsid(State, Val) ->      setelement(4, State, Val).
get_rtms(State) ->                element(5, State).
set_rtms(State, Val) ->           setelement(5, State, Val).
get_tlog_txitemids(State) ->      element(6, State).
set_tlog_txitemids(State, Val) -> setelement(6, State, Val).
get_learners(State) ->            element(7, State).
is_decided(State) ->              element(8, State).
set_decided(State, Val) ->        setelement(8, State, Val).
get_numids(State) ->              element(9, State).
get_numprepared(State) ->         element(10, State).
inc_numprepared(State) ->         setelement(10, State, 1 + element(10, State)).
get_numabort(State) ->            element(11, State).
inc_numabort(State) ->            setelement(11, State, 1 + element(11, State)).
get_numinformed(State) ->         element(12, State).
inc_numinformed(State) ->         setelement(12, State, 1 + element(12, State)).
set_numinformed(State, Val) ->    setelement(12, State, Val).
get_numpaxdecided(State) ->       element(13, State).
set_numpaxdecided(State, Val) ->  setelement(13, State, Val).
inc_numpaxdecided(State) ->       setelement(13, State, 1 + element(13, State)).
get_numtpsregistered(State) ->    element(14, State).
inc_numtpsregistered(State) ->    setelement(14, State, 1 + element(14, State)).

%% new / uninitialized / ok.
get_status(State) -> element(15, State).
set_status(State, Status) -> setelement(15, State, Status).
hold_back(Msg, State) -> setelement(16, State, [Msg | element(16, State)]).
get_hold_back(State) -> element(16, State).
set_hold_back(State, Queue) -> setelement(16, State, Queue).

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

all_tps_informed(State) ->
    %% @TODO use repl. degree
    get_numinformed(State) =:= get_numids(State) * 4.

all_pax_decided(State) ->
    %% @TODO use repl. degree
    get_numpaxdecided(State) =:= get_numids(State) * 4.

all_tps_registered(State) ->
    %% @TODO use repl. degree
    get_numtpsregistered(State) =:= get_numids(State) * 4.
