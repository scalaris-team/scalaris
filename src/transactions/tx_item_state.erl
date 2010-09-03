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
%%      The state for a single request in a transaction of a TM and RTM.
%% @version $Id$
-module(tx_item_state).
-author('schintke@onscale.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

%% Operations on tx_item_state
-export([new/1, new/3]).
-export([get_txid/1]).
-export([get_itemid/1, set_itemid/2]).
-export([get_decided/1]).
-export([set_decided/2]).
-export([inc_numprepared/1, inc_numabort/1]).
-export([newly_decided/1]).
-export([get_paxosids_rtlogs_tps/1,set_paxosids_rtlogs_tps/2]).
-export([set_tp_for_paxosid/3]).
-export([get_status/1, set_status/2]).
-export([hold_back/2, get_hold_back/1, set_hold_back/2]).

%% tx_item_state = {
%%    1 TxItemId,                   % id of the item
%%    2 tx_item_state,              % data type tag for debugging
%%    3 TxId,                       % part of transaction with id TxId
%%    4 TLogEntry,                  % corresponding transaction log entry
%%    5 Majority_for_prepared,      % required prepare votes to decide prepared
%%    6 Majority_for_abort,         % required abort votes to decide abort
%%    7 Decided?,                   % current decision status
%%    8 Numprepared,                % number of received prepare votes
%%    9 Numabort,                   % number of received abort votes
%%   10 [{PaxosID, RTLogEntry, TP}] % involved Paxos Consensus IDs
%%   11 Status (new / uninitialized / ok) % item status
%%   12 HoldBackQueue               % hold back queue when not initialized
%% }

%% @TODO maybe the following entries are also necessary?:
%%      tx_tm_helper_behaviour to use? needed? for what?,
%%      timeout before the first RTM takes over

new(ItemId) ->
    ReplDeg = config:read(replication_factor),
    {ItemId, tx_item_state, undefined_tx_id, empty_tlog_entry,
     quorum:majority_for_accept(ReplDeg), quorum:majority_for_deny(ReplDeg),
     false, 0, 0, _no_paxIds = [], uninitialized, _HoldBack = []}.
new(ItemId, TxId, TLogEntry) ->
    %% expand TransLogEntry to replicated translog entries
    RTLogEntries = apply(element(1, TLogEntry), validate_prefilter, [TLogEntry]),
    PaxosIds = [ {paxos_id, util:get_global_uid()} || _ <- RTLogEntries ],
    TPs = [ unknown || _ <- PaxosIds ],
    PaxIDsRTLogsTPs = lists:zip3(PaxosIds, RTLogEntries, TPs),
    ReplDeg = config:read(replication_factor),
    {ItemId, tx_item_state, TxId, TLogEntry,
     quorum:majority_for_accept(ReplDeg), quorum:majority_for_deny(ReplDeg),
     false, 0, 0, PaxIDsRTLogsTPs,
     uninitialized, _HoldBack = []}.

get_itemid(State) ->         element(1, State).
set_itemid(State, Val) ->    setelement(1, State, Val).
get_txid(State) ->           element(3, State).
get_maj_for_prepared(State) -> element(5, State).
get_maj_for_abort(State) ->  element(6, State).
get_decided(State) ->        element(7, State).
set_decided(State, Val) ->   setelement(7, State, Val).
get_numprepared(State) ->    element(8, State).
inc_numprepared(State) ->    setelement(8, State, element(8,State) + 1).
get_numabort(State) ->       element(9, State).
inc_numabort(State) ->       setelement(9, State, element(9,State) + 1).

get_paxosids_rtlogs_tps(State) -> element(10, State).
set_paxosids_rtlogs_tps(State, NewTPList) -> setelement(10, State, NewTPList).

%% new / uninitialized / ok.
get_status(State) ->         element(11, State).
set_status(State, Status) -> setelement(11, State, Status).
hold_back(Msg, State) ->     setelement(12, State, [Msg | element(12, State)]).
get_hold_back(State) ->      element(12, State).
set_hold_back(State, Queue) -> setelement(12, State, Queue).

newly_decided(State) ->
    case get_decided(State) of
        false ->
            Prepared = get_numprepared(State) =:= get_maj_for_prepared(State),
            Abort =    get_numabort(State) =:= get_maj_for_abort(State),
            case {Prepared, Abort} of
                {true, false} -> prepared;
                {false, true} -> abort;
                _ -> false
            end;
        _Any -> false
    end.

set_tp_for_paxosid(State, TP, PaxosId) ->
    TPList = get_paxosids_rtlogs_tps(State),
    Entry = lists:keyfind(PaxosId, 1, TPList),
    NewTPList = lists:keyreplace(PaxosId, 1, TPList, setelement(3, Entry, TP)),
    set_paxosids_rtlogs_tps(State, NewTPList).
