% @copyright 2009, 2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%      The state for a single request in a transaction of a TM & RTM.
%% @version $Id$
-module(tx_item_state).
-author('schintke@onscale.de').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

%% Operations on tx_item_state
-export([new/3]).
-export([get_txid/1]).
-export([get_itemid/1, set_itemid/2]).
-export([get_decided/1]).
-export([set_decided/2]).
-export([inc_numprepared/1, inc_numabort/1]).
-export([newly_decided/1]).
-export([get_paxosids_rtlogs_tps/1,set_paxosids_rtlogs_tps/2]).
-export([set_tp_for_paxosid/3]).

%% tx_item_state: {TxItemId,
%%                 tx_item_state,
%%                 TxId,
%%                 TLogEntry,
%%                 Majority,
%%                 decided?,
%%                 numprepared,
%%                 numabort,
%%                 [{PaxosID, RTLogEntry, TP}]


%% @TODO maybe the following entries are also necessary?:
%%               tx_tm_helper_behaviour to use? needed? for what?,
%%               timeout before the first RTM takes over
%%               [{TLogEntry, [{PaxosID, state}]}]
%%               Majority of RTMs,
%%               NumIds,
%%               NumPrepared,
%%               NumAbort,
%%               }

new(ItemId, TxId, TLogEntry) ->
    %% expand TransLogEntry to replicated translog entries
    RTLogEntries = apply(element(1, TLogEntry), validate_prefilter, [TLogEntry]),
    PaxosIds = [ {paxos_id, util:get_global_uid()} || _ <- RTLogEntries ],
    TPs = [ unknown || _ <- PaxosIds ],
    PaxIDsRTLogsTPs = lists:zip3(PaxosIds, RTLogEntries, TPs),
    {ItemId, tx_item_state, TxId, TLogEntry, config:read(quorum_factor),
     false, 0, 0, PaxIDsRTLogsTPs}.

get_itemid(State) ->         element(1, State).
set_itemid(State, Val) ->    setelement(1, State, Val).
get_txid(State) ->           element(3, State).
get_majority(State) ->       element(5, State).
get_decided(State) ->        element(6, State).
set_decided(State, Val) ->   setelement(6, State, Val).
get_numprepared(State) ->    element(7,State).
inc_numprepared(State) ->    setelement(7, State, element(7,State) + 1).
get_numabort(State) ->       element(8, State).
inc_numabort(State) ->       setelement(8, State, element(8,State) + 1).

newly_decided(State) ->
    case get_decided(State) of
        false ->
            NumPrepared = get_numprepared(State),
            NumAbort = get_numabort(State),
            case get_majority(State) of
                NumPrepared -> prepared;
                NumAbort -> abort;
                _ -> false
            end;
        _Any -> false
    end.

get_paxosids_rtlogs_tps(State) -> element(9, State).
set_paxosids_rtlogs_tps(State, NewTPList) -> setelement(9, State, NewTPList).

set_tp_for_paxosid(State, TP, PaxosId) ->
    TPList = get_paxosids_rtlogs_tps(State),
    Entry = lists:keyfind(PaxosId, 1, TPList),
    NewTPList = lists:keyreplace(PaxosId, 1, TPList, setelement(3, Entry, TP)),
    %%    io:format("NewTPList: ~p~n", [NewTPList]),
    set_paxosids_rtlogs_tps(State, NewTPList).
