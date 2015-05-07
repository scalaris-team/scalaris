%  @copyright 2012 Zuse Institute Berlin

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

%% @author Stefan Keidel <keidel@informatik.hu-berlin.de>
%% @doc Local state information needed for the leader of the S3 snapshot algorithm
%% @version $Id$
-module(snapshot_leader_state).
-author('keidel@informatik.hu-berlin.de').
-vsn('$Id$').

-export([new/0, new/4,
         get_number/1, is_in_progress/1, get_global_snapshot/1, get_client/1,
         interval_union_is_all/1, get_error_interval/1,
         set_number/2, add_snapshot/2, add_interval/2, add_error_interval/2,
         start_progress/1, stop_progress/1]).

-export_type([state/0]).

%-define(TRACE(X, Y), io:format(X, Y)).
-define(TRACE(_X, _Y), ok).

-include("scalaris.hrl").

-type(state() :: {SnapNo::non_neg_integer(), InProgress::boolean(),
                  Snapshots::ets:tid() | atom(), Interval::intervals:interval(),
                  ErrorInterval::intervals:interval(), Client::comm:mypid() | false}).

% constructors

-spec new() -> state().
new() ->
    new(0, false, false).

%% need to clean up ets table from old state
-spec new(non_neg_integer(), boolean(), comm:mypid() | false, state()) -> state().
new(Number, InProgress, Client, {_, _, OldTable, _, _, _}) -> 
    ets:delete(OldTable),
    new(Number, InProgress, Client).

-spec new(non_neg_integer(), boolean(), comm:mypid() | false) -> state().
new(Number, InProgress, Client) ->
    {Number, InProgress, ets:new(snapshot_leader_db, [ordered_set, private]),
     intervals:empty(), intervals:empty(), Client}.

% getters

-spec get_number(state()) -> non_neg_integer().
get_number(State) -> element(1, State).

-spec is_in_progress(state()) -> boolean().
is_in_progress(State) -> element(2, State).

-spec get_global_snapshot(state()) -> list().
get_global_snapshot(State) -> ets:tab2list(element(3, State)).

-spec get_client(state()) -> comm:mypid().
get_client(State) -> element(6, State).

-spec interval_union_is_all(state()) -> boolean().
interval_union_is_all({_, _, _, Interval, ErrorInterval, _} = _State) ->
    intervals:is_all(intervals:union(Interval, ErrorInterval)).

-spec get_error_interval(state()) -> intervals:interval().
get_error_interval(State) -> element(5, State).

% setters

-spec set_number(state(), non_neg_integer()) -> state().
set_number(SnapInfo, NewVal) -> 
    erlang:put(local_snap_number, NewVal),
    setelement(1, SnapInfo, NewVal).

-spec add_snapshot(state(), any()) -> state().
add_snapshot({Number, InProgress, SnapshotDB, Interval, ErrorInterval, Client}, NewSnapshot) ->
    add_snapshot_entries_to_db(SnapshotDB, NewSnapshot),
    {Number, InProgress, SnapshotDB, Interval, ErrorInterval, Client}.

-spec add_interval(state(), intervals:interval()) -> state().
add_interval({_Number, _InProgress, _SnapshotDB, Interval, _ErrorInterval, _Client}, NewInterval) ->
    {_Number, _InProgress, _SnapshotDB, intervals:union(Interval, NewInterval), _ErrorInterval, _Client}.

-spec add_error_interval(state(), intervals:interval()) -> state().
add_error_interval({_Number, _InProgress, _SnapshotDB, _Interval, ErrorInterval, _Client}, NewInterval) ->
    {_Number, _InProgress, _SnapshotDB, _Interval, intervals:union(ErrorInterval, NewInterval), _Client}.

-spec start_progress(state()) -> state().
start_progress(SnapInfo) -> setelement(2, SnapInfo, true).

-spec stop_progress(state()) -> state().
stop_progress(SnapInfo) -> setelement(2, SnapInfo, false).

% helpers

add_snapshot_entries_to_db(SnapshotDB, [Tuple | RestOfSnapshot]) ->
    Key = get_unique_key(db_entry:get_key(Tuple)),
    case ets:lookup(SnapshotDB, Key) of
        [{_Key, _Val, EntryVersion}] -> 
            case db_entry:get_version(Tuple) > EntryVersion of
                true ->
                    InsertTuple = {Key, rdht_tx:decode_value(db_entry:get_value(Tuple)), db_entry:get_version(Tuple)},
                    ets:insert(SnapshotDB, InsertTuple);
                false -> % we already stored same verion or newer -> do nothing
                    ok
            end;
        [] -> 
            InsertTuple = {Key, rdht_tx:decode_value(db_entry:get_value(Tuple)), db_entry:get_version(Tuple)},
            ets:insert(SnapshotDB, InsertTuple)
    end,
    add_snapshot_entries_to_db(SnapshotDB, RestOfSnapshot);
add_snapshot_entries_to_db(_SnapshotDB, []) ->
    ok.

get_unique_key(Key) ->
    Keys = ?RT:get_replica_keys(Key),
    ?TRACE("unique key of ~p is ~p (query ~p)~n", [Keys, lists:nth(1, lists:sort(Keys)), Key]),
    lists:nth(1, lists:sort(Keys)).
