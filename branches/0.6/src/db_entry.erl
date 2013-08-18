%% @copyright 2010-2012 Zuse Institute Berlin
%%            and onScale solutions GmbH

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
%% @doc Abstract datatype of a single DB entry.
%% @version $Id$
-module(db_entry).
-author('schintke@onscale.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new/1, new/3,
         get_key/1,
         get_value/1, set_value/3,
         get_readlock/1, inc_readlock/1, dec_readlock/1,
         get_writelock/1, set_writelock/2, unset_writelock/1,
         get_version/1,
         reset_locks/1, is_locked/1,
         is_empty/1, is_null/1, lockcount_delta/2]).

% only for unit tests:
-export([inc_version/1, dec_version/1]).

-ifdef(with_export_type_support).
-export_type([entry/0]).
-endif.

% note: do not make opaque so DB implementations can rely on an entry() being a
% tuple as defined here
% note: WriteLock is either false or the version (>= Version) that a write
% operation is working on (this allows proper cleanup - see rdht_tx_write)
-type entry_ex() ::
          {Key::?RT:key(), Value::db_dht:value(), WriteLock::false | db_dht:version(),
           ReadLock::non_neg_integer(), Version::db_dht:version()}.
-type entry_empty() ::
          {Key::?RT:key(), empty_val | db_dht:value(), WriteLock::false | -1 | db_dht:version(),
           ReadLock::non_neg_integer(), Version::-1}.
-type entry() :: entry_ex() | entry_empty().

-spec new(Key::?RT:key()) -> {?RT:key(), empty_val, false, 0, -1}.
new(Key) -> {Key, empty_val, false, 0, -1}.

-spec new(Key::?RT:key(), Value::db_dht:value(), Version::db_dht:version()) ->
    {Key::?RT:key(), Value::db_dht:value(), WriteLock::false,
     ReadLock::0, Version::db_dht:version()}.
new(Key, Value, Version) -> {Key, Value, false, 0, Version}.

-spec get_key(DBEntry::entry()) -> ?RT:key().
get_key(DBEntry) -> element(1, DBEntry).

-spec get_value(DBEntry::entry()) -> db_dht:value().
get_value(DBEntry) -> element(2, DBEntry).

-spec set_value(DBEntry::entry(), Value::db_dht:value(), Version::db_dht:version()) -> entry().
set_value(DBEntry, Value, Version) ->
    setelement(2, setelement(5, DBEntry, Version), Value).

-spec get_writelock(DBEntry::entry()) -> WriteLock::false | -1 | db_dht:version().
get_writelock(DBEntry) -> element(3, DBEntry).

-spec set_writelock(entry_ex(), false | db_dht:version()) -> entry_ex();
                   (entry_empty(), false | -1 | db_dht:version()) -> entry_empty().
set_writelock(DBEntry, WriteLock) -> setelement(3, DBEntry, WriteLock).

-spec unset_writelock(DBEntry::entry()) -> entry().
unset_writelock(DBEntry) -> set_writelock(DBEntry, false).

-spec get_readlock(DBEntry::entry()) -> ReadLock::non_neg_integer().
get_readlock(DBEntry) -> element(4, DBEntry).

-spec set_readlock(DBEntry::entry(), ReadLock::non_neg_integer()) -> entry().
set_readlock(DBEntry, ReadLock) -> setelement(4, DBEntry, ReadLock).

-spec inc_readlock(DBEntry::entry()) -> entry().
inc_readlock(DBEntry) -> set_readlock(DBEntry, get_readlock(DBEntry) + 1).

-spec dec_readlock(DBEntry::entry()) -> entry().
dec_readlock(DBEntry) ->
    case get_readlock(DBEntry) of
        0 -> log:log(warn, "Decreasing empty readlock"), DBEntry;
        N -> set_readlock(DBEntry, N - 1)
    end.

-spec get_version(DBEntry::entry()) -> db_dht:version() | -1.
get_version(DBEntry) -> element(5, DBEntry).

-spec inc_version(DBEntry::entry()) -> entry().
inc_version(DBEntry) -> setelement(5, DBEntry, get_version(DBEntry) + 1).

-spec dec_version(DBEntry::entry()) -> entry().
dec_version(DBEntry) -> setelement(5, DBEntry, get_version(DBEntry) - 1).

-spec reset_locks(DBEntry::entry()) ->
    {Key::?RT:key(), Value::db_dht:value(), WriteLock::false,
     ReadLock::0, Version::db_dht:version()} |
    {Key::?RT:key(), empty_val | db_dht:value(), WriteLock::false,
     ReadLock::0, Version::-1}.
reset_locks(DBEntry) ->
    TmpEntry = set_readlock(DBEntry, 0),
    set_writelock(TmpEntry, false).

-spec is_locked(DBEntry::entry()) -> boolean().
is_locked(DBEntry) ->
    get_readlock(DBEntry) > 0 orelse get_writelock(DBEntry) =/= false.

%% @doc Returns whether the item is an empty_val item with version -1.
%%      Note: The number of read or write locks does not matter here!
-spec is_empty(entry()) -> boolean().
is_empty({_Key, empty_val, _WriteLock, _ReadLock, -1}) -> true;
is_empty(_) -> false.

%% @doc Returns whether the item is an empty_val item with version -1 and no
%%      read or write locks.
-spec is_null(entry()) -> boolean().
is_null({_Key, empty_val, false, 0, -1}) -> true;
is_null(_) -> false.

%% @doc Helper for lock bookkeeping. Compares two db_entries and returns delta
%%      accordingly. It assumes that only related entries are compared, i.e. it
%%      is assumed that no locks are released that are not held in the first
%%      place. 
-spec lockcount_delta(db_entry:entry(), db_entry:entry()) -> integer().
lockcount_delta(OldEntry, NewEntry) ->
    TmpLC = get_readlock(NewEntry) - get_readlock(OldEntry),
    WL_old = get_writelock(OldEntry),
    WL_new = get_writelock(NewEntry),
    if WL_new =:= false andalso WL_old =/= false -> TmpLC - 1;
       WL_new =/= false andalso WL_old =:= false -> TmpLC + 1;
       true                                      -> TmpLC
    end.
