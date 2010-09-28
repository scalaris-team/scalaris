%% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-export([new/1, new/3,
         get_key/1,
         get_value/1, set_value/2,
         get_readlock/1, inc_readlock/1, dec_readlock/1,
         get_writelock/1, set_writelock/1, unset_writelock/1,
         get_version/1, inc_version/1, set_version/2,
         reset_locks/1,
         is_empty/1]).

-ifdef(with_export_type_support).
-export_type([entry/0]).
-endif.

-type(entry() :: {Key::?RT:key(), Value::?DB:value(), WriteLock::boolean(),
                  ReadLock::non_neg_integer(), Version::?DB:version()} |
                 {Key::?RT:key(), empty_val, false, 0, -1}).

-spec new(Key::?RT:key()) -> {?RT:key(), empty_val, false, 0, -1}.
new(Key) -> {Key, empty_val, false, 0, -1}.

-spec new(Key::?RT:key(), Value::?DB:value(), Version::?DB:version()) ->
    {Key::?RT:key(), Value::?DB:value(), WriteLock::false,
     ReadLock::0, Version::?DB:version()}.
new(Key, Value, Version) -> {Key, Value, false, 0, Version}.

-spec get_key(DBEntry::entry()) -> ?RT:key().
get_key(DBEntry) -> element(1, DBEntry).

-spec get_value(DBEntry::entry()) -> ?DB:value().
get_value(DBEntry) -> element(2, DBEntry).

-spec set_value(DBEntry::entry(), Value::?DB:value()) -> entry().
set_value(DBEntry, Value) -> setelement(2, DBEntry, Value).

-spec get_writelock(DBEntry::entry()) -> WriteLock::boolean().
get_writelock(DBEntry) -> element(3, DBEntry).

-spec set_writelock(DBEntry::entry(), WriteLock::boolean()) -> entry().
set_writelock(DBEntry, WriteLock) -> setelement(3, DBEntry, WriteLock).

-spec set_writelock(DBEntry::entry()) ->
    {Key::?RT:key(), Value::?DB:value(), WriteLock::true,
     ReadLock::non_neg_integer(), Version::?DB:version()} |
    {Key::?RT:key(), empty_val, true, 0, -1}.
set_writelock(DBEntry) -> set_writelock(DBEntry, true).

-spec unset_writelock(DBEntry::entry()) ->
    {Key::?RT:key(), Value::?DB:value(), WriteLock::false,
     ReadLock::non_neg_integer(), Version::?DB:version()} |
    {Key::?RT:key(), empty_val, false, 0, -1}.
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

-spec get_version(DBEntry::entry()) -> ?DB:version().
get_version(DBEntry) -> element(5, DBEntry).

-spec inc_version(DBEntry::entry()) -> entry().
inc_version(DBEntry) -> setelement(5, DBEntry, get_version(DBEntry) + 1).

-spec set_version(DBEntry::entry(), Version::?DB:version()) -> entry().
set_version(DBEntry, Version) -> setelement(5, DBEntry, Version).

-spec reset_locks(DBEntry::entry()) ->
    {Key::?RT:key(), Value::?DB:value(), WriteLock::false,
     ReadLock::0, Version::?DB:version()} |
    {Key::?RT:key(), empty_val, false, 0, -1}.
reset_locks(DBEntry) ->
    TmpEntry = set_readlock(DBEntry, 0),
    set_writelock(TmpEntry, false).

-spec is_empty({Key::?RT:key(), Value::?DB:value(), WriteLock::boolean(),
                ReadLock::non_neg_integer(), Version::?DB:version()}) -> false;
              ({Key::?RT:key(), empty_val, WriteLock::boolean(),
                ReadLock::non_neg_integer(), Version::?DB:version() | -1}) -> true.
is_empty({_Key, empty_val, _WriteLock, _ReadLock, _Version}) ->
    true;
is_empty(_) ->
    false.
