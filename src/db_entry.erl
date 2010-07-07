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

-define(TRACE(X,Y), io:format(X,Y)).
%-define(TRACE(X,Y), ok).

-export([new/1]).
-export([get_key/1]).
-export([get_value/1, set_value/2]).
-export([inc_readlock/1]).
-export([dec_readlock/1]).
-export([get_readlock/1]).
-export([get_writelock/1]).
-export([set_writelock/1]).
-export([unset_writelock/1]).
-export([get_version/1]).
-export([inc_version/1]).
-export([set_version/2]).
-export([reset_locks/1]).

%% {key,
%%  value,
%%  bool writelock,
%%  int readlock,
%%  int version}

new(Key) -> {Key, empty_val, false, 0, -1}.

get_key(DBEntry) ->            element(1, DBEntry).
get_value(DBEntry) ->          element(2, DBEntry).
set_value(DBEntry, Val) ->     setelement(2, DBEntry, Val).
get_writelock(DBEntry) ->      element(3, DBEntry).
set_writelock(DBEntry, Val) -> setelement(3, DBEntry, Val).
get_readlock(DBEntry) ->       element(4, DBEntry).
set_readlock(DBEntry, Val) ->  setelement(4, DBEntry, Val).
get_version(DBEntry) ->        element(5, DBEntry).
inc_version(DBEntry) ->        setelement(5, DBEntry, 1 + element(5, DBEntry)).
set_version(DBEntry, Val) ->   setelement(5, DBEntry, Val).

set_writelock(DBEntry) ->
    set_writelock(DBEntry, true).

unset_writelock(DBEntry) ->
    set_writelock(DBEntry, false).

inc_readlock(DBEntry) ->
    set_readlock(DBEntry, get_readlock(DBEntry) + 1).
dec_readlock(DBEntry) ->
    set_readlock(DBEntry, get_readlock(DBEntry) - 1).

reset_locks(DBEntry) ->
    TmpEntry = set_readlock(DBEntry, 0),
    set_writelock(TmpEntry, false).
