% @copyright 2013 Zuse Institute Berlin,

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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    DB back-end using ets.
%%         Two keys K and L are considered equal if K == L yields true.
%% @end
%% @version $Id$
-module(db_ets).
-author('fajerski@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(db_backend_beh).

-define(TRACE(_X, _Y), ok).
%% -define(TRACE(X, Y), ct:pal(X, Y)).

%% primitives
-export([new/1, open/1, close/1, put/2, get/2, delete/2]).
%% db info
-export([get_name/1, get_load/1]).

%% iteration
-export([foldl/3, foldl/4, foldl/5]).
-export([foldr/3, foldr/4, foldr/5]).

-type db() :: tid() | atom().
-type key() :: db_backend_beh:key(). %% '$end_of_table' is not allowed as key() or else iterations won't work!
-type entry() :: db_backend_beh:entry().

-ifdef(with_export_type_support).
-export_type([db/0]).
-endif.

%% @doc Creates new DB handle named DBName.
-spec new(DBName::nonempty_string()) -> db().
new(DBName) ->
    ets:new(list_to_atom(DBName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]).

%% @doc Open a previously existing database. Not supported by ets.
%%      A new database is created
-spec open(DBName::nonempty_string()) -> db().
open(DBName) ->
    log:log(warn, "~p: open/1 is not supported by ets, calling new/1 instead",
            [self()]),
    new(DBName).

%% @doc Closes and deletes the DB named DBName
-spec close(DBName::db()) -> true.
close(DBName) ->
    ets:delete(DBName).

%% @doc Saves arbitrary tuple Entry in DB DBName and returns the new DB.
%%      The key is expected to be the first element of Entry.
-spec put(DBName::db(), Entry::entry()) -> db().
put(DBName, Entry) ->
    ?ASSERT(element(1, Entry) =/= '$end_of_table'),
    ets:insert(DBName, Entry),
    DBName.

%% @doc Returns the entry that corresponds to Key or {} if no such tuple exists.
-spec get(DBName::db(), Key::key()) -> entry() | {}.
get(DBName, Key) ->
    case ets:lookup(DBName, Key) of
        [Entry] ->
            Entry;
        [] ->
            {}
    end.

%% @doc Deletes the tuple saved under Key and returns the new DB.
%%      If such a tuple does not exists nothing is changed.
-spec delete(DBName::db(), Key::key()) -> db().
delete(DBName, Key) ->
    ets:delete(DBName, Key),
    DBName.

%% @doc Returns the name of the DB specified in new/1.
-spec get_name(DB::db()) -> nonempty_string().
get_name(DB) ->
    erlang:atom_to_list(ets:info(DB, name)).

%% @doc Returns the current load (i.e. number of stored tuples) of the DB.
-spec get_load(DB::db()) -> non_neg_integer().
get_load(DB) ->
    ets:info(DB, size).

%% @doc Is equivalent to ets:foldl(Fun, Acc0, DB).
-spec foldl(DB::db(), Fun::fun((entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl(DB, Fun, Acc) ->
    ets:foldl(Fun, Acc, DB).

%% @doc Is equivalent to foldl(DB, Fun, Acc0, Interval, get_load(DB)).
-spec foldl(DB::db(), Fun::fun((entry(), AccIn::A) -> AccOut::A), Acc0::A,
                               Interval::db_backend_beh:interval()) -> Acc1::A.
foldl(DB, Fun, Acc, Interval) ->
    foldl(DB, Fun, Acc, Interval, ets:info(DB, size)).

%% @doc foldl iterates over DB and applies Fun(Entry, AccIn) to every element
%%      encountered in Interval. On the first call AccIn == Acc0. The iteration
%%      stops as soon as MaxNum elements have been encountered.
-spec foldl(DB::db(), Fun::fun((Entry::entry(), AccIn::A) -> AccOut::A), Acc0::A,
                               Intervall::db_backend_beh:interval(), MaxNum::non_neg_integer()) -> Acc1::A.
foldl(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldl(_DB, _Fun, Acc, {interval, _, '$end_of_table', _End, _}, _MaxNum) -> Acc;
foldl(_DB, _Fun, Acc, {interval, _, _Start, '$end_of_table', _}, _MaxNum) -> Acc;
foldl(_DB, _Fun, Acc, {interval, _, Start, End, _}, _MaxNum) when Start > End -> Acc;
foldl(DB, Fun, Acc, {element, El}, _MaxNum) -> 
    case ets:lookup(DB, El) of
        [] ->
            Acc;
        [Entry] ->
            Fun(Entry, Acc)
    end;
foldl(DB, Fun, Acc, all, MaxNum) ->
    foldl(DB, Fun, Acc, {interval, '[', ets:first(DB), ets:last(DB), ']'},
          MaxNum);
foldl(DB, Fun, Acc, {interval, '(', Start, End, RBr}, MaxNum) -> 
    foldl(DB, Fun, Acc, {interval, '[', ets:next(DB, Start), End, RBr}, MaxNum);
foldl(DB, Fun, Acc, {interval, LBr, Start, End, ')'}, MaxNum) -> 
    foldl(DB, Fun, Acc, {interval, LBr, Start, ets:prev(DB, End), ']'}, MaxNum);
foldl(DB, Fun, Acc, {interval, '[', Start, End, ']'}, MaxNum) ->
    case ets:lookup(DB, Start) of
        [] ->
            ?TRACE("foldl:~nstart: ~p~nend:   ~p~nmaxnum: ~p~nfound nothing~n",
                   [Start, End, MaxNum]),
            foldl(DB, Fun, Acc, {interval, '[', ets:next(DB, Start), End, ']'},
                  MaxNum);
        [Entry] ->
            ?TRACE("foldl:~nstart: ~p~nend:   ~p~nmaxnum: ~p~ninterval: ~p~nfound ~p~n",
                   [Start, End, MaxNum, {interval, '[', Start, End, ']'}, Entry]),
            foldl(DB, Fun, Fun(Entry, Acc), {interval, '[', ets:next(DB, Start),
                                             End, ']'}, MaxNum - 1)
    end.

%% @doc Is equivalent to ets:foldr(Fun, Acc0, DB).
-spec foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldr(DB, Fun, Acc) ->
    ets:foldr(Fun, Acc, DB).

%% @doc Is equivalent to foldr(DB, Fun, Acc0, Interval, get_load(DB)).
-spec foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, db_backend_beh:interval()) -> Acc1::A.
foldr(DB, Fun, Acc, Interval) ->
    foldr(DB, Fun, Acc, Interval, ets:info(DB, size)).

%% @doc Behaves like foldl/5 with the difference that it starts at the end of
%%      Interval and iterates towards the start of Interval.
-spec foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, db_backend_beh:interval(), non_neg_integer()) -> Acc1::A.
foldr(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldr(_DB, _Fun, Acc, {interval, _, _End, '$end_of_table', _}, _MaxNum) -> Acc;
foldr(_DB, _Fun, Acc, {interval, _, '$end_of_table', _Start, _}, _MaxNum) -> Acc;
foldr(_DB, _Fun, Acc, {interval, _, End, Start, _}, _MaxNum) when Start < End -> Acc;
foldr(DB, Fun, Acc, {element, El}, _MaxNum) -> 
    case ets:lookup(DB, El) of
        [] ->
            Acc;
        [Entry] ->
            Fun(Entry, Acc)
    end;
foldr(DB, Fun, Acc, all, MaxNum) ->
    foldr(DB, Fun, Acc, {interval, '[', ets:first(DB), ets:last(DB), ']'},
          MaxNum);
foldr(DB, Fun, Acc, {interval, '(', End, Start, RBr}, MaxNum) -> 
    foldr(DB, Fun, Acc, {interval, '[', ets:next(DB, End), Start, RBr}, MaxNum);
foldr(DB, Fun, Acc, {interval, LBr, End, Start, ')'}, MaxNum) -> 
    foldr(DB, Fun, Acc, {interval, LBr, End, ets:prev(DB, Start), ']'}, MaxNum);
foldr(DB, Fun, Acc, {interval, '[', End, Start, ']'}, MaxNum) ->
    case ets:lookup(DB, Start) of
        [] ->
            ?TRACE("foldr:~nstart: ~p~nend~p~nmaxnum: ~p~nfound nothing~n",
                   [Start, End, MaxNum]),
            foldr(DB, Fun, Acc, {interval, '[', End, ets:prev(DB, Start), ']'}, MaxNum);
        [Entry] ->
            ?TRACE("foldr:~nstart: ~p~nend ~p~nmaxnum: ~p~nfound ~p~n",
                   [Start, End, MaxNum, Entry]),
            foldr(DB, Fun, Fun(Entry, Acc), {interval, '[', End, ets:prev(DB, Start), ']'}, MaxNum -
                  1)
    end.
