% @copyright 2013-2014 Zuse Institute Berlin,

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
%% @version $Id: db_ets.erl 6270 2014-03-28 14:25:52Z fajerski@zib.de $
-module(db_mnesia).
-author('fajerski@zib.de').
-vsn('$Id: db_ets.erl 6270 2014-03-28 14:25:52Z fajerski@zib.de $').

-include("scalaris.hrl").

-behaviour(db_backend_beh).

-define(TRACE(_X, _Y), ok).
%% -define(TRACE(X, Y), io:format(X, Y)).
%% -define(TRACE(X, Y), ct:pal(X, Y)).

%% primitives
-export([new/1, new/2, open/1, close/1, put/2, get/2, delete/2]).
%% db info
-export([get_name/1, get_load/1]).

%% iteration
-export([foldl/3, foldl/4, foldl/5]).
-export([foldr/3, foldr/4, foldr/5]).

-type db() :: mnesia:tab().
-type key() :: db_backend_beh:key(). %% '$end_of_table' is not allowed as key() or else iterations won't work!
-type entry() :: db_backend_beh:entry().

-ifdef(with_export_type_support).
-export_type([db/0]).
-endif.

-export([traverse_table_and_show/1]).

traverse_table_and_show(Table_name)->
  Iterator =  fun(Rec,_)->
    io:format("~p~n",[Rec]),
    []
  end,
  case mnesia:is_transaction() of
    true -> mnesia:foldl(Iterator,[],Table_name);
    false ->
      Exec = fun({Fun,Tab}) -> mnesia:foldl(Fun, [],Tab) end,
      mnesia:activity(transaction,Exec,[{Iterator,Table_name}],mnesia_frag)
  end.

%% @doc Creates new DB handle named DBName.
-spec new(DBName::nonempty_string()) -> db().
new(DBName) ->
    %% IMPORTANT: this module only works correctly when using ordered_set ets
    %% tables. Other table types could throw bad_argument exceptions while
    %% calling ets:next/2
    DbAtom = list_to_atom(DBName),
    mnesia:create_table(DbAtom, [{disc_copies, [node()]}, {type, ordered_set}]),
    DbAtom.

%% @doc Creates new DB handle named DBName with possibility to pass Options.
-spec new(DBName::nonempty_string(), Options::[term()] ) -> db().
new(DBName, Options) ->
    %% IMPORTANT: this module only works correctly when using ordered_set ets
    %% tables. Other table types could throw bad_argument exceptions while
    %% calling ets:next/2
  DbAtom = list_to_atom(DBName),
    mnesia:create_table(DbAtom, [{disc_copies, [node()]}, {type, ordered_set} | Options]),
    DbAtom.

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
    mnesia:transaction(fun()-> mnesia:delete_table(DBName)end).

%% @doc Saves arbitrary tuple Entry or list of tuples Entries
%%      in DB DBName and returns the new DB.
%%      The key is expected to be the first element of Entry.
-spec put(DBName::db(), Entry::entry() | [Entries::entry()]) -> db().
put(DBName, []) ->
    DBName;
put(DBName, Entry) ->
    ?DBG_ASSERT(case is_list(Entry) of
                    true ->
                        lists:all(
                          fun(E) ->
                                  element(1, E) =/= '$end_of_table'
                          end,
                          Entry);
                    false ->
                        element(1, Entry) =/= '$end_of_table'
                end),
    mnesia:transaction(fun() -> mnesia:write({DBName, element(1, Entry), Entry}) end),
    DBName.

%% @doc Returns the entry that corresponds to Key or {} if no such tuple exists.
-spec get(DBName::db(), Key::key()) -> entry() | {}.
get(DBName, Key) ->
    case mnesia:transaction(fun() -> mnesia:read(DBName, Key) end) of
      {atomic, [Entry]} ->
            element(3, Entry);
      {atomic, []} ->
            {}
    end.

%% @doc Deletes the tuple saved under Key and returns the new DB.
%%      If such a tuple does not exists nothing is changed.
-spec delete(DBName::db(), Key::key()) -> db().
delete(DBName, Key) ->
    mnesia:transaction(fun()-> mnesia:delete({DBName, Key}) end),
    DBName.

%% @doc Returns the name of the DB specified in new/1.
-spec get_name(DB::db()) -> nonempty_string().
get_name(DB) ->
    erlang:atom_to_list(mnesia:table_info(DB, name)).
%% To get the name : look into the schema

%% @doc Returns the current load (i.e. number of stored tuples) of the DB.
-spec get_load(DB::db()) -> non_neg_integer().
get_load(DB) ->
    mnesia:table_info(DB, size).

%% @doc Is equivalent to ets:foldl(Fun, Acc0, DB).
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl(DB, Fun, Acc) ->
    foldl(Fun, Acc, DB).
    %foldl(DB, Fun, Acc, {'[', mnesia:first(DB), mnesia:last(DB), ']'}, mnesia:table_info(DB, size)).

%% @doc Is equivalent to foldl(DB, Fun, Acc0, Interval, get_load(DB)).
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Interval::db_backend_beh:interval()) -> Acc1::A.
foldl(DB, Fun, Acc, Interval) ->
    foldl(DB, Fun, Acc, Interval, mnesia:table_info(DB, size)).

%% @doc foldl iterates over DB and applies Fun(Entry, AccIn) to every element
%%      encountered in Interval. On the first call AccIn == Acc0. The iteration
%%      stops as soon as MaxNum elements have been encountered.
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Intervall::db_backend_beh:interval(), MaxNum::non_neg_integer()) -> Acc1::A.
foldl(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldl(_DB, _Fun, Acc, {_, '$end_of_table', _End, _}, _MaxNum) -> Acc;
foldl(_DB, _Fun, Acc, {_, _Start, '$end_of_table', _}, _MaxNum) -> Acc;
foldl(_DB, _Fun, Acc, {_, Start, End, _}, _MaxNum) when Start > End -> Acc;
foldl(DB, Fun, Acc, {El}, _MaxNum) ->
    case mnesia:read(DB, El) of
        [] ->
            Acc;
        [_Entry] ->
            Fun(El, Acc)
    end;
foldl(DB, Fun, Acc, all, MaxNum) ->
    foldl(DB, Fun, Acc, {'[', mnesia:first(DB), mnesia:last(DB), ']'},
          MaxNum);
foldl(DB, Fun, Acc, {'(', Start, End, RBr}, MaxNum) ->
    foldl(DB, Fun, Acc, {'[', mnesia:next(DB, Start), End, RBr}, MaxNum);
foldl(DB, Fun, Acc, {LBr, Start, End, ')'}, MaxNum) ->
    foldl(DB, Fun, Acc, {LBr, Start, mnesia:prev(DB, End), ']'}, MaxNum);
foldl(DB, Fun, Acc, {'[', Start, End, ']'}, MaxNum) ->
    case mnesia:read(DB, Start) of
        [] ->
            foldl(DB, Fun, Acc, {'[', mnesia:next(DB, Start), End, ']'},
                       MaxNum);
        [_Entry] ->
            foldl_iter(DB, Fun, Acc, {'[', Start, End, ']'},
                       MaxNum)
    end.

-spec foldl_iter(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Intervall::db_backend_beh:interval(), MaxNum::non_neg_integer()) -> Acc1::A.
foldl_iter(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldl_iter(_DB, _Fun, Acc, {_, '$end_of_table', _End, _}, _MaxNum) -> Acc;
foldl_iter(_DB, _Fun, Acc, {_, _Start, '$end_of_table', _}, _MaxNum) -> Acc;
foldl_iter(_DB, _Fun, Acc, {_, Start, End, _}, _MaxNum) when Start > End -> Acc;
foldl_iter(DB, Fun, Acc, {'[', Start, End, ']'}, MaxNum) ->
    ?TRACE("foldl:~nstart: ~p~nend:   ~p~nmaxnum: ~p~ninterval: ~p~n",
           [Start, End, MaxNum, {'[', Start, End, ']'}]),
    foldl_iter(DB, Fun, Fun(Start, Acc),
               {'[', mnesia:next(DB, Start), End, ']'}, MaxNum - 1).

%% @doc Is equivalent to ets:foldr(Fun, Acc0, DB).
-spec foldr(db(), fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldr(DB, Fun, Acc) ->
    foldr(DB, Fun, Acc, {'[', mnesia:first(DB), mnesia:last(DB), ']'}, mnesia:info(DB, size)).

%% @doc Is equivalent to foldr(DB, Fun, Acc0, Interval, get_load(DB)).
-spec foldr(db(), fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A, db_backend_beh:interval()) -> Acc1::A.
foldr(DB, Fun, Acc, Interval) ->
    foldr(DB, Fun, Acc, Interval, mnesia:info(DB, size)).

%% @doc Behaves like foldl/5 with the difference that it starts at the end of
%%      Interval and iterates towards the start of Interval.
-spec foldr(db(), fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A, db_backend_beh:interval(), non_neg_integer()) -> Acc1::A.
foldr(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldr(_DB, _Fun, Acc, {_, _End, '$end_of_table', _}, _MaxNum) -> Acc;
foldr(_DB, _Fun, Acc, {_, '$end_of_table', _Start, _}, _MaxNum) -> Acc;
foldr(_DB, _Fun, Acc, {_, End, Start, _}, _MaxNum) when Start < End -> Acc;
foldr(DB, Fun, Acc, {El}, _MaxNum) ->
    case mnesia:read(DB, El) of
        [] ->
            Acc;
        [_Entry] ->
            Fun(El, Acc)
    end;
foldr(DB, Fun, Acc, all, MaxNum) ->
    foldr(DB, Fun, Acc, {'[', mnesia:first(DB), mnesia:last(DB), ']'},
          MaxNum);
foldr(DB, Fun, Acc, {'(', End, Start, RBr}, MaxNum) ->
    foldr(DB, Fun, Acc, {'[', mnesia:next(DB, End), Start, RBr}, MaxNum);
foldr(DB, Fun, Acc, {LBr, End, Start, ')'}, MaxNum) ->
    foldr(DB, Fun, Acc, {LBr, End, mnesia:prev(DB, Start), ']'}, MaxNum);
foldr(DB, Fun, Acc, {'[', End, Start, ']'}, MaxNum) ->
    case mnesia:read(DB, Start) of
        [] ->
            foldr(DB, Fun, Acc, {'[', End, mnesia:prev(DB, Start), ']'},
                       MaxNum);
        [_Entry] ->
            foldr_iter(DB, Fun, Acc, {'[', End, Start, ']'},
                       MaxNum)
    end.

-spec foldr_iter(db(), fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A, db_backend_beh:interval(), non_neg_integer()) -> Acc1::A.
foldr_iter(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldr_iter(_DB, _Fun, Acc, {_, _End, '$end_of_table', _}, _MaxNum) -> Acc;
foldr_iter(_DB, _Fun, Acc, {_, '$end_of_table', _Start, _}, _MaxNum) -> Acc;
foldr_iter(_DB, _Fun, Acc, {_, End, Start, _}, _MaxNum) when Start < End -> Acc;
foldr_iter(DB, Fun, Acc, {'[', End, Start, ']'}, MaxNum) ->
    ?TRACE("foldr:~nstart: ~p~nend ~p~nmaxnum: ~p~nfound",
           [Start, End, MaxNum]),
    foldr_iter(DB, Fun, Fun(Start, Acc), {'[', End, mnesia:prev(DB, Start), ']'}, MaxNum -
          1).
