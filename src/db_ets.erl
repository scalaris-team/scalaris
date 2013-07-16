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
%% @end
%% @version $Id$
-module(db_ets).
-author('fajerski@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behaviour(backend_beh).

-define(TRACE(_X, _Y), ok).
%% -define(TRACE(X, Y), ct:pal(X, Y)).

%% primitives
-export([new/1, close/1, put/2, get/2, delete/2]).
%% db info
-export([get_name/1, get_load/1]).

-export([foldl/3, foldl/4, foldl/5]).
-export([foldr/3, foldr/4, foldr/5]).

-type db() :: ets:tid() | atom().
-type key() :: term().
-type entry() :: tuple().
-type interval() :: intervals:simple_interval().

-ifdef(with_export_type_support).
-export_type([db/0]).
-endif.

-spec new(nonempty_string()) -> db().
new(DBName) ->
    ets:new(list_to_atom(DBName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]).

-spec close(db()) -> true.
close(DBName) ->
    ets:delete(DBName).

-spec put(db(), entry()) -> db().
put(DBName, Entry) ->
    ets:insert(DBName, Entry),
    DBName.

-spec get(db(), key()) -> entry() | {}.
get(DBName, Key) ->
    case ets:lookup(DBName, Key) of
        [Entry] ->
            Entry;
        [] ->
            {}
    end.

-spec delete(db(), key()) -> db().
delete(DBName, Key) ->
    ets:delete(DBName, Key),
    DBName.

-spec get_name(DB::db()) -> nonempty_string().
get_name(DB) ->
%% @doc Returns the name of the table for open/1.
    erlang:atom_to_list(ets:info(DB, name)).

-spec get_load(DB::db()) -> non_neg_integer().
get_load(DB) ->
    ets:info(DB, size).

-spec foldl(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl(DB, Fun, Acc) ->
    ets:foldl(Fun, Acc, DB).

-spec foldl(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, interval()) -> Acc1::A.
foldl(DB, Fun, Acc, Interval) ->
    foldl(DB, Fun, Acc, Interval, ets:info(DB, size)).

-spec foldl(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, interval(), non_neg_integer()) -> Acc1::A.
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
            ?TRACE("foldl:~nstart: ~p~nend:   ~p~nmaxnum: ~p~nfound ~p~n",
                   [Start, End, MaxNum, Entry]),
            foldl(DB, Fun, Fun(Entry, Acc), {interval, '[', ets:next(DB, Start),
                                             End, ']'}, MaxNum - 1)
    end.

-spec foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldr(DB, Fun, Acc) ->
    ets:foldr(Fun, Acc, DB).

-spec foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, interval()) -> Acc1::A.
foldr(DB, Fun, Acc, Interval) ->
    foldr(DB, Fun, Acc, Interval, ets:info(DB, size)).

-spec foldr(db(), fun((entry(), AccIn::A) -> AccOut::A), Acc0::A, interval(), non_neg_integer()) -> Acc1::A.
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
