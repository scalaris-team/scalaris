% @copyright 2013-2016 Zuse Institute Berlin,

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

%% @author Tanguy Racinet
%% @doc    DB back-end using mnesia.
%%         Two keys K and L are considered equal if K == L yields true.
%% @end
-module(db_mnesia).
-author('tanracinet@gmail.com').
-vsn('$Id: db_ets.erl 6270 2014-03-28 14:25:52Z fajerski@zib.de $').

-include("scalaris.hrl").

-behaviour(db_backend_beh).

-define(TRACE(_X, _Y), ok).
%% -define(TRACE(X, Y), io:format(X, Y)).
%% -define(TRACE(X, Y), ct:pal(X, Y)).

%% primitives
-export([start/0, new/1, new/2, open/1, close/1, put/2, get/2, delete/2]).
%% db info
-export([get_persisted_tables/0, get_name/1, get_load/1,
         is_available/0, supports_feature/1]).
%% cleanup functions
-export([mnesia_tables_of/1, delete_mnesia_tables/1, close_and_delete/1]).

%% iteration
-export([foldl/3, foldl/4, foldl/5]).
-export([foldr/3, foldr/4, foldr/5]).
-export([foldl_unordered/3]).
-export([tab2list/1]).

-type db() :: atom().
-type key() :: db_backend_beh:key(). %% '$end_of_table' is not allowed as key() or else iterations won't work!
-type entry() :: db_backend_beh:entry().

-export_type([db/0]).

-export([traverse_table_and_show/1]).

-spec start() -> ok.
start() ->
  FullDataDir = config:read(db_directory) ++ "/" ++ atom_to_list(node()),
  application:set_env(mnesia, dir, FullDataDir),
%  case config:read(_type) of
  case config:read(start_type) of
    recover ->
      case mnesia:create_schema([node()]) of
        ok ->
          io:format("starting mnesia: no previous Schema to recover from.~n"),
          ok = mnesia:delete_schema([node()]),
          erlang:halt();
        {error, {_, {already_exists, _}}} ->
          io:format("starting mnesia: recovering.~n");
        Msg ->
              case util:is_unittest() of
                  true  -> ct:pal("starting mnesia recover : ~w~n", [Msg]);
                  false -> io:format("starting mnesia recover : ~w~n", [Msg])
              end,
              erlang:halt()
      end;
    _ ->
      case mnesia:create_schema([node()]) of
        ok -> ok;
        Msg ->
              case util:is_unittest() of
                  true ->
                      ct:pal("starting mnesia: ~w~n", [Msg]),
                      ct:pal("starting mnesia: maybe you tried to start a new node "
                             "while we still found persisted data of a node with the "
                             "same name. If you want to get rid of the old persisted "
                             "data, delete them using ~p.~n",
                             ["rm -rf data/" ++ atom_to_list(node())]);
                  false ->
                      io:format("starting mnesia: ~w~n", [Msg]),
                      io:format("starting mnesia: maybe you tried to start a new node "
                                "while we still found persisted data of a node with the "
                                "same name. If you want to get rid of the old persisted "
                                "data, delete them using ~p.~n",
                                ["rm -rf data/" ++ atom_to_list(node())])
              end,
              erlang:halt()
      end
  end,
  _ = application:start(mnesia),
  ok.

%% @doc traverse table and print content
-spec traverse_table_and_show(Table_name::nonempty_string()) -> ok.
traverse_table_and_show(Table_name)->
  Iterator =  fun(Rec,_)->
    log:log(warn, "~p~n",[Rec]),
    []
  end,
  case mnesia:is_transaction() of
    true -> mnesia:foldl(Iterator,[],Table_name);
    false ->
      Exec = fun({Fun,Tab}) -> mnesia:foldl(Fun, [],Tab) end,
      mnesia:activity(transaction,Exec,[{Iterator,Table_name}],mnesia_frag)
  end.

%% @doc Return all the tables owned by PidGroup
%%      NOTE: only returns tables with names according to this regular expression:
%%            <tt>^[^:]+:PidGroup(:.*)?$</tt>
-spec mnesia_tables_of(PidGroup::pid_groups:groupname()) -> [atom()].
mnesia_tables_of(PidGroup) ->
  Tabs = mnesia:system_info(tables),
  [ Tab || Tab <- Tabs,
           element(2, db_util:parse_table_name(
                     erlang:atom_to_list(Tab))) =:= PidGroup ].

%% @doc Gets a list of persisted tables.
-spec get_persisted_tables() -> [nonempty_string()].
get_persisted_tables() ->
      [atom_to_list(Table) || Table <- mnesia:system_info(tables), Table =/= schema].

%% @doc Close recursivly all mnesia tables in List
-spec delete_mnesia_tables(list()) -> ok.
delete_mnesia_tables(Tabs) ->
    _ = [close(Tab) || Tab <- Tabs],
    ok.

%% @doc Creates new DB handle named DBName.
-spec new(DBName::nonempty_string()) -> db().
new(DBName) ->
    ?TRACE("new:~nDB_name:~p~n",[DBName]),
    DbAtom = list_to_atom(DBName),
    {atomic, ok} = mnesia:create_table(DbAtom, [{disc_copies, [node()]},
                                                {type, ordered_set}]),
    DbAtom.

%% @doc Creates new DB handle named DBName with possibility to pass Options.
-spec new(DBName::nonempty_string(), Options::[term()] ) -> db().
new(DBName, Options) ->
    ?TRACE("new:~nDB_name:~p~nOption~p~n",[DBName, Options]),
    DbAtom = list_to_atom(DBName),
    {atomic, ok} = mnesia:create_table(DbAtom, [{disc_copies, [node()]},
                                                {type, ordered_set} | Options]),
    DbAtom.

%% @doc Open a previously existing database assuming the database has been
%%      restored by the start of the mnesia application.
-spec open(DBName::nonempty_string()) -> db().
open(DBName) ->
    erlang:list_to_atom(DBName).

%% @doc Closes the DB named DBName
-spec close(DB::db()) -> true.
close(DB) ->
  ?TRACE("close:~nDB_name:~p~n",[DB]),
  {atomic, ok} = mnesia:delete_table(DB),
  true.

%% @doc Closes and deletes the DB named DBName
-spec close_and_delete(DBName::db()) -> true.
close_and_delete(DBName) ->
  close(DBName).

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
    {atomic, _} = mnesia:transaction(fun() -> mnesia:write({DBName, element(1, Entry), Entry}) end),
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
    {atomic, _} = mnesia:transaction(fun()-> mnesia:delete({DBName, Key}) end),
    DBName.

%% @doc Returns the name of the DB specified in new/1.
-spec get_name(DB::db()) -> nonempty_string().
get_name(DB) ->
    erlang:atom_to_list(mnesia:table_info(DB, name)).

%% @doc Checks for modules required for this DB backend. Returns true if no 
%%      modules are missing, or else a list of missing modules
-spec is_available() -> boolean() | [atom()].
is_available() ->
    case code:which(mnesia) of
        non_existing -> [mnesia];
        _ -> true
    end.

%% @doc Returns true if the DB support a specific feature (e.g. recovery), false otherwise.
-spec supports_feature(Feature::atom()) -> boolean().
supports_feature(recover) -> true;
supports_feature(_) -> false.

%% @doc Returns the current load (i.e. number of stored tuples) of the DB.
-spec get_load(DB::db()) -> non_neg_integer().
get_load(DB) ->
    mnesia:table_info(DB, size).

%% @doc Is equivalent to ets:foldl(Fun, Acc0, DB).
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl(DB, Fun, Acc) ->
  ?TRACE("foldl/3:~n",[]),
  {atomic, First} = mnesia:transaction(fun()-> mnesia:first(DB) end),
  {atomic, Last} = mnesia:transaction(fun()-> mnesia:last(DB) end),
  foldl(DB, Fun, Acc, {'[', First, Last, ']'}, mnesia:table_info(DB, size)).

%% @doc foldl/4 iterates over DB and applies Fun(Entry, AccIn) to every element
%%      encountered in Interval. On the first call AccIn == Acc0. The iteration
%%      only apply Fun to the elements inside the Interval.
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Interval::db_backend_beh:interval()) -> Acc1::A.
foldl(DB, Fun, Acc, Interval) ->
    ?TRACE("foldl/4:~nstart:~n",[]),
    foldl(DB, Fun, Acc, Interval, mnesia:table_info(DB, size)).

%% @doc foldl/5 iterates over DB and applies Fun(Entry, AccIn) to every element
%%      encountered in Interval. On the first call AccIn == Acc0. The iteration
%%      stops as soon as MaxNum elements have been encountered.
-spec foldl(db(), fun((Key::key(), A) -> A), A,
                               db_backend_beh:interval(), non_neg_integer()) -> Acc1::A.
foldl(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldl(_DB, _Fun, Acc, {_, '$end_of_table', _End, _}, _MaxNum) -> Acc;
foldl(_DB, _Fun, Acc, {_, _Start, '$end_of_table', _}, _MaxNum) -> Acc;
foldl(_DB, _Fun, Acc, {_, Start, End, _}, _MaxNum) when Start > End -> Acc;
foldl(DB, Fun, Acc, {El}, _MaxNum) ->
    case mnesia:transaction(fun() -> mnesia:read(DB, El) end) of
      {atomic, []} ->
            Acc;
      {atomic, [_Entry]} ->
            Fun(El, Acc)
    end;
foldl(DB, Fun, Acc, all, MaxNum) ->
  {atomic, First} = mnesia:transaction(fun()-> mnesia:first(DB) end),
  {atomic, Last} = mnesia:transaction(fun()-> mnesia:last(DB) end),
  foldl(DB, Fun, Acc, {'[', First, Last, ']'}, MaxNum);
foldl(DB, Fun, Acc, {'(', Start, End, RBr}, MaxNum) ->
    {atomic, Next} = mnesia:transaction(fun()-> mnesia:next(DB, Start) end),
    foldl(DB, Fun, Acc, {'[', Next, End, RBr}, MaxNum);
foldl(DB, Fun, Acc, {LBr, Start, End, ')'}, MaxNum) ->
    {atomic, Previous} = mnesia:transaction(fun()-> mnesia:prev(DB, End) end),
    foldl(DB, Fun, Acc, {LBr, Start, Previous, ']'}, MaxNum);
foldl(DB, Fun, Acc, {'[', Start, End, ']'}, MaxNum) ->
    ?TRACE("foldl:~nstart: ~p~nend:   ~p~nmaxnum: ~p~ninterval: ~p~n",
      [Start, End, MaxNum, {'[', Start, End, ']'}]),
    case mnesia:transaction(fun()-> mnesia:read(DB, Start) end) of
      {atomic, []} ->
            {atomic, Next} = mnesia:transaction(fun()-> mnesia:next(DB, Start) end),
            foldl(DB, Fun, Acc, {'[', Next, End, ']'},
                       MaxNum);
      {atomic, [_Entry]} ->
            foldl_iter(DB, Fun, Acc, {'[', Start, End, ']'},
                       MaxNum)
    end.

%% @doc foldl_iter(/5) is a recursive function applying Fun only on elements
%%      inside the Interval. It is called by every foldl operation.
-spec foldl_iter(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Intervall::db_backend_beh:interval(), MaxNum::non_neg_integer()) -> Acc1::A.
foldl_iter(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldl_iter(_DB, _Fun, Acc, {_, '$end_of_table', _End, _}, _MaxNum) -> Acc;
foldl_iter(_DB, _Fun, Acc, {_, _Start, '$end_of_table', _}, _MaxNum) -> Acc;
foldl_iter(_DB, _Fun, Acc, {_, Start, End, _}, _MaxNum) when Start > End -> Acc;
foldl_iter(DB, Fun, Acc, {'[', Start, End, ']'}, MaxNum) ->
    ?TRACE("foldl_iter:~nstart: ~p~nend:   ~p~nmaxnum: ~p~ninterval: ~p~n",
           [Start, End, MaxNum, {'[', Start, End, ']'}]),
    {atomic, Next} = mnesia:transaction(fun()-> mnesia:next(DB, Start) end),
    foldl_iter(DB, Fun, Fun(Start, Acc), {'[', Next, End, ']'}, MaxNum - 1).

%% @doc Is equivalent to foldr(Fun, Acc0, DB).
-spec foldr(db(), fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldr(DB, Fun, Acc) ->
    {atomic, First} = mnesia:transaction(fun()-> mnesia:first(DB) end),
    {atomic, Last} = mnesia:transaction(fun()-> mnesia:last(DB) end),
    foldr(DB, Fun, Acc, {'[', First, Last, ']'}, mnesia:table_info(DB, size)).

%% @doc Is equivalent to foldr(DB, Fun, Acc0, Interval, get_load(DB)).
-spec foldr(db(), fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A, db_backend_beh:interval()) -> Acc1::A.
foldr(DB, Fun, Acc, Interval) ->
    foldr(DB, Fun, Acc, Interval, mnesia:table_info(DB, size)).

%% @doc Behaves like foldl/5 with the difference that it starts at the end of
%%      Interval and iterates towards the start of Interval.
-spec foldr(db(), fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A, db_backend_beh:interval(), non_neg_integer()) -> Acc1::A.
foldr(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldr(_DB, _Fun, Acc, {_, _End, '$end_of_table', _}, _MaxNum) -> Acc;
foldr(_DB, _Fun, Acc, {_, '$end_of_table', _Start, _}, _MaxNum) -> Acc;
foldr(_DB, _Fun, Acc, {_, End, Start, _}, _MaxNum) when Start < End -> Acc;
foldr(DB, Fun, Acc, {El}, _MaxNum) ->
    case mnesia:transaction(fun()-> mnesia:read(DB, El) end) of
      {atomic, []} ->
            Acc;
      {atomic, [_Entry]} ->
            Fun(El, Acc)
    end;
foldr(DB, Fun, Acc, all, MaxNum) ->
    {atomic, First} = mnesia:transaction(fun()-> mnesia:first(DB) end),
    {atomic, Last} = mnesia:transaction(fun()-> mnesia:last(DB) end),
    foldr(DB, Fun, Acc, {'[', First, Last, ']'}, MaxNum);
foldr(DB, Fun, Acc, {'(', End, Start, RBr}, MaxNum) ->
    {atomic, Next} = mnesia:transaction(fun()-> mnesia:next(DB, End) end),
    foldr(DB, Fun, Acc, {'[', Next, Start, RBr}, MaxNum);
foldr(DB, Fun, Acc, {LBr, End, Start, ')'}, MaxNum) ->
    {atomic, Previous} = mnesia:transaction(fun()-> mnesia:prev(DB, Start) end),
    foldr(DB, Fun, Acc, {LBr, End, Previous, ']'}, MaxNum);
foldr(DB, Fun, Acc, {'[', End, Start, ']'}, MaxNum) ->
    case mnesia:transaction(fun()-> mnesia:read(DB, Start) end) of
      {atomic, []} ->
            {atomic, Previous} = mnesia:transaction(fun()-> mnesia:prev(DB, Start) end),
            foldr(DB, Fun, Acc, {'[', End, Previous, ']'}, MaxNum);
      {atomic, [_Entry]} ->
            foldr_iter(DB, Fun, Acc, {'[', End, Start, ']'}, MaxNum)
    end.

-spec foldr_iter(db(), fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A, db_backend_beh:interval(), non_neg_integer()) -> Acc1::A.
foldr_iter(_DB, _Fun, Acc, _Interval, 0) -> Acc;
foldr_iter(_DB, _Fun, Acc, {_, _End, '$end_of_table', _}, _MaxNum) -> Acc;
foldr_iter(_DB, _Fun, Acc, {_, '$end_of_table', _Start, _}, _MaxNum) -> Acc;
foldr_iter(_DB, _Fun, Acc, {_, End, Start, _}, _MaxNum) when Start < End -> Acc;
foldr_iter(DB, Fun, Acc, {'[', End, Start, ']'}, MaxNum) ->
    ?TRACE("foldr:~nstart: ~p~nend ~p~nmaxnum: ~p~nfound",
           [Start, End, MaxNum]),
  {atomic, Previous} = mnesia:transaction(fun()-> mnesia:prev(DB, Start) end),
    foldr_iter(DB, Fun, Fun(Start, Acc), {'[', End, Previous, ']'}, MaxNum - 1).

%% @doc Works similar to foldl/3 but uses mnesia:foldl instead of our own implementation.
%% The order in which will be iterated over is unspecified, but using this fuction
%% might be faster than foldl/3 if it does not matter.
-spec foldl_unordered(DB::db(), Fun::fun((Entry::entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl_unordered(DB, Fun, Acc) ->
    % Entry = {db, key, value}
    FoldlFun = fun(Entry, AccIn) -> Fun(element(3, Entry), AccIn) end,

    {atomic, Result}  = mnesia:transaction(fun() ->
                                                mnesia:foldl(FoldlFun, Acc, DB)
                                           end),
    Result.


-spec tab2list(Table_name::db()) -> [Entries::entry()].
tab2list(Table_name) ->
    Iterator =  fun({_DBName, _Key, Entry}, Acc)->
                        [Entry | Acc]
                end,
    case mnesia:is_transaction() of
        true -> mnesia:foldl(Iterator,[],Table_name);
        false ->
            Exec = fun({Fun,Tab}) -> mnesia:foldl(Fun, [],Tab) end,
            mnesia:activity(transaction,Exec,[{Iterator,Table_name}],mnesia_frag)
    end.
