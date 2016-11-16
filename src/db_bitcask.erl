% @copyright 2016 Zuse Institute Berlin,

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    DB back-end using bitcask.
%%         `./configure --enable-bitcask=/home/scalaris/apps/bitcask/'
%%         export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/Users/scalaris/apps/bitcask/priv/
%%         resp.
%%         export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/scalaris/apps/bitcask/priv/
%% @end
-module(db_bitcask).
-author('schuett@zib.de').

-include("scalaris.hrl").

-behaviour(db_backend_beh).

-define(TRACE(_X, _Y), ok).
%% -define(TRACE(X, Y), ct:pal(X, Y)).

-define(IN(E), erlang:term_to_binary(E, [{minor_version, 1}])).
-define(OUT(E), erlang:binary_to_term(E)).

%% primitives
-export([new/1, open/1, close/1, close_and_delete/1,
         put/2, get/2, delete/2]).
%% db info
-export([get_persisted_tables/0, get_name/1, get_load/1,
         is_available/0, supports_feature/1]).

%% iteration
-export([foldl/3, foldl/4, foldl/5]).
-export([foldr/3, foldr/4, foldr/5]).
-export([foldl_unordered/3]).
-export([tab2list/1]).

-type db() :: {DB::reference(), DBName::nonempty_string()}.
-type key() :: db_backend_beh:key(). %% '$end_of_table' is not allowed as key() or else iterations won't work!
-type entry() :: db_backend_beh:entry().

-export_type([db/0]).

%% @doc Creates new DB handle named DBName.
-spec new(DBName::nonempty_string()) -> db().
new(DBName) ->
    new_db(DBName, [read_write]).

%% @doc Open a previously existing database.
-spec open(DBName::nonempty_string()) -> db().
open(DBName) ->
    new_db(DBName, [read_write]).

-spec new_db(DBName::nonempty_string(),
             BitcaskOptions::[read_write]) -> db().
new_db(DBName, BitcaskOptions) ->
    FullDir1 = [config:read(db_directory), "/", atom_to_list(node())],
    _ = case file:make_dir(FullDir1) of
        ok -> ok;
        {error, eexist} -> ok;
        {error, Error0} -> erlang:exit({db_bitcask, 'cannot create dir', FullDir1, Error0})
    end,
    FullDir = [config:read(db_directory), "/", atom_to_list(node()), "/", DBName],
    _ = case file:make_dir(FullDir) of
        ok -> ok;
        {error, eexist} -> ok;
        {error, Error1} -> erlang:exit({db_bitcask, 'cannot create dir', FullDir, Error1})
    end,
    BitcaskDir = filename:absname(lists:flatten(FullDir)),
    case bitcask:open(BitcaskDir, BitcaskOptions) of
        {error, Error} ->
            log:log(error, "[ Node ~w:db_bitcask ] ~.0p", [self(), Error]),
            erlang:error({bitcask_failed, Error});
        Handle ->
            db_bitcask_merge_extension:init(Handle, BitcaskDir),
            {Handle, DBName}
    end.

%% @doc Closes the DB named DBName
-spec close(db()) -> true.
close({DB, _DBName}) ->
    bitcask:close(DB),
    erlang:erase(bitcask_efile_port), %% @todo close port?!?
    true.

%% @doc Closes and deletes the DB named DBName
-spec close_and_delete(db()) -> true.
close_and_delete({_DB, DBName} = State) ->
    close(State),
    FullDir = lists:flatten([config:read(db_directory), "/", atom_to_list(node()), "/", DBName]),
    cleanup(FullDir), true.

%% @doc Gets a list of persisted tables.
-spec get_persisted_tables() -> [nonempty_string()].
get_persisted_tables() ->
    FullDir = [config:read(db_directory), "/", atom_to_list(node())],
    case file:list_dir(FullDir) of
        {ok, Directories} ->
            Directories; %% @todo filter for directories
        {error, enoent} -> []
    end.

%% @doc Saves arbitrary tuple Entry in DB DBName and returns the new DB.
%%      The key is expected to be the first element of Entry.
-spec put(db(), Entry::entry()) -> db().
put({DB, _DBName} = State, Entry) ->
    _ = bitcask:put(DB, term_to_binary(element(1, Entry)), term_to_binary(Entry)),
    State.

%% @doc Returns the entry that corresponds to Key or {} if no such tuple exists.
-spec get(DB::db(), Key::key()) -> entry() | {}.
get({DB, _DBName}, Key) ->
    case bitcask:get(DB, term_to_binary(Key)) of
        not_found -> {};
        {error, _Err} -> {};
        {ok, BinaryValue} ->
            binary_to_term(BinaryValue)
    end.

%% @doc Deletes the tuple saved under Key and returns the new DB.
%%      If such a tuple does not exists nothing is changed.
-spec delete(DB::db(), Key::key()) -> db().
delete({DB, _DBName} = State, Key) ->
    bitcask:delete(DB, term_to_binary(Key)),
    State.

%% @doc Returns the name of the DB specified in @see new/1.
-spec get_name(DB::db()) -> nonempty_string().
get_name({_DB, DBName}) ->
    DBName.

%% @doc Checks for modules required for this DB backend. Returns true if no
%%      modules are missing, or else a list of missing modules
-spec is_available() -> boolean() | [atom()].
is_available() ->
    case code:which(bitcask) of
        non_existing -> [bitcask];
        _ -> true
    end.

%% @doc Returns true if the DB support a specific feature (e.g. recovery), false otherwise.
-spec supports_feature(Feature::atom()) -> boolean().
supports_feature(recover) -> false;
supports_feature(_) -> false.

%% @doc Returns the current load (i.e. number of stored tuples) of the DB.
-spec get_load(DB::db()) -> non_neg_integer().
get_load({DB, _DBName}) ->
    {KeyCount, _L} = bitcask:status(DB),
    KeyCount.

%% @doc Equivalent to toke_drv:fold(Fun, Acc0, DB).
%%      Returns a potentially larger-than-memory dataset. Use with care.
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl(State, Fun, Acc) ->
    foldl_helper(State, Fun, Acc, all, -1).

%% @equiv foldl(DB, Fun, Acc0, Interval, get_load(DB))
%% @doc   Returns a potentially larger-than-memory dataset. Use with care.
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Interval::db_backend_beh:interval()) -> Acc1::A.
foldl(State, Fun, Acc, Interval) ->
    foldl_helper(State, Fun, Acc, Interval, -1).

%% @doc foldl iterates over DB and applies Fun(Entry, AccIn) to every element
%%      encountered in Interval. On the first call AccIn == Acc0. The iteration
%%      stops as soon as MaxNum elements have been encountered.
%%      Returns a potentially larger-than-memory dataset. Use with care.
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Intervall::db_backend_beh:interval(), MaxNum::non_neg_integer()) -> Acc1::A.
foldl(State, Fun, Acc, Interval, MaxNum) ->
    %% HINT
    %% Fun can only be applied in a second pass. It could do a delete (or other
    %% write op) and toke can not handle writes whiles folding.
    %% Since we reversed the order while accumulating reverse it by using lists
    %% fold but "from the other side"
    foldl_helper(State, Fun, Acc, Interval, MaxNum).

%% @private this helper enables us to use -1 as MaxNum. MaxNum == -1 signals that all
%%          data is to be retrieved.
-spec foldl_helper(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Intervall::db_backend_beh:interval(), MaxNum::integer()) -> Acc1::A.
foldl_helper({DB, _DBName}, Fun, Acc, Interval, MaxNum) ->
    Keys = get_all_keys(DB, Interval, MaxNum),
    lists:foldr(Fun, Acc, Keys).

%% @doc makes a foldr over the whole dataset.
%%      Returns a potentially larger-than-memory dataset. Use with care.
-spec foldr(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldr(State, Fun, Acc) ->
    foldr_helper(State, Fun, Acc, all, -1).

%% @equiv foldr(DB, Fun, Acc0, Interval, get_load(DB))
%% @doc   Returns a potentially larger-than-memory dataset. Use with care.
-spec foldr(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Interval::db_backend_beh:interval()) -> Acc1::A.
foldr(State, Fun, Acc, Interval) ->
    foldr_helper(State, Fun, Acc, Interval, -1).

%% @doc foldr iterates over DB and applies Fun(Entry, AccIn) to every element
%%      encountered in Interval. On the first call AccIn == Acc0. The iteration
%%      stops as soon as MaxNum elements have been encountered.
%%      Returns a potentially larger-than-memory dataset. Use with care.
-spec foldr(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Intervall::db_backend_beh:interval(), MaxNum::non_neg_integer()) -> Acc1::A.
foldr(State, Fun, Acc, Interval, MaxNum) ->
    foldr_helper(State, Fun, Acc, Interval, MaxNum).

%% @private this helper enables us to use -1 as MaxNum. MaxNum == -1 signals that all
%%          data is to be retrieved.
-spec foldr_helper(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                               Intervall::db_backend_beh:interval(), MaxNum::integer()) -> Acc1::A.
foldr_helper({DB, _DBName}, Fun, Acc, Interval, MaxNum) ->
    %% first only retrieve keys so we don't have to load the whole db into memory
    Keys = get_all_keys(DB, Interval, -1),
    CutData = case MaxNum of
                  N when N < 0 ->
                      Keys;
                  _ ->
                      lists:sublist(Keys, MaxNum)
              end,
    %% see HINT in foldl/5
    %% now retrieve actual data
    lists:foldl(Fun, Acc, CutData).

%% @doc Works similar to foldl/3 but uses toke_drv:fold instead of our own implementation.
%% The order in which will be iterated over is unspecified, but using this fuction
%% might be faster than foldl/3 if it does not matter.
-spec foldl_unordered(DB::db(), Fun::fun((Entry::entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl_unordered(State, Fun, Acc) ->
        %TODO Use native fold
        foldl(State, fun(K, AccIn) -> Fun(get(State, K), AccIn) end, Acc).

%% @private get_all_keys/3 retrieves all keys in DB that fall into Interval but
%%          not more than MaxNum. If MaxNum == -1 all Keys are retrieved. If
%%          MaxNum is positive it starts from the left in term order.
-spec get_all_keys(reference(), db_backend_beh:interval(), -1 | non_neg_integer()) ->
    [key()].
get_all_keys(DB, Interval, MaxNum) ->
    Keys = case bitcask:list_keys(DB) of
               {error, _Term} -> [];
               BinaryKeys -> [ binary_to_term(BinaryKey) || BinaryKey <- BinaryKeys]
           end,
    {_, In} = lists:foldl(fun(_, {0, _} = AccIn) ->
                                  AccIn;
                             (Key, {Max, KeyAcc} = AccIn) ->
                          case is_in(Interval, Key) of
                              true ->
                                  {Max - 1, [Key | KeyAcc]};
                              _ ->
                                  AccIn
                          end
                end, {MaxNum, []}, lists:sort(Keys)),
    In.


is_in({Key}, OtherKey) -> Key =:= OtherKey;
is_in(all, _Key) -> true;
is_in({'(', L, R, ')'}, Key) -> Key > L andalso Key < R;
is_in({'(', L, R, ']'}, Key) -> Key > L andalso ((Key < R) orelse (Key =:= R));
is_in({'[', L, R, ')'}, Key) -> ((Key > L) orelse (Key =:= L)) andalso Key < R;
is_in({'[', L, R, ']'}, Key) -> ((Key > L) orelse (Key =:= L)) andalso
                                          ((Key < R) orelse (Key =:= R)).
%% @doc Returns a list of all objects in the table Table_name.
-spec tab2list(Table_name::db()) -> [Entries::entry()].
tab2list(_Table_name) ->
    %% Not implemented yet.
    [].

cleanup(Path) ->
    Re = ".*",
    Files = filelib:fold_files(Path, Re, true, fun(File, Acc) -> [File | Acc] end, []),
    _ = [file:delete(File) || File <- Files],
    ok.
