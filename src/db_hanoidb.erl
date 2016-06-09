%% @copyright 2013 scalaris project http://scalaris.zib.de

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

%% @author Pierre M.
%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    DB back-end using HanoiDB.
%%         HanoiDB is a memory-cached disk backend.
%%         As disks are large (TB) HanoiDB can hold data much larger than RAM (GB).
%%         As disks persist data HanoiDB can be stoped and restarted without data loss.
%%         It is a pure Erlang implementation of Google's LevelDB disk-backed K/V store.
%%         See http://code.google.com/p/leveldb/ for background about storage levels.
%%         How to use scalaris with this hanoidb backend:
%%         -download https://github.com/krestenkrab/hanoidb and compile HanoiDB
%%         -make sure this db_hanoidb.erl file is in src/ (right with db_ets.erl)
%%         -rerun scalaris' configure with --enable-hanoidb
%%             ./configure --enable-hanoidb=/path/to/hanoidb
%%         -rerun make to rebuild scalaris and run tests
%%             ./make
%%             ./make test
%%         -enjoy
%%         Two keys K and L are considered equal if they match, i.e. K =:= L
%%         Made after v0.6.1 svn rev 5666.
%% @end
-module(db_hanoidb).

-include("scalaris.hrl").

-behaviour(db_backend_beh).

-define(IN(E), erlang:term_to_binary(E, [{minor_version, 1}])).
-define(OUT(E), erlang:binary_to_term(E)).

%% primitives
-export([new/1, open/1]).
-export([put/2, get/2, delete/2]).
-export([close/1, close_and_delete/1]).

%% db info
-export([get_persisted_tables/0, get_name/1, get_load/1, 
         is_available/0, supports_feature/1]).

%% iteration
-export([foldl/3, foldl/4, foldl/5]).
-export([foldr/3, foldr/4, foldr/5]).
-export([foldl_unordered/3]).
-export([tab2list/1]).

-type db() :: {DB::pid(), FileName::nonempty_string()}.
-type key() :: db_backend_beh:key(). %% '$end_of_table' is not allowed as key() or else iterations won't work!
-type entry() :: db_backend_beh:entry().

-export_type([db/0]).

-type hanoidb_config_option() ::  {compress, none | gzip | snappy | lz4}
                                | {page_size, pos_integer()}
                                | {read_buffer_size, pos_integer()}
                                | {write_buffer_size, pos_integer()}
                                | {merge_strategy, fast | predictable }
                                | {sync_strategy, none | sync | {seconds, pos_integer()}}
                                | {expiry_secs, non_neg_integer()}
                                | {spawn_opt, list()}.

%% @doc Creates new DB handle named DBName.
-spec new(DBName::nonempty_string()) -> db().
new(DBName) ->
    new_db(DBName, []). % hanoidb's default options. May need tuning.

%% @doc Re-opens an existing-on-disk database.
-spec open(DBName::nonempty_string()) -> db().
open(DBName) ->
    new_db(DBName, []). % hanoidb's default options. May need tuning.

%% @doc Creates new DB handle named DBName with options.
-spec new_db(DirName::string(), HanoiOptions::[hanoidb_config_option()]) -> db().
new_db(DBName, HanoiOptions) ->
    BaseDir = [config:read(db_directory), "/", atom_to_list(node())],
    _ = case file:make_dir(BaseDir) of
            ok -> ok;
            {error, eexist} -> ok;
            {error, Error0} -> erlang:exit({?MODULE, 'cannot create dir', BaseDir, Error0})
        end,
    
    % HanoiDB stores not in a file but a dir store
    FullDBDir = lists:flatten([BaseDir, "/", DBName]),
    case hanoidb:open(FullDBDir, HanoiOptions) of 
        {ok, Tree} ->    {Tree, DBName};
        ignore ->    log:log(error, "[ Node ~w:db_hanoidb ] ~.0p", [self(), ignore]),
                     erlang:error({hanoidb_failed, ignore});
        {error, Error2} -> log:log(error, "[ Node ~w:db_hanoidb ] ~.0p", [self(), Error2]),
                           erlang:error({hanoidb_failed, Error2})
    end.

%% @doc Closes the DB named DBName keeping its data on disk.
-spec close(DB::db()) -> true.
close({DB, _FileName}) ->
    ok = hanoidb:close(DB),
    true.
    % hanoidb:stop(). Not needed.

%% @doc Closes and deletes the DB named DBName
-spec close_and_delete(DB::db()) -> true.
close_and_delete({_DB, DBName} = State) ->
    close(State),
    % A disk backend happens in some directory
    
    DirName = [config:read(db_directory), "/", atom_to_list(node()), "/", DBName],
    
    % Delete all DB files
    {ok, Files} = file:list_dir(DirName),
    lists:foreach(fun(FileName) ->
                          FullFileName = lists:flatten([DirName, "/", FileName]),
                          case file:delete(FullFileName) of
                              ok -> ok;
                              {error, Reason} ->
                                  log:log(error, "[ Node ~w:~w ] deleting ~.0p failed: ~.0p",
                                          [self(), ?MODULE, FileName, Reason])
                          end
                  end, Files),
    
    % Delete DB dir
    case file:del_dir(DirName) of
        ok -> ok;
        {error, Reason} -> log:log(error, "[ Node ~w:db_hanoidb ] deleting ~.0p failed: ~.0p",
                                   [self(), DirName, Reason])
    end.

%% @doc Saves arbitrary tuple Entry in DB DBName and returns the new DB.
%%      The key is expected to be the first element of Entry.
-spec put(DB::db(), Entry::entry()) -> db().
put({DB, _DBName} = State, Entry) ->
    ok = hanoidb:put(DB, ?IN(element(1, Entry)), ?IN(Entry)    ),
    State.

%% @doc Returns the entry that corresponds to Key or {} if no such tuple exists.
-spec get(DB::db(), Key::key()) -> entry() | {}.
get({DB, _DBName}, Key) ->
    case hanoidb:get(DB, ?IN(Key)) of
        not_found    -> {};
        {ok, Entry}    -> ?OUT(Entry)
    end.

%% @doc Deletes the tuple saved under Key and returns the new DB.
%%      If such a tuple does not exists nothing is changed.
-spec delete(DB::db(), Key::key()) -> db().
delete({DB, _FileName} = State, Key) ->
    ok = hanoidb:delete(DB, ?IN(Key)),
    State.

%% @doc Gets a list of persisted tables.
-spec get_persisted_tables() -> [nonempty_string()].
get_persisted_tables() ->
    %% TODO: implement
    [].


%% @doc Checks for modules required for this DB backend. Returns true if no 
%%      modules are missing, or else a list of missing modules
-spec is_available() -> boolean() | [atom()].
is_available() ->
    case code:which(hanoidb) of
        non_existing -> [hanoidb];
        _ -> true
    end.

%% @doc Returns true if the DB support a specific feature (e.g. recovery), false otherwise.
-spec supports_feature(Feature::atom()) -> boolean().
supports_feature(recover) -> true;
supports_feature(_) -> false.

%% @doc Returns the name of the DB specified in @see new/1 and open/1.
-spec get_name(DB::db()) -> nonempty_string().
get_name({_DB, DBName}) ->
    DBName.

%% @doc Returns the number of stored keys.
-spec get_load(DB::db()) -> non_neg_integer().
get_load({DB, _DBName}) ->
    %% TODO: not really efficient (maybe store the load in the DB?)
    hanoidb:fold(DB, fun (_K, _V, Load) -> Load + 1 end, 0).

%% @equiv hanoidb:fold_range(DB, Fun, Acc0, #key_range{from_key = <<>>, to_key = undefined})
%% @doc Returns a potentially larger-than-memory dataset. Use with care.
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl(State, Fun, Acc) ->
    %hanoidb:fold(DB, fun (K, _V, AccIn) -> Fun(?OUT(K), AccIn) end, Acc0).
    foldl_helper(State, Fun, Acc, all, -1).

%% @equiv foldl(DB, Fun, Acc0, Interval, get_load(DB))
%% @doc   Returns a potentially larger-than-memory dataset. Use with care.
-spec foldl(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
            Interval::db_backend_beh:interval()) -> Acc1::A.
foldl(State, Fun, Acc, Interval) ->
    %hanoidb:fold_range(DB, Fun, Acc, #key_range{from_key=K1, to_key=K2}). % TODO check it is possible
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
    %% write op) but CAN HanoiDB handle writes whiles folding ? (TODO check YES?)
    %% Since we reversed the order while accumulating reverse it by using lists
    %% fold but "from the other side". TODO check this for HanoiDB
    %hanoidb:fold_range(DB, Fun, Acc, #key_range{limit=N, from_key=K1, to_key=K2}) % TODO check it is possible
    foldl_helper(State, Fun, Acc, Interval, MaxNum).

%% @private this helper enables us to use -1 as MaxNum. MaxNum == -1 signals that all
%%          data is to be retrieved.
-spec foldl_helper(DB::db(), Fun::fun((Key::key(), AccIn::A) -> AccOut::A), Acc0::A,
                   Intervall::db_backend_beh:interval(), MaxNum::integer()) -> Acc1::A.
foldl_helper({DB, _FileName}, Fun, Acc, Interval, MaxNum) ->
    Keys = get_all_keys(DB, Interval, MaxNum), % hopefully MaxNum caps it.
    lists:foldr(Fun, Acc, Keys). % db:foldL calls lists:foldR
    % TODO May be hanoidb:fold_range is less RAM intensive : no need to keep all keys in RAM at once, but continuous folding instead.

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
foldr_helper({DB, _FileName}, Fun, Acc, Interval, MaxNum) ->
    % TODO evaluate hanoidb:fold_range(DB, Fun, Acc, #key_range{limit=N, from_key=K1, to_key=K2})
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

%% @doc Works similar to foldl/3 but uses hanoidb:fold instead of our own implementation. 
%% The order in which will be iterated over is unspecified, but using this fuction
%% might be faster than foldl/3 if it does not matter.
-spec foldl_unordered(DB::db(), Fun::fun((Entry::entry(), AccIn::A) -> AccOut::A), Acc0::A) -> Acc1::A.
foldl_unordered({DB, _DBName}, Fun, Acc) ->
    hanoidb:fold(DB, fun (_K, Entry, AccIn) -> Fun(?OUT(Entry), AccIn) end, Acc).


%% @private get_all_keys/3 retrieves all keys in DB that fall into Interval but
%%          not more than MaxNum. If MaxNum == -1 all Keys are retrieved. If
%%          MaxNum is positive it starts from the left in term order.
-spec get_all_keys(pid(), db_backend_beh:interval(), -1 | non_neg_integer())
        -> [key()].
get_all_keys(DB, Interval, MaxNum) ->
    % TODO evaluate converting scalaris:Intervals to hanoidb:ranges
    % in order to leverage hanoidb:fold rather than get_all_keys+lists:fold.
    
    Keys = hanoidb:fold(DB, fun(Key, _Entry, AccIn) -> [?OUT(Key) | AccIn] end, []),
    
    {_, In} = lists:foldl(fun 
                             (_, {0, _} = AccIn) ->
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
