%  @copyright 2010-2014 Zuse Institute Berlin

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

%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%% @doc    Creates test-DBs for rrepair.
%% @end
%% @version $Id$
-module(db_generator).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("scalaris.hrl").

-export([get_db/3, get_db/4]).
-export([fill_ring/3]).
-export([insert_db/1, remove_keys/1]).
-export([is_old_value/1]).

% for tester:
-export([get_db_feeder/3, get_db_feeder/4]).
-export([fill_ring_feeder/3]).
-export([feeder_fix_rangen/2]).

-define(VersionMin, 1).
-define(VersionMax, 1048576). % 1024*1024
-define(VersionDiffMax, 512).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TYPES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([distribution/0, db_distribution/0, option/0,
              db_type/0, db_parameter/0, db_status/0, failure_type/0]).

-type distribution()    :: random |
                           uniform |
                           {non_uniform, random_bias:generator()}.
-type db_distribution() :: distribution().
-type result_k()  :: ?RT:key().
-type result_ktpl():: {?RT:key()}.
-type result_kv() :: {?RT:key(), db_dht:value()}.
-type result()    :: result_k() | result_kv() | result_ktpl().
-type option()    :: {output, list_key_val | list_key | list_keytpl}.

-type failure_type()    :: update | regen | mixed.
-type failure_quadrant():: rt_beh:segment().
-type failure_dest()    :: all | [failure_quadrant(),...].
-type db_type()         :: wiki | random.
-type db_parameter()    :: {ftype, failure_type()} |
                           {fprob, 0..100} |                  %failure probability
                           {fdest, failure_dest()} |          %quadrants in which failures are inserted / if missing all is assumed
                           {fdistribution, distribution()} |  %distribution of failures
                           {distribution, db_distribution()}. %data distribution in every quadrant
-type db_status() :: {Entries   :: non_neg_integer(),
                      Existing  :: non_neg_integer(),
                      Missing   :: non_neg_integer(),
                      Outdated  :: non_neg_integer()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_db_feeder(intervals:continuous_interval(), 0..1000, db_distribution())
        -> {intervals:continuous_interval(), non_neg_integer(), db_distribution()}.
get_db_feeder(I0, Count0, Distribution0) ->
    {I, Count, Distribution, _} = get_db_feeder(I0, Count0, Distribution0, []),
    {I, Count, Distribution}.

%% @doc This will generate a list of up to [ItemCount] keys with the requested
%%      distribution in the given interval.
-spec get_db(intervals:continuous_interval(), non_neg_integer(), db_distribution()) -> [result_k()].
get_db(I, Count, Distribution) ->
    get_db(I, Count, Distribution, []).

-spec get_db_feeder(intervals:continuous_interval(), 0..1000, db_distribution(), [option()])
        -> {intervals:continuous_interval(), non_neg_integer(), db_distribution(), [option()]}.
get_db_feeder(I, Count, Distribution = random, Options) ->
    {I, Count, Distribution, Options};
get_db_feeder(I, Count, Distribution = uniform, Options) ->
    {I, Count, Distribution, Options};
get_db_feeder(I, Count0, Distribution0 = {non_uniform, RanGen}, Options) ->
    Count = erlang:min(Count0, random_bias:numbers_left(RanGen)),
    {I, Count, feeder_fix_rangen(Distribution0, Count), Options}.

-spec get_db(intervals:continuous_interval(), non_neg_integer(), db_distribution(), [option()]) -> [result()].
get_db(Interval, ItemCount, Distribution, Options) ->
    ?DBG_ASSERT(intervals:is_continuous(Interval)),
    ?DBG_ASSERT(Distribution =:= feeder_fix_rangen(Distribution, ItemCount)),
    OutputType = proplists:get_value(output, Options, list_key),
    case Distribution of
        random -> gen_random(Interval, ItemCount, OutputType);
        uniform -> uniform_key_list([{Interval, ItemCount}], [], OutputType);
        {non_uniform, RanGen} -> non_uniform_key_list(Interval, ItemCount, RanGen, [], OutputType)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec gen_random
        (Interval::intervals:continuous_interval(), ToAdd::non_neg_integer(),
         OutputType::list_key) -> [result_k()];
        (Interval::intervals:continuous_interval(), ToAdd::non_neg_integer(),
         OutputType::list_keytpl) -> [result_ktpl()];
        (Interval::intervals:continuous_interval(), ToAdd::non_neg_integer(),
         OutputType::list_key_val) -> [result_kv()].
gen_random(I, Add, OutputType) ->
    SimpleI = intervals:get_bounds(I),
    ToAdd0 = gb_sets:from_list(?RT:get_random_in_interval(SimpleI, Add)),
    gen_random_gb_sets(SimpleI, Add - gb_sets:size(ToAdd0), OutputType, ToAdd0, 1).

-spec gen_random_gb_sets
        (Interval::intervals:simple_interval2(), ToAdd::non_neg_integer(),
         OutputType::list_key, Set::gb_sets:set(?RT:key()), Retries::non_neg_integer())
        -> [result_k()];
        (Interval::intervals:simple_interval2(), ToAdd::non_neg_integer(),
         OutputType::list_keytpl, Set::gb_sets:set(?RT:key()), Retries::non_neg_integer())
        -> [result_ktpl()];
        (Interval::intervals:simple_interval2(), ToAdd::non_neg_integer(),
         OutputType::list_key_val, Set::gb_sets:set(?RT:key()), Retries::non_neg_integer())
        -> [result_kv()].
gen_random_gb_sets(_I, ToAdd, OutputType, Set, Retry)
  when ToAdd =:= 0 orelse Retry =:= 3 ->
    % abort after 3 random keys already in Tree / probably no more free keys in I
    KeyList = gb_sets:to_list(Set),
    case OutputType of
        list_key     -> [Key || Key <- KeyList];
        list_keytpl  -> [{Key} || Key <- KeyList];
        list_key_val -> [{Key, gen_value()} || Key <- KeyList]
    end;
gen_random_gb_sets(I, ToAdd, OutputType, Set, Retry) ->
    NewKey = ?RT:get_random_in_interval(I),
    case gb_sets:is_member(NewKey, Set) of
        true ->
            gen_random_gb_sets(I, ToAdd, OutputType, Set, Retry + 1);
        false ->
            NewSet = gb_sets:add(NewKey, Set),
            gen_random_gb_sets(I, ToAdd - 1, OutputType, NewSet, 0)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec uniform_key_list
        ([{Interval::intervals:continuous_interval(), ToAdd::non_neg_integer()}],
         Acc::[result_k()], OutputType::list_key) -> [result_k()];
        ([{Interval::intervals:continuous_interval(), ToAdd::non_neg_integer()}],
         Acc::[result_ktpl()], OutputType::list_keytpl) -> [result_ktpl()];
        ([{Interval::intervals:continuous_interval(), ToAdd::non_neg_integer()}],
         Acc::[result_kv()], OutputType::list_key_val) -> [result_kv()].
uniform_key_list([], Acc, _) -> Acc;
uniform_key_list([{I, Add} | R] = Cmd, Acc, AccType) ->
    if Add > 100 ->
           case intervals:split(I, 2) of
               [I1, I2] ->
                   AddD2 = Add div 2,
                   uniform_key_list([{I1, AddD2}, {I2, AddD2 + (Add rem 2)} | R],
                                    Acc, AccType);
               [I] ->
                   uniform_key_list_no_split(Cmd, Acc, AccType)
           end;
       true ->
           uniform_key_list_no_split(Cmd, Acc, AccType)
    end.

-spec uniform_key_list_no_split
        ([{Interval::intervals:continuous_interval(), ToAdd::non_neg_integer()}],
         Acc::[result_k()], OutputType::list_key) -> [result_k()];
        ([{Interval::intervals:continuous_interval(), ToAdd::non_neg_integer()}],
         Acc::[result_ktpl()], OutputType::list_keytpl) -> [result_ktpl()];
        ([{Interval::intervals:continuous_interval(), ToAdd::non_neg_integer()}],
         Acc::[result_kv()], OutputType::list_key_val) -> [result_kv()].
uniform_key_list_no_split([{_I, 0} | R], Acc, AccType) ->
    uniform_key_list(R, Acc, AccType);
uniform_key_list_no_split([{I, Add} | R], Acc, AccType) ->
    {LBr, IL, IR, RBr} = intervals:get_bounds(I),
    Denom = Add + ?IIF(RBr =:= ')' andalso LBr =:= '(', 1, 0),
    ToAddKeys = lists:usort(
                  util:for_to_ex(
                    ?IIF(LBr =:= '(', 1, 0),
                    Add + ?IIF(LBr =:= '(', 0, -1),
                    fun(Index) ->
                            ?RT:get_split_key(IL, IR, {Index, Denom})
                    end)),
    % note: ?RT:get_split_key/3 considers the interval (IL, IR] which may not
    %       be correct here
    ToAdd =
        case AccType of
            list_key ->
                [Key || Key <- ToAddKeys, intervals:in(Key, I)];
            list_keytpl ->
                [{Key} || Key <- ToAddKeys, intervals:in(Key, I)];
            list_key_val ->
                [{Key, gen_value()} || Key <- ToAddKeys, intervals:in(Key, I)]
        end,
    uniform_key_list(R, lists:append(ToAdd, Acc), AccType).

-spec gen_value() -> db_dht:value().
gen_value() ->
    tester_value_creator:create_value(integer, 0, tester_parse_state:new_parse_state()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec non_uniform_key_list
        (Interval::intervals:continuous_interval(), ToAdd::non_neg_integer(),
         RanGen::random_bias:generator(), Acc::[result_k()],
         OutputType::list_key) -> [result_k()];
        (Interval::intervals:continuous_interval(), ToAdd::non_neg_integer(),
         RanGen::random_bias:generator(), Acc::[result_ktpl()],
         OutputType::list_keytpl) -> [result_ktpl()];
        (Interval::intervals:continuous_interval(), ToAdd::non_neg_integer(),
         RanGen::random_bias:generator(), Acc::[result_kv()],
         OutputType::list_key_val) -> [result_kv()].
non_uniform_key_list(_I, 0, _RanGen, Acc, _AccType) -> Acc;
non_uniform_key_list(I, ToAdd, RanGen, Acc, AccType) ->
    ?DBG_ASSERT(ToAdd =:= 1 orelse random_bias:numbers_left(RanGen) =:= ToAdd),
    SubIntervals = intervals:split(I, ToAdd),
    non_uniform_key_list_(SubIntervals, ToAdd, RanGen, Acc, length(Acc), AccType, 0.0).

-spec non_uniform_key_list_
        (SubIs::[intervals:continuous_interval(),...], ToAdd::non_neg_integer(),
         Fun::random_bias:generator(), Acc::[result_k()], AccLen::non_neg_integer(),
         OutputType::list_key, RoundingError::float()) -> [result_k()];
        (SubIs::[intervals:continuous_interval(),...], ToAdd::non_neg_integer(),
         Fun::random_bias:generator(), Acc::[result_ktpl()], AccLen::non_neg_integer(),
         OutputType::list_keytpl, RoundingError::float()) -> [result_ktpl()];
        (SubIs::[intervals:continuous_interval(),...], ToAdd::non_neg_integer(),
         Fun::random_bias:generator(), Acc::[result_kv()], AccLen::non_neg_integer(),
         OutputType::list_key_val, RoundingError::float()) -> [result_kv()].
non_uniform_key_list_([SubI | R], ToAdd, RanGen, Acc, AccLen, AccType, RoundingError) ->
    ?DBG_ASSERT(not intervals:is_empty(SubI)),
    {Status, V, RanGen1} = random_bias:next(RanGen),
    Add0 = V * ToAdd + RoundingError,
    Add1 = erlang:max(0, erlang:trunc(Add0)),
    Add = if Add1 + AccLen > ToAdd -> ToAdd - AccLen;
              R =:= [] ->
                  % try to add the missing items to the last sub interval
                  ToAdd - AccLen;
             true -> Add1
          end,
    %log:pal("non_uniform_key_list: add: ~p, SubI: ~p", [Add, SubI]),
    NAcc0 = if Add >= 1 -> uniform_key_list([{SubI, Add}], [], AccType);
               true     -> []
            end,
    NAcc0Len = length(NAcc0),
    NAccLen = AccLen + NAcc0Len,
    % note: the ordering of the returned keys is not important
    %       -> append to smaller list (better performance)
    case Status of
        _ when NAccLen =:= ToAdd ->
            lists:append(NAcc0, Acc);
        ok when R =/= [] ->
            NAcc = lists:append(NAcc0, Acc),
            NewRoundingError = float(Add0 - Add + Add - NAcc0Len),
            non_uniform_key_list_(R, ToAdd, RanGen1, NAcc, NAccLen, AccType, NewRoundingError);
        _ when NAccLen < ToAdd ->
            % there may still be missing items -> try to add them to the last
            % sub interval again - beware not to create duplicate keys!
            Acc1 = uniform_key_list([{SubI, ToAdd - NAccLen}], NAcc0, AccType),
            lists:append(lists:usort(Acc1), Acc)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec fill_ring_feeder(random, {1..1000, 1..1000}, [db_parameter()])
        -> {db_type(), pos_integer(), [db_parameter()]}.
fill_ring_feeder(Type, {Size0, Size1}, Params) ->
    % failures must be less than DB size!
    if Size0 >= Size1 -> DBSize = Size0, FSize = Size1;
       true           -> DBSize = Size1, FSize = Size0
    end,
    Params1 =
        case proplists:get_value(distribution, Params, uniform) of
            {non_uniform, _DBRanGen} = DBDist0 ->
                [{distribution, feeder_fix_rangen(DBDist0, DBSize)} |
                     proplists:delete(distribution, Params)];
            _ -> Params
        end,
    % set the given number of failures into the random number generator
    Params2 =
        case proplists:get_value(fdistribution, Params1, uniform) of
            {non_uniform, _FRanGen} = EDist0 ->
                [{fdistribution, feeder_fix_rangen(EDist0, FSize)} |
                     proplists:delete(fdistribution, Params1)];
            _ -> Params1
        end,
    {Type, DBSize, Params2}.

% @doc  DBSize=Number of Data Entities in DB (without replicas)
-spec fill_ring(db_type(), pos_integer(), [db_parameter()]) -> db_status().
fill_ring(Type, DBSize, Params) ->
    case Type of
        random -> fill_random(DBSize, Params);
        wiki -> fill_wiki(Params, "barwiki-latest-pages-meta-current.db")
    end.

-compile({nowarn_unused_function, {fill_random_feeder, 2}}).
-spec fill_random_feeder({1..1000, 1..1000}, [db_parameter()])
        -> {pos_integer(), [db_parameter()]}.
fill_random_feeder(Sizes0, Params0) ->
    {random, DBSize, Params} = fill_ring_feeder(random, Sizes0, Params0),
    {DBSize, Params}.

-spec fill_random(DBSize::pos_integer(), [db_parameter()]) -> db_status().
fill_random(DBSize, Params) ->
    Distr = proplists:get_value(distribution, Params, uniform),
    I = hd(intervals:split(intervals:all(), config:read(replication_factor))),
    {_LBr, LKey, RKey, _RBr} = intervals:get_bounds(I),
    I2 = intervals:new('(', LKey, RKey, ')'),
    Keys = get_db(I2, DBSize, Distr),
    {DB, DBStatus} = gen_kvv(proplists:get_value(fdistribution, Params, uniform), Keys, Params),
    insert_db(DB),
    DBStatus.

-spec fill_wiki([db_parameter()], DBFile::string()) -> db_status().
fill_wiki(_Params, DBFile) ->
    Result = os:cmd("ant -buildfile ../contrib/wikipedia/build.xml import-db "
                   "-Ddata=\"../contrib/wikipedia/" ++ DBFile ++ "\" "
                   "-Dnumber_of_importers=1 -Dmy_import_number=1 "
                   "-Dscalaris.node=\"" ++ atom_to_list(erlang:node()) ++ "\" "
                   "-Dscalaris.cookie=\"" ++ atom_to_list(erlang:get_cookie()) ++ "\""),
    io:format("~s~n", [Result]),
    %TODO
    {0, 0, 0, 0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Adds the given db entries to the DBs of the corresponding dht_node
%%      processes.
-spec insert_db(db_dht:db_as_list()) -> ok.
insert_db(KVV) ->
    Nodes = get_node_list(),
    _ = lists:foldl(
          fun(Node, ActKVV) ->
                  comm:send(Node, {get_state, comm:this(), my_range}),
                  trace_mpath:thread_yield(),
                  receive ?SCALARIS_RECV({get_state_response, NRange}, ok) end,
                  {NKVV, RestKVV} = lists:partition(
                                      fun(Entry) ->
                                              intervals:in(db_entry:get_key(Entry), NRange)
                                      end, ActKVV),
                  comm:send(Node, {add_data, comm:this(), NKVV}),
                  RestKVV
          end,
          KVV, Nodes),
    _ = [begin
             trace_mpath:thread_yield(),
             receive ?SCALARIS_RECV({add_data_reply}, ok) end
         end || _Node <- Nodes],
    ok.

%% @doc Removes all DB entries with the given keys from the corresponding
%%      dht_node processes.
-spec remove_keys([?RT:key()]) -> ok.
remove_keys(Keys) ->
    _ = [begin
             comm:send(Node, {delete_keys, comm:this(), Keys})
         end || Node <- get_node_list()],
    _ = [begin
             trace_mpath:thread_yield(),
             receive ?SCALARIS_RECV({delete_keys_reply}, ok) end
         end || _Node <- get_node_list()],
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Binomial distributions should have an N so that the number of
%%      random numbers matches the number of e.g. items they should be used for.
%%      Use this to fix the value of auto-generated generators.
%%      Also pay attention that if no approximation is used in the binomial
%%      calculation, getting random numbers for high N is expensive!
-spec feeder_fix_rangen(distribution(), pos_integer()) -> distribution().
feeder_fix_rangen({non_uniform, {{binom, _N, P, X, Approx}, CalcFun, NewStateFun}}, MaxN) ->
    {non_uniform, {{binom, erlang:max(1, MaxN - 1), P, X, Approx}, CalcFun, NewStateFun}};
feeder_fix_rangen(RanGen, _MaxN) ->
    RanGen.

-compile({nowarn_unused_function, {gen_kvv_feeder, 3}}).
-spec gen_kvv_feeder(ErrorDist::distribution(), [?RT:key()], [db_parameter()])
        -> {ErrorDist::distribution(), [?RT:key()], [db_parameter()]}.
gen_kvv_feeder(EDist0, Keys0, Params) ->
    Keys = lists:usort(Keys0),
    KeysLen0 = length(Keys),
    case EDist0 of
        {non_uniform, RanGen} ->
            KeysLen = erlang:min(KeysLen0, random_bias:numbers_left(RanGen)),
            EDist = feeder_fix_rangen(EDist0, erlang:max(1, KeysLen)),
            {EDist, lists:sublist(Keys, KeysLen), Params};
        _ ->
            {EDist0, Keys, Params}
    end.

%% @doc Generates a consistent db with errors
%%      Note: the random number generator in {non_uniform, RanGen} will start
%%            from scratch each time this function is called based on the
%%            initial state!
-spec gen_kvv(ErrorDist::distribution(), [?RT:key()], [db_parameter()]) -> {db_dht:db_as_list(), db_status()}.
gen_kvv(EDist, Keys, Params) ->
    case length(Keys) of
        0 -> {[], {_Entries = 0, _Existing = 0, _Missing = 0, _Outdated = 0}};
        KeyCount ->
            FType = proplists:get_value(ftype, Params, update),
            FProb = proplists:get_value(fprob, Params, 50),
            FDest = proplists:get_value(fdest, Params, all),
            FCount =  erlang:round(KeyCount * (FProb / 100)),
            p_gen_kvv(EDist, Keys, KeyCount, FType, FDest, FCount)
    end.

-compile({nowarn_unused_function, {p_gen_kvv_feeder, 6}}).
-spec p_gen_kvv_feeder(ErrorDist::distribution(), [?RT:key(),...], KeyCount::0,
                       failure_type(), failure_dest(), FailCount::non_neg_integer())
        -> {ErrorDist::distribution(), [?RT:key(),...], KeyCount::pos_integer(),
            failure_type(), failure_dest(), FailCount::non_neg_integer()}.
p_gen_kvv_feeder(EDist0, Keys0, _WrongKeyCount, FType, FDest, FCount) ->
    Keys = lists:usort(Keys0),
    KeysLen0 = length(Keys),
    case EDist0 of
        {non_uniform, RanGen} ->
            KeysLen = erlang:min(KeysLen0, random_bias:numbers_left(RanGen)),
            EDist = feeder_fix_rangen(EDist0, KeysLen),
            {EDist, lists:sublist(Keys, KeysLen), KeysLen, FType, FDest, FCount};
        _ ->
            {EDist0, Keys, KeysLen0, FType, FDest, FCount}
    end.

-spec p_gen_kvv(ErrorDist::distribution(), [?RT:key(),...],
                KeyCount::pos_integer(), failure_type(),
                failure_dest(), FailCount::non_neg_integer()) -> {db_dht:db_as_list(), db_status()}.
p_gen_kvv(random, Keys, KeyCount, FType, FDest, FCount) ->
    ?DBG_ASSERT(length(Keys) =:= length(lists:usort(Keys))), % unique keys
    KeysWithVersions =
        lists:zip(Keys, randoms:rand_uniform(?VersionMin, ?VersionMax, KeyCount)),
    {GoodKeys, FKeys} = util:pop_randomsubset(FCount, KeysWithVersions),
    GoodDB = lists:flatmap(fun get_rep_group/1, GoodKeys),
    {DB, O} = lists:foldl(fun(FKey, {AccAll, Out}) ->
                                  {RList, NewOut} = get_failure_rep_group(FKey, FType, FDest),
                                  {lists:append(RList, AccAll), Out + NewOut}
                          end,
                          {GoodDB, 0}, FKeys),
    Insert = length(DB),
    DBSize = KeyCount * config:read(replication_factor),
    {DB, {DBSize, Insert, DBSize - Insert, O}};
p_gen_kvv({non_uniform, RanGen}, Keys, KeyCount, FType, FDest, FCount) ->
    ?DBG_ASSERT(length(Keys) =:= length(lists:usort(Keys))), % unique keys
    ?DBG_ASSERT(KeyCount =:= 1 orelse random_bias:numbers_left(RanGen) =< KeyCount),
    KeysWithVersions =
        lists:zip(Keys, randoms:rand_uniform(?VersionMin, ?VersionMax, KeyCount)),
    FProbList = get_non_uniform_probs(RanGen),
    % note: don't use RanGen any more - we don't get the new state in the last call!
    CellLength = case length(FProbList) of
                   0 -> KeyCount + 1;
                   FProbL -> erlang:round(KeyCount / FProbL)
               end,
    FCells = lists:reverse(
               lists:keysort(1, build_failure_cells(FProbList, KeysWithVersions,
                                                    CellLength, []))),
    {DB, _, Out} =
        lists:foldl(fun({_P, Cell}, {DB, RestF, ROut}) ->
                            {NewEntry, NewOut, NewF} =
                                add_failures_to_cell(Cell, RestF, FType, FDest, {[], ROut}),
                            {lists:append(NewEntry, DB), NewF, NewOut}
                    end,
                    {[], FCount, 0}, FCells),
    Insert = length(DB),
    DBSize = KeyCount * config:read(replication_factor),
    {DB, {DBSize, Insert, DBSize - Insert, Out}};
p_gen_kvv(uniform, Keys, KeyCount, FType, FDest, FCount) ->
    ?DBG_ASSERT(length(Keys) =:= length(lists:usort(Keys))), % unique keys
    KeysWithVersions =
        lists:zip(Keys, randoms:rand_uniform(?VersionMin, ?VersionMax, KeyCount)),
    FRate = case FCount of
                0 -> KeyCount + 1;
                _ -> erlang:max(1, erlang:trunc(KeyCount / FCount))
            end,
    {DB, O, _, _} =
        lists:foldl(
          fun(Key, {AccDb, Out, Count, FCRest}) ->
                  {{RList, AddOut}, FCNew} =
                      case Count rem FRate of
                          0 when FCRest > 0 ->
                              {get_failure_rep_group(Key, FType, FDest), FCRest - 1};
                          _ -> {{get_rep_group(Key), 0}, FCRest}
                      end,
                  {lists:append(RList, AccDb), Out + AddOut, Count + 1, FCNew}
          end,
          {[], 0, 1, FCount}, KeysWithVersions),
    Insert = length(DB),
    DBSize = KeyCount * config:read(replication_factor),
    {DB, {DBSize, Insert, DBSize - Insert, O}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec add_failures_to_cell([{?RT:key(), Version::db_dht:version()}], non_neg_integer(), failure_type(), failure_dest(),
                           Acc::{[db_entry:entry()], Outdated::non_neg_integer()})
        -> Result::{[db_entry:entry()], Outdated::non_neg_integer(), RestFCount::non_neg_integer()}.
add_failures_to_cell([], FCount, _FType, _FDest, {AccE, AccO}) ->
    {AccE, AccO, FCount};
add_failures_to_cell([H | T], 0, FType, FDest, {DB, Out}) ->
    add_failures_to_cell(T, 0, FType, FDest,
                         {lists:append(get_rep_group(H), DB), Out});
add_failures_to_cell([H | T], FCount, FType, FDest, {AccEntry, AccOut}) ->
    {AddEntrys, AddOut} = get_failure_rep_group(H, FType, FDest),
    add_failures_to_cell(T, FCount - 1, FType, FDest,
                         {lists:append(AddEntrys, AccEntry), AddOut + AccOut}).

%% @doc Groups Keys into cells of equal size with failure probabilities
%%      assigned from FProbs.
-spec build_failure_cells(FProbs::[float()], Keys::[{?RT:key(), Version::db_dht:version()}],
                          CellLength::pos_integer(),
                          Acc::[{float(), [{?RT:key(), Version::db_dht:version()}]}])
        -> Result::[{float(), [{?RT:key(), Version::db_dht:version()}]}].
build_failure_cells([], [], _CellLength, Acc) ->
    Acc;
build_failure_cells([], T, _CellLength, []) ->
    [{0.0, T}];
build_failure_cells([], T, _CellLength, [{P, Cell} | Acc]) ->
    [{P, lists:append(T, Cell)} | Acc];
build_failure_cells(_P, [], _CellLength, Acc) ->
    Acc;
build_failure_cells([P | T], List, CellLength, Acc) ->
    ?DBG_ASSERT(CellLength > 0), % otherwise no progress and endless loop!
    {Cell, LT} = util:safe_split(CellLength, List),
    build_failure_cells(T, LT, CellLength, [{P, Cell}|Acc]).

-spec get_non_uniform_probs(random_bias:generator()) -> [float()].
get_non_uniform_probs(RanGen) ->
    case random_bias:next(RanGen) of
        {ok, V, RanGen1} -> [V | get_non_uniform_probs(RanGen1)];
        {last, V, exit}  -> [V]
    end.

%% TODO: we need to create random values (prefixed them with old/new for better
%%       debugging) if we want to measure bytes to transfer
-spec get_synthetic_value(old | new) -> term().
get_synthetic_value(new) ->
    val;
get_synthetic_value(old) ->
    old.

%% @doc Determines whether a given value is outdated as created by
%%      get_synthetic_value/1.
-spec is_old_value(db_dht:value()) -> boolean().
is_old_value(old) -> true;
is_old_value(_) -> false.

-spec get_rep_group({?RT:key(), Version::db_dht:version()}) -> [db_entry:entry()].
get_rep_group({Key, Version}) ->
    Value = get_synthetic_value(new),
    [db_entry:new(K, Value, Version) || K <- ?RT:get_replica_keys(Key)].

-spec get_failure_rep_group({?RT:key(), Version::db_dht:version()},
                            failure_type(), failure_dest()) ->
          {[db_entry:entry()], Outdated::0 | 1}.
get_failure_rep_group({Key, VersionNew}, FType, FDest) ->
    RepKeys = ?RT:get_replica_keys(Key),
    EKey = get_error_key(RepKeys, FDest),
    ValueNew = get_synthetic_value(new),
    RGrp = [db_entry:new(K, ValueNew, VersionNew) || K <- RepKeys, K =/= EKey],
    case get_failure_type(FType) of
        update ->
            VersionOld = erlang:max(0, VersionNew - randoms:rand_uniform(1, ?VersionDiffMax)),
            ValueOld = get_synthetic_value(old),
            {[db_entry:new(EKey, ValueOld, VersionOld) | RGrp], 1};
        regen -> {RGrp, 0}
    end.

% @doc Resolves failure type mixed.
-spec get_failure_type(failure_type()) -> regen | update.
get_failure_type(mixed) ->
    case randoms:rand_uniform(1, 3) of
        1 -> update;
        _ -> regen
    end;
get_failure_type(Type) -> Type.

% @doc Returns one key of KeyList which is in any quadrant out of DestList.
-spec get_error_key([?RT:key(),...], failure_dest()) -> ?RT:key().
get_error_key(RepKeys, all) ->
    util:randomelem(RepKeys);
get_error_key(RepKeys, QList) ->
    rr_recon:map_rkeys_to_quadrant(RepKeys, util:randomelem(QList)).

-spec get_node_list() -> [comm:mypid()].
get_node_list() ->
    mgmt_server:node_list(),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {get_list_response, List}, %% ->
            List
          )
    end.
