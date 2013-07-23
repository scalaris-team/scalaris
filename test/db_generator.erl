%  @copyright 2010-2011 Zuse Institute Berlin

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

% for tester:
-export([get_db_feeder/3, get_db_feeder/4]).
%% -export([fill_ring_feeder/3]).
-export([feeder_fix_rangen/2]).

-define(ReplicationFactor, 4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TYPES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([distribution/0, db_distribution/0,
              db_type/0, db_parameter/0, db_status/0, failure_type/0]).
-endif.

-type distribution()    :: random |
                           uniform |
                           {non_uniform, random_bias:generator()}.
-type db_distribution() :: uniform |
                           {non_uniform, random_bias:generator()}.
-type result_k()  :: ?RT:key().
-type result_kv() :: {?RT:key(), db_dht:value()}.
-type result()    :: result_k() | result_kv().
-type option()    :: {output, list_key_val | list_key}.

-type failure_type()    :: update | regen | mixed.
-type failure_quadrant():: 1..4.
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
get_db_feeder(I, Count, Distribution) -> {I, Count, feeder_fix_rangen(Distribution, Count)}.

%% @doc This will generate a list of up to [ItemCount] keys with the requested
%%      distribution in the given interval.
-spec get_db(intervals:continuous_interval(), non_neg_integer(), db_distribution()) -> [result()].
get_db(I, Count, Distribution) ->
    get_db(I, Count, Distribution, []).

-spec get_db_feeder(intervals:continuous_interval(), 0..1000, db_distribution(), [option()])
        -> {intervals:continuous_interval(), non_neg_integer(), db_distribution(), [option()]}.
get_db_feeder(I, Count, Distribution, Options) -> {I, Count, feeder_fix_rangen(Distribution, Count), Options}.

-spec get_db(intervals:continuous_interval(), non_neg_integer(), db_distribution(), [option()]) -> [result()].
get_db(Interval, ItemCount, Distribution, Options) ->
    ?ASSERT(intervals:is_continuous(Interval)),
    ?ASSERT(Distribution =:= feeder_fix_rangen(Distribution, ItemCount)),
    OutputType = proplists:get_value(output, Options, list_key),
    case Distribution of
        uniform -> uniform_key_list([{Interval, ItemCount}], [], OutputType);
        {non_uniform, RanGen} -> non_uniform_key_list(Interval, ItemCount, RanGen, [], OutputType)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec uniform_key_list
        ([{Interval::intervals:continuous_interval(), ToAdd::non_neg_integer()}],
         Acc::[result_k()], OutputType::list_key) -> [result_k()];
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
         Acc::[result_kv()], OutputType::list_key_val) -> [result_kv()].
uniform_key_list_no_split([{_I, 0} | R], Acc, AccType) ->
    uniform_key_list(R, Acc, AccType);
uniform_key_list_no_split([{I, Add} | R], Acc, AccType) ->
    {LBr, IL, IR, RBr} = intervals:get_bounds(I),
    Denom = Add + ?IIF(RBr =:= ')', 1, 0),
    ToAddKeys = lists:usort(
                  util:for_to_ex(
                    ?IIF(LBr =:= '(', 1, 0),
                    Add + ?IIF(LBr =:= '(', 0, -1),
                    fun(Index) ->
                            ?RT:get_split_key(IL, IR, {Index, Denom})
                    end)),
    ToAdd =
        case AccType of
            list_key ->
                [Key || Key <- ToAddKeys, intervals:in(Key, I)];
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
         RanGen::random_bias:generator(), Acc::[result_kv()],
         OutputType::list_key_val) -> [result_kv()].
non_uniform_key_list(_I, 0, _RanGen, Acc, _AccType) -> Acc;
non_uniform_key_list(I, ToAdd, RanGen, Acc, AccType) ->
    SubIntervals = intervals:split(I, ToAdd),
    non_uniform_key_list_(SubIntervals, ToAdd, RanGen, Acc, length(Acc), AccType, 0.0).

-spec non_uniform_key_list_
        (SubIs::[intervals:continuous_interval(),...], ToAdd::non_neg_integer(),
         Fun::random_bias:generator(), Acc::[result_k()], AccLen::non_neg_integer(),
         OutputType::list_key, RoundingError::float()) -> [result_k()];
        (SubIs::[intervals:continuous_interval(),...], ToAdd::non_neg_integer(),
         Fun::random_bias:generator(), Acc::[result_kv()], AccLen::non_neg_integer(),
         OutputType::list_key_val, RoundingError::float()) -> [result_kv()].
non_uniform_key_list_([SubI | R], ToAdd, RanGen, Acc, AccLen, AccType, RoundingError) ->
    ?ASSERT(not intervals:is_empty(SubI)),
    {Status, V, RanGen1} = random_bias:next(RanGen),
    Add0 = V * ToAdd + RoundingError,
    Add1 = erlang:max(0, erlang:trunc(Add0)),
    Add = if Add1 + AccLen > ToAdd -> ToAdd - AccLen;
             true -> Add1
          end,
    %log:pal("non_uniform_key_list: add: ~p, SubI: ~p", [Add, SubI]),
    NAcc0 = if Add >= 1 -> uniform_key_list([{SubI, Add}], [], AccType);
               true     -> []
            end,
    NAcc0Len = length(NAcc0),
    % note: NAcc0 is probably smaller than Acc, so appending this way is faster:
    NAcc = lists:append(NAcc0, Acc),
    NAccLen = AccLen + NAcc0Len,
    case Status of
        _ when NAccLen =:= ToAdd ->
            NAcc;
        ok when R =/= [] ->
            NewRoundingError = float(Add0 - Add + Add - NAcc0Len),
            non_uniform_key_list_(R, ToAdd, RanGen1, NAcc, NAccLen, AccType, NewRoundingError);
        _ when NAccLen < ToAdd ->
            % add the missing items to the last sub interval
            uniform_key_list([{SubI, ToAdd - NAccLen}], NAcc, AccType)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% not ready yet:
%% -spec fill_ring_feeder(random, 1..1000, [db_parameter()])
%%         -> {db_type(), pos_integer(), [db_parameter()]}.
%% fill_ring_feeder(Type, DBSize, Params) -> {Type, DBSize, Params}.

% @doc  DBSize=Number of Data Entities in DB (without replicas)
-spec fill_ring(db_type(), pos_integer(), [db_parameter()]) -> db_status().
fill_ring(Type, DBSize, Params) ->
    case Type of
        random -> fill_random(DBSize, Params);
        wiki -> fill_wiki(Params, "barwiki-latest-pages-meta-current.db")
    end.

% not ready yet:
%% -spec fill_random_feeder(1..1000, [db_parameter()])
%%         -> {pos_integer(), [db_parameter()]}.
%% fill_random_feeder(DBSize, Params) -> {DBSize, Params}.

-spec fill_random(DBSize::pos_integer(), [db_parameter()]) -> db_status().
fill_random(DBSize, Params) ->    
    Distr = proplists:get_value(distribution, Params, uniform),
    I = hd(intervals:split(intervals:all(), ?ReplicationFactor)),
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

-spec insert_db(db_dht:db_as_list()) -> ok.
insert_db(KVV) ->
    Nodes = get_node_list(),
    _ = lists:foldl(
          fun(Node, ActKVV) ->
                  comm:send(Node, {get_state, comm:this(), my_range}),
                  % note: receive wrapped in anonymous functions to allow
                  %       ?SCALARIS_RECV in multiple receive statements
                  NRange = fun() -> receive
                               ?SCALARIS_RECV(
                                   {get_state_response, Range}, %% ->
                                   Range
                                 )
                           end end(),
                  {NKVV, RestKVV} = lists:partition(
                                      fun(Entry) ->
                                              intervals:in(db_entry:get_key(Entry), NRange)
                                      end, ActKVV),
                  comm:send(Node, {add_data, comm:this(), NKVV}),
                  fun() -> receive ?SCALARIS_RECV({add_data_reply}, ok) end end(),
                  RestKVV
          end,
          KVV, Nodes),
    ok.

-spec remove_keys([?RT:key()]) -> ok.
remove_keys(Keys) ->
    _ = [begin
             comm:send(Node, {delete_keys, comm:this(), Keys}),
             receive ?SCALARIS_RECV({delete_keys_reply}, ok) end
         end || Node <- get_node_list()],
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Binomial distributions should have an N so that the number of
%%      random numbers matches the number of e.g. items they should be used for.
%%      Use this to fix the value of auto-generated generators.
-spec feeder_fix_rangen(distribution(), pos_integer()) -> distribution().
feeder_fix_rangen({non_uniform, {{binom, _N, P, X, Approx}, CalcFun, NewStateFun}}, MaxN) ->
    {non_uniform, {{binom, erlang:max(1, MaxN - 1), P, X, Approx}, CalcFun, NewStateFun}};
feeder_fix_rangen(RanGen, _MaxN) ->
    RanGen.

%% @doc Generates a consistent db with errors
%%      Note: the random number generator in {non_uniform, RanGen} will start
%%            from scratch each time this function is called based on the
%%            initial state!
-spec gen_kvv(ErrorDist::distribution(), [?RT:key()], [db_parameter()]) -> {db_dht:db_as_list(), db_status()}.
gen_kvv(EDist, Keys, Params) ->
    FType = proplists:get_value(ftype, Params, update),
    FProb = proplists:get_value(fprob, Params, 50),
    FDest = proplists:get_value(fdest, Params, all),
    KeyCount = length(Keys),
    FCount =  erlang:round(KeyCount * (FProb / 100)),
    p_gen_kvv(EDist, Keys, KeyCount, FType, FDest, FCount).

-spec p_gen_kvv(ErrorDist::distribution(), [?RT:key()],
                KeyCount::non_neg_integer(), failure_type(),
                failure_dest(), FailCount::non_neg_integer()) -> {db_dht:db_as_list(), db_status()}.
p_gen_kvv(random, Keys, KeyCount, FType, FDest, FCount) ->
    {FKeys, GoodKeys} = select_random_keys(Keys, FCount, []),
    GoodDB = lists:foldl(fun(Key, AccDb) -> 
                                 lists:append(get_rep_group(Key), AccDb)
                         end,
                         [], GoodKeys),
    {BadDB, O} = lists:foldl(fun(FKey, {AccBad, Out}) ->
                                     {RList, NewOut} = get_failure_rep_group(FKey, FType, FDest),
                                     {lists:append(RList, AccBad), Out + NewOut}
                             end,
                             {[], 0}, FKeys),
    Insert = length(GoodDB) + length(BadDB),
    DBSize = KeyCount * ?ReplicationFactor,
    {lists:append(GoodDB, BadDB), {DBSize, Insert, DBSize - Insert, O}};
p_gen_kvv({non_uniform, RanGen}, Keys, KeyCount, FType, FDest, FCount) ->
    FProbList = get_non_uniform_probs(RanGen, []),
    % note: don't use RanGen any more - we don't get the new state in the last call!
    NextCell = case length(FProbList) of
                   0 -> KeyCount + 1;
                   FProbL -> erlang:round(KeyCount / FProbL)
               end,
    FCells = lists:reverse(lists:keysort(1, build_failure_cells(FProbList, Keys, NextCell, []))),
    {DB, _, Out} = 
        lists:foldl(fun({_P, Cell}, {DB, RestF, ROut}) ->
                            {NewEntry, NewOut, NewF} = add_failures_to_cell(Cell, RestF, FType, FDest, {[], ROut}),
                            {lists:append(NewEntry, DB), NewF, NewOut}
                    end,
                    {[], FCount, 0}, FCells),
    Insert = length(DB),
    DBSize = KeyCount * ?ReplicationFactor,
    {DB, {DBSize, Insert, DBSize - Insert, Out}};
p_gen_kvv(uniform, Keys, KeyCount, FType, FDest, FCount) ->
    FRate = case FCount of
                0 -> KeyCount + 1;
                _ -> util:floor(KeyCount / FCount)
            end,
    {DB, O, _, _} = 
        lists:foldl(
          fun(Key, {AccDb, Out, Count, FCRest}) ->
                  {{RList, AddOut}, FCNew} = case Count rem FRate of
                                                 0 when FCRest > 0 -> 
                                                     {get_failure_rep_group(Key, FType, FDest), FCRest - 1};
                                                 _ -> {{get_rep_group(Key), 0}, FCRest}
                                             end,
                  {lists:append(RList, AccDb), Out + AddOut, Count + 1, FCNew}
          end,
          {[], 0, 1, FCount}, Keys),
    Insert = length(DB),
    DBSize = KeyCount * ?ReplicationFactor,
    {DB, {DBSize, Insert, DBSize - Insert, O}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec add_failures_to_cell([?RT:key()], non_neg_integer(), failure_type(), failure_dest(),
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

-spec build_failure_cells([float()], [?RT:key()], non_neg_integer(),
                          Acc::[{float(), [?RT:key()]}])
        -> Result::[{float(), [?RT:key()]}].
build_failure_cells([], [], _Next, Acc) ->
    Acc;
build_failure_cells([], T, _Next, [{P, Cell} | Acc]) ->
    [{P, lists:append(T, Cell)} | Acc];
build_failure_cells(_P, [], _Next, Acc) ->
    Acc;
build_failure_cells([P | T], List, Next, Acc) ->
    Cell = lists:sublist(List, Next),
    RLen = length(List),
    LT = if Next > RLen -> [];
            true -> lists:nthtail(Next, List)
         end,
    build_failure_cells(T, LT, Next, [{P, Cell}|Acc]).

-spec get_non_uniform_probs(random_bias:generator(), [float()]) -> [float()].
get_non_uniform_probs(RanGen, Acc) ->
    case random_bias:next(RanGen) of
        {ok, V, RanGen1} -> get_non_uniform_probs(RanGen1, [V | Acc]);
        {last, V, exit}  -> lists:reverse([V | Acc])
    end.

-spec get_synthetic_entry(?RT:key(), old | new) -> db_entry:entry().
get_synthetic_entry(Key, new) ->
    db_entry:new(Key, val, 2);
get_synthetic_entry(Key, old) ->
    db_entry:new(Key, old, 1).

-spec get_rep_group(?RT:key()) -> [db_entry:entry()].
get_rep_group(Key) ->
    [get_synthetic_entry(X, new) || X <- ?RT:get_replica_keys(Key)].

-spec get_failure_rep_group(?RT:key(), failure_type(), failure_dest()) -> 
          {[db_entry:entry()], Outdated::non_neg_integer()}.
get_failure_rep_group(Key, FType, FDest) ->
    RepKeys = ?RT:get_replica_keys(Key),
    EKey = get_error_key(Key, FDest),
    RGrp = [get_synthetic_entry(X, new) || X <- RepKeys, X =/= EKey],
    case get_failure_type(FType) of
        update -> {[get_synthetic_entry(EKey, old) | RGrp], 1};
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
-spec get_error_key(?RT:key(), failure_dest()) -> ?RT:key(). 
get_error_key(Key, Dest) ->
    case Dest of
        all -> util:randomelem(?RT:get_replica_keys(Key));
        QList -> rr_recon:map_key_to_quadrant(Key, util:randomelem(QList))
    end.

-spec select_random_keys([?RT:key()], non_neg_integer(), AccFail::[?RT:key()])
        -> {Fail::[?RT:key()], Rest::[?RT:key()]}.
select_random_keys(RestKeys, 0, Acc) ->  {Acc, RestKeys};
select_random_keys([] = Keys, _Count, Acc) ->  {Acc, Keys};
select_random_keys([_|_] = Keys, Count, Acc) ->
    FRep = util:randomelem(Keys),
    select_random_keys([X || X <- Keys, X =/= FRep], Count - 1, [FRep | Acc]).

-spec get_node_list() -> [comm:mypid()].
get_node_list() ->
    mgmt_server:node_list(),
    receive
        ?SCALARIS_RECV(
            {get_list_response, List}, %% ->
            List
          )
    end.
