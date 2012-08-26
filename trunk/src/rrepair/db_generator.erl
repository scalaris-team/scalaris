%  @copyright 2010-2011 Zuse Institute Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    merkle_tree_builder.erl
%%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%%% @doc    Merkle Tree construction.
%%% @end
%%% Created : 15/11/2011 by Maik Lange <MLange@informatik.hu-berlin.de>
%%%-------------------------------------------------------------------
%% @version $Id: db_generator.erl 2843 2012-03-09 09:56:42Z schintke $

-module(db_generator).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("scalaris.hrl").

-export([get_db/3, get_db/4]).
-export([fill_ring/3]).

-define(ReplicationFactor, 4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TYPES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(with_export_type_support).
-export_type([distribution/0, db_type/0, db_parameter/0, db_status/0, failure_type/0]).
-endif.

-type distribution() :: random |
                        uniform |
                        {non_uniform, random_bias:distribution_fun()}.
-type result() :: ?RT:key() | 
                  {?RT:key(), ?DB:value()}.
-type option() :: {output, list_key_val | list_key}.

-type failure_type()    :: update | regen | mixed.
-type failure_quadrant():: 1..4.
-type failure_dest()    :: all | [failure_quadrant()]. 
-type db_type()         :: wiki | random.
-type db_parameter()    :: {ftype, failure_type()} |
                           {fprob, 0..100} |                                %failure probability
                           {fdest, failure_dest()} |                        %quadrants in which failures are inserted / if missing all is assumed
                           {fdistribution, db_generator:distribution()} |   %distribution of failures
                           {distribution, db_generator:distribution()}.     %data distribution in every quadrant
-type db_status() :: {Entries   :: non_neg_integer(),
                      Existing  :: non_neg_integer(),
                      Missing   :: non_neg_integer(),
                      Outdated  :: non_neg_integer()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc This will generate a list of [ItemCount] keys with requested distribution 
%      in the given interval.
-spec get_db(intervals:interval(), non_neg_integer(), distribution()) -> [result()].
get_db(I, Count, Distribution) ->
    get_db(I, Count, Distribution, []).

-spec get_db(intervals:interval(), non_neg_integer(), distribution(), [option()]) -> [result()].
get_db(Interval, ItemCount, Distribution, Options) ->
    OutputType = proplists:get_value(output, Options, list_key),
    case Distribution of
        random -> log:log(error, "random_data_distribution is not implemented");
        uniform -> uniform_key_list([{Interval, ItemCount}], [], OutputType);
        {non_uniform, Fun} -> non_uniform_key_list(Interval, 1, ItemCount, Fun, [], OutputType)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec uniform_key_list([{Interval, ToAdd}], Acc::Result, OutputType) -> Result when
    is_subtype(Interval,   intervals:interval()),
    is_subtype(ToAdd,      non_neg_integer()),
    is_subtype(OutputType, list_key_val | list_key),
    is_subtype(Result,     [result()]).
uniform_key_list([], Acc, _) -> Acc;
uniform_key_list([{I, Add} | R], Acc, AccType) ->    
    case Add > 100 of
        true -> 
            [I1, I2] = intervals:split(I, 2),
            uniform_key_list([{I1, Add div 2}, {I2, (Add div 2) + (Add rem 2)} | R], Acc, AccType);
        false ->
            {LBr, IL, IR, RBr} = intervals:get_bounds(I),
            End = Add + ?IIF(RBr =:= ')', 1, 0),
            ToAdd = util:for_to_ex(?IIF(LBr =:= '(', 1, 0), 
                                   Add + ?IIF(LBr =:= '(', 0, -1),
                                   fun(Index) -> 
                                           Key = ?RT:get_split_key(IL, IR, {Index, End}),
                                           case AccType of
                                               list_key -> Key;
                                               list_key_val -> {Key, gen_value()}
                                           end
                                   end),
            uniform_key_list(R, lists:append(ToAdd, Acc), AccType)
    end.

gen_value() ->
    tester_value_creator:create_value(integer, 0, tester_parse_state:new_parse_state()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec non_uniform_key_list(Interval, Step::Int, ToAdd::Int, Fun, Acc::Result, OutputType) -> Result when
    is_subtype(Interval,    intervals:interval()),
    is_subtype(Int,         pos_integer()),
    is_subtype(Fun,         random_bias:distribution_fun()),
    is_subtype(OutputType,  list_key_val | list_key),
    is_subtype(Result,      [result()]).
non_uniform_key_list(I, Step, ToAdd, Fun, Acc, AccType) ->
    {Status, V} = Fun(),
    {_, LKey, RKey, _} = intervals:get_bounds(I),
    Add = erlang:round(V * ToAdd),
    NAcc = if Add >= 1 ->
                  StepSize = erlang:round((RKey - LKey) / ToAdd),                  
                  SubI = intervals:new('(', 
                                       LKey + ((Step - 1) * StepSize),
                                       LKey + (Step * StepSize),
                                       ']'),
                  uniform_key_list([{SubI, Add}], Acc, AccType);
              true -> Acc
           end,
    case Status of
        ok -> non_uniform_key_list(I, Step + 1, ToAdd, Fun, NAcc, AccType);
        last -> NAcc
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc  DBSize=Number of Data Entities in DB (without replicas)
-spec fill_ring(db_type(), pos_integer(), [db_parameter()]) -> db_status().
fill_ring(Type, DBSize, Params) ->
    case Type of
        random -> fill_random(DBSize, Params);
        wiki -> fill_wiki(DBSize, Params)
    end.

fill_random(DBSize, Params) ->    
    Distr = proplists:get_value(distribution, Params, uniform),
    I = hd(intervals:split(intervals:all(), ?ReplicationFactor)),
    {_LBr, LKey, RKey, _RBr} = intervals:get_bounds(I),
    I2 = intervals:new('(', LKey, RKey, ')'),
    Keys = get_db(I2, DBSize, Distr),
    {DB, DBStatus} = gen_kvv(proplists:get_value(fdistribution, Params, uniform), Keys, Params),
    insert_db(DB),
    DBStatus.

fill_wiki(_DBSize, _Params) ->
    %TODO
    {0, 0, 0, 0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec insert_db(?DB:db_as_list()) -> ok.
insert_db(KVV) ->
    Nodes = get_node_list(),
    _ = lists:foldl(fun(Node, ActKVV) ->
                            comm:send(Node, {get_state, comm:this(), my_range}),
                            NRange = receive
                                         {get_state_response, Range} -> Range
                                     end,
                            {NKVV, RestKVV} = lists:partition(fun(Entry) ->
                                                                      intervals:in(db_entry:get_key(Entry), NRange)
                                                              end, ActKVV),
                            comm:send(Node, {add_data, comm:this(), NKVV}),
                            receive {add_data_reply} -> ok end,
                            RestKVV
                    end,
                    KVV, Nodes),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Generates a consistent db with errors
-spec gen_kvv(ErrorDist::distribution(), [?RT:key()], [db_parameter()]) -> {?DB:db_as_list(), db_status()}.
gen_kvv(random, Keys, Params) ->
    FType = proplists:get_value(ftype, Params, update),
    FProb = proplists:get_value(fprob, Params, 50),
    FDest = proplists:get_value(fdest, Params, all),    
    KeyCount = length(Keys),
    FCount =  erlang:round(KeyCount * (FProb / 100)),
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
gen_kvv({non_uniform, Fun}, Keys, Params) ->
    FType = proplists:get_value(ftype, Params, update),
    FDest = proplists:get_value(fdest, Params, all),
    FProb = proplists:get_value(fprob, Params, 50),
    FProbList = get_non_uniform_probs(Fun, []),
    KeysL = length(Keys),
    FCount = erlang:round(KeysL * (FProb / 100)),
    NextCell = case length(FProbList) of
                   0 -> KeysL + 1;
                   FProbL -> erlang:round(KeysL / FProbL)
               end,
    FCells = lists:reverse(lists:keysort(1, build_failure_cells(FProbList, Keys, NextCell, []))),
    {DB, _, Out} = 
        lists:foldl(fun({_P, Cell}, {DB, RestF, ROut}) ->
                            {NewEntry, NewOut, NewF} = add_failures_to_cell(Cell, RestF, FType, FDest, {[], ROut}),
                            {lists:append(NewEntry, DB), NewF, NewOut}
                    end, 
                    {[], FCount, 0}, FCells),
    Insert = length(DB),
    DBSize = KeysL * ?ReplicationFactor,
    {DB, {DBSize, Insert, DBSize - Insert, Out}};
gen_kvv(uniform, Keys, Params) ->
    FType = proplists:get_value(ftype, Params, update),
    FProb = proplists:get_value(fprob, Params, 50),
    FDest = proplists:get_value(fdest, Params, all),
    KeyL = length(Keys),
    FRate = case FProb of 
                0 -> KeyL + 1;
                _ -> erlang:round(100 / FProb)
            end,
    {DB, O, _} = 
        lists:foldl(
          fun(Key, {AccDb, Out, Count}) ->
                  {RList, AddOut} = case Count rem FRate of
                                        0 -> get_failure_rep_group(Key, FType, FDest);
                                        _ -> {get_rep_group(Key), 0}
                                    end,
                  {lists:append(RList, AccDb), Out + AddOut, Count + 1}
          end, 
          {[], 0, 1}, Keys),
    Insert = length(DB),
    DBSize = KeyL * ?ReplicationFactor,    
    {DB, {DBSize, Insert, DBSize - Insert, O}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec add_failures_to_cell([?RT:key()], non_neg_integer(), failure_type(), failure_dest(), Acc) -> Result
    when is_subtype(Acc,    {[db_entry:entry()], Outdated::non_neg_integer()}),
         is_subtype(Result, {[db_entry:entry()], Outdated::non_neg_integer(), RestFCount::non_neg_integer()}).
add_failures_to_cell([], FCount, _FType, _FDest, {AccE, AccO}) ->
    {AccE, AccO, FCount};
add_failures_to_cell([H | T], 0, FType, FDest, {DB, Out}) ->
    add_failures_to_cell(T, 0, FType, FDest, 
                         {lists:append(get_rep_group(H), DB), Out});
add_failures_to_cell([H | T], FCount, FType, FDest, {AccEntry, AccOut}) ->
    {AddEntrys, AddOut} = get_failure_rep_group(H, FType, FDest),
    add_failures_to_cell(T, FCount - 1, FType, FDest, 
                         {lists:append(AddEntrys, AccEntry), AddOut + AccOut}).

-spec build_failure_cells([float()], [?RT:key()], non_neg_integer(), Acc::Res) -> Result::Res
    when is_subtype(Res, [{float(), [?RT:key()]}]).
build_failure_cells([], [], Next, Acc) ->
    Acc;
build_failure_cells([], T, Next, [{P, Cell} | Acc]) ->
    [{P, lists:append(T, Cell)} | Acc];
build_failure_cells(P, [], Next, Acc) ->
    Acc;
build_failure_cells([P | T], List, Next, Acc) ->
    Cell = lists:sublist(List, Next),
    RLen = length(List),
    LT = if Next > RLen -> [];
            true -> lists:nthtail(Next, List)
         end,
    build_failure_cells(T, LT, Next, [{P, Cell}|Acc]).

-spec get_non_uniform_probs(random_bias:distribution_fun(), [float()]) -> [float()].
get_non_uniform_probs(Fun, Acc) ->
    case Fun() of
        {ok, V} -> get_non_uniform_probs(Fun, [V|Acc]);
        {last, V} -> lists:reverse([V|Acc])
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
    EType = get_failure_type(FType),
    RGrp = [get_synthetic_entry(X, new) || X <- RepKeys, X =/= EKey],
    if EType =:= update andalso EKey =/= null ->
           {[get_synthetic_entry(EKey, old) | RGrp], 1};
       true -> {RGrp, 0}
    end.

% @doc Resolves failure type mixed.
-spec get_failure_type(failure_type()) -> failure_type().
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

-spec select_random_keys(L, non_neg_integer(), AccFail::L) -> {Fail::L, Rest::L}
    when is_subtype(L, [?RT:key()]).
select_random_keys(RestKeys, 0, Acc) -> 
    {Acc, RestKeys};
select_random_keys(Keys, Count, Acc) ->
    FRep = util:randomelem(Keys),
    select_random_keys([X || X <- Keys, X =/= FRep], Count - 1, [FRep | Acc]).

-spec get_node_list() -> [comm:mypid()].
get_node_list() ->
    mgmt_server:node_list(),
    receive
        {get_list_response, List} -> List
    end.
