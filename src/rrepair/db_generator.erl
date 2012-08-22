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

-type distribution() :: uniform |
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
                           {distribution, db_generator:distribution()}.     %in every quadrant
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
    Keys = get_db(I, DBSize, Distr),
    {DB, DBStatus} = gen_kvv(Keys, Params),
    insert_db(DB),
    DBStatus.

fill_wiki(_DBSize, _Params) ->
    %TODO
    {0, 0, 0, 0}.

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

% @doc Inserts a list of keys replicated into the ring
-spec gen_kvv([?RT:key()], [db_parameter()]) -> {?DB:db_as_list(), db_status()}.
gen_kvv(Keys, Params) ->
    FType = proplists:get_value(ftype, Params, update),
    FProb = proplists:get_value(fprob, Params, 50),
    FDest = proplists:get_value(fdest, Params, all),
    {DB, {I, M, O}} = 
        lists:foldl(
          fun(Key, {AccDb, {Ins, Mis, Out}}) ->
                  RepKeys = ?RT:get_replica_keys(Key),
                  %insert error?
                  EKey = case FProb >= randoms:rand_uniform(1, 100) of
                             true ->
                                 %error destination
                                 case FDest of
                                     all -> util:randomelem(RepKeys);
                                     QList -> rr_recon:map_key_to_quadrant(Key, util:randomelem(QList))
                                 end;
                             _ -> null
                         end,
                  %insert regen error
                  EType = case FType of 
                              mixed -> 
                                  case randoms:rand_uniform(1, 3) of
                                      1 -> update;
                                      _ -> regen
                                  end;
                              _ -> FType
                          end,
                  RGrp = [db_entry:new(X, val, 2) || X <- RepKeys, X =/= EKey],
                  %insert update error
                  {NewOut, RList} = if EType =:= update andalso EKey =/= null ->
                                           {Out + 1, [db_entry:new(EKey, old, 1) | RGrp]};
                                       true -> {Out, RGrp}
                                    end,
                  {lists:append(RList, AccDb), {Ins + length(RGrp), Mis + 4 - length(RGrp), NewOut}}
          end, 
          {[], {0, 0, 0}}, Keys),
    {DB, {length(Keys) * ?ReplicationFactor, I, M, O}}.

-spec get_node_list() -> [comm:mypid()].
get_node_list() ->
    mgmt_server:node_list(),
    receive
        {get_list_response, List} -> List
    end.
