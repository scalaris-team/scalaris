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

-type failure_type() :: update | regen | mixed.
-type db_type()      :: wiki | random.
-type db_parameter() :: {ftype, failure_type()} |
                        {fprob, 0..100} |            %failure probability
                        {distribution, db_generator:distribution()}.
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
        {non_uniform, Fun} -> non_uniform_key_list(Interval, Fun, [], OutputType)
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
            {_, IL, IR, _} = intervals:get_bounds(I),
            ToAdd = util:for_to_ex(1, Add, 
                                   fun(Index) -> 
                                           Key = ?RT:get_split_key(IL, IR, {Index, Add}),
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

-spec non_uniform_key_list(Interval, Fun, Acc::Result, OutputType) -> Result when
    is_subtype(Interval,    intervals:interval()),
    is_subtype(Fun,         random_bias:distribution_fun()),
    is_subtype(OutputType,  list_key_val | list_key),
    is_subtype(Result,      [result()]).
non_uniform_key_list(I, Fun, Acc, AccType) ->
    case Fun() of
        {ok, V} -> non_uniform_key_list(I, Fun, non_uniform_add(V, I, Acc, AccType), AccType);
        {last, V} -> non_uniform_add(V, I, Acc, AccType)
    end.

non_uniform_add(Value, I, Acc, OutType) ->
    {_, LKey, RKey, _} = intervals:get_bounds(I),    
    X = LKey + erlang:round(Value * (RKey - LKey)),
    [case OutType of
         list_key -> X;
         list_key_val -> {X, gen_value()}
     end | Acc].

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
    Keys = db_generator:get_db(I, DBSize, Distr),
    insert_random_db(Keys, Params).
fill_wiki(_DBSize, _Params) ->
    %TODO
    {0, 0, 0, 0}.

% @doc Inserts a list of keys replicated into the ring
-spec insert_random_db([?RT:key()], [db_parameter()]) -> db_status().
insert_random_db(Keys, Params) ->
    FType = proplists:get_value(ftype, Params, update),
    FProb = proplists:get_value(fprob, Params, 50),    
    {I, M, O} = 
        lists:foldl(
          fun(Key, {Ins, Mis, Out}) ->
                  RepKeys = ?RT:get_replica_keys(Key),
                  %insert error?
                  EKey = case FProb >= randoms:rand_uniform(1, 100) of
                             true -> util:randomelem(RepKeys);
                             _ -> null
                         end,
                  %insert regen error
                  {EType, WKeys} = case EKey =/= null of
                                       true ->
                                           EEType = case FType of 
                                                        mixed -> 
                                                            case randoms:rand_uniform(1, 3) of
                                                                1 -> update;
                                                                _ -> regen
                                                            end;
                                                        _ -> FType
                                                    end,
                                           {EEType, 
                                            case EEType of
                                                regen -> [X || X <- RepKeys, X =/= EKey];
                                                _ -> RepKeys
                                            end};
                                       _ -> {FType, RepKeys}
                                   end,
                  %write replica group
                  lists:foreach(
                    fun(RKey) ->
                            DBEntry = db_entry:new(RKey, val, 2),
                            api_dht_raw:unreliable_lookup(RKey, 
                                                          {set_key_entry, comm:this(), DBEntry}),
                            receive {set_key_entry_reply, _} -> ok end
                    end,
                    WKeys),
                  %insert update error
                  NewOut = if EType =:= update andalso EKey =/= null ->
                                  Msg = {set_key_entry, comm:this(), db_entry:new(EKey, old, 1)},
                                  api_dht_raw:unreliable_lookup(EKey, Msg),
                                  receive {set_key_entry_reply, _} -> ok end,
                                  Out + 1;
                              true -> Out
                           end,
                  {Ins + length(WKeys), Mis + 4 - length(WKeys), NewOut}
          end, 
          {0, 0, 0}, Keys),
    {length(Keys) * ?ReplicationFactor, I, M, O}.
