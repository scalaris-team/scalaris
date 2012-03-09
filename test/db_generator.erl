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
%% @version $Id$

-module(db_generator).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("scalaris.hrl").

-export([get_db/3, get_db/4]).

-type distribution() :: uniform.% |
                        %{binomial, P::float()}.
-type result() :: ?RT:key() | 
                  {?RT:key(), ?DB:value()}.
-type option() :: {output, list_key_val | list_key}.          

-ifdef(with_export_type_support).
-export_type([distribution/0]).
-endif.

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
        _ -> []
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
