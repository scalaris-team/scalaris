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
%% @version $Id: $

-module(db_generator).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("scalaris.hrl").

-export([get_db/3]).

-type distribution() :: uniform.

-ifdef(with_export_type_support).
-export_type([distribution/0]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_db(intervals:interval(), non_neg_integer(), distribution()) -> [?RT:key()].
get_db(Interval, ItemCount, Distribution) ->
    case Distribution of
        uniform -> uniform_key_list([{Interval, ItemCount}], []);
        _ -> []
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec uniform_key_list([{Interval, ToAdd}], Acc::[Key]) -> [Key] when
    is_subtype(Interval, intervals:interval()),
    is_subtype(ToAdd,    non_neg_integer()),
    is_subtype(Key,      ?RT:key()).
uniform_key_list([], KeyList) -> KeyList;
uniform_key_list([{I, Add} | R], KeyList) ->    
    case Add > 100 of
        true -> 
            [I1, I2] = intervals:split(I, 2),
            uniform_key_list([{I1, Add div 2}, {I2, (Add div 2) + (Add rem 2)} | R], KeyList);
        false -> 
            {_, IL, IR, _} = intervals:get_bounds(I),
            ToAdd = util:for_to_ex(1, Add, 
                                   fun(Index) -> 
                                           ?RT:get_split_key(IL, IR, {Index, Add}) 
                                   end),
            uniform_key_list(R, lists:append(ToAdd, KeyList))                                  
    end.
