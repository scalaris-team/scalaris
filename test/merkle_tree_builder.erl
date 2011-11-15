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

-module(merkle_tree_builder).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("scalaris.hrl").

-export([build/3]).

-type distribution() :: uniform.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build(Tree, AddCount, Distribution) -> MerkleTree when
    is_subtype(Tree,         merkle_tree:merkle_tree()),
    is_subtype(AddCount,     pos_integer()),
    is_subtype(Distribution, distribution()),
    is_subtype(MerkleTree,   merkle_tree:merkle_tree()).
build(Tree, AddCount, uniform) ->
    build_tree_uniform(Tree, [{merkle_tree:get_interval(Tree), AddCount}]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

build_tree_uniform(Tree, []) ->
    merkle_tree:gen_hash(Tree);
build_tree_uniform(Tree, [{I, Add} | R]) ->    
    case Add > 100 of
        true -> 
            [I1, I2] = intervals:split(I, 2),
            build_tree_uniform(Tree, [{I1, Add div 2}, {I2, (Add div 2) + (Add rem 2)} | R]);
        false -> 
            {_, IL, IR, _} = intervals:get_bounds(I),
            ToAdd = util:for_to_ex(1, Add, 
                                   fun(Index) -> 
                                           ?RT:get_split_key(IL, IR, {Index, Add}) 
                                   end),
            NTree = lists:foldl(fun(Key, AccTree) -> 
                                        merkle_tree:insert(Key, Key, AccTree) 
                                end,
                                Tree, ToAdd),
            build_tree_uniform(NTree, R)                                  
    end.
