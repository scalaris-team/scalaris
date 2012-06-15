% @copyright 2011,2012 Zuse Institute Berlin
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
%
%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    Approximate Reconciliation Tree (ART) implementation
%%         Represents a packed merkle tree.
%% @end
%% @reference
%%         2002 - J.Byers, J.Considine, M.Mitzenmachen
%%              Fast Approximate Reconcilication of Set Differences
%%              - BU Computer Science TR
%% @end
%% @version $Id$

-module(art).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([new/0, new/1, new/2,
         get_interval/1, get_correction_factor/1, get_config/1,
         lookup/2]).

-ifdef(with_export_type_support).
-export_type([art/0, art_config/0]).
-endif.

%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type art_config_param() :: {correction_factor, non_neg_integer()} |
                            {inner_bf_fpr,      float()} |
                            {leaf_bf_fpr,       float()}.
-type art_config()       :: [art_config_param()].

-type art() :: { art,
                 Config     :: art_config(), 
                 Interval   :: intervals:interval(),
                 InnerNodes :: ?REP_BLOOM:bloom_filter() | empty,
                 Leafs      :: ?REP_BLOOM:bloom_filter() | empty }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns default art configuration.
-spec default_art_config() -> art_config().
default_art_config() ->
    [{correction_factor, 1},
     {inner_bf_fpr, 0.01},
     {leaf_bf_fpr, 0.1}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec new() -> art().
new() -> 
    {art, default_art_config(), intervals:empty(), empty, empty}.

-spec new(merkle_tree:merkle_tree()) -> art().
new(Tree) ->
    new(Tree, default_art_config()).

-spec new(merkle_tree:merkle_tree(), art_config()) -> art().
new(Tree, _Config) ->
    Config = merge_prop_lists(default_art_config(), _Config),
    {InnerCount, LeafCount} = merkle_tree:size_detail(Tree),
    InnerBF = ?REP_BLOOM:new(InnerCount, proplists:get_value(inner_bf_fpr, Config)),
    LeafBF = ?REP_BLOOM:new(LeafCount, proplists:get_value(leaf_bf_fpr, Config)),
    {IBF, LBF} = fill_bloom(merkle_tree:iterator(Tree), InnerBF, LeafBF),
    ?TRACE("INNER=~p~nLeaf=~p", [?REP_BLOOM:print(IBF), ?REP_BLOOM:print(LBF)]),    
    {art, Config, merkle_tree:get_interval(Tree), IBF, LBF}.    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc returns interval of the packed merkle tree (art-structure)
-spec get_interval(art()) -> intervals:interval().
get_interval({art, _, I, _, _}) -> I.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    
-spec get_correction_factor(art()) -> non_neg_integer().
get_correction_factor({art, Config, _, _, _}) ->
    proplists:get_value(correction_factor, Config).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_config(art()) -> art_config().
get_config({art, Config, _, _, _}) ->
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec lookup(merkle_tree:mt_node(), art()) -> boolean().
lookup(Node, Art) ->
    lookup_cf([{Node, get_correction_factor(Art)}], Art).

-spec lookup_cf([{Node, CF}], Art) -> Result when
    is_subtype(Art,    art()),
    is_subtype(Node,   merkle_tree:mt_node()),
    is_subtype(CF,     non_neg_integer()),        %correction factor
    is_subtype(Result, boolean()).
lookup_cf([], _Art) ->
    true;
lookup_cf([{Node, 0} | L], {art, _Conf, _I, IBF, LBF} = Art) ->
    NodeHash = merkle_tree:get_hash(Node),
    BF = ?IIF(merkle_tree:is_leaf(Node), LBF, IBF),
    ?TRACE("NodeHash=~p", [NodeHash]),
    ?REP_BLOOM:is_element(BF, NodeHash) andalso
        lookup_cf(L, Art);
lookup_cf([{Node, CF} | L], {art, _Conf, _I, IBF, LBF} = Art) ->
    NodeHash = merkle_tree:get_hash(Node),
    IsLeaf = merkle_tree:is_leaf(Node),
    BF = ?IIF(IsLeaf, LBF, IBF),
    ?TRACE("NodeHash=~p~nIsLeaf=~p", [NodeHash, IsLeaf]),
    case ?REP_BLOOM:is_element(BF, NodeHash) of
        false -> false;
        true -> case IsLeaf of
                    true -> lookup_cf(L, Art);
                    false ->
                        NL = lists:append(lists:map(fun(X) -> {X, CF - 1} end, 
                                                    merkle_tree:get_childs(Node)), 
                                          L),
                        lookup_cf(NL, Art)
                end                        
    end.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec fill_bloom(Iterator, Inner1::BF, Leaf1::BF) -> {Inner2::BF, Leaf2::BF} when
      is_subtype(Iterator,  merkle_tree:mt_iter()),
      is_subtype(BF,        ?REP_BLOOM:bloom_filter()).
fill_bloom(Iter, IBF, LBF) ->
    Next = merkle_tree:next(Iter),
    case Next of
        none -> {IBF, LBF};
        {Node, Iter2} ->
            {IBF2, LBF2} = 
                case merkle_tree:is_leaf(Node) of
                    true -> {IBF, ?REP_BLOOM:add(LBF, merkle_tree:get_hash(Node))};
                    false -> {?REP_BLOOM:add(IBF, merkle_tree:get_hash(Node)), LBF} 
                end,
            fill_bloom(Iter2, IBF2, LBF2)
    end.

merge_prop_lists(DefList, ListB) ->
    lists:foldl(fun({Key, Val}, Acc) ->                         
                        [{Key, proplists:get_value(Key, ListB, Val)} | Acc] 
                end, [], DefList).
    
