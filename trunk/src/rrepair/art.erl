% @copyright 2011 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    Approximate Reconciliation Tree (ART) implementation
%%         Represents a packed merkle tree.
%%         SRC: 2002 - J.Byers, J.Considine, M.Mitzenmachen
%%              Fast Approximate Reconcilication of Set Differences
%%              - BU Computer Science TR
%% @end
%% @version $Id$

-module(art).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([new/0, new/1, 
         %get_merkle_tree/1,
         get_interval/1,
         lookup/2]).

-ifdef(with_export_type_support).
-export_type([art/0, art_config/0]).
-endif.

%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(art_config,
        {
         correction_factor = 1 :: non_neg_integer()
         }).
-type art_config() :: #art_config{}.

-type art() :: { art,
                 Config     :: art_config(), 
                 Interval   :: intervals:interval(),
                 InnerNodes :: ?REP_BLOOM:bloom_filter() | empty,
                 Leafs      :: ?REP_BLOOM:bloom_filter() | empty }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec new() -> art().
new() -> {art, #art_config{}, intervals:empty(), empty, empty}.

-spec new(merkle_tree:merkle_tree()) -> art().
new(Tree) ->
    {InnerCount, LeafCount} = merkle_tree:size_detail(Tree),
    InnerBF = ?REP_BLOOM:new(InnerCount, 0.01),
    LeafBF = ?REP_BLOOM:new(LeafCount, 0.1),
    {IBF, LBF} = fill_bloom(merkle_tree:iterator(Tree), InnerBF, LeafBF),
    {art, #art_config{}, merkle_tree:get_interval(Tree), IBF, LBF}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc returns interval of the packed merkle tree (art-structure)
-spec get_interval(art()) -> intervals:interval().
get_interval({art, _, I, _, _}) -> I.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-spec get_merkle_tree(intervals:interval()) -> merkle_tree:merkle_tree().
%get_merkle_tree(I) ->
%    merkle_tree:new(I, [{branch_factor, 32}, 
%                        {bucket_size, 64}, 
%                        {gen_hash_on, value}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec lookup(art(), merkle_tree:mt_node()) -> boolean().
lookup(Art, Node) ->
    lookup_cf(Art, Node, get_correction_factor(Art)).

-spec lookup_cf(Art, Node, CF) -> Result when
    is_subtype(Art,    art()),
    is_subtype(Node,   merkle_tree:mt_node()),
    is_subtype(CF,     non_neg_integer()),        %correction factor
    is_subtype(Result, boolean()).

lookup_cf({art, I, IBF, LBF}, Node, 0) ->
    NodeOk = intervals:in(merkle_tree:get_interval(Node), I),
    NodeHash = merkle_tree:get_hash(Node),
    case merkle_tree:is_leaf(Node) of
        true when NodeOk -> ?REP_BLOOM:is_element(LBF, NodeHash);
        false when NodeOk -> ?REP_BLOOM:is_element(IBF, NodeHash);
        _ -> false
    end;
lookup_cf({art, I, IBF, LBF} = Art, Node, CF) ->
    NodeOk = intervals:in(merkle_tree:get_interval(Node), I),
    NodeHash = merkle_tree:get_hash(Node),
    case merkle_tree:is_leaf(Node) of
        true when NodeOk -> ?REP_BLOOM:is_element(LBF, NodeHash);                                
        false when NodeOk -> ?REP_BLOOM:is_element(IBF, NodeHash);
        _ -> false
    end
        andalso
        lists:foldl(fun(X, Res) -> Res andalso lookup_cf(Art, X, CF - 1) end, 
                    true, 
                    merkle_tree:get_childs(Node)).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    
get_correction_factor({art, Config, _, _, _}) ->
    Config#art_config.correction_factor.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec fill_bloom(Iterator, InnerBloom1, LeafBloom1) -> {InnerBloom2, LeafBloom2} when
      is_subtype(Iterator,    merkle_tree:mt_iter()),
      is_subtype(InnerBloom1, ?REP_BLOOM:bloom_filter()),
      is_subtype(LeafBloom1,  ?REP_BLOOM:bloom_filter()),
      is_subtype(InnerBloom2, ?REP_BLOOM:bloom_filter()),
      is_subtype(LeafBloom2,  ?REP_BLOOM:bloom_filter()).
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
