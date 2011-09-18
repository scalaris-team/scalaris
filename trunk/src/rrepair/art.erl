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
%%         SRC: 2002 - J.Byers, J.Considine, M.Mitzenmachen
%%              Fast Approximate Reconcilication of Set Differences
%%              - BU Computer Science TR
%% @end
%% @version $Id$

-module(art).

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([new/0, new/1, get_merkle_tree/1]).

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
                 art_config(), 
                 intervals:interval(),
                 InnerNodes :: ?REP_BLOOM:bloom_filter() | empty,
                 Leafs :: ?REP_BLOOM:bloom_filter() | empty }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec new() -> art().
new() -> {art, #art_config{}, intervals:empty(), empty, empty}.

-spec new(merkle_tree:merkle_tree()) -> art().
new(Tree) ->
    build(Tree).

-spec build(merkle_tree:merkle_tree()) -> art().
build(Tree) ->
    {InnerCount, LeafCount} = merkle_tree:size_detail(Tree),
    InnerBF = ?REP_BLOOM:new(InnerCount, 0.01),
    LeafBF = ?REP_BLOOM:new(LeafCount, 0.1),
    {IBF, LBF} = fill_bloom(merkle_tree:iterator(Tree), InnerBF, LeafBF),
    {art, #art_config{}, merkle_tree:get_interval(Tree), IBF, LBF}.

-spec get_merkle_tree(intervals:interval()) -> merkle_tree:merkle_tree().
get_merkle_tree(I) ->
    merkle_tree:new(I, [{branch_factor, 32}, 
                        {bucket_size, 64}, 
                        {gen_hash_on, value}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec fill_bloom(Iterator, InnerBloom1, LeafBloom1) -> {InnerBloom2, LeafBloom2} when
      Iterator    :: merkle_tree:mt_iter(),
      InnerBloom1 :: ?REP_BLOOM:bloom_filter(),
      LeafBloom1  :: ?REP_BLOOM:bloom_filter(),
      InnerBloom2 :: ?REP_BLOOM:bloom_filter(),
      LeafBloom2  :: ?REP_BLOOM:bloom_filter().

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
