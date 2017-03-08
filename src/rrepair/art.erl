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
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-include("record_helpers.hrl").
-include("scalaris.hrl").

-export([new/1, new/2, default_config/0,
         get_interval/1, get_config/1,
         get_property/2,
         lookup/2]).
-export([merkle_leaf_hf/2]).

-export_type([art/0, config/0]).

%-define(TRACE(X,Y), io:format("~w: [~p] " ++ X ++ "~n", [?MODULE, self()] ++ Y)).
-define(TRACE(X,Y), ok).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type config_param() :: {correction_factor, non_neg_integer()} |
                        {inner_bf_fpr,      float()} |
                        {leaf_bf_fpr,       float()}.
-type config()       :: [config_param()].

-type art() :: { art,
                 CorrectionFactor :: non_neg_integer(),
                 InnerBfFpr :: float(),
                 LeafBfFpr  :: float(),
                 Interval   :: intervals:interval(),
                 InnerNodes :: bloom:bloom_filter(),
                 Leafs      :: bloom:bloom_filter()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Returns default art configuration.
-spec default_config() -> config().
default_config() ->
    [{correction_factor, 1},
     {inner_bf_fpr, 0.01},
     {leaf_bf_fpr, 0.1}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new(merkle_tree:merkle_tree()) -> art().
new(Tree) ->
    new(Tree, default_config()).

-spec new(merkle_tree:merkle_tree(), config()) -> art().
new(Tree, Config0) ->
    Config = merge_prop_lists(default_config(), Config0),
    CorrectionFactor = proplists:get_value(correction_factor, Config),
    InnerBfFpr = proplists:get_value(inner_bf_fpr, Config),
    LeafBfFpr = proplists:get_value(leaf_bf_fpr, Config),
    {InnerCount, LeafCount, _EmptyLeafCount, _ItemCount} = merkle_tree:size_detail(Tree),
    InnerBF = bloom:new_fpr(InnerCount, InnerBfFpr),
    LeafBF = bloom:new_fpr(LeafCount, LeafBfFpr),
    {IBF, LBF} = fill_bloom(merkle_tree:iterator(Tree), InnerBF, LeafBF),
    ?TRACE("INNER=~p~nLeaf=~p", [bloom:print(IBF), bloom:print(LBF)]),
    {art, CorrectionFactor, InnerBfFpr, LeafBfFpr,
     merkle_tree:get_interval(Tree), IBF, LBF}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc returns interval of the packed merkle tree (art-structure)
-spec get_interval(art()) -> intervals:interval().
get_interval({art, _CorFac, _IBfFpr, _LBfFpr, Interval, _IBF, _LBF}) -> Interval.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_property(art(), correction_factor) -> non_neg_integer();
                  (art(), items_count) -> non_neg_integer();
                  (art(), inner_bf | leaf_bf) -> bloom:bloom_filter().
get_property({art, CorFac, _IBfFpr, _LBfFpr, _Interval, _IBF, _LBF}, correction_factor) ->
    CorFac;
get_property({art, _CorFac, _IBfFpr, _LBfFpr, _Interval, _IBF, LBF}, items_count) ->
    bloom:get_property(LBF, items_count);
get_property({art, _CorFac, _IBfFpr, _LBfFpr, _Interval, IBF, _LBF}, inner_bf) ->
    IBF;
get_property({art, _CorFac, _IBfFpr, _LBfFpr, _Interval, _IBF, LBF}, leaf_bf) ->
    LBF.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_config(art()) -> config().
get_config({art, CorFac, IBfFpr, LBfFpr, _Interval, _IBF, _LBF}) ->
    [{correction_factor, CorFac},
     {inner_bf_fpr,      IBfFpr},
     {leaf_bf_fpr,       LBfFpr}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec lookup(merkle_tree:mt_node(), art()) -> boolean().
lookup(Node, {art, CorrectionFactor, _IBfFpr, _LBfFpr, _Interval, IBF, LBF}) ->
    lookup_cf([{Node, CorrectionFactor}], IBF, LBF).

-spec lookup_cf([{Node, CF}], IBF::BF, LBF::BF) -> Result when
    is_subtype(BF,     bloom:bloom_filter()),
    is_subtype(Node,   merkle_tree:mt_node()),
    is_subtype(CF,     non_neg_integer()),        %correction factor
    is_subtype(Result, boolean()).
lookup_cf([{Node, 0} | L], IBF, LBF) ->
    NodeHash = merkle_tree:get_hash(Node),
    BF = ?IIF(merkle_tree:is_leaf(Node), LBF, IBF),
    ?TRACE("NodeHash=~p", [NodeHash]),
    bloom:is_element(BF, NodeHash) andalso lookup_cf(L, IBF, LBF);
lookup_cf([{Node, CF} | L], IBF, LBF) ->
    NodeHash = merkle_tree:get_hash(Node),
    IsLeaf = merkle_tree:is_leaf(Node),
    BF = ?IIF(IsLeaf, LBF, IBF),
    ?TRACE("NodeHash=~p~nIsLeaf=~p", [NodeHash, IsLeaf]),
    case bloom:is_element(BF, NodeHash) of
        false -> false;
        true  -> if IsLeaf -> lookup_cf(L, IBF, LBF);
                    true   -> Childs = merkle_tree:get_childs(Node),
                              NL = prepend_merkle_childs(L, Childs, CF - 1),
                              lookup_cf(NL, IBF, LBF)
                 end
    end;
lookup_cf([], _IBF, _LBF) ->
    true.

%% @doc Prepends the given merkle_tree Childs to the LookupList with the given
%%      correction factor.
-spec prepend_merkle_childs(LookupList::[{Node, CF}], Childs::[Node],
                            ChildCF::non_neg_integer()) -> [{Node, CF}] when
    is_subtype(Node,   merkle_tree:mt_node()),
    is_subtype(CF,     non_neg_integer()).        %correction factor
prepend_merkle_childs(L, [], _ChildCF) -> L;
prepend_merkle_childs(L, [Child | Rest], ChildCF) ->
    [{Child, ChildCF} | prepend_merkle_childs(L, Rest, ChildCF)].
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec fill_bloom(merkle_tree:mt_iter(), Inner1::BF, Leaf1::BF) -> {Inner2::BF, Leaf2::BF} when
      is_subtype(BF,        bloom:bloom_filter()).
fill_bloom(Iter, IBF, LBF) ->
    {InnerHashes, LeafHashes} = merkle_get_hashes(Iter, [], []),
    {bloom:add_list(IBF, InnerHashes), bloom:add_list(LBF, LeafHashes)}.

-spec merkle_get_hashes(merkle_tree:mt_iter(), InnerHashes1::HashL, LeafHashes1::HashL)
        -> {InnerHashes2::HashL, LeafHashes2::HashL} when
          is_subtype(HashL, [merkle_tree:mt_node_key()]).
merkle_get_hashes(Iter, InnerHashes, LeafHashes) ->
    case merkle_tree:next(Iter) of
        none -> {InnerHashes, LeafHashes};
        {Node, Iter2} ->
            NodeHash = merkle_tree:get_hash(Node),
            case merkle_tree:is_leaf(Node) of
                true ->
                    merkle_get_hashes(Iter2, InnerHashes, [NodeHash | LeafHashes]);
                false ->
                    merkle_get_hashes(Iter2, [NodeHash | InnerHashes], LeafHashes)
            end
    end.

merge_prop_lists(DefList, ListB) ->
    lists:foldl(fun({Key, Val}, Acc) ->
                        [{Key, proplists:get_value(Key, ListB, Val)} | Acc]
                end, [], DefList).

%% @doc Leaf hash fun to use for the embedded merkle tree.
-spec merkle_leaf_hf(merkle_tree:mt_bucket(), intervals:interval()) -> binary().
merkle_leaf_hf([], I) ->
    ?CRYPTO_SHA(term_to_binary(I));
merkle_leaf_hf([_|_] = Bucket, _I) ->
    ?CRYPTO_SHA(term_to_binary(Bucket)).
