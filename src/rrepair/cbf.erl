% @copyright 2016 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    Counting Bloom Filter implementation
%% @end
%% @reference D. Guo, M. Li
%%          <em>Set Reconciliation via Counting Bloom Filters</em>
%%          2013 IEEE Transactions on Knowledge and Data Engineering 25.10
%% @version $Id$
-module(cbf).
-author('kruber@zib.de').
-author('mlange@informatik.hu-berlin.de').

-include("record_helpers.hrl").
-include("scalaris.hrl").

-define(REP_HFS, hfs_plain). % hash function set implementation to use

-export([new_fpr/2, new_fpr/3, new_bpi/3, new_bin/3, new/2,
         add/2, add_list/2, remove/2, remove_list/2,
         is_element/2, item_count/1,
         to_bloom/1]).
-export([equals/2, join/2, minus/2, print/1]).

% for tests:
-export([get_property/2]).
-export([p_add_list/4]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(cbf, {
                filter        = ?required(cbf, filter) :: array:array(integer()),
                hfs           = ?required(cbf, hfs)    :: ?REP_HFS:hfs(),    %HashFunctionSet
                items_count   = 0                      :: non_neg_integer()  %number of inserted items
               }).
-opaque cbf() :: #cbf{}.
-type key() :: any().

-export_type([cbf/0, key/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Creates a new counting bloom filter with the default (optimal) hash
%%      function set based on the given false positive rate.
-spec new_fpr(MaxItems::non_neg_integer(), FPR::float()) -> cbf().
new_fpr(MaxItems, FPR) ->
    {K, Size} = bloom:calc_HF_num_Size_opt(MaxItems, FPR),
    new(Size, ?REP_HFS:new(K)).

%% @doc Creates a new counting bloom filter with the given hash function set
%%      based on the given false positive rate.
-spec new_fpr(MaxItems::non_neg_integer(), FPR::float(), ?REP_HFS:hfs() | non_neg_integer())
        -> cbf().
new_fpr(MaxItems, FPR, Hfs) ->
    Size = bloom:calc_least_size(MaxItems, FPR, ?REP_HFS:size(Hfs)),
    new(Size, Hfs).

%% @doc Creates a new counting bloom filter with the given hash function set and
%%      a fixed number of positions (bits in standard bloom filters) per item.
-spec new_bpi(MaxItems::non_neg_integer(), BitsPerItem::number(), ?REP_HFS:hfs() | non_neg_integer())
        -> cbf().
new_bpi(MaxItems, BitPerItem, Hfs) ->
    new(util:ceil(BitPerItem * MaxItems), Hfs).

%% @doc Creates a new counting bloom filter with the given binary, hash
%%      function set and item count.
-spec new_bin(Filter::array:array(integer()), ?REP_HFS:hfs() | non_neg_integer(), ItemsCount::non_neg_integer())
        -> cbf().
new_bin(Filter, HfCount, ItemsCount) when is_integer(HfCount) ->
    new_bin(Filter, ?REP_HFS:new(HfCount), ItemsCount);
new_bin(Filter, Hfs, ItemsCount) ->
    #cbf{filter = Filter, hfs = Hfs, items_count = ItemsCount}.

%% @doc Creates a new counting bloom filter.
-spec new(BitSize::pos_integer(), ?REP_HFS:hfs() | non_neg_integer()) -> cbf().
new(BitSize, HfCount) when is_integer(HfCount) ->
    new(BitSize, ?REP_HFS:new(HfCount));
new(BitSize, Hfs) ->
    #cbf{filter = array:new(BitSize, {default,0}), hfs = Hfs, items_count = 0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Adds one item to the counting bloom filter.
-spec add(cbf(), key()) -> cbf().
add(#cbf{hfs = Hfs, items_count = FilledCount,
         filter = Filter} = Bloom, Item) ->
    BFSize = array:size(Filter),
    Bloom#cbf{filter = p_add_list(Hfs, BFSize, Filter, [Item]),
              items_count = FilledCount + 1}.

%% @doc Adds multiple items to the counting bloom filter.
-spec add_list(cbf(), [key()]) -> cbf().
add_list(#cbf{hfs = Hfs,
              items_count = FilledCount,
              filter = Filter
             } = Bloom, Items) ->
    BFSize = array:size(Filter),
    ItemsL = length(Items),
    F = p_add_list(Hfs, BFSize, Filter, Items),
    Bloom#cbf{filter = F, items_count = FilledCount + ItemsL}.

-compile({inline, [p_add_list/4, p_change_list_/6]}).

%% @doc Helper to add items to the counting bloom filter.
-spec p_add_list(Hfs::?REP_HFS:hfs(), BFSize::pos_integer(),
                 BF1::array:array(integer()), Items::[key()])
-> BF2::array:array(integer()).
p_add_list(_Hfs, _BFSize, BF, []) -> BF;
p_add_list(Hfs, 1, BF, _Items = [_|_]) -> array:set(0, ?REP_HFS:size(Hfs), BF);
p_add_list(Hfs, BFSize, BF, Items = [_|_]) ->
    Positions = lists:flatmap(fun(Item) ->
                                      ?REP_HFS:apply_val_rem(Hfs, Item, BFSize)
                              end, Items),
    [Pos | Rest] = lists:sort(Positions),
    p_change_list_(Rest, Pos, [1 | lists:duplicate(Pos, 0)],
                   BFSize, BF, fun erlang:'+'/2).

%% @doc Helper increasing or decreasing counters by first accumulating all
%%      counters in a list and merging it with the old list.
-spec p_change_list_(Positions::[non_neg_integer()], CurPos::non_neg_integer(),
                     Counters::[non_neg_integer(),...], BFSize::non_neg_integer(),
                     BF::array:array(integer()),
                     ChangeFun::fun((non_neg_integer(), non_neg_integer())
                                   -> integer()))
-> BF2::array:array(integer()).
p_change_list_([], CurPos, Counters, BFSize, BF, ChangeFun) ->
    Counters1 = lists:reverse(Counters, lists:duplicate(erlang:max(0, BFSize - CurPos - 1), 0)),
    Counters2 = lists:zipwith(ChangeFun, array:to_list(BF), Counters1),
    array:from_list(Counters2);
p_change_list_([CurPos | Positions], CurPos, [CurCount | Counters], BFSize, BF, ChangeFun) ->
    p_change_list_(Positions, CurPos, [CurCount + 1 | Counters], BFSize, BF, ChangeFun);
p_change_list_([NewPos | Positions], CurPos, Counters, BFSize, BF, ChangeFun) ->
    Counters1 = lists:duplicate(NewPos - CurPos - 1, 0) ++ Counters,
    p_change_list_(Positions, NewPos, [1 | Counters1], BFSize, BF, ChangeFun).

%% @doc Removes one item from the counting bloom filter.
%%      (may introduce false negatives if removing an item not added previously)
-spec remove(cbf(), key()) -> cbf().
remove(#cbf{hfs = Hfs, items_count = FilledCount,
              filter = Filter} = Bloom, Item) ->
    BFSize = array:size(Filter),
    Bloom#cbf{filter = p_remove_list(Hfs, BFSize, Filter, [Item]),
              items_count = FilledCount + 1}.

%% @doc Removes multiple items from the counting bloom filter.
%%      (may introduce false negatives if removing an item not added previously)
-spec remove_list(cbf(), [key()]) -> cbf().
remove_list(#cbf{hfs = Hfs,
                 items_count = FilledCount,
                 filter = Filter
                } = Bloom, Items) ->
    BFSize = array:size(Filter),
    ItemsL = length(Items),
    F = p_remove_list(Hfs, BFSize, Filter, Items),
    Bloom#cbf{filter = F, items_count = FilledCount + ItemsL}.

-compile({inline, [p_remove_list/4]}).

%% @doc Helper to remove items from the counting bloom filter.
-spec p_remove_list(Hfs::?REP_HFS:hfs(), BFSize::pos_integer(),
                    BF1::array:array(integer()), Items::[key()])
        -> BF2::array:array(integer()).
p_remove_list(_Hfs, _BFSize, BF, []) -> BF;
p_remove_list(_Hfs, 1, BF, _Items = [_|_]) -> array:set(0, 0, BF);
p_remove_list(Hfs, BFSize, BF, Items = [_|_]) ->
    Positions = lists:flatmap(fun(Item) ->
                                      ?REP_HFS:apply_val_rem(Hfs, Item, BFSize)
                              end, Items),
    [Pos | Rest] = lists:sort(Positions),
    p_change_list_(Rest, Pos, [1 | lists:duplicate(Pos, 0)],
                   BFSize, BF, fun erlang:'-'/2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns true if the counting bloom filter contains this item.
-spec is_element(cbf(), key()) -> boolean().
is_element(#cbf{items_count = 0}, _Item) ->
    false;
is_element(#cbf{hfs = Hfs, filter = Filter}, Item) ->
    BFSize = array:size(Filter),
    Positions = ?REP_HFS:apply_val_rem(Hfs, Item, BFSize),
    check_counters(Filter, Positions).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the number of items inserted into this counting bloom filter.
-spec item_count(cbf()) -> non_neg_integer().
item_count(#cbf{items_count = ItemsCount}) -> ItemsCount.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Joins two counting bloom filters so that the returned counting bloom
%%      filter represents their union.
-spec join(cbf(), cbf()) -> cbf().
join(#cbf{items_count = 0, hfs = Hfs1} = _BF1,
     #cbf{hfs = Hfs2} = BF2) ->
    ?ASSERT(?REP_HFS:size(Hfs1) =:= ?REP_HFS:size(Hfs2)),
    ?ASSERT(get_property(BF2, size) =:= get_property(_BF1, size)),
    BF2;
join(#cbf{hfs = Hfs1} = BF1,
     #cbf{items_count = 0, hfs = Hfs2} = _BF2) ->
    ?ASSERT(?REP_HFS:size(Hfs1) =:= ?REP_HFS:size(Hfs2)),
    ?ASSERT(get_property(BF1, size) =:= get_property(_BF2, size)),
    BF1;
join(#cbf{items_count = Items1, filter = F1, hfs = Hfs},
     #cbf{items_count = Items2, filter = F2}) ->
    Size = array:size(F1),
    ?ASSERT(Size =:= array:size(F2)),
    if Items1 > Items2 ->
           FSmall = F2, FBig = F1, ok;
       true ->
           FSmall = F1, FBig = F2, ok
    end,
    NewF = array:sparse_foldl(fun(I, X, Acc) ->
                                      array:set(I, array:get(I, Acc) + X, Acc)
                              end, FBig, FSmall),
    #cbf{filter = NewF, hfs = Hfs,
         items_count = Items1 + Items2 %approximation
        }.

%% @doc Subtracts counting bloom filter A from B so that the returned
%%      counting bloom filter that approximates the set difference (with false
%%      positives and false negatives!).
-spec minus(A::cbf(), B::cbf()) -> cbf().
minus(#cbf{items_count = 0, hfs = Hfs1} = BF1,
      #cbf{hfs = Hfs2} = _BF2) ->
    ?ASSERT(?REP_HFS:size(Hfs1) =:= ?REP_HFS:size(Hfs2)),
    ?ASSERT(get_property(_BF2, size) =:= get_property(BF1, size)),
    BF1;
minus(#cbf{hfs = Hfs1} = BF1,
      #cbf{items_count = 0, hfs = Hfs2} = _BF2) ->
    ?ASSERT(?REP_HFS:size(Hfs1) =:= ?REP_HFS:size(Hfs2)),
    ?ASSERT(get_property(BF1, size) =:= get_property(_BF2, size)),
    BF1;
minus(#cbf{items_count = Items1, filter = F1, hfs = Hfs},
      #cbf{items_count = Items2, filter = F2}) ->
    Size = array:size(F1),
    ?ASSERT(Size =:= array:size(F2)),
    NewF = array:sparse_foldl(fun(I, X, Acc) ->
                                      array:set(I, array:get(I, Acc) - X, Acc)
                              end, F1, F2),
    #cbf{filter = NewF, hfs = Hfs,
         items_count = Items1 - Items2 %approximation
        }.

-spec to_bloom(cbf()) -> bloom:bloom_filter().
to_bloom(#cbf{items_count = 0, filter = F, hfs = Hfs}) ->
    bloom:new(array:size(F), Hfs);
to_bloom(#cbf{items_count = ItemsCount, filter = F, hfs = Hfs}) ->
%%     % note: this is faster than using array:sparse_fold[r,l]/3 to get the positions
%%     Positions0 = array:sparse_to_list(array:sparse_map(fun(I, _) -> I end, F)),
%%     % position 0 may be missing since 0 is the default value, too
%%     Positions = case array:get(0, F) of
%%                     0 -> Positions0;
%%                     _ -> [0 | Positions0]
%%                 end,
    % this seems even faster:
    Positions = lists:reverse(
                  element(2, lists:foldl(
                            fun(0, {N, L}) -> {N+1, L};
                               (_, {N, L}) -> {N+1, [N | L]}
                            end, {0, []}, array:to_list(F)))),
    BFSize = array:size(F),
    BFBits = bloom:p_add_positions(Positions, <<0:BFSize>>, BFSize),
    bloom:new_bin(BFBits, Hfs, ItemsCount).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether two counting bloom filters are equal.
-spec equals(cbf(), cbf()) -> boolean().
equals(#cbf{ items_count = Items1, filter = Filter1 },
       #cbf{ items_count = Items2, filter = Filter2 }) ->
    Items1 =:= Items2 andalso
        Filter1 =:= Filter2.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns counting bloom filter debug information.
-spec print(cbf()) -> [{atom(), any()}].
print(#cbf{filter = Filter, hfs = Hfs, items_count = NumItems} = Bloom) ->
    Size = array:size(Filter),
    HCount = ?REP_HFS:size(Hfs),
    [{filter_size, Size},
     {filter_byte_size, byte_size(term_to_binary(Filter, [compressed]))},
     {filter_as_list_byte_size, byte_size(term_to_binary(array:to_list(Filter), [compressed]))},
     {hash_fun_num, HCount},
     {items_inserted, NumItems},
     {act_fpr, get_property(Bloom, fpr)},
     {compression_rate, ?IIF(NumItems =:= 0, 0.0, Size / NumItems)}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_property(cbf(), fpr) -> float();
                  (cbf(), size) -> non_neg_integer();
                  (cbf(), filter) -> array:array(integer());
                  (cbf(), hfs_size) -> non_neg_integer();
                  (cbf(), hfs) -> ?REP_HFS:hfs();
                  (cbf(), items_count) -> non_neg_integer().
get_property(#cbf{filter = Filter, hfs = Hfs, items_count = NumItems}, fpr) ->
    Size = array:size(Filter),
    bloom:calc_FPR(Size, NumItems, ?REP_HFS:size(Hfs));
get_property(#cbf{filter = Filter}, size)        ->
    array:size(Filter);
get_property(#cbf{filter = X}     , filter)      -> X;
get_property(#cbf{hfs = X}        , hfs_size)    -> ?REP_HFS:size(X);
get_property(#cbf{hfs = X}        , hfs)         -> X;
get_property(#cbf{items_count = X}, items_count) -> X.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% bit/counter position operations
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Checks whether all counters are non-zero at the given positions.
-spec check_counters(array:array(integer()), Positions::[non_neg_integer()]) -> boolean().
check_counters(Filter, [Pos | Positions]) ->
    array:get(Pos, Filter) =/= 0 andalso check_counters(Filter, Positions);
check_counters(_, []) ->
    true.
