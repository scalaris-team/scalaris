% @copyright 2011-2016 Zuse Institute Berlin

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
%% @doc    Bloom Filter implementation
%% @end
%% @reference A. Broder, M. Mitzenmacher
%%          <em>Network Applications of Bloom Filters: A Survey</em>
%%          2004 Internet Mathematics 1(4)
%% @version $Id$
-module(bloom).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-include("record_helpers.hrl").
-include("scalaris.hrl").

-define(REP_HFS, hfs_plain). %HashFunctionSet selection for usage by bloom filter

-export([new_fpr/2, new_fpr/3, new_bpi/3, new_bin/3, new/2,
         add/2, add_list/2, is_element/2, filter/2, filter_neg/2,
         filter_neg_and_add/3,
         item_count/1]).
-export([equals/2, join/2, get_property/2, print/1]).

% needed by other Bloom filter implementations or rr_recon:
-export([calc_least_size/3,
         calc_HF_num_Size_opt/2, calc_FPR/3,
         select_best/7,
         p_add_positions/3,
         resize/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(bloom, {
                filter        = ?required(bloom, filter) :: bitstring(),
                hfs           = ?required(bloom, hfs)    :: ?REP_HFS:hfs(),    %HashFunctionSet
                items_count   = 0                        :: non_neg_integer()  %number of inserted items
               }).
-opaque bloom_filter() :: #bloom{}.
-type key() :: any().

-export_type([bloom_filter/0, key/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Creates a new bloom filter with the default (optimal) hash function set
%%      based on the given false positive rate.
-spec new_fpr(MaxItems::non_neg_integer(), FPR::float()) -> bloom_filter().
new_fpr(MaxItems, FPR) ->
    {K, Size} = calc_HF_num_Size_opt(MaxItems, FPR),
    new(Size, ?REP_HFS:new(K)).

%% @doc Creates a new bloom filter with the given hash function set
%%      based on the given false positive rate.
-spec new_fpr(MaxItems::non_neg_integer(), FPR::float(), ?REP_HFS:hfs() | non_neg_integer())
        -> bloom_filter().
new_fpr(MaxItems, FPR, Hfs) ->
    Size = calc_least_size(MaxItems, FPR, ?REP_HFS:size(Hfs)),
    new(Size, Hfs).

%% @doc Creates a new bloom filter with the given hash function set and a fixed
%%      number of bits per item.
-spec new_bpi(MaxItems::non_neg_integer(), BitsPerItem::number(), ?REP_HFS:hfs() | non_neg_integer())
        -> bloom_filter().
new_bpi(MaxItems, BitPerItem, Hfs) ->
    new(util:ceil(BitPerItem * MaxItems), Hfs).

%% @doc Creates a new bloom filter with the given binary, hash function set and
%%      item count.
-spec new_bin(Filter::bitstring(), ?REP_HFS:hfs() | non_neg_integer(), ItemsCount::non_neg_integer())
        -> bloom_filter().
new_bin(Filter, HfCount, ItemsCount) when is_integer(HfCount) ->
    new_bin(Filter, ?REP_HFS:new(HfCount), ItemsCount);
new_bin(Filter, Hfs, ItemsCount) ->
    #bloom{filter = Filter, hfs = Hfs, items_count = ItemsCount}.

%% @doc Creates a new bloom filter.
-spec new(BitSize::pos_integer(), ?REP_HFS:hfs() | non_neg_integer()) -> bloom_filter().
new(BitSize, HfCount) when is_integer(HfCount) ->
    new(BitSize, ?REP_HFS:new(HfCount));
new(BitSize, Hfs) ->
    #bloom{filter = <<0:BitSize>>, hfs = Hfs, items_count = 0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Adds one item to the bloom filter.
-spec add(bloom_filter(), key()) -> bloom_filter().
add(#bloom{hfs = Hfs, items_count = FilledCount,
           filter = Filter} = Bloom, Item) ->
    BFSize = erlang:bit_size(Filter),
    Bloom#bloom{filter = p_add_list(Hfs, BFSize, Filter, [Item]),
                items_count = FilledCount + 1}.

%% @doc Adds multiple items to the bloom filter.
-spec add_list(bloom_filter(), [key()]) -> bloom_filter().
add_list(#bloom{hfs = Hfs,
                items_count = FilledCount,
                filter = Filter
               } = Bloom, Items) ->
    BFSize = erlang:bit_size(Filter),
    F = p_add_list(Hfs, BFSize, Filter, Items),
    ItemsL = length(Items),
    Bloom#bloom{filter = F, items_count = FilledCount + ItemsL}.

-compile({inline, [p_add_list/4, p_add_positions/3]}).

%% @doc Helper to set item bits, optimised for a large number of items /
%%      positions to set.
-spec p_add_list(Hfs::?REP_HFS:hfs(), BFSize::pos_integer(),
                 BF1::bitstring(), Items::[key()]) -> BF2::bitstring().
p_add_list(_Hfs, _BFSize, BF, []) -> BF;
p_add_list(_Hfs, 1, _BF, _Items = [_|_]) -> <<1:1>>;
p_add_list(Hfs, BFSize, BF, Items = [_|_]) ->
    Positions = lists:flatmap(fun(Item) ->
                                      ?REP_HFS:apply_val_rem(Hfs, Item, BFSize)
                              end, Items),
    p_add_positions(Positions, BF, BFSize).

-spec p_add_positions(Positions::[non_neg_integer(),...], BF1::bitstring(),
                      BFSize::pos_integer()) -> BF2::bitstring().
p_add_positions(Positions, BF, BFSize) ->
    [Pos | Rest] = lists:usort(Positions),
    PosInByte = Pos rem 8,
    PreBitsNum = Pos - PosInByte,
    AccPosBF = (2#10000000 bsr PosInByte),
    p_add_list_(Rest, AccPosBF, [<<0:PreBitsNum>>], PreBitsNum, BF, BFSize).

p_add_list_([Pos | Rest], AccPosBF, AccBF, AccBFSize, BF, BFSize) when Pos - AccBFSize < 8 ->
    % Pos in same byte
    PosInByte = Pos rem 8,
    AccPosBF2 = AccPosBF bor (2#10000000 bsr PosInByte),
    p_add_list_(Rest, AccPosBF2, AccBF, AccBFSize, BF, BFSize);
p_add_list_([Pos | Rest], AccPosBF, AccBF, AccBFSize, BF, BFSize) ->
    % Pos in next byte
    PosInByte = Pos rem 8,
    PreBitsNum2 = Pos - PosInByte,
    DiffBits = PreBitsNum2 - AccBFSize - 8,
    % add AccPosBF to AccBF
    AccBF2 = [<<AccPosBF:8, 0:DiffBits>> | AccBF],
    % make new AccPosBF
    AccPosBF2 = (2#10000000 bsr PosInByte),
    p_add_list_(Rest, AccPosBF2, AccBF2, AccBFSize + 8 + DiffBits, BF, BFSize);
p_add_list_([], AccPosBF, AccBF, AccBFSize, BF, BFSize) ->
    RestBits = BFSize - AccBFSize,
    LastBitString = if RestBits >= 8 ->
                           <<AccPosBF:RestBits/little>>;
                       true ->
                           % be careful with incomplete bytes (see set_bits/3)
                           AccPosBF1 = AccPosBF bsr (8 - RestBits),
                           <<AccPosBF1:RestBits/little>>
                    end,
    AccBFBin = erlang:list_to_bitstring(lists:reverse([LastBitString | AccBF])),
    case BF of
        <<0:BFSize>> ->
            AccBFBin;
        _ ->
            % merge AccBF2 and BF:
            Result = util:bin_or(BF, AccBFBin),
            % this also makes suce that BF and AccBFBin both have the same size:
            ?ASSERT(erlang:bit_size(Result) =:= BFSize),
            Result
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns true if the bloom filter contains this item.
-spec is_element(bloom_filter(), key()) -> boolean().
is_element(#bloom{items_count = 0}, _Item) ->
    false;
is_element(#bloom{hfs = Hfs, filter = Filter}, Item) ->
    BFSize = erlang:bit_size(Filter),
    Positions = ?REP_HFS:apply_val_rem(Hfs, Item, BFSize),
    check_bits(Filter, Positions).

%% @doc Returns all items present in the bloom filter.
-spec filter(bloom_filter(), Items::[key()]) -> [key()].
filter(#bloom{items_count = 0}, _Items) ->
    [];
filter(#bloom{hfs = Hfs, filter = Filter}, Items) ->
    BFSize = erlang:bit_size(Filter),
    [Item || Item <- Items,
             check_bits(Filter, ?REP_HFS:apply_val_rem(Hfs, Item, BFSize))].

%% @doc Returns all items not present in the bloom filter.
-spec filter_neg(bloom_filter(), Items::[key()]) -> [key()].
filter_neg(#bloom{items_count = 0}, Items) ->
    Items;
filter_neg(#bloom{hfs = Hfs, filter = Filter}, Items) ->
    BFSize = erlang:bit_size(Filter),
    [Item || Item <- Items,
             not check_bits(Filter, ?REP_HFS:apply_val_rem(Hfs, Item, BFSize))].

%% @doc Returns all items not present in the bloom filter and adds all items
%%      to a Bloom filter of the same structure in parallel.
%%      This way, the bit positions do not need to be determined twice
%%      (using costly hash functions) compared to executing these two
%%      functions separately.
%% @see filter_neg/2
%% @see add_list/2
-spec filter_neg_and_add(BFFilter::bloom_filter(), Items::[key()],
                         BFAdd::bloom_filter()) -> {[key()], BFAdd2::bloom_filter()}.
filter_neg_and_add(#bloom{items_count = 0, hfs = Hfs, filter = Filter}, Items,
                   BFAdd = #bloom{hfs = Hfs, filter = FilterAdd}) ->
    ?ASSERT(erlang:bit_size(Filter) =:= erlang:bit_size(FilterAdd)),
    {Items, bloom:add_list(BFAdd, Items)};
filter_neg_and_add(#bloom{hfs = Hfs, filter = Filter}, Items,
                   BFAdd = #bloom{hfs = Hfs, filter = FilterAdd, items_count = FilledCountAdd}) ->
    BFSize = erlang:bit_size(Filter),
    BFSize = erlang:bit_size(FilterAdd), % needs to be equal!
    IPositions = [{Item, ?REP_HFS:apply_val_rem(Hfs, Item, BFSize)} || Item <- Items],
    FilteredItems = [Item || {Item, Pos} <- IPositions,
                             not check_bits(Filter, Pos)],
    % from p_add_list/4:
    FilterAdd2 =
        if Items =:= [] -> FilterAdd;
           BFSize =:= 1 -> <<1:1>>;
           true ->
               Positions = lists:flatmap(fun({_Item, Pos}) -> Pos end, IPositions),
               p_add_positions(Positions, FilterAdd, BFSize)
        end,
    % from add_list/2:
    ItemsL = length(Items),
    BFAdd2 = BFAdd#bloom{filter = FilterAdd2, items_count = FilledCountAdd + ItemsL},
    {FilteredItems, BFAdd2}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the number of items inserted into this bloom filter.
-spec item_count(bloom_filter()) -> non_neg_integer().
item_count(#bloom{items_count = ItemsCount}) -> ItemsCount.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Joins two bloom filter so that the returned bloom filter represents
%%      their union.
-spec join(bloom_filter(), bloom_filter()) -> bloom_filter().
join(#bloom{items_count = 0, hfs = Hfs1} = _BF1,
     #bloom{hfs = Hfs2} = BF2) ->
    ?ASSERT(?REP_HFS:size(Hfs1) =:= ?REP_HFS:size(Hfs2)),
    ?ASSERT(get_property(BF2, size) =:= get_property(_BF1, size)),
    BF2;
join(#bloom{hfs = Hfs1} = BF1,
     #bloom{items_count = 0, hfs = Hfs2} = _BF2) ->
    ?ASSERT(?REP_HFS:size(Hfs1) =:= ?REP_HFS:size(Hfs2)),
    ?ASSERT(get_property(BF1, size) =:= get_property(_BF2, size)),
    BF1;
join(#bloom{items_count = Items1, filter = F1, hfs = Hfs},
     #bloom{items_count = Items2, filter = F2}) ->
    Size = erlang:bit_size(F1),
    ?ASSERT(Size =:= erlang:bit_size(F2)),
    NewF = util:bin_or(F1, F2),
    ?ASSERT(erlang:bit_size(NewF) =:= Size),
    #bloom{filter = NewF, hfs = Hfs,
           items_count = Items1 + Items2 %approximation
           }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether two bloom filters are equal.
-spec equals(bloom_filter(), bloom_filter()) -> boolean().
equals(#bloom{ items_count = Items1, filter = Filter1 },
       #bloom{ items_count = Items2, filter = Filter2 }) ->
    Items1 =:= Items2 andalso
        Filter1 =:= Filter2.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Returns bloom filter debug information.
-spec print(bloom_filter()) -> [{atom(), any()}].
print(#bloom{filter = Filter, hfs = Hfs, items_count = NumItems} = Bloom) ->
    Size = erlang:bit_size(Filter),
    HCount = ?REP_HFS:size(Hfs),
    [{filter_bit_size, Size},
     {struct_byte_size, byte_size(term_to_binary(Bloom))},
     {hash_fun_num, HCount},
     {items_inserted, NumItems},
     {act_fpr, get_property(Bloom, fpr)},
     {compression_rate, ?IIF(NumItems =:= 0, 0.0, Size / NumItems)}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_property(bloom_filter(), fpr) -> float();
                  (bloom_filter(), size) -> non_neg_integer();
                  (bloom_filter(), filter) -> bitstring();
                  (bloom_filter(), hfs_size) -> non_neg_integer();
                  (bloom_filter(), hfs) -> ?REP_HFS:hfs();
                  (bloom_filter(), items_count) -> non_neg_integer().
get_property(#bloom{filter = Filter, hfs = Hfs, items_count = NumItems}, fpr) ->
    Size = erlang:bit_size(Filter),
    calc_FPR(Size, NumItems, ?REP_HFS:size(Hfs));
get_property(#bloom{filter = Filter}, size)        ->
    erlang:bit_size(Filter);
get_property(#bloom{filter = X}     , filter)      -> X;
get_property(#bloom{hfs = X}        , hfs_size)    -> ?REP_HFS:size(X);
get_property(#bloom{hfs = X}        , hfs)         -> X;
get_property(#bloom{items_count = X}, items_count) -> X.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% bit operations
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Checks whether all bits are set at the given positions.
-spec check_bits(bitstring(), Positions::[non_neg_integer()]) -> boolean().
% V1
%% check_bits(Filter, [Pos | Positions]) ->
%%     PreBytes = Pos div 8,
%%     <<_:PreBytes/binary, CheckByte:8, _/binary>> = Filter,
%%     case 0 =/= CheckByte band (1 bsl (Pos rem 8)) of
%%         true -> check_bits(Filter, Positions);
%%         false -> false
%%     end;
%% check_bits(_, []) ->
%%     true.

% V 2 - 12 % faster than V1
check_bits(Filter, [Pos | Positions]) ->
    case Filter of
        <<_:Pos/bitstring, 1:1, _/bitstring>> ->
            check_bits(Filter, Positions);
        _ -> false
    end;
check_bits(_, []) ->
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Common bloom filter calculations
%%
%%       Definitions:
%%       N  = BF maximum number of elements
%%       M  = BF bit size
%%       FP = probability of a false positive, 0<=FP<=1 (also: FPR)
%%       k  = number of hash functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Calculates the optimal number of hash functions K and Bloom filter
%%      size M when K = ln(2)*M/N and M = N * log_2(1/FP) / ln(2)
%%      with N elements and a false positive probability FP.
-spec calc_HF_num_Size_opt(N::non_neg_integer(), FP::float())
        -> {K::pos_integer(), M::pos_integer()}.
calc_HF_num_Size_opt(N, FP) ->
    K0 = - util:log2(FP), % = log_2(1 / FP)
    K_Min = util:floor(K0),
    K_Max = util:ceil(K0),
    M_Min = calc_least_size(N, FP, K_Min),
    M_Max = calc_least_size(N, FP, K_Max),
    FPR_Min = calc_FPR(M_Min, N, K_Min),
    FPR_Max = calc_FPR(M_Max, N, K_Max),
    % unfortunately, we can't ensure the following two conditions due to
    % floating point precision issues near 1 in both calc_least_size/3 and calc_FPR/3
    % instead, use the best fitting option (see below)
    %?DBG_ASSERT(FPR_Min =< FP),
    %?DBG_ASSERT(FPR_Max =< FP),
%%     log:pal("Bloom (~g): K=~B, M=~B (FP=~g) vs. K=~B, M=~B (FP=~g)",
%%             [FP, K_Min, M_Min, FPR_Min, K_Max, M_Max, FPR_Max]),
    {K, M, _FPX} = select_best(FP, K_Min, M_Min, FPR_Min,
                               K_Max, M_Max, FPR_Max),
    {K, M}.

%% @doc Given two different options to create a Bloom filter with false
%%      positive rates (or any other measure, e.g. failure rate) FP1 and FP2
%%      and a target false positive rate FP, selects the best option that has
%%      FPX =&lt; FP and the lowest MX.
-spec select_best(FP::float(), K1::pos_integer(), M1::pos_integer(), FP1::float(),
                  K2::pos_integer(), M2::pos_integer(), FP2::float())
        -> {K::pos_integer(), M::pos_integer(), FP::float()}.
select_best(FP, K1, M1, FP1, K2, M2, FP2) ->
    if FP1 =< FP andalso FP2 =< FP ->
           if M1 < M2 ->
                  {K1, M1, FP1};
              M1 =:= M2 andalso FP1 < FP2 ->
                  {K1, M1, FP1};
              true ->
                  {K2, M2, FP2}
           end;
       FP1 =< FP -> % andalso FP2 > FP
           {K1, M1, FP1};
       FP2 =< FP -> % andalso FP1 > FP
           {K2, M2, FP2};
       % all below have FP1 > FP andalso FP2 > FP
       FP1 < FP2 ->
           {K1, M1, FP1};
       FP1 == FP2 andalso M1 =< M2 ->
           {K1, M1, FP1};
       true ->
           {K2, M2, FP2}
    end.

%% @doc Calculates the number of bits needed by a bloom filter to have a false
%%      positive probability of FP using K hash functions and up to N-Elements.
%%      M = 1/(1-(1-(FP)^(1/K))^(1/(KN)))
-spec calc_least_size(N::non_neg_integer(), FP::float(), K::pos_integer())
        -> M::pos_integer().
calc_least_size(_N, FP, _K) when FP == 0 -> 1;
calc_least_size(0, _FP, _K) -> 1;
calc_least_size(N, FP, K) ->
    % util:ceil(1 / (1 - math:pow(1 - math:pow(FP, 1 / K), 1 / (K * N))))
    util:ceil(1 / util:pow1p(1 - math:pow(FP, 1 / K), 1 / (K * N))).

%% @doc Calculates FP for an M-bit large bloom filter with K hash funtions
%%      and a maximum number of N elements.
%%      FP = (1-(1-1/M)^(K*N))^K
-spec calc_FPR(M::pos_integer(), N::non_neg_integer(), K::pos_integer())
        -> FP::float().
calc_FPR(_M, 0, _K) ->
    0.0;
calc_FPR(1, _N, _K) ->
    1.0;
calc_FPR(M, N, K) ->
    %math:pow(1 - math:pow(1 - 1/M, K * N), K).
    % more precise version taking floating point precision near 1 into acccount:
    Inner = -math:exp(K * N * util:log1p(-1 / M)),
    if Inner == -1.0 -> 0.0;
       true -> math:exp(K * util:log1p(Inner))
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Increases Val until Val rem Div == 0.
-spec resize(Val::non_neg_integer(), Div::pos_integer()) -> NewVal::non_neg_integer().
resize(Val, Div) ->
    case Val rem Div of
        0   -> Val;
        Rem -> Val + Div - Rem
    end.
