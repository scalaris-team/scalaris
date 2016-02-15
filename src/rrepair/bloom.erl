% @copyright 2011-2012 Zuse Institute Berlin

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

-export([new_fpr/2, new_fpr/3, new_bpi/3, new_bin/3,
         add/2, add_list/2, is_element/2, item_count/1]).
-export([equals/2, join/2, print/1]).

% for tests:
-export([get_property/2]).
-export([p_add_list_v1/4, p_add_list_v2/4, resize/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(bloom, {
                size          = ?required(bloom, size):: pos_integer(),    %bit-length of the bloom filter - requirement: size rem 8 = 0
                filter        = <<>>                  :: binary(),         %length = size div 8 (<<>> allowed when empty)
                hfs           = ?required(bloom, hfs) :: ?REP_HFS:hfs(),   %HashFunctionSet
                items_count   = 0                     :: non_neg_integer() %number of inserted items
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
    new_(Size, ?REP_HFS:new(K)).

%% @doc Creates a new bloom filter with the given hash function set
%%      based on the given false positive rate.
-spec new_fpr(MaxItems::non_neg_integer(), FPR::float(), ?REP_HFS:hfs())
        -> bloom_filter().
new_fpr(MaxItems, FPR, Hfs) ->
    Size = calc_least_size(MaxItems, FPR, ?REP_HFS:size(Hfs)),
    new_(Size, Hfs).

%% @doc Creates a new bloom filter with the given hash function set and a fixed
%%      number of bits per item.
-spec new_bpi(MaxItems::non_neg_integer(), BitsPerItem::float(), ?REP_HFS:hfs())
        -> bloom_filter().
new_bpi(MaxItems, BitPerItem, Hfs) ->
    new_(util:ceil(BitPerItem * MaxItems), Hfs).

%% @doc Creates a new bloom filter with the given binary, hash function set and
%%      item count.
-spec new_bin(Filter::binary(), ?REP_HFS:hfs(), ItemsCount::non_neg_integer())
        -> bloom_filter().
new_bin(Filter, Hfs, ItemsCount) ->
    BitSize = erlang:max(erlang:bit_size(Filter), 8),
    ?ASSERT((BitSize rem 8) =:= 0),
    #bloom{size = BitSize, filter = Filter, hfs = Hfs, items_count = ItemsCount}.

%% @doc Creates a new bloom filter.
-spec new_(BitSize::pos_integer(), ?REP_HFS:hfs()) -> bloom_filter().
new_(BitSize, Hfs) ->
    NewSize = resize(BitSize, 8),
    ?DBG_ASSERT((NewSize rem 8) =:= 0),
    #bloom{size = NewSize, hfs = Hfs, items_count = 0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Adds one item to the bloom filter.
-spec add(bloom_filter(), key()) -> bloom_filter().
add(#bloom{size = BFSize, hfs = Hfs, items_count = FilledCount,
           filter = Filter} = Bloom, Item) ->
    Bloom#bloom{filter = p_add_list_v1(Hfs, BFSize, Filter, [Item]),
                items_count = FilledCount + 1}.

%% @doc Adds multiple items to the bloom filter.
-spec add_list(bloom_filter(), [key()]) -> bloom_filter().
add_list(#bloom{size = BFSize,
           hfs = Hfs,
           items_count = FilledCount,
           filter = Filter
          } = Bloom, Items) ->
    F = p_add_list_v2(Hfs, BFSize, Filter, Items),
    ItemsL = length(Items),
    Bloom#bloom{filter = F, items_count = FilledCount + ItemsL}.

-compile({inline, [p_add_list_v1/4, p_add_list_v1_/4,
                   p_add_list_v2/4]}).

% V1 - good for few items / positions to set
% faster than lists:foldl
-spec p_add_list_v1(Hfs::?REP_HFS:hfs(), BFSize::non_neg_integer(),
                    BF1::binary(), Items::[key()]) -> BF2::binary().
p_add_list_v1(_Hfs, _BFSize, BF, []) -> BF;
p_add_list_v1(Hfs, BFSize, <<>>, Items) ->
    p_add_list_v1(Hfs, BFSize, <<0:BFSize>>, Items);
p_add_list_v1(Hfs, BFSize, BF, Items) ->
    p_add_list_v1_(Hfs, BFSize, BF, Items).

-spec p_add_list_v1_(Hfs::?REP_HFS:hfs(), BFSize::non_neg_integer(),
                    BF1::binary(), Items::[key()]) -> BF2::binary().
p_add_list_v1_(Hfs, BFSize, BF, [Item | Items]) ->
    Positions = ?REP_HFS:apply_val_rem(Hfs, Item, BFSize),
    p_add_list_v1_(Hfs, BFSize, set_bits(BF, Positions), Items);
p_add_list_v1_(_Hfs, _BFSize, BF, []) ->
    BF.

% V2 - good for large number of items / positions to set
-spec p_add_list_v2(Hfs::?REP_HFS:hfs(), BFSize::non_neg_integer(),
                    BF1::binary(), Items::[key()]) -> BF2::binary().
p_add_list_v2(_Hfs, _BFSize, BF, []) -> BF;
p_add_list_v2(Hfs, BFSize, BF, Items) ->
    Positions = lists:flatmap(fun(Item) ->
                                      ?REP_HFS:apply_val_rem(Hfs, Item, BFSize)
                              end, Items),
    [Pos | Rest] = lists:usort(Positions),
    PosInByte = Pos rem 8,
    PreBitsNum = Pos - PosInByte,
    AccPosBF = (2#10000000 bsr PosInByte),
    p_add_list_v2_(Rest, AccPosBF, [<<0:PreBitsNum>>], PreBitsNum, BF, BFSize).

p_add_list_v2_([Pos | Rest], AccPosBF, AccBF, AccBFSize, BF, BFSize) when Pos - AccBFSize < 8 ->
    % Pos in same byte
    PosInByte = Pos rem 8,
    AccPosBF2 = AccPosBF bor (2#10000000 bsr PosInByte),
    p_add_list_v2_(Rest, AccPosBF2, AccBF, AccBFSize, BF, BFSize);
p_add_list_v2_([Pos | Rest], AccPosBF, AccBF, AccBFSize, BF, BFSize) ->
    % Pos in next byte
    PosInByte = Pos rem 8,
    PreBitsNum2 = Pos - PosInByte,
    DiffBits = PreBitsNum2 - AccBFSize - 8,
    % add AccPosBF to AccBF
    AccBF2 = [<<AccPosBF:8, 0:DiffBits>> | AccBF],
    % make new AccPosBF
    AccPosBF2 = (2#10000000 bsr PosInByte),
    p_add_list_v2_(Rest, AccPosBF2, AccBF2, AccBFSize + 8 + DiffBits, BF, BFSize);
p_add_list_v2_([], AccPosBF, AccBF, AccBFSize, BF, BFSize) ->
    RestBits = BFSize - AccBFSize - 8,
    AccBFBin = erlang:list_to_binary(lists:reverse([<<AccPosBF:8, 0:RestBits>> | AccBF])),
    case BF of
        <<>> -> AccBFBin;
        _ ->
            <<AccBF2Nr:BFSize>> = AccBFBin,
            % merge AccBF2 and BF
            <<BFNr:BFSize>> = BF,
            ResultNr = AccBF2Nr bor BFNr,
            <<ResultNr:BFSize>>
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc returns true if the bloom filter contains item
-spec is_element(bloom_filter(), key()) -> boolean().
is_element(#bloom{filter = <<>>}, _Item) ->
    false;
is_element(#bloom{size = BFSize, hfs = Hfs, filter = Filter}, Item) ->
    Positions = ?REP_HFS:apply_val_rem(Hfs, Item, BFSize),
    check_Bits(Filter, Positions).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Gets the number of items inserted into this bloom filter.
-spec item_count(bloom_filter()) -> non_neg_integer().
item_count(#bloom{items_count = ItemsCount}) -> ItemsCount.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc joins two bloom filter, returned bloom filter represents their union
-spec join(bloom_filter(), bloom_filter()) -> bloom_filter().
join(#bloom{size = Size, items_count = Items1,
            filter = <<>>, hfs = Hfs},
     #bloom{size = Size, items_count = Items2,
            filter = F2}) ->
    #bloom{size = Size, filter = F2, hfs = Hfs,
           items_count = Items1 + Items2 %approximation
           };
join(#bloom{size = Size, items_count = Items1, filter = F1, hfs = Hfs},
     #bloom{size = Size, items_count = Items2, filter = <<>>}) ->
    #bloom{size = Size, filter = F1, hfs = Hfs,
           items_count = Items1 + Items2 %approximation
           };
join(#bloom{size = Size, items_count = Items1, filter = F1, hfs = Hfs},
     #bloom{size = Size, items_count = Items2, filter = F2}) ->
    <<F1Val : Size>> = F1,
    <<F2Val : Size>> = F2,
    NewFVal = F1Val bor F2Val,
    #bloom{size = Size, filter = <<NewFVal:Size>>, hfs = Hfs,
           items_count = Items1 + Items2 %approximation
           }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Checks whether two bloom filters are equal.
-spec equals(bloom_filter(), bloom_filter()) -> boolean().
equals(#bloom{ size = Size1, items_count = Items1, filter = Filter1 },
       #bloom{ size = Size2, items_count = Items2, filter = Filter2 }) ->
    Size1 =:= Size2 andalso
        Items1 =:= Items2 andalso
        Filter1 =:= Filter2.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Return bloom filter debug information.
-spec print(bloom_filter()) -> [{atom(), any()}].
print(#bloom{size = Size, hfs = Hfs, items_count = NumItems} = Bloom) ->
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
                  (bloom_filter(), filter) -> binary();
                  (bloom_filter(), hfs) -> ?REP_HFS:hfs();
                  (bloom_filter(), items_count) -> non_neg_integer().
get_property(#bloom{size = Size, hfs = Hfs, items_count = NumItems}, fpr) ->
    calc_FPR(Size, NumItems, ?REP_HFS:size(Hfs));
get_property(#bloom{size = X}       , size)        -> X;
get_property(#bloom{filter = X}     , filter)      -> X;
get_property(#bloom{hfs = X}        , hfs)         -> X;
get_property(#bloom{items_count = X}, items_count) -> X.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% bit operations
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Sets all filter-bits at given positions to 1
-spec set_bits(binary(), [non_neg_integer()]) -> binary().
set_bits(Filter, [Pos | Positions]) ->
    PreByteNum = Pos div 8,
    PosInByte = Pos rem 8,
    <<PreBin:PreByteNum/binary, OldByte:8, PostBin/binary>> = Filter,
    NewBinary = case (OldByte bor (2#10000000 bsr PosInByte)) of
                    OldByte -> Filter;
                    NewByte -> <<PreBin/binary, NewByte:8, PostBin/binary>>
                end,
    set_bits(NewBinary, Positions);
set_bits(Filter, []) ->
    Filter.

%% V2 -> 1/3 slower than V1
%% set_bits(Filter, [Pos | Positions]) ->
%%     <<A:Pos/bitstring, B:1/bitstring, C/bitstring>> = Filter,
%%     NewBinary = case B of
%%                     0 -> Filter;
%%                     _ -> <<A:Pos/bitstring, 1:1, C/bitstring>>
%%                 end,
%%     set_bits(NewBinary, Positions).

% @doc Checks if all bits are set on a given position list
-spec check_Bits(binary(), [non_neg_integer()]) -> boolean().
% V1
%% check_Bits(Filter, [Pos | Positions]) ->
%%     PreBytes = Pos div 8,
%%     <<_:PreBytes/binary, CheckByte:8, _/binary>> = Filter,
%%     case 0 =/= CheckByte band (1 bsl (Pos rem 8)) of
%%         true -> check_Bits(Filter, Positions);
%%         false -> false
%%     end;

% V 2 - 12 % faster than V1
check_Bits(Filter, [Pos | Positions]) ->
    case Filter of
        <<_:Pos/bitstring, 1:1, _/bitstring>> -> check_Bits(Filter, Positions);
        _ -> false
    end;
check_Bits(_, []) ->
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
%%      size M when K = ln(2)*M/N and M = N * log_2(1/FP) / ln(2) (aligned to
%%      full bytes) with N elements and a false positive probability FP.
-spec calc_HF_num_Size_opt(N::non_neg_integer(), FP::float())
        -> {K::pos_integer(), M::pos_integer()}.
calc_HF_num_Size_opt(N, FP) ->
    K0 = - util:log2(FP), % = log_2(1 / FP)
    K_Min = util:floor(K0),
    K_Max = util:ceil(K0),
    M_Min = resize(calc_least_size(N, FP, K_Min), 8),
    M_Max = resize(calc_least_size(N, FP, K_Max), 8),
    FPR_Min = calc_FPR(M_Min, N, K_Min),
    FPR_Max = calc_FPR(M_Max, N, K_Max),
    % unfortunately, we can't ensure the following two conditions due to
    % floating point precision issues near 1 in both calc_least_size/3 and calc_FPR/3
    % instead, use the best fitting option (see below)
    %?DBG_ASSERT(FPR_Min =< FP),
    %?DBG_ASSERT(FPR_Max =< FP),
%%     log:pal("Bloom (~g): K=~B, M=~B (FP=~g) vs. K=~B, M=~B (FP=~g)",
%%             [FP, K_Min, M_Min, FPR_Min, K_Max, M_Max, FPR_Max]),
    if FPR_Min =< FP andalso FPR_Max =< FP ->
           if M_Min < M_Max -> {K_Min, M_Min};
              M_Min =:= M_Max andalso FPR_Min < FPR_Max -> {K_Min, M_Min};
              true -> {K_Max, M_Max}
           end;
       FPR_Min =< FP andalso FPR_Max > FP -> {K_Min, M_Min};
       FPR_Min > FP andalso FPR_Max =< FP -> {K_Max, M_Max};
       FPR_Min > FP andalso FPR_Max > FP andalso FPR_Max > FPR_Min -> {K_Min, M_Min};
       FPR_Min > FP andalso FPR_Max > FP -> {K_Max, M_Max}
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
    P = math:pow(FP, 1 / K),
    if P =< 0.01 ->
           % rr_recon:calc_n_subparts_p1e/2 uses a more precise Taylor expansion
           util:ceil(1 / rr_recon:calc_n_subparts_p1e(K * N, P));
       true ->
           % if P is not near 0, this is more precise (and vice versa):
           % use the series representation of 1 - p^(1/x) for x=N*K, p = 1 - P
           % 1-p^(1/x) = sum_(n=-1)^-infinity (-(log^(-n)(p))/((-n)!)) k^n
           % http://www.wolframalpha.com/input/?i=series+1-p^%281%2Fx%29
           LogP = math:log(1-P), LogP2 = LogP * LogP, LogP3 = LogP2 * LogP,
           LogP4 = LogP3 * LogP, LogP5 = LogP4 * LogP,
           X = K*N, X2 = X * X, X3 = X2 * X, X4 = X3 * X, X5 = X4 * X,
           Series = -LogP / X - LogP2 / (2*X2) - LogP3 / (6*X3) - LogP4 / (24*X4)
                        - LogP5 / (120*X5), % +O[P^6]
           util:ceil(1 / Series)
    end.

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
