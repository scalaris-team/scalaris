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

-export([calc_HF_num/2, calc_HF_numEx/2,
         calc_least_size/3,
         calc_FPR/3]).

% for tests:
-export([get_property/2]).
-export([p_add_list_v1/4, p_add_list_v2/4, resize/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(bloom, {
                size          = ?required(bloom, size):: pos_integer(),    %bit-length of the bloom filter - requirement: size rem 8 = 0
                filter        = <<>>                  :: binary(),         %length = size div 8
                hfs           = ?required(bloom, hfs) :: ?REP_HFS:hfs(),   %HashFunctionSet
                items_count   = 0                     :: non_neg_integer() %number of inserted items
               }).
-opaque bloom_filter() :: #bloom{}.
-type key() :: any().

-ifdef(with_export_type_support).
-export_type([bloom_filter/0, key/0]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Creates a new bloom filter with the default (optimal) hash function set
%%      based on the given false positive rate.
-spec new_fpr(MaxItems::non_neg_integer(), FPR::float()) -> bloom_filter().
new_fpr(MaxItems, FPR) ->
    Hfs = ?REP_HFS:new(calc_HF_numEx(MaxItems, FPR)),
    new_fpr(MaxItems, FPR, Hfs).

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
    BitSize = erlang:bit_size(Filter),
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
    % choose version according to the number of elements to add:
    % when setting around 16*3 positions, V2 is faster
    MinLengthForV2 = erlang:max(1, (16 * 3) div ?REP_HFS:size(Hfs)),
    ItemsL = length(Items),
    F = if ItemsL >= MinLengthForV2 ->
               p_add_list_v2(Hfs, BFSize, Filter, Items);
           true ->
               p_add_list_v1(Hfs, BFSize, Filter, Items)
        end,
    Bloom#bloom{filter = F, items_count = FilledCount + ItemsL}.

-compile({inline, [p_add_list_v1/4, p_add_list_v1_/4,
                   p_add_list_v2/4]}).

% V1 - good for few items / positions to set
% faster than lists:foldl
-spec p_add_list_v1(Hfs::?REP_HFS:hfs(), BFSize::non_neg_integer(),
                    BF1::binary(), Items::[key()]) -> BF2::binary().
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
    p_add_list_v2_(Rest, AccPosBF, <<0:PreBitsNum>>, BF, BFSize).

p_add_list_v2_([Pos | Rest], AccPosBF, AccBF, BF, BFSize) when Pos - erlang:bit_size(AccBF) < 8 ->
    % Pos in same byte
    PosInByte = Pos rem 8,
    AccPosBF2 = AccPosBF bor (2#10000000 bsr PosInByte),
    p_add_list_v2_(Rest, AccPosBF2, AccBF, BF, BFSize);
p_add_list_v2_([Pos | Rest], AccPosBF, AccBF, BF, BFSize) ->
    PosInByte = Pos rem 8,
    PreBitsNum2 = Pos - PosInByte,
    DiffBits = PreBitsNum2 - erlang:bit_size(AccBF) - 8,
    % add AccPosBF to AccBF
    AccBF2 = <<AccBF/binary, AccPosBF:8, 0:DiffBits>>,
    % make new AccPosBF
    AccPosBF2 = (2#10000000 bsr PosInByte),
    p_add_list_v2_(Rest, AccPosBF2, AccBF2, BF, BFSize);
p_add_list_v2_([], AccPosBF, AccBF, BF, BFSize) ->
    RestBits = BFSize - erlang:bit_size(AccBF) - 8,
    case BF of
        <<>> -> <<AccBF/binary, AccPosBF:8, 0:RestBits>>;
        _ ->
            <<AccBF2Nr:BFSize>> = <<AccBF/binary, AccPosBF:8, 0:RestBits>>,
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
        <<_:Pos/bitstring, 0:1, _/bitstring>> -> false
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

%% @doc Calculates the optimal number of hash functions for a bloom filter
%%      with N elements and a false positive probability FP.
-spec calc_HF_numEx(N::non_neg_integer(), FP::float()) -> pos_integer().
calc_HF_numEx(N, FP) ->
    M = calc_least_size_opt(N, FP),
    calc_HF_num(M, N).

%% @doc Calculates optimal number of hash functions for
%%      an M-bit large BloomFilter with a maximum of N Elements.
-spec calc_HF_num(M::pos_integer(), N::non_neg_integer()) -> K::pos_integer().
calc_HF_num(M, N) ->
    K = math:log(2) * M / erlang:max(N, 1),
    K_Min = util:floor(K),
    K_Max = util:ceil(K),
    FP_Min = calc_FPR(M, N, K_Min),
    FP_Max = calc_FPR(M, N, K_Max),
    if FP_Min =< FP_Max -> K_Min;
       true             -> K_Max
    end.

%% @doc Calculates the number of bits needed by a bloom filter to have a false
%%      positive probability of FP using up to N elements and the optimal
%%      number of hash functions K = ln(2)*M/N.
%%      M = N * log_2(1/FP) / ln(2) = - N * log_2(FP) / ln(2)
-spec calc_least_size_opt(N::non_neg_integer(), FP::float()) -> M::pos_integer().
calc_least_size_opt(_N, FP) when FP == 0 -> 1;
calc_least_size_opt(0, _FP) -> 1;
calc_least_size_opt(N, FP) ->
    util:ceil(- N * util:log2(FP) / math:log(2)).

%% @doc Calculates the number of bits needed by a bloom filter to have a false
%%      positive probability of FP using K hash functions and up to N-Elements.
%%      M = 1/(1-(1-(FP)^(1/K))^(1/(KN)))
-spec calc_least_size(N::non_neg_integer(), FP::float(), K::pos_integer())
        -> M::pos_integer().
calc_least_size(_N, FP, _K) when FP == 0 -> 1;
calc_least_size(0, _FP, _K) -> 1;
calc_least_size(N, FP, K) ->
    util:ceil(1 / (1 - (math:pow(1 - math:pow(FP, 1 / K), 1 / (K * N))))).

%% @doc Calculates FP for an M-bit large bloom filter with K hash funtions
%%      and a maximum number of N elements.
%%      FP = (1-(1-1/M)^(K*N))^K
-spec calc_FPR(M::pos_integer(), N::non_neg_integer(), K::pos_integer())
        -> FP::float().
calc_FPR(M, N, K) ->
    math:pow(1 - math:pow(1 - 1/M, K * N), K).

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
