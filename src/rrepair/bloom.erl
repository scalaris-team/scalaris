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

-export([new/2, new/3, new/4, add/2, is_element/2]).
-export([equals/2, join/2, print/1]).

-export([calc_HF_num/1, calc_HF_num/2, calc_HF_numEx/2,
         calc_least_size/2,
         calc_FPR/2, calc_FPR/3]).

% for tests:
-export([get_property/2]).
-export([p_add_list_v1/4, p_add_list_v2/4, resize/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(bloom, {
                size          = 0                     :: non_neg_integer(),%bit-length of the bloom filter - requirement: size rem 8 = 0
                filter        = <<>>                  :: binary(),         %length = size div 8
                hfs           = ?required(bloom, hfs) :: ?REP_HFS:hfs(),   %HashFunctionSet
                max_items     = undefined             :: non_neg_integer() | undefined, %expected number of items
                items_count   = 0                     :: non_neg_integer() %number of inserted items
               }).
%-opaque bloom_filter() :: #bloom{}.
-type bloom_filter() :: #bloom{}. %make opaque causes lots of dialyzer warnings
-type key() :: binary() | integer() | float() | boolean() | atom() | tuple() | ?RT:key().

-ifdef(with_export_type_support).
-export_type([bloom_filter/0, key/0]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API  
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Creates new bloom filter with default hash function set
-spec new(non_neg_integer(), float()) -> bloom_filter().
new(MaxItems, FPR) ->
    Hfs = ?REP_HFS:new(calc_HF_numEx(MaxItems, FPR)),
    new(MaxItems, FPR, Hfs).

-spec new(non_neg_integer(), float() | integer(), ?REP_HFS:hfs()) -> bloom_filter().
new(MaxItems, FPR, Hfs) when is_float(FPR) ->
    Size = resize(calc_least_size(MaxItems, FPR), 8),
    new_(Size, MaxItems, Hfs);
new(MaxItems, BitPerItem, Hfs) ->
    new_(resize(BitPerItem * MaxItems, 8), MaxItems, Hfs).

-spec new(non_neg_integer(), float(), ?REP_HFS:hfs(), [key()]) -> bloom_filter().
new(MaxItems, FPR, Hfs, Items) ->
    BF = new(MaxItems, FPR, Hfs),
    add(BF, Items).

% @doc creates a new bloom filter
-spec new_(non_neg_integer(), non_neg_integer(), ?REP_HFS:hfs()) -> bloom_filter().
new_(BitSize, MaxItems, Hfs) when BitSize rem 8 =:= 0 ->
    #bloom{
           size = BitSize,
           max_items = MaxItems,
           hfs = Hfs,
           items_count = 0
          };
new_(BitSize, MaxItems, Hfs) ->
    new(resize(BitSize, 8), MaxItems, Hfs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Adds one or more items to the bloom filter.
-spec add(bloom_filter(), key() | [key()]) -> bloom_filter().
add(#bloom{size = BFSize,
           hfs = Hfs,
           items_count = FilledCount,
           filter = Filter
          } = Bloom, Items) when is_list(Items) ->
    % choose version according to the number of elements to add:
    % when setting around 16*3 positions, V2 is faster
    MinLengthForV2 = erlang:max(1, (16 * 3) div ?REP_HFS:size(Hfs)),
    ItemsL = length(Items),
    F = if ItemsL >= MinLengthForV2 ->
               p_add_list_v2(Hfs, BFSize, Filter, Items);
           true ->
               p_add_list_v1(Hfs, BFSize, Filter, Items)
        end,
    Bloom#bloom{filter = F, items_count = FilledCount + ItemsL};
add(#bloom{size = BFSize,
           hfs = Hfs,
           items_count = FilledCount,
           filter = Filter
          } = Bloom, Item) ->
    Bloom#bloom{filter = p_add_list_v1(Hfs, BFSize, Filter, [Item]),
                items_count = FilledCount + 1}.

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

%% @doc joins two bloom filter, returned bloom filter represents their union
-spec join(bloom_filter(), bloom_filter()) -> bloom_filter().
join(#bloom{size = Size, max_items = ExpItem1, items_count = Items1,
            filter = <<>>, hfs = Hfs},
     #bloom{size = Size, max_items = ExpItem2, items_count = Items2,
            filter = F2}) ->
    #bloom{size = Size, filter = F2, hfs = Hfs,
           max_items = erlang:max(ExpItem1, ExpItem2),
           items_count = Items1 + Items2 %approximation
           };
join(#bloom{size = Size, max_items = ExpItem1, items_count = Items1,
            filter = F1, hfs = Hfs},
     #bloom{size = Size, max_items = ExpItem2, items_count = Items2,
            filter = <<>>}) ->
    #bloom{size = Size, filter = F1, hfs = Hfs,
           max_items = erlang:max(ExpItem1, ExpItem2),
           items_count = Items1 + Items2 %approximation
           };
join(#bloom{size = Size, max_items = ExpItem1, items_count = Items1,
            filter = F1, hfs = Hfs},
     #bloom{size = Size, max_items = ExpItem2, items_count = Items2,
            filter = F2}) ->
    <<F1Val : Size>> = F1,
    <<F2Val : Size>> = F2,
    NewFVal = F1Val bor F2Val,
    #bloom{size = Size, filter = <<NewFVal:Size>>, hfs = Hfs,
           max_items = erlang:max(ExpItem1, ExpItem2),
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
print(#bloom{max_items = MaxItems, size = Size, hfs = Hfs,
             items_count = NumItems} = Bloom) ->
    HCount = ?REP_HFS:size(Hfs),
    [{filter_bit_size, Size},
     {struct_byte_size, byte_size(term_to_binary(Bloom))},
     {hash_fun_num, HCount},
     {max_items, MaxItems},
     {items_inserted, NumItems},
     {act_fpr, get_property(Bloom, fpr)},
     {compression_rate, Size / MaxItems}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_property(bloom_filter(), atom()) -> not_found | term().
get_property(#bloom{size = Size, hfs = Hfs, items_count = NumItems}, fpr) ->
    calc_FPR(Size, NumItems, ?REP_HFS:size(Hfs));
get_property(Bloom, Property) ->
    FieldNames = record_info(fields, bloom),
    {_, N} = lists:foldl(fun(I, {Nr, Res}) -> case I =:= Property of
                                                  true -> {Nr + 1, Nr};
                                                  false -> {Nr + 1, Res}
                                              end 
                         end, {1, -1}, FieldNames),
    if
        N =:= -1 -> not_found;
        true -> erlang:element(N + 1, Bloom)
    end.

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
    <<_:Pos/bitstring, C:1, _/bitstring>> = Filter,
    case C of
        1 -> check_Bits(Filter, Positions);
        0 -> false
    end;

check_Bits(_, []) -> 
    true.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Common bloom filter calculations
%
%       Definitions:
%       N=BF maximum element number
%       M=BF bit size
%       C=Compression Rate M/N
%       E=FPR 0<=E<=1 (FPR=false-positive rate)
%       k=Number of hash functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc  Calculates optimal number of hash functions for
%       an M-bit large BloomFilter with a maximum of N Elements.
-spec calc_HF_num(pos_integer(), non_neg_integer()) -> pos_integer().
calc_HF_num(M, N) ->
    calc_HF_num(util:ceil(M / erlang:max(N, 1))).

% @doc Calculates optimal number of hash functions for 
%      a given compression rate of C.
-spec calc_HF_num(pos_integer()) -> pos_integer().
calc_HF_num(C) when C > 0 ->
    K = C * math:log(2),
    K_Min = util:floor(K),
    K_Max = util:ceil(K),
    A = calc_FPR(C, K_Max),
    B = calc_FPR(C, K_Min),
    case erlang:min(A, B) of
        A -> K_Max;
        B -> K_Min
    end.

% @doc Calculates opt. number of hash functions to
%      code N elements into a bloom filter with false positive rate of FPR.
-spec calc_HF_numEx(non_neg_integer(), float()) -> pos_integer().
calc_HF_numEx(N, FPR) ->
    M = calc_least_size(N, FPR),
    calc_HF_num(M, N).

% @doc  Calculates leasts bit size of a bloom filter
%       with a bounded false-positive rate FPR up to N-Elements.
-spec calc_least_size(non_neg_integer(), float()) -> pos_integer().
calc_least_size(_, FPR) when FPR == 0 -> 1;
calc_least_size(0, _) -> 1;
calc_least_size(N, FPR) ->
    util:ceil((N * util:log2(math:exp(1)) * util:log2(1 / FPR))).

% @doc  Calculates FPR for an M-bit large bloom_filter with K Hashfuntions 
%       and a maximum of N elements.
%       FPR = (1-e^(-kn/m))^k
%       M = number of BF-Bits
-spec calc_FPR(pos_integer(), pos_integer(), pos_integer()) -> float().
calc_FPR(M, N, K) -> 
    math:pow(1 - math:exp((-K * N) / M), K).

-spec calc_FPR(pos_integer(), pos_integer()) -> float().
calc_FPR(C, K) ->
    math:pow(1 - math:exp(-K / C), K).

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
