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

-include("record_helpers.hrl").

-behaviour(bloom_beh).

-include("scalaris.hrl").

-export([get_property/2]). %for Tests

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(bloom, {
                size          = 0                     :: non_neg_integer(),%bit-length of the bloom filter - requirement: size rem 8 = 0
                filter        = <<>>                  :: binary(),         %length = size div 8
                hfs           = ?required(bloom, hfs) :: ?REP_HFS:hfs(),   %HashFunctionSet
                max_items     = undefined             :: non_neg_integer() | undefined, %extected number of items
                items_count   = 0                     :: non_neg_integer() %number of inserted items
               }).
-type bloom_filter_t() :: #bloom{}.

-include("bloom_beh.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API  
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc creates a new bloom filter
-spec new_(integer(), integer(), ?REP_HFS:hfs()) -> bloom_filter_t().
new_(BitSize, MaxItems, Hfs) when BitSize rem 8 =:= 0 ->
    #bloom{
           size = BitSize,
           filter = <<0:BitSize>>,
           max_items = MaxItems,
           hfs = Hfs,
           items_count = 0
          };
new_(BitSize, MaxItems, Hfs) ->
    new(resize(BitSize, 8), MaxItems, Hfs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc adds a range of items to bloom filter
-spec add_list_(bloom_filter_t(), [key()]) -> bloom_filter_t().
add_list_(Bloom, Items) ->
    #bloom{
           size = BFSize, 
           hfs = Hfs, 
           items_count = FilledCount,
           filter = Filter
          } = Bloom,
    F = p_add_list(Hfs, BFSize, Filter, Items),
    Bloom#bloom{
                filter = F,
                items_count = FilledCount + length(Items)
               }.

% faster than lists:foldl
p_add_list(Hfs, BFSize, Acc, [Item | Items]) ->
    Pos = apply(element(1, Hfs), apply_val_rem, [Hfs, Item, BFSize]),
    p_add_list(Hfs, BFSize, set_Bits(Acc, Pos), Items);
p_add_list(_Hfs, _BFSize, Acc, []) -> 
    Acc.    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc returns true if the bloom filter contains item
-spec is_element_(bloom_filter_t(), key()) -> boolean().
is_element_(#bloom{size = BFSize, hfs = Hfs, filter = Filter}, Item) -> 
    Positions = apply(element(1, Hfs), apply_val_rem, [Hfs, Item, BFSize]), 
    check_Bits(Filter, Positions).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc joins two bloom filter, returned bloom filter represents their union
-spec join_(bloom_filter(), bloom_filter()) -> bloom_filter().
join_(#bloom{size = Size1, max_items = ExpItem1, items_count = Items1, 
             filter = F1, hfs = Hfs}, 
      #bloom{size = Size2, max_items = ExpItem2, items_count = Items2, 
             filter = F2}) when Size1 =:= Size2 ->
    <<F1Val : Size1>> = F1,
    <<F2Val : Size2>> = F2,
    NewFVal = F1Val bor F2Val,
    #bloom{
           size = Size1,
           filter = <<NewFVal:Size1>>,                            
           max_items = erlang:max(ExpItem1, ExpItem2), 
           hfs = Hfs,                              
           items_count = Items1 + Items2 %approximation            
           }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc checks equality of two bloom filters
-spec equals_(bloom_filter(), bloom_filter()) -> boolean().
equals_(#bloom{ size = Size1, items_count = Items1, filter = Filter1 }, 
        #bloom{ size = Size2, items_count = Items2, filter = Filter2 }) ->
    Size1 =:= Size2 andalso
        Items1 =:= Items2 andalso
        Filter1 =:= Filter2.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc bloom filter debug information
-spec print_(bloom_filter_t()) -> [{atom(), any()}].
print_(#bloom{ max_items = MaxItems, 
               size = Size,
               hfs = Hfs,
               items_count = NumItems } = Bloom) -> 
    HCount = apply(element(1, Hfs), size, [Hfs]),
    [{filter_bit_size, Size},
     {struct_byte_size, byte_size(term_to_binary(Bloom))},
     {hash_fun_num, HCount},
     {max_items, MaxItems},
     {items_inserted, NumItems},
     {act_fpr, get_property(Bloom, fpr)},
     {compression_rate, Size / MaxItems}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_property(bloom_filter_t(), atom()) -> not_found | term().
get_property(#bloom{ size = Size, hfs = Hfs, items_count = NumItems }, fpr) ->
    calc_FPR(Size, NumItems, apply(element(1, Hfs), size, [Hfs]));
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
-spec set_Bits(binary(), [integer()]) -> binary().
set_Bits(Filter, [Pos | Positions]) ->
    PreByteNum = Pos div 8,
    <<PreBin:PreByteNum/binary, OldByte:8, PostBin/binary>> = Filter,    
    NewByte = OldByte bor (2#10000000 bsr (Pos rem 8)),
    NewBinary = case NewByte of
                    OldByte -> Filter;
                    _ -> <<PreBin/binary, NewByte:8, PostBin/binary>>
                end,
    set_Bits(NewBinary, Positions);
set_Bits(Filter, []) -> 
    Filter.

% ->V2 -> 1/3 slower than V1
%% set_Bits(Filter, [Pos | Positions]) ->
%%     <<A:Pos/bitstring, B:1/bitstring, C/bitstring>> = Filter,
%%     NewBinary = case B of
%%                     0 -> Filter;
%%                     _ -> <<A:Pos/bitstring, 1:1, C/bitstring>>
%%                 end,
%%     set_Bits(NewBinary, Positions).

% @doc Checks if all bits are set on a given position list
-spec check_Bits(binary(), [integer()]) -> boolean().
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
%% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec ln(X::number()) -> float().
ln(X) -> 
    util:log(X, math:exp(1)).

% @doc Increases Val until Val rem Div == 0.
-spec resize(integer(), integer()) -> integer().
resize(Val, Div) when (Val rem Div) =:= 0 -> 
    Val;
resize(Val, Div) -> 
    resize(Val + 1, Div).
