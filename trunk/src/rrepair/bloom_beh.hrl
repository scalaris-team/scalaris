% @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc    Bloom Filter Behaviour
%% @end
%% @version $Id$

-export([new/2, new/3, new/4, add/2, is_element/2]).
-export([equals/2, join/2, print/1]).

-export([calc_HF_num/1, calc_HF_num/2, calc_HF_numEx/2,
         calc_least_size/2,
         calc_FPR/2, calc_FPR/3]).

%% types
%-opaque bloom_filter() :: bloom_filter_t().
-type bloom_filter() :: bloom_filter_t(). %make opaque causes lots of dialyzer warnings
-type key() :: binary() | integer() | float() | boolean() | atom() | tuple() | ?RT:key().

-ifdef(with_export_type_support).
-export_type([bloom_filter/0, key/0]).
-endif.

%% Functions

-spec new(integer(), float() | integer(), ?REP_HFS:hfs()) -> bloom_filter().
new(MaxItems, FPR, Hfs) when is_float(FPR) ->
    Size = resize(calc_least_size(MaxItems, FPR), 8),
    new_(Size, MaxItems, Hfs);
new(BitPerItem, MaxItems, Hfs) ->
    new_(resize(BitPerItem * MaxItems, 8), MaxItems, Hfs).

% @doc Creates new bloom filter with default hash function set
-spec new(integer(), float()) -> bloom_filter().
new(MaxItems, FPR) when is_float(FPR) ->
    Hfs = ?REP_HFS:new(calc_HF_numEx(MaxItems, FPR)),
    new(MaxItems, FPR, Hfs).

-spec new(integer(), float(), ?REP_HFS:hfs(), [key()]) -> bloom_filter().
new(MaxItems, FPR, Hfs, Items) ->
    BF = new(MaxItems, FPR, Hfs),
    add(BF, Items).

-spec add(bloom_filter(), key() | [key()]) -> bloom_filter().
add(Bloom, Item) when is_list(Item) -> add_list_(Bloom, Item);
add(Bloom, Item) -> add_list_(Bloom, [Item]).

-spec is_element(bloom_filter(), key()) -> boolean().
is_element(Bloom, Item) -> is_element_(Bloom, Item).

-spec equals(bloom_filter(), bloom_filter()) -> boolean().
equals(Bloom1, Bloom2) -> equals_(Bloom1, Bloom2).

-spec print(bloom_filter()) -> [{atom(), any()}].
print(Bloom) -> print_(Bloom). 

-spec join(bloom_filter(), bloom_filter()) -> bloom_filter().
join(Bloom1, Bloom2) -> join_(Bloom1, Bloom2).

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
    K = C * ln(2),
    K_Min = util:floor(K),
    K_Max = util:ceil(K),
    A = calc_FPR(C, K_Max),
    B = calc_FPR(C, K_Min),
    case util:min(A, B) =:= A of
        true -> K_Max;
        _ -> K_Min
    end.

% @doc Calculates opt. number of hash functions to
%      code N elements into a bloom filter with false positive rate of FPR.
-spec calc_HF_numEx(integer(), float()) -> integer().
calc_HF_numEx(N, FPR) ->
    M = calc_least_size(N, FPR),
    calc_HF_num(M, N).

% @doc  Calculates leasts bit size of a bloom filter
%       with a bounded false-positive rate FPR up to N-Elements.
-spec calc_least_size(non_neg_integer(), float()) -> pos_integer().
calc_least_size(_, FPR) when FPR =:= 0 -> 1;
calc_least_size(0, _) -> 1;
calc_least_size(N, FPR) ->
    util:ceil((N * util:log(math:exp(1), 2) * util:log(1 / FPR, 2))). 

% @doc  Calculates FPR for an M-bit large bloom_filter with K Hashfuntions 
%       and a maximum of N elements.
%       FPR = (1-e^(-kn/m))^k
%       M = number of BF-Bits
-spec calc_FPR(pos_integer(), pos_integer(), pos_integer()) -> float().
calc_FPR(M, N, K) -> 
    math:pow(1 - math:pow(math:exp(1), (-K*N) / M), K).

-spec calc_FPR(pos_integer(), pos_integer()) -> float().
calc_FPR(C, K) ->
    math:pow(1 - math:pow(math:exp(1), -K / C), K).
