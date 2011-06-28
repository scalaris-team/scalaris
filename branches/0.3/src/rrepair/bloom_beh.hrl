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

-export([new/3, add/2, addRange/2, is_element/2]).
-export([equals/2, join/2, print/1]).

-export([calc_HF_num/2,
         calc_HF_numEx/2,
         calc_least_size/2,
         calc_FPR/3]).

%% types
%-opaque bloomFilter() :: bloomFilter_t().
-type bloomFilter() :: bloomFilter_t(). %make opaque causes lots of dialyzer warnings
-type key() :: any().

-ifdef(with_export_type_support).
-export_type([bloomFilter/0]).
-endif.

%% Functions

-spec new(integer(), float(), ?REP_HFS:hfs()) -> bloomFilter().
new(MaxElements, FPR, Hfs) -> new_(MaxElements, FPR, Hfs).

-spec add(bloomFilter(), key()) -> bloomFilter().
add(Bloom, Item) -> addRange_(Bloom, [Item]).

-spec addRange(bloomFilter(), [key()]) -> bloomFilter().
addRange(Bloom, Items) -> addRange_(Bloom, Items).

-spec is_element(bloomFilter(), key()) -> boolean().
is_element(Bloom, Item) -> is_element_(Bloom, Item).

-spec equals(bloomFilter(), bloomFilter()) -> boolean().
equals(Bloom1, Bloom2) -> equals_(Bloom1, Bloom2).

-spec print(bloomFilter()) -> ok.
print(Bloom) -> print_(Bloom). 

-spec join(bloomFilter(), bloomFilter()) -> bloomFilter().
join(Bloom1, Bloom2) -> join_(Bloom1, Bloom2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Common bloom filter calculations
%
%       Definitions:
%       N=BF maximum element number
%       M=BF bit size
%       E=FPR 0<=E<=1 (FPR=false-positive rate)
%       k=Number of hash functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc  Calculates optimal number of hash functions - 
%       prefers smaller values (truncates decimals)
-spec calc_HF_num(integer(), integer()) -> integer().
calc_HF_num(M,N) -> 
	trunc(ln(2) * (M / N)).

-spec calc_HF_numEx(integer(), float()) -> integer().
calc_HF_numEx(N, FPR) ->
	M = calc_least_size(N, FPR),
	calc_HF_num(M,N).

% @doc  Calculates leasts bit size of a bloom filter
%       with a bounded false-positive rate up to MaxElements.
-spec calc_least_size(integer(), float()) -> integer().
calc_least_size(N, FPR) -> 
	trunc(N * util:log(math:exp(1), 2) * util:log(1 / FPR, 2)). 

% @doc  Calculates FPR for an M-bit large BloomFilter with K Hashfuntions 
%       and a maximum of N elements.
% 	   FPR = (1-e^(-kn/m))^k
% 	   M = number of BF-Bits
-spec calc_FPR(integer(), integer(), integer()) -> float().
calc_FPR(M, N, K) -> 
	math:pow(1 - math:pow(math:exp(1), -K*N / M), K).


%% private function specs - TO IMPLEMENT if behaviour is used
-spec new_(integer(), float(), ?REP_HFS:hfs()) -> bloomFilter_t().					 
-spec addRange_(bloomFilter_t(), [key()]) -> bloomFilter_t().
-spec is_element_(bloomFilter_t(), key()) -> boolean().
-spec equals_(bloomFilter(), bloomFilter()) -> boolean().
-spec join_(bloomFilter(), bloomFilter()) -> bloomFilter().
-spec print_(bloomFilter_t()) -> ok.
