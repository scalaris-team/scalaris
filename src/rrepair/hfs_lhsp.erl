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
%% @doc    Less hashing, same performance hash function set container
%% @end
%% @reference
%%         Implementation of a hash function set proposed in 
%%         2006 by A. Kirsch, M. Mitzenmacher - 
%%         "Less Hashing, Same Performance: Building a Better Bloom Filter 
%%         Build k Hash functions of the form g_i(x) = h_1(X) + i * h_2(X)
%%  
%%         Used MD5 Hash-Function like in 
%%         2000 - L.Fan, P. Cao., JA, ZB : 
%%               "Summary Cache: A Scalable Wide-Area Web Cache Sharing Protocol" 
%%               (Counting Bloom Filters Paper)
%% @version $Id$
-module(hfs_lhsp).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

% types
-behaviour(hfs_beh).

-type itemKey() :: any().
-type hfs_fun() :: fun((binary()) -> non_neg_integer() | binary()).
-type hfs_t()   :: {hfs_lhsp, Hf_count::pos_integer(), H1_fun::hfs_fun(), H2_fun::hfs_fun()}.
-type hfs()     :: hfs_t(). %todo make opaque

-ifdef(with_export_type_support).
-export_type([hfs/0]).
-endif.

-export([new/1, new/2, apply_val/2, apply_val/3, apply_val_rem/3]).
-export([size/1]).

% for tester:
-export([new_feeder/2, apply_val_feeder/3,
         tester_create_hfs_fun/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc returns a new lhsp hfs with default functions
-spec new(pos_integer()) -> hfs_t().
new(HFCount) -> 
    new([fun erlang:adler32/1, fun erlang:md5/1], HFCount).

-spec new_feeder({hfs_fun(), hfs_fun()}, pos_integer())
        -> {[hfs_fun(),...], pos_integer()}.
new_feeder({H1, H2}, HFCount) ->
    {[H1, H2], HFCount}.

-spec new([hfs_fun(),...], pos_integer()) -> hfs_t().
new([H1, H2], HFCount) ->
    {hfs_lhsp, HFCount, H1, H2}.

% @doc Applies Val to all hash functions in container HC
-spec apply_val(hfs_t(), itemKey()) -> [non_neg_integer(),...].
apply_val({hfs_lhsp, K, H1, H2}, Val) ->
    ValBin = term_to_binary(Val),
    HV1 = hash_value(ValBin, H1),
    HV2 = hash_value(ValBin, H2),
    util:for_to_ex(0, K - 1, fun(I) -> HV1 + I * HV2 end).

% @doc Applies Val to all hash functions in container HC and returns only remainders 
-spec apply_val_rem(hfs_t(), itemKey(), pos_integer()) -> [non_neg_integer(),...].
apply_val_rem({hfs_lhsp, K, H1, H2}, Val, Rem) ->
    ValBin = term_to_binary(Val),
    HV1 = hash_value(ValBin, H1),
    HV2 = hash_value(ValBin, H2),
    util:for_to_ex(0, K - 1, fun(I) -> (HV1 + I * HV2) rem Rem end).

-spec apply_val_feeder(hfs_t(), pos_integer(), itemKey())
        -> {hfs_t(), pos_integer(), itemKey()}.
apply_val_feeder({hfs_lhsp, K, H1, H2}, I, Val) ->
    {{hfs_lhsp, K, H1, H2}, erlang:min(K, I), Val}.
    
% @doc Apply hash function I to given value; I = 1..hfs_size
-spec apply_val(hfs_t(), pos_integer(), itemKey()) -> non_neg_integer().
apply_val({hfs_lhsp, K, H1, H2}, I, Val) when I =< K ->
    ValBin = term_to_binary(Val),
    HV1 = hash_value(ValBin, H1),
    HV2 = hash_value(ValBin, H2),
    HV1 + (I - 1) * HV2.   

% @doc Returns number of hash functions in the container
-spec size(hfs_t()) -> pos_integer().
size({hfs_lhsp, K, _, _}) -> 
    K.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% private functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec hash_value(binary(), hfs_fun()) -> pos_integer().
hash_value(Val, HashFun) ->
    H = HashFun(Val),
    case erlang:is_binary(H) of
        true ->
            Size = erlang:bit_size(H),
            <<R:Size>> = H,
            R;
        false -> H
    end.

-spec tester_create_hfs_fun(1..2) -> hfs_fun().
tester_create_hfs_fun(1) -> fun erlang:adler32/1;
tester_create_hfs_fun(2) -> fun erlang:md5/1.
