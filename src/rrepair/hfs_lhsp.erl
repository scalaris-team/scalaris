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

-include("scalaris.hrl").

% types
-behaviour(hfs_beh).

-type itemKey() :: any().
-opaque hfs()   :: {hfs_lhsp, Hf_count::pos_integer(), H1_fun::hfs_beh:hfs_fun(), H2_fun::hfs_beh:hfs_fun()}.

-export_type([hfs/0]).

-export([new/1, apply_val/2, apply_val/3, apply_val_rem/3]).
-export([size/1]).

% for tester:
-export([apply_val_feeder/3,
         tester_create_hfs/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc returns a new lhsp hfs with default functions
-spec new(pos_integer()) -> hfs().
new(HFCount) ->
    {hfs_lhsp, HFCount, {fun erlang:adler32/1, 32}, {fun erlang:md5/1, 128}}.

% @doc Applies Val to all hash functions in container HC
-spec apply_val(hfs(), itemKey()) -> [non_neg_integer(),...].
apply_val({hfs_lhsp, K, H1, H2}, Val) ->
    ValBin = erlang:term_to_binary(Val),
    HV1 = hash_value(ValBin, H1),
    if K > 1 ->
           HV2 = hash_value(ValBin, H2),
           apply_val_helper(K - 1, HV2, [HV1]);
       true ->
           [HV1]
    end.

-compile({nowarn_unused_function, apply_val_helper_feeder/3}).

-spec apply_val_helper_feeder(
        Hf_count::1..100, HashValue2::non_neg_integer(), Acc::Hashes)
        -> {Hf_count::1..100, HashValue2::non_neg_integer(), Acc::Hashes}
        when is_subtype(Hashes, [non_neg_integer(),...]).
apply_val_helper_feeder(HF_count, HV2, Acc) ->
    {HF_count, HV2, Acc}.

%% @doc Helper for apply_val/2.
-spec apply_val_helper(Hf_count::pos_integer(), HashValue2::non_neg_integer(),
                       Acc::Hashes) -> Hashes
        when is_subtype(Hashes, [non_neg_integer(),...]).
apply_val_helper(0, _HV2, Acc) ->
    Acc;
apply_val_helper(N, HV2, [H|_] = L) ->
    apply_val_helper(N - 1, HV2, [H + HV2 | L]).

%% @doc Applies Val to all hash functions in container HC and returns only
%%      remainders of divisions by Rem.
-spec apply_val_rem(HC::hfs(), Val::itemKey(), Rem::2..16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
        -> [non_neg_integer(),...].
apply_val_rem({hfs_lhsp, K, H1, H2}, Val, Rem) when Rem > 1 ->
    ValBin = erlang:term_to_binary(Val),
    HV1 = hash_value(ValBin, H1) rem Rem,
    % TODO: what if both hashes are 0?
    if K > 1 ->
           HV2 = hash_value(ValBin, H2) rem Rem,
           if HV2 =:= 0 ->
                  apply_val_rem_helper(K - 1, HV1, Rem, [HV2]);
              true ->
                  apply_val_rem_helper(K - 1, HV2, Rem, [HV1])
           end;
       true ->
           [HV1]
    end.

-compile({nowarn_unused_function, apply_val_rem_helper_feeder/4}).

-spec apply_val_rem_helper_feeder(
        Hf_count::1..100, HashValue2::non_neg_integer(), Rem::pos_integer(),
        Acc::Hashes)
        -> {Hf_count::1..100, HashValue2::non_neg_integer(), Rem::pos_integer(),
            Acc::Hashes}
        when is_subtype(Hashes, [non_neg_integer(),...]).
apply_val_rem_helper_feeder(HF_count, HV2, Rem, Acc) ->
    {HF_count, HV2, Rem, Acc}.

%% @doc Helper for apply_val_rem/3.
-spec apply_val_rem_helper(Hf_count::pos_integer(), HashValue2::non_neg_integer(),
                           Rem::pos_integer(), Acc::Hashes) -> Hashes
        when is_subtype(Hashes, [non_neg_integer(),...]).
apply_val_rem_helper(0, _HV2, _Rem, Acc) ->
    Acc;
apply_val_rem_helper(N, HV2, Rem, [H|_] = L) ->
    apply_val_rem_helper(N - 1, HV2, Rem, [(H + HV2) rem Rem | L]).

-spec apply_val_feeder(hfs(), pos_integer(), itemKey())
        -> {hfs(), pos_integer(), itemKey()}.
apply_val_feeder({hfs_lhsp, K, H1, H2}, I, Val) ->
    {{hfs_lhsp, K, H1, H2}, erlang:min(K, I), Val}.
    
%% @doc Apply hash function I to given value; I = 1..hfs_size.
%%      NOTE: When multiple different I are needed, prefer apply_val/2 since
%%            that function is faster.
-spec apply_val(hfs(), pos_integer(), itemKey()) -> non_neg_integer().
apply_val({hfs_lhsp, K, H1, H2}, I, Val) when I =< K ->
    ValBin = erlang:term_to_binary(Val),
    HV1 = hash_value(ValBin, H1),
    if I > 1 ->
           HV2 = hash_value(ValBin, H2),
           HV1 + (I - 1) * HV2;
       true ->
           HV1
    end.

%% @doc Returns number of hash functions in the container
-spec size(hfs()) -> pos_integer().
size({hfs_lhsp, K, _, _}) ->
    K.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% private functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, [hash_value/2]}).

-spec hash_value(binary(), hfs_beh:hfs_fun()) -> non_neg_integer().
hash_value(Val, {HashFun, HashBits}) ->
    H = HashFun(Val),
    if erlang:is_binary(H) ->
           <<R:HashBits>> = H,
           R;
       true -> H
    end.

-spec tester_create_hfs({hfs_lhsp, Hf_count::1..100, H1_fun::hfs_beh:hfs_fun(),
                         H2_fun::hfs_beh:hfs_fun()}) -> hfs().
tester_create_hfs({hfs_lhsp, Hf_count, H1_fun, H2_fun}) ->
    {hfs_lhsp, Hf_count, H1_fun, H2_fun}.
