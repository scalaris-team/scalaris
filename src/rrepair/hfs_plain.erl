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
%% @doc    Plain hashing, k-times using md5
%% @end
%% @version $Id$
-module(hfs_plain).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

% types
-behaviour(hfs_beh).

-type itemKey() :: any().
-opaque hfs()   :: {hfs_plain, Hf_count::pos_integer(), HashFun::hfs_beh:hfs_fun()}.

-export_type([hfs/0]).

-export([new/1, apply_val/2, apply_val/3, apply_val_rem/3]).
-export([size/1]).

% for tester:
-export([apply_val_feeder/3, apply_val_rem_feeder/3,
         tester_create_hfs/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc returns a new hfs with default functions
-spec new(pos_integer()) -> hfs().
new(HFCount) ->
    {hfs_plain, HFCount, {fun(X) -> ?CRYPTO_SHA(X) end, 160}}.

%% @doc Hashes Val K-times as defined by the HFS.
-spec apply_val(hfs(), itemKey()) -> [non_neg_integer(),...].
apply_val({hfs_plain, K, {HashFun, HashBits}}, Val) ->
    lists:flatten(
      util:for_to_ex(1, K,
                     fun(I) ->
                             hash_value(erlang:term_to_binary({Val, I}),
                                        HashFun, HashBits, 1)
                     end)).

-spec apply_val_rem_feeder(hfs(), itemKey(), Rem::pos_integer())
        -> {hfs(), itemKey(), Rem::pos_integer()}.
apply_val_rem_feeder({hfs_plain, _K, {_HashFun, HashBits}} = HFS, Val, Rem) ->
    {HFS, Val, erlang:max(2, Rem rem (1 bsl HashBits))}.

%% @doc Hashes Val K-times as defined by the HFS and returns only
%%      remainders of divisions by Rem.
-spec apply_val_rem(hfs(), Val::itemKey(), Rem::2..16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
        -> [non_neg_integer(),...].
apply_val_rem({hfs_plain, K, {HashFun, HashBits}}, Val, Rem) when Rem > 1 ->
    RemBits = util:ceil(util:log2(Rem)),
    Partitions = HashBits div RemBits,
    ?ASSERT2(Partitions > 0, 'hashfun not suitable for chosen remainder'), %% too few bits
    HashBitsEach = HashBits div Partitions,
    ?DBG_ASSERT(HashBitsEach > 0),
    Hashes =
        lists:flatten(
          util:for_to_ex(1, (K + Partitions - 1) div Partitions,
                         fun(I) ->
                                 L = hash_value(
                                       erlang:term_to_binary({Val, I}),
                                       HashFun, HashBitsEach, Partitions),
                                 [X rem Rem || X <- L]
                         end)),
    element(1, lists:split(K, Hashes)).

-spec apply_val_feeder(hfs(), pos_integer(), itemKey())
        -> {hfs(), pos_integer(), itemKey()}.
apply_val_feeder({hfs_plain, K, _HashFun} = HFS, I, Val) ->
    {HFS, erlang:min(K, I), Val}.
    
%% @doc Hashes Val with the I'th hash function as defined by the HFS.
%%      (I = 1..hfs_size).
%%      NOTE: When multiple different I are needed, prefer apply_val/2 since
%%            that function is faster.
-spec apply_val(hfs(), pos_integer(), itemKey()) -> non_neg_integer().
apply_val({hfs_plain, K, {HashFun, HashBits}}, I, Val) when I =< K ->
    [H] = hash_value(erlang:term_to_binary({Val, I}), HashFun, HashBits, 1),
    H.

%% @doc Returns number of hash functions in the container
-spec size(hfs()) -> pos_integer().
size({hfs_plain, K, _HashFun}) ->
    K.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% private functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, [hash_value/4, split_bin/3]}).

-spec hash_value(binary(), fun((binary()) -> non_neg_integer() | binary()),
                 HashBitsEach::pos_integer(), Partitions::pos_integer())
        -> [non_neg_integer()].
hash_value(Val, HashFun, HashBitsEach, Partitions) ->
    H = HashFun(Val),
    if erlang:is_binary(H) ->
           split_bin(H, Partitions, HashBitsEach);
       true ->
           split_bin(<<H:(Partitions*HashBitsEach)>>, Partitions, HashBitsEach)
    end.

%% @doc Splits Bin into Remaining chunks of Size each.
-spec split_bin(Bin::bitstring(), Remaining::non_neg_integer(),
                Size::pos_integer()) -> [non_neg_integer()].
split_bin(_Bin, 0, _Size) -> [];
split_bin(Bin, Remaining, Size) ->
    <<R1:Size, Rest/bitstring>> = Bin,
    [R1 | split_bin(Rest, Remaining - 1, Size)].

-spec tester_create_hfs({hfs_plain, Hf_count::1..100, HashFun::hfs_beh:hfs_fun()}) -> hfs().
tester_create_hfs({hfs_plain, Hf_count, HashFun}) ->
    {hfs_plain, Hf_count, HashFun}.
