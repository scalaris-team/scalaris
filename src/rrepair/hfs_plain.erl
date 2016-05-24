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

% types
-behaviour(hfs_beh).

-type itemKey() :: any().
-opaque hfs()   :: {hfs_plain, Hf_count::pos_integer(), HashFun::hfs_beh:hfs_fun()}.

-export_type([hfs/0]).

-export([new/1, apply_val/2, apply_val/3, apply_val_rem/3]).
-export([size/1]).

% for tester:
-export([apply_val_feeder/3,
         tester_create_hfs/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc returns a new hfs with default functions
-spec new(pos_integer()) -> hfs().
new(HFCount) ->
    {hfs_plain, HFCount, fun erlang:md5/1}.

%% @doc Hashes Val K-times as defined by the HFS.
-spec apply_val(hfs(), itemKey()) -> [non_neg_integer(),...].
apply_val({hfs_plain, K, HashFun}, Val) ->
    util:for_to_ex(1, K,
                   fun(I) ->
                           hash_value(erlang:term_to_binary({Val, I}), HashFun)
                   end).

%% @doc Hashes Val K-times as defined by the HFS and returns only
%%      remainders of divisions by Rem.
-spec apply_val_rem(hfs(), Val::itemKey(), Rem::pos_integer())
        -> [non_neg_integer(),...].
apply_val_rem({hfs_plain, K, HashFun}, Val, Rem) ->
    util:for_to_ex(1, K,
                   fun(I) ->
                           hash_value(
                             erlang:term_to_binary({Val, I}), HashFun) rem Rem
                   end).

-spec apply_val_feeder(hfs(), pos_integer(), itemKey())
        -> {hfs(), pos_integer(), itemKey()}.
apply_val_feeder({hfs_plain, K, HashFun}, I, Val) ->
    {{hfs_plain, K, HashFun}, erlang:min(K, I), Val}.
    
%% @doc Hashes Val with the I'th hash function as defined by the HFS.
%%      (I = 1..hfs_size).
%%      NOTE: When multiple different I are needed, prefer apply_val/2 since
%%            that function is faster.
-spec apply_val(hfs(), pos_integer(), itemKey()) -> non_neg_integer().
apply_val({hfs_plain, K, HashFun}, I, Val) when I =< K ->
    hash_value(erlang:term_to_binary({Val, I}), HashFun).

%% @doc Returns number of hash functions in the container
-spec size(hfs()) -> pos_integer().
size({hfs_plain, K, _HashFun}) ->
    K.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% private functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-compile({inline, [hash_value/2]}).

-spec hash_value(binary(), hfs_beh:hfs_fun()) -> non_neg_integer().
hash_value(Val, HashFun) ->
    H = HashFun(Val),
    if erlang:is_binary(H) ->
           Size = erlang:bit_size(H),
           <<R:Size>> = H,
           R;
       true -> H
    end.

-spec tester_create_hfs({hfs_plain, Hf_count::1..100, HashFun::hfs_beh:hfs_fun()}) -> hfs().
tester_create_hfs({hfs_plain, Hf_count, HashFun}) ->
    {hfs_plain, Hf_count, HashFun}.
