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
%% @doc    HashFunctionSet Behaviour
%% @end
%% @version $Id$
-module(hfs_beh).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-include("scalaris.hrl").

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.
-export([tester_create_hfs_fun/1]).

-export_type([hfs_fun/0]).

-ifdef(have_callback_support).
-type itemKey() :: any().
-type hfs()     :: term().
-callback new(pos_integer()) -> hfs().
-callback apply_val(hfs(), itemKey()) -> [non_neg_integer(),...].
-callback apply_val(hfs(), pos_integer(), itemKey()) -> non_neg_integer().
-callback apply_val_rem(hfs(), itemKey(), pos_integer()) -> [non_neg_integer(),...].
-callback size(hfs()) -> pos_integer().
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {new, 1},
     {apply_val, 2},
     {apply_val, 3},
     {apply_val_rem, 3},
     {size, 1}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.

-type hfs_fun() :: {fun((binary()) -> non_neg_integer() | binary()),
                    HashBits::pos_integer()}.

-spec tester_create_hfs_fun(1..3) -> hfs_fun().
tester_create_hfs_fun(1) -> {fun erlang:adler32/1, 32};
tester_create_hfs_fun(2) -> {fun erlang:md5/1, 128};
tester_create_hfs_fun(3) -> {fun(X) -> ?CRYPTO_SHA(X) end, 160}.
