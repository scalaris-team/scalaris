%  @copyright 2008-2018 Zuse Institute Berlin

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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc Implementation of a PN-Counter (Positive-Negative Counter) state-based CRDT.
%% @end
%% @version $Id$

-module(pncounter).
-author('skrzypczak@zib.de').
-vsn('Id$').

-include("scalaris.hrl").
-define(R, config:read(replication_factor)).

-export([update_add_fun/2]).
-export([update_subtract_fun/2]).
-export([query_counter/1, query_counter_fun/0]).

-behaviour(crdt_beh).

-opaque crdt() :: {gcounter:crdt(), gcounter:crdt()}.

-include("crdt_beh.hrl").


-spec new() -> crdt().
new() -> {gcounter:new(), gcounter:new()}.

-spec merge(crdt(), crdt()) -> crdt().
merge({P1, N1}, {P2, N2}) -> {gcounter:merge(P1, P2), gcounter:merge(N1, N2)}.

-spec eq(crdt(), crdt()) -> boolean().
eq({P1, N1}, {P2, N2}) -> gcounter:eq(P1, P2) andalso gcounter:eq(N1, N2).

-spec lt(crdt(), crdt()) -> boolean().
lt({P1, N1}, {P2, N2}) ->
    (gcounter:lt(P1, P2) andalso not gcounter:lt(N2, N1)) orelse
    (gcounter:lt(N1, N2) andalso not gcounter:lt(P2, P1)).

%%%%%%%%%%%%%%% Available update and query functions

-spec update_add_fun(non_neg_integer(), non_neg_integer()) -> crdt:update_fun().
update_add_fun(ReplicaNr, ToAdd) ->
    fun(_CRDT={P, N}) ->
        {gcounter:update_nth(P, ReplicaNr, ToAdd), N}
    end.

-spec update_subtract_fun(non_neg_integer(), non_neg_integer()) -> crdt:update_fun().
update_subtract_fun(ReplicaNr, ToSubtract) ->
    fun(_CRDT={P, N}) ->
        {P, gcounter:update_nth(N, ReplicaNr, ToSubtract)}
    end.

-spec query_counter(crdt()) -> non_neg_integer().
query_counter(_CRDT={P, N}) -> gcounter:query_counter(P) - gcounter:query_counter(N).

-spec query_counter_fun() -> crdt:query_fun().
query_counter_fun() -> fun ?MODULE:query_counter/1.

