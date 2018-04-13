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
%% @doc Implementation of a G-Counter (Grow-only Counter) state-based CRDT.
%% @end
%% @version $Id$

-module(gcounter).
-author('skrzypczak@zib.de').
-vsn('Id$').

-include("scalaris.hrl").
-define(R, config:read(replication_factor)).

-export([update_add/3]).
-export([query_counter/1]).

-export([update_nth/3]). %% internal for usage in pncounter

-behaviour(crdt_beh).

-opaque crdt() :: [non_neg_integer()].

-include("crdt_beh.hrl").


-spec new() -> crdt().
new() -> [0 || _ <- lists:seq(1, ?R)].

-spec merge(crdt(), crdt()) -> crdt().
merge(CRDT1, CRDT2) -> lists:zipwith(fun(A, B) -> max(A,  B) end, CRDT1, CRDT2).

-spec eq(crdt(), crdt()) -> boolean().
eq(CRDT1, CRDT2) -> CRDT1 =:= CRDT2.

-spec lt(crdt(), crdt()) -> boolean().
lt(CRDT1, CRDT2) ->
    {LT, GT} = lists:foldl(fun({E1, E2}, {AccL, AccG}) ->
                                {AccL orelse E1 < E2, AccG orelse E1 > E2}
                           end, {false, false}, lists:zip(CRDT1, CRDT2)),
    LT andalso not GT.

%%%%%%%%%%%%%%% Available update and query functions

-spec update_add(non_neg_integer(), non_neg_integer(), crdt()) -> crdt().
update_add(ReplicaId, ToAdd, CRDT) -> update_nth(CRDT, ReplicaId, ToAdd).

-spec query_counter(crdt()) -> non_neg_integer().
query_counter(CRDT) -> lists:foldl(fun(E, Acc) -> E + Acc end, 0, CRDT).

%%%%%%%%%%%%%%% Internal
-spec update_nth(crdt(), non_neg_integer(), non_neg_integer()) -> crdt().
update_nth(CRDT, 0, _ToAdd) -> CRDT;
update_nth(CRDT, Idx, _ToAdd) when length(CRDT) < Idx -> CRDT;
update_nth(CRDT, Idx, ToAdd) -> update_nth(CRDT, Idx, ToAdd, 1).

-spec update_nth(crdt(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> crdt().
update_nth(_CRDT=[H|T], Cur, ToAdd, Cur) -> [H + ToAdd | T];
update_nth(_CRDT=[H|T], Idx, ToAdd, Cur) -> [H | update_nth(T, Idx, ToAdd, Cur+1)].


