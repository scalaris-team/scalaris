% @copyright 2014-2018 Zuse Institute Berlin,

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
%% @doc    API to increase/decrease a replicated counter using a
%%         state-based PN-Counter CRDT .
%% @end
-module(pncounter_on_cseq).
-author('skrzypczak@zib.de').

-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/1]).
-export([read_eventual/1]).
-export([read_state/1]).

-export([add/2]).
-export([add_eventual/2]).
-export([subtract/2]).
-export([subtract_eventual/2]).

%% Reads counter with strong consistency semantics
-spec read(client_key()) -> {ok, client_value()}.
read(Key) ->
    crdt_on_cseq:read(Key, pncounter, fun pncounter:query_counter/1).

%% Reads the full state with strong consistentcy semantics
-spec read_state(client_key()) -> {ok, client_value()}.
read_state(Key) ->
    crdt_on_cseq:read(Key, pncounter, fun crdt:query_noop/1).

%% Reads counter with eventual consistency semantics
-spec read_eventual(client_key()) -> {ok, client_value()}.
read_eventual(Key) ->
    crdt_on_cseq:read_eventual(Key, pncounter, fun pncounter:query_counter/1).

%% Increase counter with strong consistency semantics
-spec add(client_key(), non_neg_integer()) -> ok.
add(Key, ToAdd) ->
    UpdateFun = fun(ReplicaId, CRDT) -> pncounter:update_add(ReplicaId, ToAdd, CRDT) end,
    crdt_on_cseq:write(Key, pncounter, UpdateFun).

%% Increase counter with evenutally strong consistency semantics
-spec add_eventual(client_key(), non_neg_integer()) -> ok.
add_eventual(Key, ToAdd) ->
    UpdateFun = fun(ReplicaId, CRDT) -> pncounter:update_add(ReplicaId, ToAdd, CRDT) end,
    crdt_on_cseq:write_eventual(Key, pncounter, UpdateFun).

%% Decrease counter with strong consistency semantics
-spec subtract(client_key(), non_neg_integer()) -> ok.
subtract(Key, ToSubtract) ->
    UpdateFun = fun(ReplicaId, CRDT) -> pncounter:update_subtract(ReplicaId, ToSubtract, CRDT) end,
    crdt_on_cseq:write(Key, pncounter, UpdateFun).

%% Decrease counter with evenutally strong consistency semantics
-spec subtract_eventual(client_key(), non_neg_integer()) -> ok.
subtract_eventual(Key, ToSubtract) ->
    UpdateFun = fun(ReplicaId, CRDT) -> pncounter:update_subtract(ReplicaId, ToSubtract, CRDT) end,
    crdt_on_cseq:write_eventual(Key, pncounter, UpdateFun).
