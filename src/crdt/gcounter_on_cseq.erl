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
%% @doc    API to increment a replicated counter using a
%%         state-based G-Counter CRDT.
%% @end
-module(gcounter_on_cseq).
-author('skrzypczak@zib.de').

-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/1]).
-export([read_state/1]).

-export([inc/1]).

%% Reads counter with strong consistency semantics
-spec read(client_key()) -> {ok, client_value()}.
read(Key) ->
    crdt_on_cseq:read(Key, gcounter, fun gcounter:query_counter/1).

%% Reads the full state with strong consistentcy semantics
-spec read_state(client_key()) -> {ok, client_value()}.
read_state(Key) ->
    crdt_on_cseq:read(Key, gcounter, fun crdt:query_noop/1).

%% Increments counter with strong consistency semantics
-spec inc(client_key()) -> ok.
inc(Key) ->
    UpdateFun = fun(ReplicaId, CRDT) -> gcounter:update_add(ReplicaId, 1, CRDT) end,
    crdt_on_cseq:write(Key, gcounter, UpdateFun).

