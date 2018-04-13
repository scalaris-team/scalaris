% @copyright 2014-2017 Zuse Institute Berlin,

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
    read_helper(Key, fun crdt_proposer:read/5, fun pncounter:query_counter/1).

%% Reads the full state with strong consistentcy semantics
-spec read_state(client_key()) -> {ok, client_value()}.
read_state(Key) ->
    read_helper(Key, fun crdt_proposer:read/5, fun crdt:query_noop/1).

%% Reads counter with eventual consistency semantics
-spec read_eventual(client_key()) -> {ok, client_value()}.
read_eventual(Key) ->
    read_helper(Key, fun crdt_proposer:read_eventual/5, fun pncounter:query_counter/1).

-spec read_helper(client_key(), fun((_,_,_,_,_) -> ok), crdt:query_fun()) ->
    {ok, client_value()}.
read_helper(Key, APIFun, QueryFun) ->
    APIFun(crdt_db, self(), ?RT:hash_key(Key), pncounter, QueryFun),
    receive
        ?SCALARIS_RECV({read_done, CounterValue}, {ok, CounterValue})
    after 1000 ->
        log:log("read hangs ~p~n", [erlang:process_info(self(), messages)]),
        receive
            ?SCALARIS_RECV({read_done, CounterValue},
                            begin
                                log:log("~p read was only slow at key ~p~n",
                                        [self(), Key]),
                                {ok, CounterValue}
                            end)
        end
    end.

%% Increase counter with strong consistency semantics
-spec add(client_key(), non_neg_integer()) -> ok.
add(Key, ToAdd) ->
    UpdateFun = fun(ReplicaId, CRDT) -> pncounter:update_add(ReplicaId, ToAdd, CRDT) end,
    write_helper(Key, fun crdt_proposer:write/5, UpdateFun).

%% Increase counter with evenutally strong consistency semantics
-spec add_eventual(client_key(), non_neg_integer()) -> ok.
add_eventual(Key, ToAdd) ->
    UpdateFun = fun(ReplicaId, CRDT) -> pncounter:update_add(ReplicaId, ToAdd, CRDT) end,
    write_helper(Key, fun crdt_proposer:write_eventual/5, UpdateFun).

%% Decrease counter with strong consistency semantics
-spec subtract(client_key(), non_neg_integer()) -> ok.
subtract(Key, ToSubtract) ->
    UpdateFun = fun(ReplicaId, CRDT) -> pncounter:update_subtract(ReplicaId, ToSubtract, CRDT) end,
    write_helper(Key, fun crdt_proposer:write/5, UpdateFun).

%% Decrease counter with evenutally strong consistency semantics
-spec subtract_eventual(client_key(), non_neg_integer()) -> ok.
subtract_eventual(Key, ToSubtract) ->
    UpdateFun = fun(ReplicaId, CRDT) -> pncounter:update_subtract(ReplicaId, ToSubtract, CRDT) end,
    write_helper(Key, fun crdt_proposer:write_eventual/5, UpdateFun).

-spec write_helper(client_key(), fun((_,_,_,_,_) -> ok), crdt:update_fun()) -> ok.
write_helper(Key, APIFun, UpdateFun) ->
    APIFun(crdt_db, self(), ?RT:hash_key(Key), pncounter, UpdateFun),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({write_done}, ok)
    after 1000 ->
        log:log("~p write hangs at key ~p, ~p~n",
                [self(), Key, erlang:process_info(self(), messages)]),
        receive
            ?SCALARIS_RECV({write_done},
                            begin
                                log:log("~p write was only slow at key ~p~n",
                                        [self(), Key]),
                                ok
                            end)
        end
    end.
