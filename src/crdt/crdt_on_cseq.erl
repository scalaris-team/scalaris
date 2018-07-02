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
%% @doc    API to CRDT-based state machine replication
%% @end
-module(crdt_on_cseq).
-author('skrzypczak@zib.de').

-include("scalaris.hrl").
-include("client_types.hrl").

%% API with linearizable semantics (strong consistency)
-export([read/3]).
-export([write/3]).

%% API with strong eventual consistency semantics
-export([read_eventual/3]).
-export([write_eventual/3]).

-spec read(client_key(), crdt:crdt_module(), crdt:query_fun()) -> {ok, client_value()}.
read(Key, DataType, QueryFun) ->
    Mod = crdt:proposer_module(),
    FunName = read,
    read_helper(Key, Mod, FunName, DataType, QueryFun).

-spec read_eventual(client_key(), crdt:crdt_module(), crdt:query_fun()) -> {ok, client_value()}.
read_eventual(Key, DataType, QueryFun) ->
    Mod = crdt:proposer_module(),
    FunName = read_eventual,
    read_helper(Key, Mod, FunName, DataType, QueryFun).

-spec read_helper(client_key(), module(), atom(), crdt:crdt_module(), crdt:query_fun()) ->
    {ok, client_value()}.
read_helper(Key, ApiMod, ApiFun, DataType, QueryFun) ->
    ApiMod:ApiFun(crdt_db, self(), ?RT:hash_key(Key), DataType, QueryFun),
    trace_mpath:thread_yield(),
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


-spec write(client_key(), crdt:crdt_module(), crdt:update_fun()) -> ok.
write(Key, DataType, UpdateFun) ->
    Mod = crdt:proposer_module(),
    FunName = write,
    write_helper(Key, Mod, FunName, DataType, UpdateFun).

-spec write_eventual(client_key(), crdt:crdt_module(), crdt:update_fun()) -> ok.
write_eventual(Key, DataType, UpdateFun) ->
    Mod = crdt:proposer_module(),
    FunName = write_eventual,
    write_helper(Key, Mod, FunName, DataType, UpdateFun).

-spec write_helper(client_key(), module(), atom(), crdt:crdt_module(), crdt:update_fun()) -> ok.
write_helper(Key, ApiMod, ApiFun, DataType, UpdateFun) ->
    ApiMod:ApiFun(crdt_db, self(), ?RT:hash_key(Key), DataType, UpdateFun),
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

