%  @copyright 2007-2011 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    grouped_node API
%% @end
%% @version $Id$
-module(group_api).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("client_types.hrl").

-export([paxos_read/1, paxos_write/2,
         bench_read/1, bench_write/1,
         quorum_read/1,
         route/2]).

-spec quorum_read(client_key()) -> any().
quorum_read(Key) ->
    HashedKey = ?RT:hash_key(Key),
    Req = {quorum_read, comm:this(), HashedKey},
    route(Req, HashedKey),
    receive
        {quorum_read_response, HashedKey, Value} ->
            Value
    end.

-spec paxos_read(client_key()) -> any().
paxos_read(Key) ->
    HashedKey = ?RT:hash_key(Key),
    Req = {paxos_read, comm:this(), HashedKey},
    route(Req, HashedKey),
    receive
        {paxos_read_response, {value, Value, Version}} ->
            {value, Value, Version};
        X ->
            X
    end.

-spec paxos_write(client_key(), any()) -> any().
paxos_write(Key, Value) ->
    HashedKey = ?RT:hash_key(Key),
    Req = {paxos_write, comm:this(), HashedKey, Value},
    route(Req, HashedKey),
    receive
        {paxos_write_response, HashedKey, Value} ->
            ok;
        X ->
            X
    end.

-spec route(any(), ?RT:key()) -> ok.
route(Message, HashedKey) ->
    Node = pid_groups:find_a(group_node),
    comm:send_local(Node, {route, HashedKey, 0, Message}),
    ok.

-spec bench_read(pos_integer()) -> ok.
bench_read(N) ->
    Before = erlang:now(),
    run_read(N),
    After = erlang:now(),
    Diff = timer:now_diff(After, Before),
    io:format("~ps ; ~p1/s~n", [Diff / 1000000.0, N / Diff * 1000000.0]),
    ok.

-spec bench_write(pos_integer()) -> ok.
bench_write(N) ->
    Before = erlang:now(),
    run_write(N),
    After = erlang:now(),
    Diff = timer:now_diff(After, Before),
    io:format("~ps ; ~p1/s~n", [Diff / 1000000.0, N / Diff * 1000000.0]),
    ok.

run_read(0) ->
    ok;
run_read(N) ->
    paxos_read("1"),
    run_read(N - 1).

run_write(0) ->
    ok;
run_write(N) ->
    paxos_write("1", 2),
    run_write(N - 1).
