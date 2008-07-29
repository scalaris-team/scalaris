%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : pubsub_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/pubsub/*.erl
%%%
%%% Created :  22 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(pubsub_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [test_db].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    Pid = spawn(fun () -> process_dictionary:start_link_for_unittest(), boot_sup:start_link(), timer:sleep(20000) end),
    timer:sleep(12000),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    exit(Pid, kill),
    ok.

test_db(_Config) ->
    ?equals(pubsub.pubsub_api:get_subscribers("TestTopic"), []),
    ?equals(pubsub.pubsub_api:subscribe("TestTopic", "http://localhost:8000/pubsub.yaws"), ok),
    %ct:print(default, "~p~n", [pubsub.pubsub_api:get_subscribers("TestTopic")]),
    ?equals(pubsub.pubsub_api:get_subscribers("TestTopic"), ["http://localhost:8000/pubsub.yaws"]),
    ?equals(pubsub.pubsub_api:publish("TestTopic", "TestContent"), ok),
    ok.

