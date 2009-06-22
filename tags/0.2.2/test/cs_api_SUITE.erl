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
%%% File    : cs_api_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/cs_api.erl
%%%
%%% Created :  31 Jul 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(cs_api_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [read, write, test_and_set].

suite() ->
    [
     {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    Pid = spawn(fun () ->
			process_dictionary:start_link_for_unittest(), 
			boot_sup:start_link(), 
			timer:sleep(120000) 
		end),
    timer:sleep(15000),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    exit(Pid, kill),
    ok.

read(_Config) ->
    ?equals(cs_api:read("ReadKey"), {fail, not_found}),
    ?equals(cs_api:write("ReadKey", "IsSet"), ok),
    ?equals(cs_api:read("ReadKey"), "IsSet"),
    ok.

write(_Config) ->
    ?equals(cs_api:write("WriteKey", "IsSet"), ok),
    ?equals(cs_api:read("WriteKey"), "IsSet"), 
    ?equals(cs_api:write("WriteKey", "IsSet2"), ok),
    ?equals(cs_api:read("WriteKey"), "IsSet2"),
    ok.

test_and_set(_Config) ->
    ?equals(cs_api:test_and_set("TestAndSetKey", "", "IsSet"), ok),
    ?equals(cs_api:test_and_set("TestAndSetKey", "", "IsSet"), {fail, {key_changed, "IsSet"}}),
    ?equals(cs_api:test_and_set("TestAndSetKey", "IsSet", "IsSet2"), ok),
    ok.
