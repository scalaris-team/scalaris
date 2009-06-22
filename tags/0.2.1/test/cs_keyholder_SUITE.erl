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
%%% File    : cs_keyholder_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/cs_keyholder.erl
%%%
%%% Created :  12 Jun 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(cs_keyholder_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [getset_key].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    Pid = spawn(fun () -> 
			config:start_link(["scalaris.cfg"]),
			crypto:start(),
			process_dictionary:start_link_for_unittest(), 
			cs_keyholder:start_link(foo), 
			timer:sleep(5000) 
		end),
    timer:sleep(1000),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    exit(Pid, normal),
    ok.

getset_key(_Config) ->
    util:dump2(),
    process_dictionary:register_process(foo, foo, self()),
    cs_keyholder:get_key(),
    cs_keyholder:set_key("getset_key"),
    ?equals(cs_keyholder:get_key(), "getset_key"),
    ok.

