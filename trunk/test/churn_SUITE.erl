%  Copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : churn_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for transactions under churn
%%%
%%% Created :  27 Jul 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(churn_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [transactions_one_failure_four_nodes].

suite() ->
    [
     {timetrap, {seconds, 120}}
    ].

init_per_suite(Config) ->
    file:set_cwd("../bin"),
    Pid = unittest_helper:make_ring_with_ids(?RT:get_replica_keys(?RT:hash_key(0))),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    %error_logger:tty(false),
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    unittest_helper:stop_ring(Pid),
    ok.

transactions_one_failure_four_nodes(_) ->
    ?equals(ok, cs_api_v2:write(0, 1)),
    ?equals(1, cs_api_v2:read(0)),
    admin:del_nodes(1),
    unittest_helper:check_ring_size(3),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:wait_for_stable_ring_deep(),
    wait_for(fun () ->
                     1 == cs_api_v2:read(0)
             end),
    ?equals(1, cs_api_v2:read(0)),
    ?equals(ok, cs_api_v2:write(0, 2)),
    ?equals(2, cs_api_v2:read(0)),
    ok.

wait_for(F) ->
    case F() of
        true ->
            ok;
        false ->
            timer:sleep(1000),
            wait_for(F)
    end.
