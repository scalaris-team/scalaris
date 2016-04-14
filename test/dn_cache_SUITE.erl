%  @copyright 2010-2015 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Test suite for the dn_cache module.
%% @end
%% @version $Id$
-module(dn_cache_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

all() ->
    [dn_detection].

suite() ->
    [
     {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) ->
    unittest_helper:start_minimal_procs(Config, [], true).

end_per_suite(Config) ->
    _ = unittest_helper:stop_minimal_procs(Config),
    ok.

dn_detection(Config) ->
    pid_groups:join_as(dn_cache_group, node),
    config:write(zombieDetectorInterval, 10),
    NodePid = fake_node(),
    NodePidG = comm:make_global(NodePid),
    Node = node:new(NodePidG, ?RT:hash_key("0"), 0),
    {ok, _DNCachePid} = dn_cache:start_link(dn_cache_group),
    
    dn_cache:subscribe(),
    comm:send(NodePidG, {sleep}),
    dn_cache:add_zombie_candidate(Node),
    ?expect_no_message(),
    
    comm:send(NodePidG, {continue}),
    ?expect_message({zombie, Node}),
    
    exit(NodePid, kill),
    % there might be more zombie messages:
    ?consume_all_messages({zombie, Node}),
    
    Config.

fake_node() ->
    element(1, unittest_helper:start_subprocess(
              fun() -> pid_groups:join_as(dn_cache_group, node) end,
              fun fake_process/0)).

fake_process() ->
    receive
        {ping, Pid} ->
            comm:send(Pid, {pong, dht_node}),
            fake_process();
        {sleep} ->
            receive
                {continue} -> fake_process()
            end
    end.
