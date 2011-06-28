%  @copyright 2010-2011 Zuse Institute Berlin
%  @end
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
%%% File    dn_cache_SUITE.erl
%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Test suite for the dn_cache module.
%%% @end
%%% Created : 26 Aug 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(dn_cache_SUITE).

-author('kruber@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").

all() ->
    [dn_detection].

suite() ->
    [
     {timetrap, {seconds, 30}}
    ].

-spec spawn_config_processes(Config::[tuple()]) -> pid().
spawn_config_processes(Config) ->
    ok = unittest_helper:fix_cwd(),
    error_logger:tty(true),
    unittest_helper:start_process(
      fun() ->
              {ok, _GroupsPid} = pid_groups:start_link(),
              {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
              ConfigOptions = unittest_helper:prepare_config([{config, [{log_path, PrivDir}]}]),
              {ok, _ConfigPid} = config:start_link2(ConfigOptions),
              {ok, _LogPid} = log:start_link(),
              {ok, _CommPid} = sup_comm_layer:start_link(),
              comm_server:set_local_address({127,0,0,1}, unittest_helper:get_scalaris_port())
      end).

-spec stop_config_processes(pid()) -> ok.
stop_config_processes(Pid) ->
    error_logger:tty(false),
    log:set_log_level(none),
    exit(Pid, kill),
    unittest_helper:stop_pid_groups(),
    ok.

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    Pid = spawn_config_processes(Config2),
    [{wrapper_pid, Pid} | Config2].

end_per_suite(Config) ->
    {wrapper_pid, Pid} = lists:keyfind(wrapper_pid, 1, Config),
    stop_config_processes(Pid),
    _ = unittest_helper:end_per_suite(Config),
    ok.

dn_detection(Config) ->
    pid_groups:join_as("dn_cache_group", node),
    config:write(zombieDetectorInterval, 10),
    NodePid = fake_node(),
    NodePidG = comm:make_global(NodePid),
    Node = node:new(NodePidG, ?RT:hash_key("0"), 0),
    {ok, _DNCachePid} = dn_cache:start_link("dn_cache_group"),
    
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
    unittest_helper:start_subprocess(fun() -> pid_groups:join_as("dn_cache_group", node) end, fun fake_process/0).

fake_process() ->
    receive
        {ping, Pid} ->
            comm:send(Pid, {pong}),
            fake_process();
        {sleep} ->
            receive
                {continue} -> fake_process()
            end
    end.
