%% @copyright 2014 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Thorsten Schuett <schuett@zib.de>
%% @author Nico Kruber <kruber@zib.de>
%% @version $Id: api_tx_SUITE.erl 5051 2013-07-26 12:21:14Z kruber@zib.de $
-module(api_tx_proto_sched_SUITE).
-author('schintke@zib.de').
-vsn('$Id: api_tx_SUITE.erl 5051 2013-07-26 12:21:14Z kruber@zib.de $').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

%% start proto scheduler for this suite
-define(proto_sched(Action),
        fun() -> %% use fun to have fresh, locally scoped variables
                case Action of
                    start ->
                        %% ct:pal("Starting proto scheduler"),
                        proto_sched:thread_num(1),
                        proto_sched:thread_begin();
                    stop ->
                        %% is a ring running?
                        case erlang:whereis(pid_groups) =:= undefined
                            orelse pid_groups:find_a(proto_sched) =:= failed of
                            true -> ok;
                            false ->
                                %% then finalize proto_sched run:
                                %% try to call thread_end(): if this
                                %% process was running the proto_sched
                                %% thats fine, otherwise thread_end()
                                %% will raise an exception
                                proto_sched:thread_end(),
                                proto_sched:wait_for_end(),
                                ct:pal("Proto scheduler stats: ~.2p",
                                       %%[proto_sched:info_shorten_messages(
                                       %%   proto_sched:get_infos(), 200)]),
                                       [lists:keydelete(nums_chosen_from, 1,
                                        lists:keydelete(delivered_msgs, 1,
                                        proto_sched:get_infos()))]),
                                proto_sched:cleanup()
                        end
                end
        end()).
-include("api_tx_SUITE.hrl").

groups() ->
    [%% implementation in api_tx_SUITE.hrl
     %% (shared with api_tx_proto_sched_SUITE.erl)
     {proto_sched_ready, [sequence],
      proto_sched_ready_tests()}
    ].


all()   -> [ {group, proto_sched_ready} ].

suite() -> [ {timetrap, {seconds, 200}} ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        write_test_race_mult_rings -> %% this case creates its own ring
            Config;
        tester_encode_decode -> %% this case does not need a ring
            Config;
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
            timer:sleep(1000),
            ?ASSERT(ok =:= unittest_helper:check_ring_size_fully_joined(4)),
            unittest_helper:wait_for_stable_ring_deep(),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.
