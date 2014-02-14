% @copyright 2010-2013 Zuse Institute Berlin

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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc    Unit tests for the map reduce protocol.
%% @end
%% @version $Id$
-author('fajerski@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("unittest.hrl").

tests_avail() ->
    %% [test_sane_result].
    [test_sane_result,
     test_join].
    %%  test_leave].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% stop ring from previous test case (it may have run into a timeout)
    unittest_helper:stop_ring(),
    %% make new ring
    unittest_helper:make_ring(2),
    %% wait for all nodes to finish their join before writing data
    unittest_helper:check_ring_size_fully_joined(2),
    Pid = erlang:spawn(fun add_data/0),
    util:wait_for_process_to_die(Pid),
    %% wait a bit for the rm-processes to settle
    timer:sleep(500),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ?proto_sched(stop),
    unittest_helper:stop_ring(),
    ok.

test_sane_result(_Config) ->
    ?proto_sched(start),
    Res = api_mr:start_job(get_wc_job_erl()),
    %% each word only occurs once
    check_results(Res),
    ok.

test_join(_Config) ->
    ?proto_sched(start),
    Pids = pid_groups:find_all(dht_node),
    ct:pal("setting breakpoint before starting reduce phase"),
    NextPhase = fun(Msg, State) ->
            case Msg of
                {mr, next_phase, JobId, 1, _Interval} ->
                    comm:send_local(self(), Msg),
                    drop_single;
                _ ->
                    false
            end
    end,
    [gen_component:bp_set_cond(Pid, NextPhase, mr_bp) || Pid <- Pids],
    ct:pal("starting job that triggers breakpoint"),
    MrPid = spawn_link(fun() ->
                    ct:pal("starting mr job"),
                    Res = api_mr:start_job(get_wc_job_erl()),
                    ct:pal("mr job finished"),
                    check_results(Res)
            end),
    timer:sleep(1000),
    ct:pal("adding node to provoke slide"),
    _ = api_vm:add_nodes(2),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size_fully_joined(4),
    ct:pal("ring fully joined (4)"),
    ct:pal("removing breakpoints"),
    [gen_component:bp_del(Pid, mr_bp) || Pid <- Pids],
    util:wait_for_process_to_die(MrPid),
    ok.

test_leave(_Config) ->
    _ = api_vm:add_nodes(2),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size_fully_joined(4),
    ?proto_sched(start),
    Res = api_mr:start_job(get_wc_job_erl()),
    _ = api_vm:kill_nodes(2),
    unittest_helper:wait_for_stable_ring(),
    unittest_helper:check_ring_size_fully_joined(2),
    check_results(Res),
    ok.

get_wc_job_erl() ->
    Map = fun({_Key, Line}) ->
        Tokens = string:tokens(Line, " \n,.;:?!()\"'-_"),
        [{string:to_lower(X),1} || X <- Tokens]
    end,
    Reduce = fun(KVList) ->
            lists:map(fun({K, V}) ->
                              {K, lists:sum(V)}
                        end, KVList)
    end,
    {[{map, {erlanon, Map}},
      {reduce, {erlanon, Reduce}}],
     []}.

add_data() ->
    Data = [{"1", "MapReduce allows for distributed processing of the map and reduction operations."}
            , {"2", "Provided each mapping operation can run independently, all maps may be performed in parallel."}
            , {"3", "This example data set contains one single word only once."}
            , {"4", "Now I am too lazy to construct another sentence that actually makes sense."}
           ],
    [api_tx:write(Key, {Key, Value}) || {Key, Value} <- Data],
    ok.

check_results(Results) ->
    ?equals(length(Results),
            lists:foldl(fun({_Word, Count}, AccIn) ->
                                AccIn + Count
                        end, 0, Results)).
