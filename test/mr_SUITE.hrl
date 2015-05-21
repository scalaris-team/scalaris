% @copyright 2010-2014 Zuse Institute Berlin

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

-include("scalaris.hrl").
-include("unittest.hrl").

-define(DATA, [{"1", "MapReduce allows for distributed processing of the map and reduction operations"}
                , {"3", "Provided each mapping operation can run independently all maps may be performed in parallel"}
                , {"5", "This example data set contains one single word only once"}
                , {"7", "Now I am too lazy to construct another sentence that actually makes sense"}
           ]).

tests_avail() ->
    [test_sane_result]. %%, test_error_on_kill].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% make new ring
    unittest_helper:make_ring(2),
    %% wait for all nodes to finish their join before writing data
    unittest_helper:check_ring_size_fully_joined(2),
    unittest_helper:wait_for_stable_ring_deep(),
    Pid = erlang:spawn(fun add_data/0),
    util:wait_for_process_to_die(Pid),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

test_sane_result(_Config) ->
    ?proto_sched2(setup, 1),
    ?proto_sched(start),

    Res = api_mr:start_job(get_wc_job_erl()),

    %% each word only occurs once
    ?proto_sched(stop),
    ?proto_sched2(cleanup, []),
    check_results(Res),
    ok.

test_error_on_kill(_Config) ->
    ?proto_sched2(setup, 2),
    MrPid = spawn_link(fun() ->
                               ?proto_sched(start),
                               Res = api_mr:start_job(get_wc_job_erl()),
                               ?equals(Res, {error, node_died}),
                               ?proto_sched(stop)
                       end),
    KillPid = spawn_link(fun() ->
                                 ?proto_sched(start),
                                 [_] = api_vm:kill_nodes(1),
                                 ?proto_sched(stop)
                         end),
    ?proto_sched2(cleanup, [MrPid, KillPid]),
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
    {[{map, erlanon, Map},
      {reduce, erlanon, Reduce}],
     []}.

add_data() ->
    X = lists:duplicate(length(?DATA), {ok}),
    X = [api_tx:write(Key, {Key, Value}) || {Key, Value} <- ?DATA],
    ok.

check_results(Results) ->
    Mapped = lists:foldl(fun({_K, V}, Acc) ->
                                   T = string:tokens(string:to_lower(V), " "),
                                   [{W, 1} || W <- T] ++ Acc
                           end, [], ?DATA),
    Expected = lists:foldl(fun({K, V}, Acc) ->
                                   case lists:keyfind(K, 1, Acc) of
                                       false ->
                                           [{K, V} | Acc];
                                       {T, C} ->
                                           lists:keyreplace(T, 1, Acc, {T, C + V})
                                   end
                           end, [], Mapped),
    ?equals(length(Expected),
            length(Results)),
    Zipped = lists:zip(lists:sort(Expected),
                       lists:sort(Results)),
    _ = [?equals(X, Y) || {X, Y} <- Zipped],
    ok.
