%% @copyright 2012-2013 Zuse Institute Berlin

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
%% @doc    Unit tests for l_on_cseq based on a mockup dht_node
%% @end
%% @version $Id$
-module(mockup_l_on_cseq_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{merge_tests, [sequence], [
                                test_merge
                               ]}
    ].

all() ->
    [
     {group, merge_tests}
     ].

suite() -> [ {timetrap, {seconds, 90}} ].

group(merge_tests) ->
    [{timetrap, {seconds, 400}}];
group(_) ->
    suite().


init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_group(Group, Config) ->
    unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(TestCase, Config) ->
    case TestCase of
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            Ids = rt_simple:get_replica_keys(0),
            unittest_helper:make_ring_with_ids(Ids, [{config, [{log_path, PrivDir},
                                                               {leases, true}]}]),
            mockup_l_on_cseq:start_link(),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% merge unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_merge(_Config) ->
    {L1, L2} = mockup_l_on_cseq:create_two_adjacent_leases(),
    % join group
    Pid = pid_groups:find_a(mockup_l_on_cseq),
    pid_groups:join(pid_groups:group_of(Pid)),
    % do merge
    % evil, but l_on_cseq:lease_merge sends to a real dht_node
    comm:send_local(Pid, {l_on_cseq, merge, L1, L2, self()}),
    receive
        {merge, success, _L2, _L1} -> ok
    end,
    % no renews during merge!
    ?assert(0 =:= mockup_l_on_cseq:get_renewal_counter()),
    % check L1
    case l_on_cseq:read(l_on_cseq:get_id(L1)) of
        {ok, L1_} -> ?assert(l_on_cseq:get_aux(L1_) =:= {invalid,merge,stopped});
        {fail, not_found} -> ?assert(false)
    end,
    % check L2
    case l_on_cseq:read(l_on_cseq:get_id(L2)) of
        {ok, L2_} ->
            ?assert(l_on_cseq:get_range(L2_) =:= intervals:union(l_on_cseq:get_range(L1),
                                                                 l_on_cseq:get_range(L2)));
        {fail, not_found} -> ?assert(false)
    end,
    true.
