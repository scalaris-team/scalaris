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
%% @doc    Unit tests using the protocol scheduler
%% @end
%% @version $Id$
-module(proto_sched_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [
     %{tester_tests, [sequence], []},
     {rbr_tests, [sequence], [
                              test_qwrite_qwrite_qread
                             ]}
    ].

all() ->
    [
     %{group, tester_tests},
     %{group, rbr_tests}  <- hier anschalten
     ].

suite() -> [ {timetrap, {seconds, 120}} ].

%group(tester_tests) ->
%    [{timetrap, {seconds, 400}}];
group(rbr_tests) ->
    [{timetrap, {seconds, 4}}].


init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    _ = unittest_helper:end_per_suite(Config),
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(TestCase, Config) ->
    case TestCase of
        _ ->
            %% stop ring from previous test case (it may have run into a timeout
            unittest_helper:stop_ring(),
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(1, [{config, [{log_path, PrivDir}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

test_qwrite_qwrite_qread(_Config) ->
    DB = lease_db1,
    ContentCheck = fun content_check/3,
    Self = comm:reply_as(self(), 2, {test_rbr, '_'}),
    Id = 1,
    Value1 = value_1,
    Value2 = value_2,

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% begin test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    proto_sched:start(),
    proto_sched:start_deliver(),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, Value1),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, Value2),
    receive
        X1 ->
            ct:pal("got ~p", [X1])
    end,
    receive
        X2 ->
            ct:pal("got ~p", [X2])
    end,
    proto_sched:stop(),
    proto_sched:cleanup(),
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% end test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    rbrcseq:qread(DB, Self, Id),
    receive
        X3 ->
            ct:pal("got ~p", [X3])
    end,
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% content checks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
content_check(_Value, _WriteFilter, _Cookie) ->
    {true, null}.

