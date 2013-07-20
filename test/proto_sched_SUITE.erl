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
                              test_qwrite_qwrite_qread,
                              test_kv_on_cseq_write_2
                             ]}
    ].

all() ->
    [
     %{group, tester_tests},
     {group, rbr_tests}
     ].

suite() -> [ {timetrap, {seconds, 120}} ].

%group(tester_tests) ->
%    [{timetrap, {seconds, 400}}];
group(rbr_tests) ->
    [{timetrap, {seconds, 200}}].


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
            unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    unittest_helper:stop_ring(),
    Config.

test_kv_on_cseq_write_2(_Config) ->
    iter(fun kv_on_cseq_write/0, 1000).

kv_on_cseq_write() ->
    proto_sched:start(),
    Pid = self(),
    spawn(fun() ->
                  proto_sched:start(),
                  kv_on_cseq:write([], 7),
                  Pid ! ok
          end),
    spawn(fun() ->
                  proto_sched:start(),
                  kv_on_cseq:write([], 8),
                  Pid ! ok
          end),
    timer:sleep(10),
    proto_sched:start_deliver(),
    receive ok -> ok end,
    receive ok -> ok end,
    proto_sched:stop(),
    _Infos = proto_sched:get_infos(),
    proto_sched:cleanup().

test_qwrite_qwrite_qread(_Config) ->
    iter(fun qwrite_qwrite_qread/0, 1000).

qwrite_qwrite_qread() ->
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
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, Value1),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, Value2),
    proto_sched:start_deliver(),
    receive_answer(),
    receive_answer(),
    proto_sched:stop(),
    Infos = proto_sched:get_infos(),
    proto_sched:cleanup(),
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% end test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    rbrcseq:qread(DB, Self, Id),
    {test_rbr, {qread_done, _, _, Val}} = receive_answer(),
    %ct:pal("got ~p", [Val]),
    %ct:pal("~p", [Infos]),
    case Val of
        Value1 -> ok;
        Value2 -> ok;
        X ->
            ct:pal("~p", [Infos]),
            ?ct_fail("unexpected result ~p", X)
    end,
    ok.

receive_answer() ->
    receive
        ?SCALARIS_RECV(X1, X1)
        end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utlities
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
content_check(_Value, _WriteFilter, _Cookie) ->
    {true, null}.

iter(_Fun, 0) ->
    ok;
iter(Fun, 1) ->
    Fun(), ok;
iter(Fun, Count) ->
    Fun(),
    iter(Fun, Count - 1).
