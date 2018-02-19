%% @copyright 2012-2018 Zuse Institute Berlin

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

-dialyzer([{no_return, basic_start_bench_and_kill_it/1}]).

%%-define(TRACE(X,Y), log:log("proto_sched_SUITE: " ++ X,Y)).
-define(TRACE(X,Y), ok).

groups() ->
    [
     %{tester_tests, [sequence], []},
     {basic_proto_sched_tests, [sequence], %% {repeat, 100}],
      [ basic_empty_sched,basic_empty_sched,
        basic_client_ping_pong,
        basic_client_ping_pong2,
        basic_client_ping_pong_xing,
        basic_begin_after_running,
        basic_duplicate_begin,
        basic_uninfected_thread_end,
        basic_duplicate_thread_num,
        basic_cleanup_before_thread_end,
        basic_cleanup_infected,
        basic_cleanup_non_existing,
        basic_cleanup_DOWN_interplay,
        basic_cleanup_send_interplay,
        basic_thread_yield_outside_proto_sched,
        basic_slow_msg_handler,
        basic_bench_increment%,
%%        basic_start_bench_and_kill_it
      ]},
     {rbr_tests, [sequence],
      [ test_kv_on_cseq_read,
        test_kv_on_cseq_read_2,
        test_kv_on_cseq_write,
        test_kv_on_cseq_write_2,
        test_qwrite_qwrite_qread
      ]}
    ].

all() ->
    [
     {group, basic_proto_sched_tests},
     %{group, tester_tests},
     {group, rbr_tests}
     ].

suite() -> [ {timetrap, {seconds, 12}} ].

%group(tester_tests) ->
%    [{timetrap, {seconds, 400}}];
group(basic_proto_sched_tests) ->
    [{timetrap, {seconds, 10}}];
group(rbr_tests) ->
    [{timetrap, {seconds, 40}}];
group(_) ->
    suite().

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_ring(4, [{config, [{log_path, PrivDir}]}]),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

basic_empty_sched(_Config) ->
    %% an empty thread should be a valid run
    proto_sched:thread_num(1),
    ?ASSERT(not proto_sched:infected()),
    proto_sched:thread_begin(),
    ?ASSERT(proto_sched:infected()),
    proto_sched:thread_end(),
    ?ASSERT(not proto_sched:infected()),
    proto_sched:wait_for_end(),
    ?ASSERT(not proto_sched:infected()),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    ?ASSERT(not proto_sched:infected()),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_client_ping_pong(_Config) ->
    %% sending ourselves a single message and receiving it again
    proto_sched:thread_num(1),
    proto_sched:thread_begin(),
    ?ASSERT(proto_sched:infected()),
    comm:send_local(self(), {hello_world}),
    proto_sched:thread_yield(),
%%    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({hello_world},
                       begin
                           ?ASSERT(proto_sched:infected()),
                           ok
                       end)
        end,
    ?ASSERT(proto_sched:infected()),
    proto_sched:thread_end(),
    ?ASSERT(not proto_sched:infected()),
    proto_sched:wait_for_end(),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_client_ping_pong2(_Config) ->
    %% sending ourselves a single message and receiving it again
    %% done in two independent processes
    proto_sched:thread_num(2),
    spawn(fun() ->
                  proto_sched:thread_begin(),
                  comm:send_local(self(), {hello_world}),
                  proto_sched:thread_yield(),
                  receive
                      ?SCALARIS_RECV({hello_world},
                                     begin
                                         ?ASSERT(proto_sched:infected()),
                                         ok
                                     end)
                      end,
                  ?ASSERT(proto_sched:infected()),
                  proto_sched:thread_end()
          end),
    proto_sched:thread_begin(),
    ?ASSERT(proto_sched:infected()),
    comm:send_local(self(), {hello_world}),
    proto_sched:thread_yield(),
    receive
        ?SCALARIS_RECV({hello_world},
                       begin
                           ?ASSERT(proto_sched:infected()),
                           ok
                       end)
        end,
    ?ASSERT(proto_sched:infected()),
    proto_sched:thread_end(),
    ?ASSERT(not proto_sched:infected()),
    proto_sched:wait_for_end(),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_client_ping_pong_xing(_Config) ->
    %% two processes sending each other a single message and receiving
    %% one from the other process. (Xing means 'crossing').

    %% to protect from main thread being faster than the spawned
    %% process and thereby observing early cleanups, the spawned
    %% process first receives and then sends. So, the main thread
    %% waits for the spawned process to finish.
    Parent = self(),
    ct:pal("Parent is: ~p", [Parent]),
    Child = spawn(fun() ->
                          %% not infected
                          trace_mpath:thread_yield(),
                          %% not infected
                          ct:pal("Child receives"),
                          receive
                              ?SCALARIS_RECV({from_parent},
                                             begin
                                                 ?ASSERT(proto_sched:infected()),
                                                 ok
                                             end)
                              end,
                          ?ASSERT(proto_sched:infected()),
                          comm:send_local(Parent, {from_child})
          end),

    proto_sched:thread_num(1),
    ct:pal("Child is: ~p", [Child]),
    proto_sched:thread_begin(),
    ?ASSERT(proto_sched:infected()),
    ct:pal("Parent sends"),
    comm:send_local(Child, {from_parent}),
    proto_sched:thread_yield(),
    ct:pal("Parent receives"),
    receive
        ?SCALARIS_RECV({from_child},
                       begin
                           ?ASSERT(proto_sched:infected()),
                           ok
                       end)
        end,
    ?ASSERT(proto_sched:infected()),
    ct:pal("Parent ends"),
    proto_sched:thread_end(),
    ?ASSERT(not proto_sched:infected()),
    proto_sched:wait_for_end(),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_begin_after_running(_Config) ->
    %% when a proto_sched runs, it should not allow further threads to
    %% join, because when the new thread joins could depend on the
    %% executing environment, so reproducability could be violated
    proto_sched:thread_num(1),
    Pid = self(),
    Child = spawn(fun() ->
                          proto_sched:thread_begin(),
                          Pid ! {started},
                          receive {done} -> ok end,
                          proto_sched:thread_end()
          end),
    receive {started} -> ok end,
    ?expect_exception(proto_sched:thread_begin(),
                      throw, 'proto_sched:thread_begin-but_already_running'),
    Child ! {done},
    proto_sched:wait_for_end(),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_duplicate_begin(_Config) ->
    %% each execution thread should only claim to begin once.
    proto_sched:thread_num(1),
    proto_sched:thread_begin(),
    ct:pal("We were scheduled and can start our thread"),
    ?ASSERT(proto_sched:infected()),
    ct:pal("We try to begin again.."),
    ?expect_exception(proto_sched:thread_begin(),
                      throw, duplicate_thread_begin),
    proto_sched:thread_end(),
    proto_sched:wait_for_end(),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_uninfected_thread_end(_Config) ->
    %% thread_end should only be called in infected contexts.
    ?ASSERT(not proto_sched:infected()),
    ?expect_exception(proto_sched:thread_end(),
                      throw, duplicate_or_uninfected_thread_end),
    proto_sched:thread_num(1),
    proto_sched:thread_begin(),
    proto_sched:thread_end(),
    %% duplicate end raises the same error...
    ?expect_exception(proto_sched:thread_end(),
                      throw, duplicate_or_uninfected_thread_end),
    proto_sched:wait_for_end(),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_duplicate_thread_num(_Config) ->
    %% the thread_num is defined once and fixed until
    %% proto_sched:cleanup().
    proto_sched:thread_num(1),
    ?expect_exception(proto_sched:thread_num(2),
                      throw, 'proto_sched:thread_num_failed'),
    proto_sched:thread_begin(),
    proto_sched:thread_end(),
    proto_sched:wait_for_end(),
    proto_sched:cleanup().

basic_cleanup_before_thread_end(_Config) ->
    %% if a steering thread calls proto_sched:cleanup() the remaining
    %% messages and threads finish without proto_sched and these
    %% threads are allowed to call proto_sched;end() in an uninfected
    %% environment.
    proto_sched:thread_num(1),
    Parent = self(),
    Child =
        spawn(fun() ->
                      proto_sched:thread_begin(),
                      ?TRACE("Sending parent", []),
                      %% to steer the flow control, we have to send
                      %% directly here (uninfected)... (normally forbidden)
                      Parent ! {ready_for_cleanup},
                      proto_sched:thread_yield(),
                      ?TRACE("Receive guard", []),
                      receive
                          ?SCALARIS_RECV({hello_world},
                                         begin
                                             ?ASSERT(proto_sched:infected()),
                                             ok
                                         end)
                          end,
                      ?ASSERT(proto_sched:infected()),
                      ?TRACE("Call thread_end", []),
                      proto_sched:thread_end()
              end),

    receive
        {ready_for_cleanup} ->
            ok
    end,
    ?TRACE("Initiating cleanup", []),
    proto_sched:wait_for_end(),
    %% TODO: check statistics
    proto_sched:cleanup(),
    ?TRACE("Sending back", []),
    comm:send_local(Child, {hello_world}),
    ?ASSERT(not proto_sched:infected()).

basic_cleanup_infected(_Config) ->
    %% inside a proto_sched (before thread_end) cleanup is prohibited.
    proto_sched:thread_num(1),
    proto_sched:thread_begin(),
    ?expect_exception(proto_sched:cleanup(),
                      throw, 'proto_sched:cleanup_called_infected'),
    proto_sched:thread_end(),
    proto_sched:wait_for_end(),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_cleanup_non_existing(_Config) ->
    %% cleanup of a non-existing traceid is prohibited.
    ?expect_exception(proto_sched:cleanup(),
                      throw, 'proto_sched:cleanup_trace_not_found').

basic_cleanup_DOWN_interplay(_Config) ->
    %% cleanup before 'DOWN' detected, but delivered.
    %% using the status field for 'to_be_cleaned' made problems with
    %% 'DOWN' reports, so it had to be separated in a separate record field
    proto_sched:thread_num(1),
    Parent = self(),
    _Child =
        spawn(fun() ->
                      proto_sched:thread_begin(),
                      ?TRACE("Sending parent", []),
                      %% to steer the flow control, we have to send
                      %% directly here (uninfected)... (normally forbidden)
                      Parent ! {ready_for_cleanup},
                      %% let cleanup arrive at proto_sched
                      timer:sleep(300)
                      %% generate a 'DOWN' report by ending this process
              end),

    receive
        {ready_for_cleanup} ->
            ok
    end,
    proto_sched:cleanup(),
    ?ASSERT(not proto_sched:infected()).

basic_cleanup_send_interplay(_Config) ->
    %% cleanup before 'DOWN' detected, but delivered.
    %% using the status field for 'to_be_cleaned' made problems with
    %% 'DOWN' reports, so it had to be separated in a separate record field
    proto_sched:thread_num(1),
    Parent = self(),
    _Child =
        spawn(fun() ->
                      proto_sched:thread_begin(),
                      ?TRACE("Sending parent", []),
                      %% to steer the flow control, we have to send
                      %% directly here (uninfected)... (normally forbidden)
                      Parent ! {ready_for_cleanup},
                      %% let cleanup arrive at proto_sched
                      timer:sleep(300),
                      %% log a send
                      comm:send_local(Parent, {child_done})
              end),

    receive
        {ready_for_cleanup} ->
            ok
    end,
    proto_sched:cleanup(),
    %% we could/have to receive {child_done}?

    ?ASSERT(not proto_sched:infected()).



basic_thread_yield_outside_proto_sched(_Config) ->
    %% a thread_yield outside a proto_sched is prohibited.
    ?expect_exception(proto_sched:thread_yield(),
                      throw, 'yield_outside_thread_start_thread_end').

basic_slow_msg_handler(_Config) ->
    %% keep exec token longer than a second. This should raise a
    %% warning from proto_sched (some output).
    proto_sched:thread_num(1),
    proto_sched:thread_begin(),
    timer:sleep(2000),
    proto_sched:thread_end(),
    proto_sched:cleanup(),
    ok.


basic_bench_increment(_Config) ->
    %% let run a short bench:increment with proto_sched
    proto_sched:thread_num(1),
    proto_sched:thread_begin(),
    {ok, _} = bench:increment(2,2),
    proto_sched:thread_end(),
    proto_sched:wait_for_end(),
    _ = proto_sched:get_infos(),
    unittest_helper:print_proto_sched_stats(at_end_if_failed),
    %% TODO: check statistics
    proto_sched:cleanup().

basic_start_bench_and_kill_it(_Config) ->
    %% we start a long running bench:increment and kill it during
    %% execution to see what happens with proto_sched with killed
    %% processes. (Similarily done in dht_node_move_SUITE).
    BenchPid = spawn(fun() ->
                          proto_sched:thread_begin(),
                          {ok, _} = bench:increment(10,1000),
                          %% will be killed by steering thread
                          util:do_throw(bench_was_to_fast)
          end),

    proto_sched:thread_num(1),
    %% spawned process starts execution
    timer:sleep(500),
    erlang:exit(BenchPid, 'kill'),
    proto_sched:wait_for_end(),
    %% TODO: check statistics
    proto_sched:cleanup().

test_kv_on_cseq_read(_Config) ->
    util:for_to(1, 100, fun kv_on_cseq_read/1).

kv_on_cseq_read(I) ->
    %% read key with one thread
    {ok} = kv_on_cseq:write("a", I),
    proto_sched:thread_num(1, I),
    proto_sched:thread_begin(I),
    {ok, I} = kv_on_cseq:read("a"),
    proto_sched:thread_end(I),
    ?ASSERT(not proto_sched:infected()),
    proto_sched:wait_for_end(I),
    _Infos = proto_sched:get_infos(I),
    unittest_helper:print_proto_sched_stats(at_end_if_failed, I),
    proto_sched:cleanup(I).

test_kv_on_cseq_read_2(_Config) ->
    util:for_to(1, 100, fun kv_on_cseq_read_2/1).

kv_on_cseq_read_2(I) ->
    %% concurrently try to read the same key with two threads
    {ok} = kv_on_cseq:write("a", I),
    Pid = self(),
    spawn(fun() ->
                  proto_sched:thread_begin(I),
                  {ok, I} = kv_on_cseq:read("a"),
                  proto_sched:thread_end(I),
                  Pid ! ok
          end),
    spawn(fun() ->
                  proto_sched:thread_begin(I),
                  {ok, I} = kv_on_cseq:read("a"),
                  proto_sched:thread_end(I),
                  Pid ! ok
          end),
    proto_sched:thread_num(2, I),
    ?ASSERT(not proto_sched:infected()),
    receive ok -> ok end,
    receive ok -> ok end,
    proto_sched:wait_for_end(I),
    _Infos = proto_sched:get_infos(I),
    unittest_helper:print_proto_sched_stats(at_end_if_failed, I),
    proto_sched:cleanup(I).

test_kv_on_cseq_write(_Config) ->
    util:for_to(1, 100, fun kv_on_cseq_write/1).

kv_on_cseq_write(_I) ->
    %% write key with one thread
    proto_sched:thread_num(1, _I),
    proto_sched:thread_begin(_I),
    ?TRACE("Thread1 start write", []),
    {ok} = kv_on_cseq:write("a", _I),
    ?TRACE("Thread1 finished write", []),
    proto_sched:thread_end(_I),
    proto_sched:wait_for_end(_I),
    _Infos = proto_sched:get_infos(_I),
    unittest_helper:print_proto_sched_stats(at_end_if_failed, _I),
    proto_sched:cleanup(_I).

test_kv_on_cseq_write_2(_Config) ->
    util:for_to(1, 100, fun kv_on_cseq_write_2/1).

kv_on_cseq_write_2(_I) ->
    %% concurrently try to write the same key with two threads
    ?TRACE("New intance ~p", [_I]),

    Pid = self(),
    spawn(fun() ->
                  proto_sched:thread_begin(_I),
                  ?TRACE("Thread1 start write ~p", [_I]),
                  {ok} = kv_on_cseq:write("a", 7),
                  ?TRACE("Thread1 finished write ~p", [_I]),
                  proto_sched:thread_end(_I),
                  Pid ! ok
          end),
    spawn(fun() ->
                  proto_sched:thread_begin(_I),
                  ?TRACE("Thread2 start write ~p", [_I]),
                  {ok} = kv_on_cseq:write("a", 8),
                  ?TRACE("Thread2 finished write ~p", [_I]),
                  proto_sched:thread_end(_I),
                  Pid ! ok
          end),
    proto_sched:thread_num(2, _I),
    ?ASSERT(not proto_sched:infected()),
    receive ok -> ok end,
    receive ok -> ok end,
    ?TRACE("Both done", []]),
    proto_sched:wait_for_end(_I),
    _Infos = proto_sched:get_infos(_I),
    unittest_helper:print_proto_sched_stats(at_end_if_failed, _I),
    proto_sched:cleanup(_I).

test_qwrite_qwrite_qread(_Config) ->
    util:for_to(1, 100, fun qwrite_qwrite_qread/1).

qwrite_qwrite_qread(_I) ->
    DB = {lease_db, 1},
    ContentCheck = fun content_check/3,
    Self = comm:reply_as(self(), 2, {test_rbr, '_'}),
    Id = ?RT:hash_key("1"),
    Value1 = value_1,
    Value2 = value_2,

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% begin test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    spawn(fun() ->
                  proto_sched:thread_begin(),
                  rbrcseq:qwrite(DB, Self, Id, ?MODULE, ContentCheck, Value1),
                  ?TRACE("Thread 1 done", []),
                  proto_sched:thread_end()
          end),
    spawn(fun() ->
                  proto_sched:thread_begin(),
                  rbrcseq:qwrite(DB, Self, Id, ?MODULE, ContentCheck, Value2),
                  ?TRACE("Thread 2 done", []),
                  proto_sched:thread_end()
          end),
    proto_sched:thread_num(3),
    proto_sched:thread_begin(),
    ?TRACE("Receive 1", []),
    receive_answer(),
    ?TRACE("Receive 2", []),
    receive_answer(),
    ?TRACE("Done", []),
    proto_sched:thread_end(),

    proto_sched:wait_for_end(),
    Infos = proto_sched:get_infos(),
    %log:log("~.0p", [Infos]),
    proto_sched:cleanup(),
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% end test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    rbrcseq:qread(DB, Self, Id, l_on_cseq),
    {test_rbr, {qread_done, _, _, _, Val}} = receive_answer(),
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
    trace_mpath:thread_yield(),
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
