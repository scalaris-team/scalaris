%% @copyright 2013-2019 Zuse Institute Berlin
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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    Unit tests for rbrcseq which simulate specific interleaving of messages.
%%         The purpose of this suite is to reliably replicate rare corner-cases by slowing
%%         downe specific links during certain times during the tests.
%%         The tests depend on the gen_component breakpoint mechanism to delay specific messages.
%%         Each test case assumes a specific replication factor (usually 4).
%% @end
%% @version $Id$
-module(rbr_interleaving_SUITE).
-author('skrzypczak@zib.de').
-vsn('$Id$').

-compile(export_all).

-define(TRACE(X,Y), ok).
%-define(TRACE(X,Y), ct:pal(X, Y)).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

all() -> [
            test_link_slowing,
            test_link_slowing2,
            test_interleaving,
            test_write_once_1,
            test_write_once_2,
            test_write_once_3,
            test_read_retry_returns_older,
            test_read_retry_returns_newer,
            test_read_write_commuting
         ].

suite() -> [ {timetrap, {seconds, 400}} ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir}, {replication_factor, 4}]}]),
    unittest_helper:check_ring_size_fully_joined(config:read(replication_factor)),
    [{stop_ring, true} | Config].

test_link_slowing(_Config) ->
    %% Slow down a link. One replica should not receive any prbr messages during the
    %% test. After the slow link is removed (messages will be flushed), all replicase should
    %% be consistent. Assumes R=4.
    get_notified_by_message(1, 2, write),
    Link = slow_link(1, 2),
    {ok, _} = write_via_node(1, "1", filter_list_append(), "TestWrite"),

    %% replica 2 should be empty, and the other three have the value written
    ?equals(prbr_values(), [[["TestWrite"]],
                            [],
                            [["TestWrite"]],
                            [["TestWrite"]]]),

    remove_slow_link(Link),
    _= wait_until_notification(1),

    %% all replicas should have received the written value
    ?equals(prbr_values(), [[["TestWrite"]],
                            [["TestWrite"]],
                            [["TestWrite"]],
                            [["TestWrite"]]]).

test_link_slowing2(_Config) ->
    %% slow down one link, but use a different client to send write request
    %% slow link should have no impact. Assumes R=4.
    _Link = slow_link(1, 2),

    get_notified_by_message(2, [1,2,3,4], write),
    {ok, _} = write_via_node(2, "1", filter_list_append(), "TestWrite"),
    _= wait_until_notification(4),

    %% all replicas should have received the written value
    ?equals(prbr_values(), [[["TestWrite"]],
                            [["TestWrite"]],
                            [["TestWrite"]],
                            [["TestWrite"]]]).

test_interleaving(_Config) ->
    %% This test simulates the following interleaving of operations:
    %% (4 nodes with R=4, the nodes are called 1,2,3,4)
    %%
    %% Three requests are made from three different clients.
    %% 1. Client A Starts a write operation, but has only written replica
    %%      on node 1 so far (has read all replicas in its read phase)
    %% 2. Client B Executes a read which only has read replicas 2,3,4 yet
    %%      (read has returned since majority replied)
    %% 3. Client C Executes a write. In its read phase and it gets replies
    %%      from 2,3,4 first; After that write on every replica

    Key = "A",

    %% write of client A
    get_notified_by_message(1, [2,3,4], round_request),
    get_notified_by_message(1, 1, write),
    _ = slow_link(1, [2,3,4], write),
    spawn(fun() -> write_via_node(1, Key, filter_list_append(), "WriteA") end),
    _= wait_until_notification(4),

    %% read of client B
    _LinkB = slow_link(2, 1),
    {ok, _} = read_via_node(2, Key, element(1, filter_list_append())),

    %% write of client C
    get_notified_by_message( 3, 1, write),
    LinkC = slow_link(3, 1),
    {ok, _} = write_via_node(3, Key, filter_list_append(), "WriteB"),
    remove_slow_link(LinkC),
    _ = wait_until_notification(1),

    ct:pal("PRBR state after interleaved operations: ~n~p", [prbr_data()]),
    %% Test that there aren't two different values
    %% with the same write round.
    PrbrData = prbr_w_rounds_with_values(),
    ValList = lists:usort(lists:flatten(PrbrData)),
    case ValList of
        [A, B] ->
            ?compare_w_note(fun(E1, E2) -> element(1, E1) =/= element(1, E2) end,
                            A, B, "Same write round for different values!");
        [_A] -> ok;
        _ ->
            ct:fail("More than two different values/rounds! ~nprbr data:~n~p", [PrbrData])
    end,

    %% Do a read over replica 1, 2, 3
    %% It should be an inconsistent read and currently diverging replica 1
    %% should be repaired,
    get_notified_by_message(4, 4, write),
    LinkD = slow_link(4, 4),
    {ok, _} = read_via_node(4, Key, element(1, filter_list_append())),
    remove_slow_link(LinkD),
    _ = wait_until_notification(1),

    ct:pal("PRBR state after inconsistent read: ~n~p", [prbr_data()]),
    PrbrData2 = prbr_values(),
    ValList2 = lists:usort(PrbrData2),
    %% do not compare write rounds as the write messages to acceptor 4 might not
    %% have been received and processed yet.
    ?equals_w_note(length(ValList2), 1, "All replicas should have the same value").

test_write_once_1(_Config) ->
    %% This tests case tests if a write through correctly notifies the original proposer
    %% of the write.
    %% Write A is started, but does not finish because two write messages are delayed.
    %% A subsequent write (or read) should detect the write in progress and triggers a
    %% write through which will finish write A before B is started.
    TestPid = self(),
    Key = "1234",

    get_notified_by_message(1, [1,2], write),
    _ = slow_link(1, 3, write),
    _ = slow_link(1, 4, write),

    % start write A which will not finish since it only gets two write ack.
    spawn(fun() ->
            {ok, _} = write_via_node(1, Key, filter_list_append(), "WriteA"),
            TestPid ! {write_a_done}
          end),

    % wait until A has written the two remaining replicas
    _ = wait_until_notification(2),

    % write B should now trigger a write through which should finish write A
    % once write B retries, a notification should be sent to write A
    {ok, _} = write_via_node(2, Key, filter_list_append(), "WriteB"),

    receive {write_a_done} -> ok
    after 10000 ->
        ?ct_fail("Write through has not notified original proposer in a timely manner", [])
    end,
    {ok, Value} = read_via_node(3, Key, element(1, filter_list_append())),
    ?equals_w_note(Value, ["WriteB", "WriteA"], "Values must match exactly due to interleaving").

test_write_once_2(_Config) ->
    %% This tests does the following:
    %% There is an unsuccessfull write A that writes on replica 4
    %% Write B is successful by writing on replica 1-3. but the proposer retries
    %% because it receives the deny message from replica 4 first.
    %% Write C sees consistent quorum 1-3 and proposes its follow-up value
    %% which is written on all replicas
    %% Write B retries but it should be notified before sending write messages.
    %% --> Write B should only be written once

    TestPid = self(),
    Key = "123",

    print_prbr_data(),

    %% Write A: phase 1 reaches all replicas, write msg only replica 4
    get_notified_by_message(1, 4, write),
    get_notified_by_message(1, [1,2,3,4], round_request),
    _ = slow_link(1, [1,2,3], write),
    spawn(fun() -> _ = write_via_node(1, Key, filter_list_append(), "WriteA") end),
    _ = wait_until_notification(5),

    print_prbr_data(),
    ct:pal("write A done"),

    %% Write B: writes replica 1 to 3. gets deny from replica 4 before quorum of acks
    %% --> it retries its request
    get_notified_by_message(2, [1,2,3,4], round_request),
    get_notified_by_message(2, [1,2,3,4], read),
    get_notified_by_message(2, [1,2,3,4], write),

    SlowLinksB1 = slow_link(2, 4, round_request),
    SlowLinksB3 = slow_link(2, 3, write),
    SlowLinksB4 = slow_link(2, 2, read),
    spawn(fun() -> _ =
            write_via_node(2, Key, filter_list_append(), "WriteB"),
            TestPid ! {write_b_done}
          end),
    _ = wait_until_notification(5),
    print_prbr_data(),

    ct:pal("flush slow link 4"),
    flush_slow_link(SlowLinksB1),
    _ = wait_until_notification(3),
    print_prbr_data(),

    ct:pal("flush slow link 3"),
    flush_slow_link(SlowLinksB3),
    _ = wait_until_notification(2),
    print_prbr_data(),

    %% Write C: observer consistent quorum from repliica 1 to 3
    %% write any follow-up value to all replicas
    get_notified_by_message(3, [1,2,3,4], write),
    SlowLinkC = slow_link(3, 4, round_request),
    _ = write_via_node(3, Key, filter_list_append(), "WriteC"),
    remove_slow_link(SlowLinkC),
    _ = wait_until_notification(4),
    ct:pal("write C done"),
    print_prbr_data(),

    %% Write B: should not be able to retry as it gets notified earlier
    ct:pal("remove slowness from write b"),
    remove_slow_link(SlowLinksB4),
    remove_slow_link(SlowLinksB3),
    remove_slow_link(SlowLinksB1),

    receive {write_b_done} -> ok end,
    print_prbr_data(),

    %% Read and check if WriteB exists only once
    {ok, Value} = read_via_node(4, Key, element(1, filter_list_append())),
    ?equals_w_note(Value, ["WriteC", "WriteB"], "Values must match exactly with this interleaving"),

    ok.

test_write_once_3(_Config) ->
    %% This tests does the following:
    %% There is an unsuccessfull write A that writes on replica 4
    %% Write B is successful by writing on replica 1-3. but the proposer retries
    %% because it receives the deny message from replica 4 first.
    %% Write B retries but it should be see that it has consistent quorum from
    %% previous attempt
    %% --> Write B should only be written once

    TestPid = self(),
    Key = "123",

    print_prbr_data(),

    %% Write A: phase 1 reaches all replicas, write msg only replica 4
    get_notified_by_message(1, [1,2,3,4], round_request),
    get_notified_by_message(1, 4, write),
    _ = slow_link(1, [1,2,3], write),
    spawn(fun() -> _ = write_via_node(1, Key, filter_list_append(), "WriteA") end),
    _ = wait_until_notification(5),

    print_prbr_data(),
    ct:pal("write A done"),

    %% Write B: writes replica 1 to 3. gets deny from replica 4 before quorum of acks
    %% --> it retries its request
    get_notified_by_message(2, [1,2,3,4], round_request),
    get_notified_by_message(2, [1,2,3,4], read),
    get_notified_by_message(2, [1,2,3,4], write),

    SlowLinksB1 = slow_link(2, 4, round_request),
    SlowLinksB3 = slow_link(2, 3, write),
    SlowLinksB4 = slow_link(2, 2, read),
    spawn(fun() -> _ =
            write_via_node(2, Key, filter_list_append(), "WriteB"),
            TestPid ! {write_b_done}
          end),
    _ = wait_until_notification(5),
    print_prbr_data(),

    ct:pal("flush slow link 4"),
    flush_slow_link(SlowLinksB1),
    _ = wait_until_notification(3),
    print_prbr_data(),

    ct:pal("flush slow link 3"),
    flush_slow_link(SlowLinksB3),
    _ = wait_until_notification(2),
    print_prbr_data(),

    %% Write B: should not be able to retry as it gets notified earlier
    ct:pal("remove slowness from write b"),
    remove_slow_link(SlowLinksB4),
    remove_slow_link(SlowLinksB3),
    remove_slow_link(SlowLinksB1),

    receive {write_b_done} -> ok end,
    print_prbr_data(),

    %% Read and check if WriteB exists only once
    {ok, Value} = read_via_node(4, Key, element(1, filter_list_append())),
    ?equals_w_note(Value, ["WriteB"], "Values must match exactly with this interleaving"),

    ok.

test_read_retry_returns_older(_Config) ->
    %% This test checks if a delayed round requests responses properly complete
    %% a read request if an inconsistent quorum was already established.
    %% Setup:
    %%  A: Write value to replica 1
    %%  B: Submit read... this will observer only replies from replica 1-3
    %%      thus moving from round_request to read phase.
    %%     Afterwards, B receives replies from replica 4.. thus 3 replies with no value in total
    %%     B should deliver an empty value without triggering a write-through
    TestPid = self(),
    Key = "123",

    % Write A: Write only to replica 1
    ct:pal("partial write to replica 1"),
    print_prbr_data(),
    get_notified_by_message(1, 1, write),
    _ = slow_link(1, [2, 3, 4], write),
    spawn(fun() -> write_via_node(1, Key, filter_list_append(), "WriteA") end),
    _ = wait_until_notification(1),
    print_prbr_data(),

    %% Execute read B
    ct:pal("inconsistent read"),
    get_notified_by_message(2, 1, read),
    _ = slow_link(2, [2,3,4], read),
    LinkC = slow_link(2, 4, round_request),
    spawn(fun() ->
        {ok, Value} = read_via_node(2, Key, element(1, filter_list_append())),
            TestPid ! {read_req_done, Value}
        end),
    % message_received -> internally seen inconsistent quorum and moved to read stage
    _ = wait_until_notification(1),
    print_prbr_data(),
    %% deliver slow messages from replica 4
    ct:pal("remove slow link"),
    remove_slow_link(LinkC),
    print_prbr_data(),
    receive {read_req_done, Value} ->
        ?equals_w_note(Value, [], "Values must match exactly with this interleaving")
    end,
    %% check that no write-through happened (max write round of 1)
    WriteThroughRounds = lists:filter(fun(E) -> E > 1 end, lists:flatten(prbr_w_rounds())),
    ?equals_w_note(WriteThroughRounds, [], "No write through should have happend"),
    ok.

test_read_retry_returns_newer(_Config) ->
    %% This test checks if a delayed round requests responses properly complete
    %% a read request if an inconsistent quorum was already established.
    %% Setup:
    %%  A: Write value to replica 1-3
    %%  B: Submit read... this will observer only replies from replica 2-4
    %%      thus moving from round_request to read phase.
    %%     Afterwards, B receives replies from replica 1.. thus 3 replies with new value in total
    %%     B should deliver the new value without triggering a write-through
    TestPid = self(),
    Key = "123",

    % Write A: Write to replica 1-3
    ct:pal("write to replica 1-3"),
    print_prbr_data(),
    get_notified_by_message(1, [1,2,3], write),
    _ = slow_link(1, 4, write),
    spawn(fun() -> write_via_node(1, Key, filter_list_append(), "WriteA") end),
    _ = wait_until_notification(3),
    print_prbr_data(),

    %% Execute read B
    ct:pal("inconsistent read"),
    get_notified_by_message(2, 4, read),
    _ = slow_link(2, [1,2,3], read),
    LinkC = slow_link(2, 1, round_request),
    spawn(fun() ->
        {ok, Value} = read_via_node(2, Key, element(1, filter_list_append())),
            TestPid ! {read_req_done, Value}
        end),

    % message_received -> internally seen inconsistent quorum and moved to read stage
    _ = wait_until_notification(1),
    print_prbr_data(),

    %% deliver slow messages from replica 1
    ct:pal("remove slow link"),
    remove_slow_link(LinkC),
    print_prbr_data(),
    receive {read_req_done, Value} ->
        ?equals_w_note(Value, ["WriteA"], "Values must match exactly with this interleaving")
    end,
    %% check that no write-through happened (max write round of 1)
    WriteThroughRounds = lists:filter(fun(E) -> E > 1 end, lists:flatten(prbr_w_rounds())),
    ?equals_w_note(WriteThroughRounds, [], "No write through should have happend"),
    ok.

test_read_write_commuting(_Config) ->
    %% Write tuple {A,B} on every replica. Update second element to C for
    %% 3 out of 4 replicas. Read first element and make sure the outdated replica
    %% is included. Since operations commute, no write-through should have been
    %% triggered.
    Key = "123",

    % write baseline
    get_notified_by_message(1, [1,2,3,4], write),
    _ = write_via_node(1, Key, {fun prbr:noop_read_filter/1,
                                fun ?MODULE:cc_noop/3,
                                fun prbr:noop_write_filter/3},
                       {"A", "B"}),
    _ = wait_until_notification(4),

    _ = slow_link(1, 4, write),
    _ = write_via_node(1, Key, {fun ?MODULE:rf_second/1,
                                fun ?MODULE:cc_noop/3,
                                fun ?MODULE:wf_second/3}, "C"),

    PrbrValuesBeforeRead = prbr_values(),
    _ = slow_link(4, 1),
    {ok, "A"} = read_via_node(4, Key, fun ?MODULE:rf_first/1),
    PrbrValuesAfterRead = prbr_values(),

    ?equals_w_note(PrbrValuesBeforeRead, PrbrValuesAfterRead,
                   "Read was independent from write and thus should not have caused a "
                   "write through"),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec get_commuting_wf_for_rf(prbr:read_filter()) ->
        [prbr:write_filter()].
get_commuting_wf_for_rf(ReadFilter) ->
    {name, Name} = erlang:fun_info(ReadFilter, name),
    {module, Module} = erlang:fun_info(ReadFilter, module),
    case {Module, Name} of
        {?MODULE, rf_first} ->
            [fun ?MODULE:wf_second/3];
        _ ->
            []
    end.

-spec rf_first(prbr_bottom | {any(), any()}) -> any().
rf_first(prbr_bottom) -> prbr_bottom;
rf_first({A, _B}) -> A.
-spec rf_second(prbr_bottom | {any(), any()}) -> any().
rf_second(prbr_bottom) -> prbr_bottom;
rf_second({_A,B}) -> B.

-spec cc_noop(any(), any(), any()) ->
        {true, none}.
cc_noop(_, _, _) -> {true, none}.
-spec wf_second({any(), any()}, any(), any()) ->
        {{any(), any()}, none}.
wf_second({A, _B}, _UI, WriteVal) -> {{A, WriteVal}, none}.

%% @doc Simple set of filter which append the given value to a list
filter_list_append() ->
    RF = fun (prbr_bottom) -> [];
             (DBEntry) -> DBEntry
         end,
    CC = fun (_ReadVal_, _WF, _WriteVal) -> {true, none} end,
    WF = fun (prbr_bottom, _UI, WriteVal) -> {[WriteVal], none};
             (DBEntry, _UI, WriteVal) -> {[WriteVal | DBEntry], none}
         end,
    {RF, CC, WF}.

%% @doc Sends a read requests via node number ViaKvNr (lexicographically order by pid).
%% Blocks until read is done.
read_via_node(ViaKvNr, Key, ReadFilter) ->
    Pid = nth(ViaKvNr, kv_db),
    Msg = {qround_request, self(), '_', ?RT:hash_key(Key), ?MODULE, ReadFilter, read, 1},
    comm:send_local(Pid, {request_init, _ClinetPosInMsg=2, _OpenReqPos=3, Msg}),
    receive
        ?SCALARIS_RECV({qread_done, _, _, _, Value}, {ok, Value})
    end.

%% @doc Sends a write requests via node number ViaKvNr (lexicographically order by pid).
%% Blocks until write is done.
write_via_node(ViaKvNr, Key, Filter, Value) ->
    Pid = nth(ViaKvNr, kv_db),
    Msg = {qwrite, self(), '_', ?RT:hash_key(Key), ?MODULE, Filter, Value, 20},
    comm:send_local(Pid, {request_init, _ClientPos=2, _OpenReqPos=3, Msg}),
    receive
        ?SCALARIS_RECV({qwrite_done, _, _, _, RetValue}, {ok, RetValue});
        ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason}, Reason)
    end.

%% @doc Notifies process PidToNotify if process nth(ToId, ToType) received a message
%% of type MessageType from process nth(FromId, FromType).
%% ATTENTION: If the corresponding link is slowed by slow_link/[4,5,6] this method must be called
%% BEFORE slow_link. Otherwise two notifications might be received for the same message.
%% Todo? (Works only for ToType=dht_node so far).
get_notified_by_message(PidToNotify, FromId, FromType, ToId, ToType, MessageType) ->
    BpName = bp_name("notify_" ++ atom_to_list(MessageType), FromId, FromType, ToId, ToType),
    ToPid = nth(ToId, ToType),
    NotifyFun = notify_fun(PidToNotify, nth(FromId, FromType), ToPid,
                            ToType, MessageType, BpName),
    gen_component:bp_set_cond(ToPid, NotifyFun, BpName).

%% @doc Wrapper for get_notified_by_message/6
get_notified_by_message(FromId, ToIds, MessageType) ->
    get_notified_by_message(self(), FromId, ToIds, MessageType).
    
get_notified_by_message(PidToNotify, FromId, ToIds, MessageType) when is_list(ToIds) ->
    [get_notified_by_message(PidToNotify, FromId, ToId, MessageType) || ToId <- ToIds],
    ok;
get_notified_by_message(PidToNotify, FromId, ToId, MessageType) ->
    get_notified_by_message(PidToNotify, FromId, kv_db, ToId, dht_node, MessageType).

wait_until_notification(NotificationCount) ->
    _ = [receive {message_received} -> ok end || _ <- lists:seq(1, NotificationCount)].

notify_fun(PidToNotify, FromPid, ToPid, _ToType=dht_node, MessageType, BpName) ->
    fun(Msg, _State) ->
        case Msg of
            _ when element(1, Msg) =:= prbr andalso
                    element(2, Msg) =:= MessageType andalso
                    element(3, element(1, element(5, Msg))) =:= FromPid ->
                ?TRACE("Notify ~p message on ~p received: ~n~p", [PidToNotify, ToPid, Msg]),
                gen_component:bp_del(ToPid, BpName),
                comm:send_local(PidToNotify, {message_received}),
                false;
            _ -> false
        end
    end.

%% @doc Helper for test outputs
print_prbr_data() ->
    ct:pal("PRBR state: ~n~p", [prbr_data()]).

%% @doc Gets all information stored in prbr for all nodes.
prbr_data() ->
     [begin
        comm:send_local(N, {prbr, tab2list_raw, kv_db, self()}),
        receive
            {kv_db, List} -> List
        end
      end || N <- lists:sort(pid_groups:find_all(dht_node))].

%% @doc Returns all value for each node.
prbr_values() ->
    [
        [prbr:entry_val(E) || E <- Replica]
    || Replica <- prbr_data()].

%% @doc Returns all {write_round, value} tuples for each node.
%%      Removes write_through infos
prbr_w_rounds_with_values() ->
    [
        [{pr:set_wti(element(3, E), none), prbr:entry_val(E)} || E <- Replica]
    || Replica <- prbr_data()].

%% @doc Returns all write round numbers for each node as a list.
prbr_w_rounds() ->
    [
        [pr:get_r(element(3, E)) || E <- Replica] 
    || Replica <- prbr_data()].

%% @doc Flush all slow messages of a link
flush_slow_link({_BPName, LoopPid, _Node}) ->
    comm:send_local(LoopPid, {flush}).


%% @doc Stops slowing messages down and flushes message queue.
remove_slow_link([]) -> ok;
remove_slow_link(LinkList) when is_list(LinkList) ->
    [remove_slow_link(Link) || Link <- LinkList],
    ok;
remove_slow_link({BPName, LoopPid, Node}) ->
    gen_component:bp_del(Node, BPName),
    comm:send_local(LoopPid, {flush_and_stop}).


%% @doc Wrappers for slow_link/5.
slow_link(FromNodeId, ToNodeIds) -> slow_link(FromNodeId, ToNodeIds, always_slow).

slow_link(FromNodeId, ToNodeIds, FastUntilMessageType) when is_list(ToNodeIds) ->
    [slow_link(FromNodeId, To, FastUntilMessageType) || To <- ToNodeIds];
slow_link(FromNodeId, ToNodeId, FastUntilMessageType) ->
    slow_link(FromNodeId, kv_db, ToNodeId, dht_node, FastUntilMessageType).

%% @doc See slow_link/5. But link is slow from the beginning.
slow_link(From, FromType, To, ToType) ->
    slow_link(From, FromType, To, ToType, always_slow).

%% @doc Delays messages from From to To. Returns a link-info tuple.
%%      Link behaves normally until a message of type FastUntilMessageType is received.
%%      Starting with this message, all received messages between these two PIDs are queued
%%      until flush_link/1 or remove_slow_link/1 is called.
%%      From/To are integer ids representing the nths Pid in PidGroup FromType/ToType.
%%      Affected messages in prbr are: round_request, read and write. Tab2list is not affected.
%%      No messages are thrown away and the delivery order is unchanged.
slow_link(From, FromType, To, ToType, FastUntilMessageType) ->
    FromPid = nth(From, FromType),
    ToPid = nth(To, ToType),
    BpName = bp_name("slow_", From, FromType, To, ToType),
    slow_link(FromPid, FromType, ToPid, ToType, BpName, FastUntilMessageType).

slow_link(FromPid, FromType, ToPid, ToType, BPName, FastUntilMessageType) ->
    {LoopPid, BpFun} = slow_link_fun(FromPid, FromType, ToPid, ToType, FastUntilMessageType),
    gen_component:bp_set_cond(ToPid, BpFun, BPName),
    {BPName, LoopPid, ToPid}.

%% @doc Delays all round_request, write and read messages received by prbr from PID
%%      From, on DHT node with PID To. The link starts delivering all queued messages as
%%      soon as a flush message was received.
%%      tab2list_raw messages are not delayed.
slow_link_fun(From, _FromType, To, _ToType=dht_node, FastUntilMessageType) ->
    LoopPid = spawn(?MODULE, slow_loop, [To, FastUntilMessageType]),
    BpFun = fun (Msg, _State) ->
        case Msg of
            %% prbr round_request, write and read messages seventh
            %% element is the datatype. This is abused to ensure that
            %% a message is only delayed once
            _ when element(7, Msg) =:= rbr_interleave_SUITE_dont_delay ->
                ?TRACE("Deliver delayed message: ~n~p", [Msg]),
                false;
           %% delay a prbr round_request, write or read message if it commes
           %% from PID From.
            _ when element(1, Msg) =:= prbr andalso
                       element(3, element(1, element(5, Msg))) =:= From ->
                ?TRACE("Delay message: ~n~p", [Msg]),
                %% change Datatype in message since it is not used in this unit test suite.
                %% marks messages which where already delayed.
                NewMsg = setelement(7, Msg, rbr_interleave_SUITE_dont_delay),
                MsgType = element(2, Msg),
                comm:send_local(LoopPid, {delay, MsgType, NewMsg}),
                drop_single;
            _ ->
                false
        end
    end,
    {LoopPid, BpFun}.

slow_loop(To, always_slow) ->
    slow_loop(To, always_slow, [], true);
slow_loop(To, FastUntil) ->
    slow_loop(To, FastUntil, [], false).
slow_loop(To, FastUntil, MsgQueue, _IsSlow=false) ->
    receive
        {delay, FastUntil, Msg} ->
            slow_loop(To, FastUntil, [Msg | MsgQueue], true);
        {delay, _Type, Msg} ->
            comm:send_local(To, Msg),
            slow_loop(To, FastUntil, MsgQueue, false)
    end;
slow_loop(To, FastUntil, MsgQueue, _IsSlow=true) ->
    receive
        {delay, _Type, Msg} ->
            slow_loop(To, FastUntil, [Msg | MsgQueue], true);
        {flush} ->
            [comm:send_local(To, Msg) || Msg <- lists:reverse(MsgQueue)],
            slow_loop(To, FastUntil, [], true);
        {flush_and_stop} ->
            [comm:send_local(To, Msg) || Msg <- lists:reverse(MsgQueue)]
    end.

nth_dht_node(N) -> nth(N, dht_node).
nth_kv_db(N) -> nth(N, kv_db).

nth(N, PidGroup) -> nth_pid(N, pid_groups:find_all(PidGroup)).
nth_pid(N, Pids) -> lists:nth(N, lists:sort(Pids)).

%% @doc Generate a breakpoint name
bp_name(Prefix, FromId, FromType, ToId, ToType) ->
    BPNameString = Prefix ++ " " ++ integer_to_list(FromId) ++ "," ++ atom_to_list(FromType)
                    ++ "|" ++ integer_to_list(ToId) ++ "," ++ atom_to_list(ToType),
    list_to_atom(BPNameString). %% ugh, dynamic creation of atoms...
