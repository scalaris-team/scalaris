%% @copyright 2012-2016 Zuse Institute Berlin

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
%% @doc    Unit tests for crash recovery with leases
%% @end
%% @version $Id$
-module(crash_recovery_SUITE).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

groups() ->
    [{crash_recovery_tests,[sequence], [
                                        test_crash_recovery,
                                        test_crash_recovery_one_new_node,
                                        test_crash_recovery_one_outdated_node,
                                        test_crash_recovery_bad_owner_pids%,
                                        %% requires node rejoin after lost active lease
                                        %% test_crash_recovery_two_outdated_nodes
                                       ]},
     {repeater, [{repeat, 30}], [{group, crash_recovery_tests}]}
    ].

all() ->
    [
     {group, crash_recovery_tests}
     ].

suite() -> [ {timetrap, {seconds, 40}} ].

group(crash_recovery_tests) ->
    [{timetrap, {seconds, 400}}];
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
    unittest_helper:make_symmetric_ring([{config, [{log_path, PrivDir},
                                                   {leases, true}]}]),
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% crash recovery test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_crash_recovery(_Config) ->
    % do nothing
    F = fun(_DHTNodes) ->
                ok
        end,

    generic_crash_recovery_test(F).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% crash recovery test with one 'new' node
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_crash_recovery_one_new_node(_Config) ->
    % delete the lease dbs on the first node
    F = fun(DHTNodes) ->
                % drop the four lease_dbs on the first node
                erase_lease_dbs(hd(DHTNodes))
        end,

    generic_crash_recovery_test(F).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% crash recovery test with one node having outdated data
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_crash_recovery_one_outdated_node(_Config) ->
    % delete the lease dbs on the first node
    F = fun(DHTNodes) ->
                % manipulate rounds on the first node
                change_lease_replicas(hd(DHTNodes))
        end,

    generic_crash_recovery_test(F).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% crash recovery test with *two* nodes having outdated data
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_crash_recovery_two_outdated_nodes(_Config) ->
    % delete the manipulate dbs on *two* nodes
    F = fun(DHTNodes) ->
                % manipulate rounds on two nodes
                [N1, N2 | _Tail] = DHTNodes,
                change_lease_replicas(N1),
                change_lease_replicas(N2)
        end,

    generic_crash_recovery_test(F).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% crash recovery test with change all owner pids
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_crash_recovery_bad_owner_pids(_Config) ->
    % XXXX
    F = fun(DHTNodes) ->
                change_owner_pids(DHTNodes)
        end,

    generic_crash_recovery_test(F).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% generic crash recovery test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
generic_crash_recovery_test(DoBadThings) ->
    % we create a ring with four nodes. We stop lease renewal on all
    % nodes and wait for the leases to timeout.
    DHTNodes = pid_groups:find_all(dht_node),

    % stop all nodes
    ct:pal("cr: stop all nodes"),
    _ = [lease_helper:intercept_lease_renew(Node) || Node <- DHTNodes],
    lease_helper:wait_for_number_of_valid_active_leases(0),
    [gen_component:bp_del(Node, block_trigger) || Node <- DHTNodes],

    % do bad things
    DoBadThings(DHTNodes),

    % trigger renewal on all nodes
    ct:pal("cr: renew all nodes ~p", [DHTNodes]),
    [ comm:send_local(Node, {l_on_cseq, renew_leases}) || Node <- DHTNodes],

    % wait for leases to reappear
    ct:pal("cr: wait for leases to reappear"),
    %% tolerate one failed node, as long as a correct ring is created
    lease_checker2:wait_for_clean_leases(400, [{ring_size_range,
                                                config:read(replication_factor)-1,
                                                config:read(replication_factor)}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% functions manipulating the node state
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
erase_lease_dbs(Node) ->
    F = fun(State) ->
                LeaseDBs = [{lease_db, I} || I <- lists:seq(1, config:read(replication_factor))],
                lists:foldl(fun (DB, Acc) ->
                                    dht_node_state:set_prbr_state(Acc, DB, prbr:init(DB))
                            end, State, LeaseDBs)
        end,
    change_node_state(Node, F).


change_lease_replicas(Node) ->
    F = fun(State) ->
                [reset_read_and_write_rounds(State, {lease_db, I})
                 || I <- lists:seq(1, config:read(replication_factor))],
                State
        end,
    change_node_state(Node, F).

change_node_state(Node, F) ->
    comm:send_local(Node, {set_state, comm:this(), F}),
    receive
        {set_state_response, _NewState} ->
            ok
    end.

change_owner_pids(DHTNodes) ->
    Self = comm:this(),
    F = fun(State) ->
                [change_owner_pid(Self, State, {lease_db, I})
                 || I <- lists:seq(1, config:read(replication_factor))],
                State
        end,
    lists:foreach(fun (Node) ->
                          comm:send_local(Node, {set_state, comm:this(), F}),
                          receive
                              {set_state_response, _NewState} ->
                                  ok
                          end
                  end, DHTNodes).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% functions manipulating prbr state
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

change_owner_pid(Pid, State, DBName) ->
    LeaseDB = dht_node_state:get(State, DBName),
    ct:pal("LeaseDB ~p", [LeaseDB]),
    _ = [
         case l_on_cseq:is_a_lease(Lease) of
             true ->
                 prbr:set_entry({Key,
                                 {ReadRound, ReadClientId, ReadWriteFilter},
                                 {WriteRound, WriteClientId, WriteWriteFilter},
                                 l_on_cseq:set_owner(Lease, Pid), LastWriteFilter
                                }, LeaseDB);
             false ->
                 ct:fail("the lease db contains a ~p, which is not a lease record", [Lease])
         end
      || {Key,
          {ReadRound, ReadClientId, ReadWriteFilter},
          {WriteRound, WriteClientId, WriteWriteFilter},
          Lease, LastWriteFilter}
             <- prbr:tab2list_raw_unittest(LeaseDB)],
    ok.


reset_read_and_write_rounds(State, DBName) ->
    LeaseDB = dht_node_state:get(State, DBName),
    _ = [ prbr:set_entry(prbr:new(Key, Value), LeaseDB)
          || {Key, _R_Read, _R_Write, Value, _LastWriteFilter}
                 <- prbr:tab2list_raw_unittest(LeaseDB)],
    ok.
