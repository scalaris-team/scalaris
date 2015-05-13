%% @copyright 2012-2014 Zuse Institute Berlin

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
                                        test_crash_recovery_bad_owner_pids,
                                        test_crash_recovery_two_outdated_nodes
                                       ]}
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

init_per_group(Group, Config) -> unittest_helper:init_per_group(Group, Config).

end_per_group(Group, Config) -> unittest_helper:end_per_group(Group, Config).


init_per_testcase(_TestCase, Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    Ids = unittest_helper:get_evenly_spaced_keys(4),
    unittest_helper:make_ring_with_ids(Ids, [{config, [{log_path, PrivDir},
                                                       {leases, true}]}]),
    [{stop_ring, true} | Config].

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

    generic_crash_recovery_test(F, 4).

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

    generic_crash_recovery_test(F, 3).

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

    generic_crash_recovery_test(F, 4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% crash recovery test with *two* nodes having outdated data
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_crash_recovery_two_outdated_nodes(_Config) ->
    % delete the manipulate dbs on *two* nodes
    F = fun(DHTNodes) ->
                % manipulate rounds on the first node
                [N1, N2, _N3, _N4] = DHTNodes,
                change_lease_replicas(N1),
                change_lease_replicas(N2)
        end,

    generic_crash_recovery_test(F, 4).

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

    generic_crash_recovery_test(F, 4).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% generic crash recovery test
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
generic_crash_recovery_test(DoBadThings, ExpectedLeases) ->
    % we create a ring with four nodes. We stop lease renewal on all
    % nodes and wait for the leases to timeout.
    DHTNodes = pid_groups:find_all(dht_node),

    % stop all nodes
    ct:pal("cr: stop all nodes"),
    [lease_helper:intercept_lease_renew(Node) || Node <- DHTNodes],
    lease_helper:wait_for_number_of_valid_active_leases(0),
    [gen_component:bp_del(Node, block_trigger) || Node <- DHTNodes],

    % do bad things
    DoBadThings(DHTNodes),

    % trigger renewal on all nodes
    ct:pal("cr: renew all nodes ~p", [DHTNodes]),
    [ comm:send_local(Node, {l_on_cseq, renew_leases}) || Node <- DHTNodes],
    
    % wait for leases to reappear
    ct:pal("cr: wait for leases to reappear"),
    lease_helper:wait_for_number_of_valid_active_leases(ExpectedLeases).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% functions manipulating the node state
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
erase_lease_dbs(Node) ->
    F = fun(State) ->
                State1 = dht_node_state:set_prbr_state(State , leases_1, prbr:init(lease_db1)),
                State2 = dht_node_state:set_prbr_state(State1, leases_1, prbr:init(lease_db1)),
                State3 = dht_node_state:set_prbr_state(State2, leases_1, prbr:init(lease_db1)),
                State4 = dht_node_state:set_prbr_state(State3, leases_1, prbr:init(lease_db1)),
                State4
        end,
    change_node_state(Node, F).


change_lease_replicas(Node) ->
    F = fun(State) ->
                reset_read_and_write_rounds(State, leases_1, lease_db1),
                reset_read_and_write_rounds(State, leases_2, lease_db2),
                reset_read_and_write_rounds(State, leases_3, lease_db3),
                reset_read_and_write_rounds(State, leases_4, lease_db4),
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
                change_owner_pid(Self, State, leases_1, lease_db1),
                change_owner_pid(Self, State, leases_2, lease_db2),
                change_owner_pid(Self, State, leases_3, lease_db3),
                change_owner_pid(Self, State, leases_4, lease_db4),
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

change_owner_pid(Pid, State, DBName, _PRBRName) ->
    LeaseDB = dht_node_state:get_prbr_state(State, DBName),
    ct:pal("LeaseDB ~p", [LeaseDB]),
    [ 
      prbr:set_entry({Key, 
                      {ReadRound, ReadClientId, ReadWriteFilter}, 
                      {WriteRound, WriteClientId, WriteWriteFilter}, 
                      l_on_cseq:set_owner(Lease, Pid)}, LeaseDB)

      || {Key, 
          {ReadRound, ReadClientId, ReadWriteFilter}, 
          {WriteRound, WriteClientId, WriteWriteFilter},
          Lease} 
             <- prbr:tab2list_raw_unittest(LeaseDB)],
    ok.


reset_read_and_write_rounds(State, DBName, _PRBRName) ->
    LeaseDB = dht_node_state:get_prbr_state(State, DBName),
    ct:pal("LeaseDB ~p", [LeaseDB]),
    [ 
      prbr:set_entry({Key, 
                      {ReadRound, ReadClientId, ReadWriteFilter}, 
                      {WriteRound, WriteClientId, WriteWriteFilter}, 
                      unittest_crash_recovery}, LeaseDB)

      || {Key, 
          {ReadRound, ReadClientId, ReadWriteFilter}, 
          {WriteRound, WriteClientId, WriteWriteFilter},
          _Value} 
             <- prbr:tab2list_raw_unittest(LeaseDB)],
    ok.
