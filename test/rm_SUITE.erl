%  @copyright 2010-2015 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    TODO: Add description to rm_SUITE
%% @end
%% @version $Id$
-module(rm_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [tester_update_id, tester_update_id2_1, tester_update_id2_2,
     tester_update_id2_3, tester_update_id2_4].

suite() ->
    [
     {timetrap, {seconds, 180}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ...
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_update_id(?RT:key(), ?RT:key()) -> true.
prop_update_id(OldId, NewId) ->
    Ring = unittest_helper:make_ring_with_ids([OldId], [{config, [pdb:get(log_path, ?MODULE)]}]),
    change_id_and_check(OldId, NewId),
    change_id_and_check(NewId, OldId),
    unittest_helper:stop_ring(Ring),
    true.

tester_update_id(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    pdb:set({log_path, PrivDir}, ?MODULE),
    tester:test(rm_SUITE, prop_update_id, 2, 10).

%% @doc Changes a node's ID and checks that the change has been performed.
%%      Precondition: existing scalaris ring with at least one node.
-spec change_id_and_check(OldId::?RT:key() | unknown, NewId::?RT:key()) -> true.
change_id_and_check(OldId, NewId) ->
    DhtNode = comm:make_global(pid_groups:find_a(dht_node)),
    comm:send(DhtNode, {get_node_details, comm:this(), [node]}),
    OldNode = receive
                  {get_node_details_response, OldNodeDetails} ->
                      node_details:get(OldNodeDetails, node)
              end,
    case OldId =/= unknown of
        true -> ?equals(node:id(OldNode), OldId);
        _ -> ok
    end,
%%     ct:pal("ct: ~p -> ~p~n", [node:id(OldNode), NewId]),
    
    comm:send(DhtNode, {rm, subscribe, self(), rm_SUITE,
                        fun(OldN, NewN, _Reason) -> OldN =/= NewN end,
                        fun(Pid, Tag, OldNeighbors, NewNeighbors, _Reason) ->
                                comm:send_local(Pid, {rm_changed, Tag, OldNeighbors, NewNeighbors})
                        end, inf}),
    comm:send(DhtNode, {rm, update_my_id, NewId}),
    comm:send(DhtNode, {rm, unsubscribe, self(), rm_SUITE}),
    
    % check that the new ID has been set:
    comm:send(DhtNode, {get_node_details, comm:this(), [node, pred, succ]}),
    receive
        {get_node_details_response, NewNodeDetails} ->
            NewNode = node_details:get(NewNodeDetails, node),
            Pred = node_details:get(NewNodeDetails, pred),
            Succ = node_details:get(NewNodeDetails, succ),
            case node:id(OldNode) =/= NewId andalso
                     ((NewNode =:= Pred) andalso (NewNode =:= Succ) % 1-element ring
                     orelse % two nodes:
                          ((Pred =:= Succ) andalso (node:id(Pred) =/= NewId))
                     orelse % at least three nodes:
                          intervals:in(NewId,
                                       intervals:new('(', node:id(Pred), node:id(Succ), ')'))) of
                true ->
                    ?equals(node:id(NewNode), NewId),
                    ?equals(node:id_version(NewNode), node:id_version(OldNode) + 1),
                    check_subscr_node_update(OldNode, NewNode),
                    % now wait for all rm processes of the other nodes to know about the new id (at least in pred/succ):
                    util:wait_for(
                      fun() ->
                              Nodes =
                                  lists:flatten(
                                    [begin
                                         comm:send(comm:make_global(N),
                                                   {get_node_details, comm:this(), [node, pred, succ]}),
                                         receive
                                             {get_node_details_response, ND} ->
                                                 [node_details:get(ND, node),
                                                  node_details:get(ND, pred),
                                                  node_details:get(ND, succ)]
                                         end
                                     end || N <- pid_groups:find_all(dht_node)]),
                              not lists:any(fun(N) -> N =:= OldNode end, Nodes)
                      end);
                _ ->
                    ?equals(NewNode, OldNode),
                    ?expect_no_message()
            end
    end,
    true.

-spec check_subscr_node_update(OldNode::node:node_type(), NewNode::node:node_type()) -> any().
check_subscr_node_update(OldNode, NewNode) ->
    receive
        {rm_changed, rm_SUITE, OldNeighbors, NewNeighbors} ->
            ?equals(nodelist:node(OldNeighbors), OldNode),
            ?equals(nodelist:node(NewNeighbors), NewNode)
    after 1000 ->
        ActualMessage =
            receive
                X -> X
            after 0 -> no_message
            end,
            ?ct_fail("expected message {rm_changed, rm_SUITE, OldNeighbors, NewNeighbors} but got \"~.0p\"~n", [ActualMessage])
    end.

-spec prop_update_id2(?RT:key()) -> true.
prop_update_id2(NewId) ->
    change_id_and_check(unknown, NewId).

tester_update_id2_1(Config) ->
    Config2 = unittest_helper:start_minimal_procs(Config, [], true),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config2),
    RandomSubset = util:random_subset(1, ?RT:get_replica_keys(?RT:hash_key("0"))),
    _ = unittest_helper:stop_minimal_procs(Config2),
    Ring = unittest_helper:make_ring_with_ids(
             RandomSubset,
             [{config, [{log_path, PrivDir}]}]),
    tester:test(rm_SUITE, prop_update_id2, 1, 1000),
    unittest_helper:stop_ring(Ring).

tester_update_id2_2(Config) ->
    Config2 = unittest_helper:start_minimal_procs(Config, [], true),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config2),
    RandomSubset = util:random_subset(2, ?RT:get_replica_keys(?RT:hash_key("0"))),
    _ = unittest_helper:stop_minimal_procs(Config2),
    Ring = unittest_helper:make_ring_with_ids(
             RandomSubset,
             [{config, [{log_path, PrivDir}]}]),
    tester:test(rm_SUITE, prop_update_id2, 1, 100),
    unittest_helper:stop_ring(Ring).

tester_update_id2_3(Config) ->
    Config2 = unittest_helper:start_minimal_procs(Config, [], true),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config2),
    RandomSubset = util:random_subset(3, ?RT:get_replica_keys(?RT:hash_key("0"))),
    _ = unittest_helper:stop_minimal_procs(Config2),
    Ring = unittest_helper:make_ring_with_ids(
             RandomSubset,
             [{config, [{log_path, PrivDir}]}]),
    tester:test(rm_SUITE, prop_update_id2, 1, 100),
    unittest_helper:stop_ring(Ring).

tester_update_id2_4(Config) ->
    Config2 = unittest_helper:start_minimal_procs(Config, [], true),
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config2),
    RandomSubset = util:random_subset(4, ?RT:get_replica_keys(?RT:hash_key("0"))),
    _ = unittest_helper:stop_minimal_procs(Config2),
    Ring = unittest_helper:make_ring_with_ids(
             RandomSubset,
             [{config, [{log_path, PrivDir}]}]),
    tester:test(rm_SUITE, prop_update_id2, 1, 100),
    unittest_helper:stop_ring(Ring).
