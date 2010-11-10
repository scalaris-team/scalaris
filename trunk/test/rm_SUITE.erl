%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [tester_update_id, tester_update_id2, tester_update_id2_1].

suite() ->
    [
     {timetrap, {seconds, 180}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    unittest_helper:end_per_suite(Config),
    ok.

init_per_testcase(TestCase, Config) ->
    case TestCase of
        skip_test ->
            {skip, "skip this test"};
        _ ->
            Config
    end.

end_per_testcase(_TestCase, _Config) ->
    unittest_helper:stop_ring(),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ...
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_update_id(?RT:key(), ?RT:key()) -> boolean().
prop_update_id(OldId, NewId) ->
    Ring = unittest_helper:make_ring_with_ids([OldId]),
    change_id_and_check(OldId, NewId),
    unittest_helper:stop_ring(Ring),
    true.

tester_update_id(_Config) ->
    tester:test(rm_SUITE, prop_update_id, 2, 10).

%% @doc Changes a node's ID and checks that the change has been performed.
%%      Precondition: existing scalaris ring with at least one node.
-spec change_id_and_check(OldId::?RT:key() | unknown, NewId::?RT:key()) -> true.
change_id_and_check(OldId, NewId) ->
    RM = comm:make_global(pid_groups:find_a(ring_maintenance)),
    comm:send_to_group_member(RM, dht_node, {get_node_details, comm:this(), [node, predlist, succlist]}),
    OldNode = receive
                  {get_node_details_response, OldNodeDetails} ->
                      node_details:get(OldNodeDetails, node)
              end,
    case OldId =/= unknown of
        true -> ?equals(node:id(OldNode), OldId);
        _ -> ok
    end,
%%     ct:pal("ct: ~p -> ~p~n", [node:id(OldNode), NewId]),
    
    comm:send(RM, {subscribe, self(), rm_SUITE, fun rm_loop:subscribe_default_filter/2, fun rm_loop:send_changes_to_subscriber/4}),
    comm:send(RM, {update_id, NewId}),
    comm:send(RM, {unsubscribe, self(), rm_SUITE}),
    
    % check that the new ID has been set:
    comm:send_to_group_member(RM, dht_node, {get_node_details, comm:this(), [node, pred, succ]}),
    receive
        {get_node_details_response, NewNodeDetails} ->
            NewNode = node_details:get(NewNodeDetails, node),
            Pred = node_details:get(NewNodeDetails, pred),
            Succ = node_details:get(NewNodeDetails, succ),
            case node:id(OldNode) =/= NewId andalso
                     ((NewNode =:= Pred) andalso (NewNode =:= Succ) orelse
                     intervals:in(NewId,
                                  intervals:new('(', node:id(Pred), node:id(Succ), ')'))) of
                true ->
                    ?equals(node:id(NewNode), NewId),
                    ?equals(node:id_version(NewNode), node:id_version(OldNode) + 1),
                    check_subscr_node_update(OldNode, NewNode);
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

-spec prop_update_id2(?RT:key()) -> boolean().
prop_update_id2(NewId) ->
    change_id_and_check(unknown, NewId).

tester_update_id2(_Config) ->
    Ring = unittest_helper:make_ring_with_ids(fun() -> [?RT:hash_key(0)] end),
    tester:test(rm_SUITE, prop_update_id2, 1, 1000),
    unittest_helper:stop_ring(Ring).

tester_update_id2_1(_Config) ->
    Ring = unittest_helper:make_ring_with_ids(fun() -> ?RT:get_replica_keys(?RT:hash_key(0)) end),
    tester:test(rm_SUITE, prop_update_id2, 1, 100),
    unittest_helper:stop_ring(Ring).
