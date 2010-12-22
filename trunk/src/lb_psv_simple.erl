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
%% @doc    Simple passive load balancing sampling k nodes and choosing the
%%         one that reduces the standard deviation the most.
%%         Uses randomly sampled IDs.
%% @end
%% @version $Id$
-module(lb_psv_simple).
-author('kruber@zib.de').
-vsn('$Id$ ').

-behaviour(lb_psv_beh).
-include("lb_psv_beh.hrl").

-ifdef(with_export_type_support).
-export_type([custom_message/0]).
-endif.

-type custom_message() :: none().

%% @doc Gets the number of IDs to sample during join.
%%      Note: this is executed at the joining node.
get_number_of_samples([First | _Rest] = _ContactNodes) ->
    comm:send_local(self(), {join, get_number_of_samples,
                             conf_get_number_of_samples(), First}).

%% @doc Sends the number of IDs to sample during join to the joining node.
%%      Note: this is executed at the existing node.
get_number_of_samples_remote(SourcePid) ->
    comm:send(SourcePid, {join, get_number_of_samples,
                          conf_get_number_of_samples(), comm:this()}).

%% @doc Creates a join operation if a node would join at my node with the
%%      given key. This will simulate the join operation and return a lb_op()
%%      with all the data needed in sort_candidates/1.
%%      Note: this is executed at an existing node.
create_join(DhtNodeState, SelectedKey, SourcePid) ->
    Candidate =
        try
            MyNode = dht_node_state:get(DhtNodeState, node),
            MyNodeId = node:id(MyNode),
            case SelectedKey of
                MyNodeId ->
                    log:log(warn, "[ Node ~w ] join requested for my ID, "
                                "sending no_op...", [self()]),
                    lb_op:no_op();
                _ ->
                    MyPredId = dht_node_state:get(DhtNodeState, pred_id),
                    MyLoad = dht_node_state:get(DhtNodeState, load),
                    DB = dht_node_state:get(DhtNodeState, db),
                    Interval = node:mk_interval_between_ids(MyPredId, SelectedKey),
                    OtherLoadNew = ?DB:get_load(DB, Interval),
                    MyLoadNew = MyLoad - OtherLoadNew,
                    MyNodeDetails = node_details:set(
                                      node_details:set(node_details:new(), node, MyNode), load, MyLoad),
                    MyNodeDetailsNew = node_details:set(MyNodeDetails, load, MyLoadNew),
                    OtherNodeDetails = node_details:set(node_details:new(), load, 0),
                    OtherNodeDetailsNew = node_details:set(
                                            node_details:set(node_details:new(), new_key, SelectedKey), load, OtherLoadNew),
                    lb_op:slide_op(OtherNodeDetails, MyNodeDetails,
                                   OtherNodeDetailsNew, MyNodeDetailsNew)
            end
        catch
            Error:Reason ->
                log:log(error, "[ Node ~w ] could not create a candidate "
                            "for another node: ~.0p:~.0p~n"
                            "  SelectedKey: ~.0p, SourcePid: ~.0p~n  State: ~.0p",
                        [self(), Error, Reason, SelectedKey, SourcePid, DhtNodeState]),
                lb_op:no_op()
        end,
    comm:send(SourcePid, {join, get_candidate_response, SelectedKey, Candidate}),
    DhtNodeState.

%% @doc Sorts all provided operations so that the one with the biggest change
%%      of the standard deviation is at the front.
%%      Note: this is executed at the joining node.
sort_candidates(Ops) -> lb_common:bestStddev(Ops, plus_infinity).

-spec process_join_msg(custom_message(), SourcePid::comm:mypid(),
        DhtNodeState::dht_node_state:state()) -> unknown_event.
process_join_msg(_Msg, _SourcePid, _DhtNodeState) ->
    unknown_event.

%% @doc Checks whether config parameters of the passive load balancing
%%      algorithm exist and are valid.
check_config() ->
    config:is_integer(lb_psv_simple_samples) and
    config:is_greater_than_equal(lb_psv_simple_samples, 1).

%% @doc Gets the number of nodes to sample (set in the config files).
-spec conf_get_number_of_samples() -> pos_integer().
conf_get_number_of_samples() ->
    config:read(lb_psv_simple_samples).
