%  @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc    Passive load balancing using gossip to sample O(log(N)) nodes (at
%%         least lb_psv_gossip_min_samples samples) and choose the
%%         one that reduces the standard deviation the most.
%%         If there is no load address ranges are split in halves, otherwise
%%         no more load is moved than is needed to have the average load on one
%%         of the two nodes.
%% @end
%% @version $Id$
-module(lb_psv_gossip).
-author('kruber@zib.de').
-vsn('$Id$ ').

%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p)~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State), ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p)~n", [self(), Msg, State])).

-behaviour(lb_psv_beh).
-include("lb_psv_beh.hrl").

-ifdef(with_export_type_support).
-export_type([custom_message/0]).
-endif.

-type custom_message() :: {gossip_get_values_best_response, gossip_state:values()}.

%% @doc Gets the number of IDs to sample during join.
%%      Note: this is executed at the joining node.
-spec get_number_of_samples(Conn::dht_node_join:connection()) -> ok.
get_number_of_samples({_, ContactNode} = Connection) ->
    comm:send(ContactNode, {join, number_of_samples_request, comm:this(),
                            ?MODULE, Connection}).

%% @doc Sends the number of IDs to sample during join to the joining node.
%%      Note: this is executed at the existing node.
-spec get_number_of_samples_remote(
        SourcePid::comm:mypid(), Conn::dht_node_join:connection()) -> ok.
get_number_of_samples_remote(SourcePid, Connection) ->
    MyPidWithCookie =
        comm:self_with_cookie({join, ?MODULE, {get_samples, SourcePid, Connection}}),
    GossipPid = pid_groups:get_my(gossip),
    comm:send_local(GossipPid, {get_values_best, MyPidWithCookie}).

%% @doc Creates a join operation if a node would join at my node with the
%%      given key. This will simulate the join operation and return a lb_op()
%%      with all the data needed in sort_candidates/1.
%%      Note: this is executed at an existing node.
-spec create_join(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key(),
                  SourcePid::comm:mypid(), Conn::dht_node_join:connection())
        -> dht_node_state:state().
create_join(DhtNodeState, SelectedKey, SourcePid, Conn) ->
    case dht_node_state:get(DhtNodeState, slide_pred) of
        null ->
            MyPidWithCookie =
                comm:self_with_cookie({join, ?MODULE,
                                       {create_join, SelectedKey, SourcePid, Conn}}),
            GossipPid = pid_groups:get_my(gossip),
            Msg = {get_values_best, MyPidWithCookie},
            ?TRACE_SEND(GossipPid, Msg),
            comm:send_local(GossipPid, Msg);
        _ ->
            % postpone message:
            Msg = {join, get_candidate, SourcePid, SelectedKey, ?MODULE, Conn},
            ?TRACE_SEND(SourcePid, Msg),
            _ = comm:send_local_after(100, self(), Msg),
            ok
    end,
    DhtNodeState.

-spec create_join2(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key(),
                   SourcePid::comm:mypid(), BestValues::gossip_state:values(),
                   Conn::dht_node_join:connection()) -> dht_node_state:state().
create_join2(DhtNodeState, SelectedKey, SourcePid, BestValues, Conn) ->
    case dht_node_state:get(DhtNodeState, slide_pred) of
        null ->
            Candidate =
                try
                    MyNode = dht_node_state:get(DhtNodeState, node),
                    MyNodeId = node:id(MyNode),
                    MyLoad = dht_node_state:get(DhtNodeState, load),
                    {SplitKey, OtherLoadNew} =
                        case MyLoad >= 2 of
                            true ->
%%                                 ct:pal("[ ~.0p ] trying split by load", [self()]),
                                TargetLoad =
                                    case gossip_state:get(BestValues, avgLoad) of
                                        unknown -> util:floor(MyLoad / 2);
                                        AvgLoad when AvgLoad > 0 andalso MyLoad > AvgLoad ->
                                            util:floor(erlang:min(MyLoad - AvgLoad, AvgLoad));
                                        _       -> util:floor(MyLoad / 2)
                                    end,
%%                                 ct:pal("T: ~.0p, My: ~.0p, Avg: ~.0p~n", [TargetLoad, MyLoad, gossip_state:get(BestValues, avgLoad)]),
                                try lb_common:split_by_load(DhtNodeState, TargetLoad)
                                catch
                                    throw:'no key in range' ->
                                        %%                                 log:log(info, "[ Node ~w ] could not split load - no key in my range, "
                                        %%                                             "splitting address range instead", [self()]),
                                        lb_common:split_my_range(DhtNodeState, SelectedKey)
                                end;
                            _ -> % split address range (fall-back):
%%                                 ct:pal("[ ~.0p ] trying split by address range", [self()]),
                                lb_common:split_my_range(DhtNodeState, SelectedKey)
                        end,
                    MyPredId = node:id(dht_node_state:get(DhtNodeState, pred)),
                    case SplitKey of
                        MyNodeId ->
                            log:log(warn, "[ Node ~w ] join requested for my ID (~.0p), "
                                        "sending no_op...", [self(), MyNodeId]),
                            lb_op:no_op();
                        MyPredId ->
                            log:log(warn, "[ Node ~w ] join requested for my pred's ID (~.0p), "
                                        "sending no_op...", [self(), MyPredId]),
                            lb_op:no_op();
                        _ ->
                            MyLoadNew = MyLoad - OtherLoadNew,
                            MyNodeDetails1 = node_details:set(node_details:new(), node, MyNode),
                            MyNodeDetails = node_details:set(MyNodeDetails1, load, MyLoad),
                            MyNodeDetailsNew = node_details:set(MyNodeDetails, load, MyLoadNew),
                            OtherNodeDetails = node_details:set(node_details:new(), load, 0),
                            OtherNodeDetailsNew1 = node_details:set(node_details:new(), new_key, SplitKey),
                            OtherNodeDetailsNew2 = node_details:set(OtherNodeDetailsNew1, load, OtherLoadNew),
                            Interval = node:mk_interval_between_ids(MyPredId, SplitKey),
                            OtherNodeDetailsNew = node_details:set(OtherNodeDetailsNew2, my_range, Interval),
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
            Msg = {join, get_candidate_response, SelectedKey, Candidate, Conn},
            ?TRACE_SEND(SourcePid, Msg),
            comm:send(SourcePid, Msg);
        _ ->
            % postpone message:
            MyPidWithCookie =
                comm:self_with_cookie({join, ?MODULE,
                                       {create_join, SelectedKey, SourcePid, Conn}}),
            GossipPid = pid_groups:get_my(gossip),
            Msg = {get_values_best, MyPidWithCookie},
            ?TRACE_SEND(GossipPid, Msg),
            _ = comm:send_local_after(100, GossipPid, Msg),
            ok
    end,
    DhtNodeState.

%% @doc Sorts all provided operations so that the one with the biggest change
%%      of the standard deviation is at the front. In case of no load changes,
%%      the operation with the largest address range at the joining node will
%%      be at the front.
%%      Note: this is executed at the joining node.
-spec sort_candidates(Ops::[lb_op:lb_op()]) -> [lb_op:lb_op()].
sort_candidates(Ops) ->
    lb_common:bestStddev(Ops, plus_infinity, fun lb_psv_split:my_sort_fun/2).

-spec process_join_msg(custom_message(),
        {get_samples, SourcePid::comm:mypid(), Conn::dht_node_join:connection()} |
            {create_join, SelectedKey::?RT:key(), SourcePid::comm:mypid(), Conn::dht_node_join:connection()},
        DhtNodeState::dht_node_state:state()) -> dht_node_state:state().
process_join_msg({gossip_get_values_best_response, BestValues},
                 {get_samples, SourcePid, Conn}, DhtNodeState) ->
    MinSamples = conf_get_min_number_of_samples(),
    Samples = case gossip_state:get(BestValues, size) of
                  unknown -> MinSamples;
                  % always round up:
                  Size    -> erlang:max(util:ceil((util:log2(Size))), MinSamples)
              end, 
    comm:send(SourcePid, {join, get_number_of_samples, Samples, Conn}),
    DhtNodeState;
process_join_msg({gossip_get_values_best_response, BestValues},
                 {create_join, SelectedKey, SourcePid, Conn}, DhtNodeState) ->
    create_join2(DhtNodeState, SelectedKey, SourcePid, BestValues, Conn).

%% @doc Checks whether config parameters of the passive load balancing
%%      algorithm exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(lb_psv_gossip_min_samples) and
    config:cfg_is_greater_than_equal(lb_psv_gossip_min_samples, 1).

%% @doc Gets the minnimum number of nodes to sample (set in the config files).
-spec conf_get_min_number_of_samples() -> pos_integer().
conf_get_min_number_of_samples() ->
    config:read(lb_psv_gossip_min_samples).
