%  @copyright 2010-2012 Zuse Institute Berlin

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
%%         least lb_psv_samples samples) and choose the
%%         one that reduces the standard deviation the most.
%%         If there is no load address ranges are split according to
%%         lb_psv_split_fallback, i.e. in halves or using the selected keys,
%%         otherwise no more load is moved than is needed to have the average
%%         load on one of the two nodes.
%% @end
%% @version $Id$
-module(lb_psv_gossip).
-author('kruber@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p)~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State), ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p)~n", [self(), Msg, State])).

-behaviour(lb_psv_beh).
-include("lb_psv_beh.hrl").

-export_type([custom_message/0]).

-type custom_message() ::
          {gossip_get_values_best_response, gossip_load:load_info()} |
          {get_split_key_response, SplitKey::{?RT:key(), TakenLoad::non_neg_integer()}}.

%% @doc Gets the number of IDs to sample during join.
%%      Note: this is executed at the joining node.
-spec get_number_of_samples(Conn::dht_node_join:connection()) -> ok.
get_number_of_samples({_, ContactNode} = Connection) ->
    Msg = {join, number_of_samples_request, comm:this(), ?MODULE, Connection},
    ?TRACE_SEND(ContactNode, Msg),
    comm:send(ContactNode, Msg).

%% @doc Sends the number of IDs to sample during join to the joining node.
%%      Note: this is executed at the existing node.
-spec get_number_of_samples_remote(
        SourcePid::comm:mypid(), Conn::dht_node_join:connection()) -> ok.
get_number_of_samples_remote(SourcePid, Connection) ->
    SPid = comm:reply_as(self(), 3,
                         {join, ?MODULE, '_',
                          {get_samples, SourcePid, Connection}}),
    gossip_load:get_values_best([{source_pid, SPid}]).

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
            SPid = comm:reply_as(self(), 3,
                                 {join, ?MODULE, '_',
                                  {create_join2, SelectedKey, SourcePid, Conn}}),
            gossip_load:get_values_best([{source_pid, SPid}]);
        _ ->
            % postpone message:
            Msg = {join, get_candidate, SourcePid, SelectedKey, ?MODULE, Conn},
            ?TRACE_SEND(self(), Msg),
            _ = comm:send_local_after(100, self(), Msg),
            ok
    end,
    DhtNodeState.

-spec create_join2(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key(),
                   SourcePid::comm:mypid(), BestValues::gossip_load:load_info(),
                   Conn::dht_node_join:connection()) -> dht_node_state:state().
create_join2(DhtNodeState, SelectedKey, SourcePid, BestValues, Conn) ->
    case dht_node_state:get(DhtNodeState, slide_pred) of
        null ->
            Neighbors = dht_node_state:get(DhtNodeState, neighbors),
            MyNodeId = nodelist:nodeid(Neighbors),
            try
                MyLoad = dht_node_state:get(DhtNodeState, load),
                if MyLoad >= 2 ->
%%                        log:pal("[ ~.0p ] trying split by load", [self()]),
                       TargetLoad =
                           case gossip_load:load_info_get(avgLoad, BestValues) of
                               unknown -> util:floor(MyLoad / 2);
                               AvgLoad when AvgLoad > 0 andalso MyLoad > AvgLoad ->
                                   util:floor(erlang:min(MyLoad - AvgLoad, AvgLoad));
                               _       -> util:floor(MyLoad / 2)
                           end,
%%                        log:pal("T: ~.0p, My: ~.0p, Avg: ~.0p~n", [TargetLoad, MyLoad, gossip_load:load_info_get(avgLoad, BestValues)]),
                       MyPredId = node:id(nodelist:pred(Neighbors)),
                       SPid = comm:reply_as(self(), 3,
                                            {join, ?MODULE, '_',
                                             {create_join3, SelectedKey, SourcePid, Conn}}),
                       DBCache = pid_groups:get_my(dht_node_db_cache),

                       Msg = {get_split_key, dht_node_state:get(DhtNodeState, db),
                              dht_node_state:get(DhtNodeState, full_range), MyPredId, MyNodeId,
                              TargetLoad, forward, SPid},
                       ?TRACE_SEND(DBCache, Msg),
                       comm:send_local(DBCache, Msg),
                       DhtNodeState;
                   true -> % fall-back
                       create_join3(DhtNodeState, SelectedKey, SourcePid, {MyNodeId, 0}, Conn)
                end
            catch
                Error:Reason -> % fall-back
                    log:log(error, "[ Node ~w ] failed to get split key "
                                "for another node: ~.0p:~.0p~n"
                                "  SelectedKey: ~.0p, SourcePid: ~.0p~n  State: ~.0p",
                            [self(), Error, Reason, SelectedKey, SourcePid, DhtNodeState]),
                    create_join3(DhtNodeState, SelectedKey, SourcePid, {MyNodeId, 0}, Conn)
            end;
        _ ->
            % postpone message:
            SPid = comm:reply_as(self(), 3,
                                 {join, ?MODULE, '_',
                                  {create_join2, SelectedKey, SourcePid, Conn}}),
            gossip_load:get_values_best([{source_pid, SPid}, {send_after, 100}]),
            DhtNodeState
    end.

-spec create_join3(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key(),
                   SourcePid::comm:mypid(),
                   SplitKey::{?RT:key(), TakenLoad::non_neg_integer()},
                   Conn::dht_node_join:connection()) -> dht_node_state:state().
create_join3(DhtNodeState, SelectedKey, SourcePid, SplitKey0, Conn) ->
    case dht_node_state:get(DhtNodeState, slide_pred) of
        null ->
            Candidate =
                try
                    MyNode = dht_node_state:get(DhtNodeState, node),
                    MyNodeId = node:id(MyNode),
                    {SplitKey, OtherLoadNew} =
                        case SplitKey0 of
                            {MyNodeId, _TargetLoadNew} -> % fall-back
%%                                 log:log(info, "[ Node ~w ] could not split load - no key in my range, "
%%                                             "using fall-back instead", [self()]),
                                case config:read(lb_psv_split_fallback) of
                                    split_address -> % split address range:
%%                                         log:pal("[ ~.0p ] trying split by address range", [self()]),
                                        lb_common:split_my_range(DhtNodeState, SelectedKey);
                                    keep_key -> % keep (randomly selected) key:
                                        lb_common:split_by_key(DhtNodeState, SelectedKey)
                                end;
                            X -> X
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
                            MyLoad = dht_node_state:get(DhtNodeState, load),
                            MyLoadNew = MyLoad - OtherLoadNew,
                            MyNodeDetails1 = node_details:set(node_details:new(), node, MyNode),
                            MyNodeDetails = node_details:set(MyNodeDetails1, load, MyLoad),
                            MyNodeDetailsNew = node_details:set(MyNodeDetails, load, MyLoadNew),
                            OtherNodeDetails = node_details:set(node_details:new(), load, 0),
                            OtherNodeDetailsNew1 = node_details:set(node_details:new(), new_key, SplitKey),
                            OtherNodeDetailsNew2 = node_details:set(OtherNodeDetailsNew1, load, OtherLoadNew),
                            Interval = node:mk_interval_between_ids(MyPredId, SplitKey),
                            OtherNodeDetailsNew = node_details:set(OtherNodeDetailsNew2, my_range, Interval),
                            lb_op:slide_op(comm:this(), OtherNodeDetails, MyNodeDetails,
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
            % re-start with create_join2 (it can then create a new up-to-date request for a split key):
            SPid = comm:reply_as(self(), 3,
                                 {join, ?MODULE, '_',
                                  {create_join2, SelectedKey, SourcePid, Conn}}),
            gossip_load:get_values_best([{source_pid, SPid}, {send_after, 100}]),
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
    lb_common:bestStddev(util:shuffle(Ops), plus_infinity,
                         fun lb_psv_split:my_sort_fun/2).

-spec process_join_msg(custom_message(),
        {get_samples, SourcePid::comm:mypid(), Conn::dht_node_join:connection()} |
            {create_join2, SelectedKey::?RT:key(), SourcePid::comm:mypid(), Conn::dht_node_join:connection()} |
            {create_join3, SelectedKey::?RT:key(), SourcePid::comm:mypid(), BestValues::gossip_load:load_info(), Conn::dht_node_join:connection()},
        DhtNodeState::dht_node_state:state()) -> dht_node_state:state().
process_join_msg({gossip_get_values_best_response, BestValues},
                 {get_samples, SourcePid, Conn}, DhtNodeState) ->
    MinSamples = conf_get_min_number_of_samples(),
    Samples = case gossip_load:load_info_get(size, BestValues) of
                  unknown -> MinSamples;
                  % always round up:
                  Size    -> erlang:max(util:ceil((util:log2(Size))), MinSamples)
              end,
    comm:send(SourcePid, {join, get_number_of_samples, Samples, Conn}),
    DhtNodeState;
process_join_msg({gossip_get_values_best_response, BestValues},
                 {create_join2, SelectedKey, SourcePid, Conn}, DhtNodeState) ->
    create_join2(DhtNodeState, SelectedKey, SourcePid, BestValues, Conn);
process_join_msg({get_split_key_response, SplitKey},
                 {create_join3, SelectedKey, SourcePid, Conn}, DhtNodeState) ->
    create_join3(DhtNodeState, SelectedKey, SourcePid, SplitKey, Conn).

%% @doc Checks whether config parameters of the passive load balancing
%%      algorithm exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(lb_psv_samples) and
    config:cfg_is_greater_than_equal(lb_psv_samples, 1) and
    config:cfg_is_in(lb_psv_split_fallback, [split_address, keep_key]).

%% @doc Gets the minnimum number of nodes to sample (set in the config files).
-spec conf_get_min_number_of_samples() -> pos_integer().
conf_get_min_number_of_samples() ->
    config:read(lb_psv_samples).
