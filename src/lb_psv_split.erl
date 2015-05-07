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
%% @doc    Simple passive load balancing sampling k nodes and choosing the
%%         one that reduces the standard deviation the most.
%%         Splits loads in half, if there is no load address ranges are split
%%         according to lb_psv_split_fallback, i.e. in halves or using the
%%         selected keys.
%% @end
%% @version $Id$
-module(lb_psv_split).
-author('kruber@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p)~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State), ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p)~n", [self(), Msg, State])).

-behaviour(lb_psv_beh).
-include("lb_psv_beh.hrl").

-export_type([custom_message/0]).

-export([my_sort_fun/2]).

-type custom_message() ::
          {get_split_key_response, SplitKey::{?RT:key(), TakenLoad::non_neg_integer()}}.

%% @doc Gets the number of IDs to sample during join.
%%      Note: this is executed at the joining node.
-spec get_number_of_samples(Conn::dht_node_join:connection()) -> ok.
get_number_of_samples(Connection) ->
    comm:send_local(self(), {join, get_number_of_samples,
                             conf_get_number_of_samples(), Connection}).

%% @doc Sends the number of IDs to sample during join to the joining node.
%%      Note: this is executed at the existing node.
-spec get_number_of_samples_remote(
        SourcePid::comm:mypid(), Conn::dht_node_join:connection()) -> ok.
get_number_of_samples_remote(SourcePid, Connection) ->
    comm:send(SourcePid, {join, get_number_of_samples,
                          conf_get_number_of_samples(), Connection}).

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
            Neighbors = dht_node_state:get(DhtNodeState, neighbors),
            MyNodeId = nodelist:nodeid(Neighbors),
            try
                MyLoad = dht_node_state:get(DhtNodeState, load),
                if MyLoad >= 2 ->
%%                        log:pal("[ ~.0p ] trying split by load", [self()]),
                       TargetLoad = util:floor(MyLoad / 2),
%%                        log:pal("T: ~.0p, My: ~.0p~n", [TargetLoad, MyLoad]),
                       MyPredId = node:id(nodelist:pred(Neighbors)),
                       SPid = comm:reply_as(self(), 3,
                                            {join, ?MODULE, '_',
                                             {create_join2, SelectedKey, SourcePid, Conn}}),
                       DBCache = pid_groups:get_my(dht_node_db_cache),
                       
                       Msg = {get_split_key, dht_node_state:get(DhtNodeState, db),
                              dht_node_state:get(DhtNodeState, full_range), MyPredId, MyNodeId,
                              TargetLoad, forward, SPid},
                       ?TRACE_SEND(DBCache, Msg),
                       comm:send_local(DBCache, Msg),
                       DhtNodeState;
                   true -> % fall-back
                       create_join2(DhtNodeState, SelectedKey, SourcePid, {MyNodeId, 0}, Conn)
                end
            catch
                Error:Reason -> % fall-back
                    log:log(error, "[ Node ~w ] failed to get split key "
                                "for another node: ~.0p:~.0p~n"
                                "  SelectedKey: ~.0p, SourcePid: ~.0p~n  State: ~.0p",
                            [self(), Error, Reason, SelectedKey, SourcePid, DhtNodeState]),
                    create_join2(DhtNodeState, SelectedKey, SourcePid, {MyNodeId, 0}, Conn)
            end;
        _ ->
            % postpone message:
            Msg = {join, get_candidate, SourcePid, SelectedKey, ?MODULE, Conn},
            ?TRACE_SEND(SourcePid, Msg),
            _ = comm:send_local_after(100, self(), Msg),
            DhtNodeState
    end.

-spec create_join2(DhtNodeState::dht_node_state:state(), SelectedKey::?RT:key(),
                   SourcePid::comm:mypid(),
                   SplitKey::{?RT:key(), TakenLoad::non_neg_integer()},
                   Conn::dht_node_join:connection()) -> dht_node_state:state().
create_join2(DhtNodeState, SelectedKey, SourcePid, SplitKey0, Conn) ->
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
%%                                             "splitting address range instead", [self()]),
                                lb_common:split_my_range(DhtNodeState, SelectedKey);
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
%%                             log:pal("MK: ~.0p, SK: ~.0p~n", [MyNodeId, SplitKey]),
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
            % re-start with create_join (it can then create a new up-to-date request for a split key):
            Msg = {join, get_candidate, SourcePid, SelectedKey, ?MODULE, Conn},
            ?TRACE_SEND(SourcePid, Msg),
            _ = comm:send_local_after(100, self(), Msg),
            ok
    end,
    DhtNodeState.

%% @doc Sort function for two operations and their Sum2Change.
%%      Op1 will be preferred over Op2, i.e. Op1 is smaller than Op2, if its
%%      Sum2Change is smaller or if it is equal and its new node's address
%%      space is larger.
-spec my_sort_fun(Op1::{lb_op:lb_op(), integer()},
                  Op2::{lb_op:lb_op(), integer()}) -> boolean().
my_sort_fun({Op1, Op1Change}, {Op2, Op2Change}) ->
    if Op1Change < Op2Change -> true;
       true ->
           case config:read(lb_psv_split_fallback) of
               split_address when Op1Change =:= Op2Change ->
                   Op1NewInterval = node_details:get(lb_op:get(Op1, n1_new), my_range),
                   {_, Op1NewPredId, Op1NewMyId, _} =
                       intervals:get_bounds(Op1NewInterval),
                   Op1NewRange = try ?RT:get_range(Op1NewPredId, Op1NewMyId)
                                 catch throw:not_supported -> 0
                                 end,
                   Op2NewInterval = node_details:get(lb_op:get(Op2, n1_new), my_range),
                   {_, Op2NewPredId, Op2NewMyId, _} =
                       intervals:get_bounds(Op2NewInterval),
                   Op2NewRange = try ?RT:get_range(Op2NewPredId, Op2NewMyId)
                                 catch throw:not_supported -> 0
                                 end,
                   Op2NewRange =< Op1NewRange;
               split_address -> false;
               keep_key -> Op1Change =:= Op2Change
           end
    end.

%% @doc Sorts all provided operations so that the one with the biggest change
%%      of the standard deviation is at the front. In case of no load changes,
%%      the operation with the largest address range at the joining node will
%%      be at the front.
%%      Note: this is executed at the joining node.
-spec sort_candidates(Ops::[lb_op:lb_op()]) -> [lb_op:lb_op()].
sort_candidates(Ops) ->
    lb_common:bestStddev(util:shuffle(Ops), plus_infinity, fun my_sort_fun/2).

-spec process_join_msg(comm:message(), State::any(),
        DhtNodeState::dht_node_state:state()) -> unknown_event.
process_join_msg({get_split_key_response, SplitKey},
                 {create_join2, SelectedKey, SourcePid, Conn}, DhtNodeState) ->
    create_join2(DhtNodeState, SelectedKey, SourcePid, SplitKey, Conn);
process_join_msg(_Msg, _State, _DhtNodeState) ->
    unknown_event.

%% @doc Checks whether config parameters of the passive load balancing
%%      algorithm exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(lb_psv_samples) and
    config:cfg_is_greater_than_equal(lb_psv_samples, 1) and
    config:cfg_is_in(lb_psv_split_fallback, [split_address, keep_key]).

%% @doc Gets the number of nodes to sample (set in the config files).
-spec conf_get_number_of_samples() -> pos_integer().
conf_get_number_of_samples() ->
    config:read(lb_psv_samples).
