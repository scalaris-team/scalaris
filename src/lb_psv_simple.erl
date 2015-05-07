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
%%         Uses randomly sampled IDs.
%% @end
%% @version $Id$
-module(lb_psv_simple).
-author('kruber@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p)~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State), ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n  State: ~.0p)~n", [self(), Msg, State])).

-behaviour(lb_psv_beh).
-include("lb_psv_beh.hrl").

-export_type([custom_message/0]).

-type custom_message() :: none().

%% @doc Gets the number of IDs to sample during join.
%%      Note: this is executed at the joining node.
-spec get_number_of_samples(Conn::dht_node_join:connection()) -> ok.
get_number_of_samples(Connection) ->
    ?TRACE_SEND(self(), {join, get_number_of_samples,
                             conf_get_number_of_samples(), Connection}),
    comm:send_local(self(), {join, get_number_of_samples,
                             conf_get_number_of_samples(), Connection}).

%% @doc Sends the number of IDs to sample during join to the joining node.
%%      Note: this is executed at the existing node.
-spec get_number_of_samples_remote(
        SourcePid::comm:mypid(), Conn::dht_node_join:connection()) -> ok.
get_number_of_samples_remote(SourcePid, Connection) ->
    ?TRACE_SEND(SourcePid, {join, get_number_of_samples,
                          conf_get_number_of_samples(), Connection}),
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
            Candidate =
                try
                    MyNode = dht_node_state:get(DhtNodeState, node),
                    MyNodeId = node:id(MyNode),
                    case SelectedKey of
                        MyNodeId ->
                            log:log(warn, "[ Node ~w ] join requested for my ID (~.0p), "
                                        "sending no_op...", [self(), MyNodeId]),
                            lb_op:no_op();
                        _ ->
                            MyLoad = dht_node_state:get(DhtNodeState, load),
                            {SelectedKey, OtherLoadNew} =
                                lb_common:split_by_key(DhtNodeState, SelectedKey),
                            MyLoadNew = MyLoad - OtherLoadNew,
                            MyNodeDetails = node_details:set(
                                              node_details:set(node_details:new(), node, MyNode), load, MyLoad),
                            MyNodeDetailsNew = node_details:set(MyNodeDetails, load, MyLoadNew),
                            OtherNodeDetails = node_details:set(node_details:new(), load, 0),
                            OtherNodeDetailsNew = node_details:set(
                                                    node_details:set(node_details:new(), new_key, SelectedKey), load, OtherLoadNew),
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
            % postpone message:
            Msg = {join, get_candidate, SourcePid, SelectedKey, ?MODULE, Conn},
            ?TRACE_SEND(SourcePid, Msg),
            _ = comm:send_local_after(100, self(), Msg),
            ok
    end,
    DhtNodeState.

%% @doc Sorts all provided operations so that the one with the biggest change
%%      of the standard deviation is at the front.
%%      Note: this is executed at the joining node.
-spec sort_candidates(Ops::[lb_op:lb_op()]) -> [lb_op:lb_op()].
sort_candidates(Ops) -> lb_common:bestStddev(util:shuffle(Ops), plus_infinity).

-spec process_join_msg(comm:message(), State::any(),
        DhtNodeState::dht_node_state:state()) -> unknown_event.
process_join_msg(_Msg, _State, _DhtNodeState) ->
    unknown_event.

%% @doc Checks whether config parameters of the passive load balancing
%%      algorithm exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(lb_psv_samples) and
    config:cfg_is_greater_than_equal(lb_psv_samples, 1).

%% @doc Gets the number of nodes to sample (set in the config files).
-spec conf_get_number_of_samples() -> pos_integer().
conf_get_number_of_samples() ->
    config:read(lb_psv_samples).
