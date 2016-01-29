%  @copyright 2007-2016 Zuse Institute Berlin

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
%% @doc    dht_node lookup algorithm (interacts with the dht_node process)
%% @end
%% @version $Id$
-module(dht_node_lookup).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("lookup.hrl").

-export([lookup_aux_leases/4, lookup_decision_chord/4, lookup_fin/4, lookup_aux_failed/3, lookup_fin_failed/3]).

-export([envelope/2]).

-export_type([data/0]).

-type enveloped_message() :: {pos_integer(), f, comm:message()}.

-spec envelope(pos_integer(), comm:message()) -> enveloped_message().
envelope(Nth, Msg) ->
    ?DBG_ASSERT('_' =:= element(Nth, Msg)),
    {Nth, f, Msg}.

%% userdevguide-begin dht_node_lookup:routing
%% @doc Check the lease and translate to lookup_fin or forward to rt_loop
%%      accordingly.
-spec lookup_aux_leases(State::dht_node_state:state(), Key::intervals:key(),
                       Hops::non_neg_integer(), Msg::comm:message()) -> ok.
lookup_aux_leases(State, Key, Hops, Msg) ->
    %% When the neighborhood information from rm in rt_loop and leases disagree
    %% over responsibility, the lookup is forwared to the successor instead of
    %% asking rt. We can not simply forward this message to rt_loop though:
    %% rt_loop thinks (based on the neighborhood from rm) that the node is
    %% responsible and forwards the msg as a lookup_fin msg to its dht_node. The
    %% dht_node decides (based on the lease, see lookup_fin_leases) that the
    %% node is not responsible and forwards the message to the next node. When
    %% the message arrives again at the node, the same steps repeat. As
    %% joining/sliding needs lookup messages, the new leases are never
    %% established and the message circle endlessly.
    case leases:is_responsible(State, Key) of
        false ->
            FullMsg = {?lookup_aux, Key, Hops, Msg},
            comm:send_local(pid_groups:get_my(routing_table), FullMsg);
        _ ->
            ERT = dht_node_state:get(State, rt),
            Neighbors = dht_node_state:get(State, neighbors),
            WrappedMsg = ?RT:wrap_message(Key, Msg, ERT, Neighbors, Hops),
            comm:send_local(self(),
                            {?lookup_fin, Key, ?HOPS_TO_DATA(Hops), WrappedMsg})
    end.

%% @doc Decide, whether a lookup_aux message should be translated into a lookup_fin
%%      message
-spec lookup_decision_chord(State::dht_node_state:state(), Key::intervals:key(),
                       Hops::non_neg_integer(), Msg::comm:message()) -> ok.
lookup_decision_chord(State, Key, Hops, Msg) ->
    Neighbors = dht_node_state:get(State, neighbors),
    Succ = node:pidX(nodelist:succ(Neighbors)),
    % NOTE: re-evaluate that the successor is really (still) responsible:
    case intervals:in(Key, nodelist:succ_range(Neighbors)) of
        true ->
            %% log:log(warn, "[dht_node] lookup_fin on lookup_decision"),
            NewMsg = {?lookup_fin, Key, ?HOPS_TO_DATA(Hops + 1), Msg},
            comm:send(Succ, NewMsg, [{shepherd, pid_groups:get_my(routing_table)}]);
        _ ->
            %% log:log(warn, "[dht_node] lookup_aux on lookup_decision"),
            NewMsg = {?lookup_aux, Key, Hops + 1, Msg},
            comm:send(Succ, NewMsg, [{shepherd, pid_groups:get_my(routing_table)},
                                     {group_member, routing_table}])
    end.

%% check_local_leases(DHTNodeState, MyRange) ->
%%     LeaseList = dht_node_state:get(DHTNodeState, lease_list),
%%     ActiveLease = lease_list:get_active_lease(LeaseList),
%%     PassiveLeases = lease_list:get_passive_leases(LeaseList),
%%     ActiveInterval = case ActiveLease of
%%                          empty ->
%%                              intervals:empty();
%%                          _ ->
%%                              l_on_cseq:get_range(ActiveLease)
%%                      end,
%%     LocalCorrect = MyRange =:= ActiveInterval,
%%     log:pal("~p rm =:= leases: ~w. lease=~w. my_range=~w",
%%               [self(), LocalCorrect, ActiveInterval, MyRange]).


%% @doc Find the node responsible for Key and send him the message Msg.
-spec lookup_fin(State::dht_node_state:state(), Key::intervals:key(),
                 Data::data(), Msg::comm:message()) -> dht_node_state:state().
lookup_fin(State, Key, Hops, Msg) ->
    case config:read(leases) of
        true ->
            lookup_fin_leases(State, Key, Hops, Msg);
        _ ->
            lookup_fin_chord(State, Key, Hops, Msg)
    end.

-spec lookup_fin_chord(State::dht_node_state:state(), Key::intervals:key(),
                       Data::data(), Msg::comm:message()) -> dht_node_state:state().
lookup_fin_chord(State, Key, Data, Msg) ->
    MsgFwd = dht_node_state:get(State, msg_fwd),
    FwdList = [P || {I, P} <- MsgFwd, intervals:in(Key, I)],
    Hops = ?HOPS_FROM_DATA(Data),
    case FwdList of
        []    ->
            case dht_node_state:is_db_responsible__no_msg_fwd_check(Key, State) of
                true ->
                    %comm:send_local(pid_groups:get_my(dht_node_monitor),
                    %                {lookup_hops, Hops}),
                    %Unwrap = ?RT:unwrap_message(Msg, State),
                    %gen_component:post_op(Unwrap, State);
                    deliver(State, Msg, false, Hops);
                false ->
                    % do not warn if
                    % a) received lookup_fin due to a msg_fwd while sliding and
                    %    before the other node removed the message forward or
                    % b) our pred is not be aware of our ID change yet (after
                    %    moving data to our successor) yet
                    SlidePred = dht_node_state:get(State, slide_pred),
                    SlideSucc = dht_node_state:get(State, slide_succ),
                    Neighbors = dht_node_state:get(State, neighbors),
                    InSlideIntervalFun =
                        fun(SlideOp) ->
                                slide_op:is_slide(SlideOp) andalso
                                    slide_op:get_sendORreceive(SlideOp) =:= 'send' andalso
                                    intervals:in(Key, slide_op:get_interval(SlideOp))
                        end,
                    case lists:any(InSlideIntervalFun, [SlidePred, SlideSucc]) orelse
                             intervals:in(Key, nodelist:succ_range(Neighbors)) of
                        true -> ok;
                        false ->
                            DBRange = dht_node_state:get(State, db_range),
                            DBRange2 = [begin
                                            case intervals:is_continuous(Interval) of
                                                true -> {intervals:get_bounds(Interval), Id};
                                                _    -> {Interval, Id}
                                            end
                                        end || {Interval, Id} <- DBRange],
                            log:log(warn,
                                    "[ ~.0p ] Routing is damaged (~p)!! Trying again...~n"
                                    "  myrange:~p~n  db_range:~p~n  msgfwd:~p~n  Key:~p~n"
                                    "  pred: ~.4p~n  node: ~.4p~n  succ: ~.4p",
                                    [self(), Data, intervals:get_bounds(nodelist:node_range(Neighbors)),
                                     DBRange2, MsgFwd, Key, nodelist:pred(Neighbors),
                                     nodelist:node(Neighbors), nodelist:succ(Neighbors)])
                    end,
                    lookup_decision_chord(State, Key, Hops, Msg),
                    State
            end;
        [Pid] -> comm:send(Pid, {?lookup_fin, Key, ?HOPS_TO_DATA(Hops + 1), Msg}),
                 State
    end.

-spec lookup_fin_leases(State::dht_node_state:state(), Key::intervals:key(),
                        Data::data(), Msg::comm:message()) -> dht_node_state:state().
lookup_fin_leases(State, Key, Data, Msg) ->
    Hops = ?HOPS_FROM_DATA(Data),
    case leases:is_responsible(State, Key) of
        true ->
            deliver(State, Msg, true, Hops);
        maybe ->
            deliver(State, Msg, false, Hops);
        false ->
            %% We are here because the neighborhood information from rm in rt_loop
            %% and leases disagree over responsibility. One cause for this case
            %% can be join/sliding. Our successor still has the lease for our range.
            %% But rm already believes that we are responsible for our range. The
            %% solution is to forward the lookup to our successor instead of asking rt.
            %% Simply forwarding a lookup_fin msg would lead to linear routing,
            %% therefore we use lookup_aux. See also lookup_aux().
            %% log:log("lookup_fin fail: ~p", [self()]),
            NewMsg = {?lookup_aux, Key, Hops + 1, Msg},
            comm:send(dht_node_state:get(State, succ_pid), NewMsg),
            State
    end.
%% userdevguide-end dht_node_lookup:routing

-spec lookup_aux_failed(dht_node_state:state(), Target::comm:mypid(),
                        Msg::{?lookup_aux, Key::?RT:key(), Hops::pos_integer(), Msg::comm:message()}) -> ok.
lookup_aux_failed(State, _Target, {?lookup_aux, Key, Hops, Msg} = _Message) ->
    log:log(warn, "[dht_node] lookup_aux_failed. Target: ~p. Msg: ~p.", [_Target, _Message]),
    _ = comm:send_local_after(100, self(), {?lookup_aux, Key, Hops + 1, Msg}),
    State.

-spec lookup_fin_failed(dht_node_state:state(), Target::comm:mypid(),
                        Msg::{?lookup_fin, Key::?RT:key(), Data::data(), Msg::comm:message()}) -> ok.
lookup_fin_failed(State, _Target, {?lookup_fin, Key, Data, Msg} = _Message) ->
    log:log(warn, "[dht_node] lookup_aux_failed. Target: ~p. Msg: ~p.", [_Target, _Message]),
    _ = comm:send_local_after(100, self(), {?lookup_aux, Key, ?HOPS_FROM_DATA(Data) + 1, Msg}),
    State.

deliver(State, Msg, Consistency, Hops) ->
    %log:log("lookup_fin success: ~p ~p", [self(), Msg]),
    case config:read(dht_node_monitor) of
        true ->
            comm:send_local(pid_groups:get_my(dht_node_monitor),
                            {lookup_hops, Hops});
        _ -> ok
    end,
    Unwrap = ?RT:unwrap_message(Msg, State),
    case Unwrap of
        {Nth, f, InnerMsg} ->
            gen_component:post_op(erlang:setelement(Nth, InnerMsg, Consistency), State);
        _ ->
            gen_component:post_op(Unwrap, State)
    end.
