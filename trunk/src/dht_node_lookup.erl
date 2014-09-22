%  @copyright 2007-2014 Zuse Institute Berlin

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

-export([lookup_aux/4, lookup_fin/4,
         lookup_aux_failed/3, lookup_fin_failed/3]).

-export([envelope/2]).

-ifdef(with_export_type_support).
-export_type([data/0]).
-endif.

-type enveloped_message() :: {pos_integer(), f, comm:message()}.

-spec envelope(pos_integer(), comm:message()) -> enveloped_message().
envelope(Nth, Msg) ->
    ?DBG_ASSERT('_' =:= element(Nth, Msg)),
    {Nth, f, Msg}.

-ifdef(enable_debug).
% add source information to debug routing damaged messages
-define(HOPS_TO_DATA(Hops), {comm:this(), Hops}).
-define(HOPS_FROM_DATA(Data), element(2, Data)).
-type data() :: {Source::comm:mypid(), Hops::non_neg_integer()}.
-else.
-define(HOPS_TO_DATA(Hops), Hops).
-define(HOPS_FROM_DATA(Data), Data).
-type data() :: Hops::non_neg_integer().
-endif.

%% userdevguide-begin dht_node_lookup:routing
%% @doc Find the node responsible for Key and send him the message Msg.
-spec lookup_aux(State::dht_node_state:state(), Key::intervals:key(),
                 Hops::non_neg_integer(), Msg::comm:message()) -> ok.
lookup_aux(State, Key, Hops, Msg) ->
    case config:read(leases) of
        true ->
            lookup_aux_leases(State, Key, Hops, Msg);
        _ ->
            lookup_aux_chord(State, Key, Hops, Msg)
end.

-spec lookup_aux_chord(State::dht_node_state:state(), Key::intervals:key(),
                       Hops::non_neg_integer(), Msg::comm:message()) -> ok.
lookup_aux_chord(State, Key, Hops, Msg) ->
    WrappedMsg = ?RT:wrap_message(Key, Msg, State, Hops),
    case ?RT:next_hop(State, Key) of
        {succ, P} -> % found node -> terminate
            comm:send(P, {?lookup_fin, Key, ?HOPS_TO_DATA(Hops + 1), WrappedMsg}, [{shepherd, self()}]);
        {other, P} ->
            comm:send(P, {?lookup_aux, Key, Hops + 1, WrappedMsg}, [{shepherd, self()}])
    end.

-spec lookup_aux_leases(State::dht_node_state:state(), Key::intervals:key(),
                       Hops::non_neg_integer(), Msg::comm:message()) -> ok.
lookup_aux_leases(State, Key, Hops, Msg) ->
    case leases:is_responsible(State, Key) of
        true ->
            comm:send_local(dht_node_state:get(State, monitor_proc),
                            {lookup_hops, Hops}),
            DHTNode = pid_groups:find_a(dht_node),
            %log:log("aux -> fin: ~p ~p~n", [self(), DHTNode]),
            comm:send_local(DHTNode,
                            {?lookup_fin, Key, ?HOPS_TO_DATA(Hops + 1), Msg});
        maybe ->
            DHTNode = pid_groups:find_a(dht_node),
            %log:log("aux -> fin: ~p ~p~n", [self(), DHTNode]),
            comm:send_local(DHTNode,
                            {?lookup_fin, Key, ?HOPS_TO_DATA(Hops + 1), Msg});
        false ->
            WrappedMsg = ?RT:wrap_message(Key, Msg, State, Hops),
            %log:log("lookup_aux_leases route ~p~n", [self()]),
            P = element(2, ?RT:next_hop(State, Key)),
            %log:log("lookup_aux_leases route ~p -> ~p~n", [self(), P]),
            comm:send(P, {?lookup_aux, Key, Hops + 1, WrappedMsg}, [{shepherd, self()}])
    end.

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
                    %comm:send_local(dht_node_state:get(State, monitor_proc),
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
                    lookup_aux(State, Key, Hops, Msg),
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
            log:log("lookup_fin fail: ~p", [self()]),
            lookup_aux(State, Key, Hops, Msg),
            State
    end.

%% userdevguide-end dht_node_lookup:routing

-spec lookup_aux_failed(dht_node_state:state(), Target::comm:mypid(),
                        Msg::comm:message()) -> ok.
lookup_aux_failed(State, _Target, {?lookup_aux, Key, Hops, Msg} = _Message) ->
    %io:format("lookup_aux_failed(State, ~p, ~p)~n", [_Target, _Message]),
    _ = comm:send_local_after(100, self(), {?lookup_aux, Key, Hops + 1, Msg}),
    State.

-spec lookup_fin_failed(dht_node_state:state(), Target::comm:mypid(),
                        Msg::comm:message()) -> ok.
lookup_fin_failed(State, _Target, {?lookup_fin, Key, Data, Msg} = _Message) ->
    %io:format("lookup_fin_failed(State, ~p, ~p)~n", [_Target, _Message]),
    _ = comm:send_local_after(100, self(), {?lookup_aux, Key, ?HOPS_FROM_DATA(Data) + 1, Msg}),
    State.

deliver(State, Msg, Consistency, Hops) ->
    %log:log("lookup_fin success: ~p ~p", [self(), Msg]),
    comm:send_local(dht_node_state:get(State, monitor_proc),
                    {lookup_hops, Hops}),
    Unwrap = ?RT:unwrap_message(Msg, State),
    case Unwrap of
        {Nth, f, InnerMsg} ->
            gen_component:post_op(erlang:setelement(Nth, InnerMsg, Consistency), State);
        _ ->
            gen_component:post_op(Unwrap, State)
    end.
