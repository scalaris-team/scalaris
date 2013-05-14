%% @copyright 2010-2013 Zuse Institute Berlin

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

%% @author Thorsten Schütt <schuett@zib.de>
%% @doc    Slide protocol used for lease-based ring maintenance (also see the
%%         dht_node_move module).
%% @end
%% @version $Id$
-module(slide_leases).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p~n", [self(), Pid, Msg])).

-export([prepare_join_send/2, prepare_rcv_data/2,
         prepare_send_data1/3, prepare_send_data2/3,
         update_rcv_data1/3, update_rcv_data2/3,
         prepare_send_delta1/3, prepare_send_delta2/3,
         finish_delta1/3, finish_delta2/3,
         finish_delta_ack1/4, finish_delta_ack2/3]).

-spec prepare_join_send(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_join_send(State, SlideOp) ->
    % can be ignored for leases
    {ok, State, SlideOp}.

-spec prepare_rcv_data(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_rcv_data(State, SlideOp) ->
    % do nothing
    io:format("prepare_rcv_data~n", []),
    {ok, State, SlideOp}.

-spec prepare_send_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_data1(State, SlideOp, ReplyPid) ->
    % do nothing
    io:format("prepare_send_data1~n", []),
    send_continue_msg(ReplyPid),
    {ok, State, SlideOp}.

-spec prepare_send_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_data2(State, SlideOp, {continue}) ->
    % do nothing
    io:format("prepare_send_data2~n", []),
    {ok, State, SlideOp}.

-spec update_rcv_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
update_rcv_data1(State, SlideOp, ReplyPid) ->
    % do nothing
    io:format("update_rcv_data1~n", []),
    send_continue_msg(ReplyPid),
    {ok, State, SlideOp}.

-spec update_rcv_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
update_rcv_data2(State, SlideOp, {continue}) ->
    % do nothing
    io:format("update_rcv_data2~n", []),
    {ok, State, SlideOp}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% split lease and disable lease
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prepare_send_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                          ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_delta1(State, OldSlideOp, ReplyPid) ->
    % start to split own range
    % @todo
    io:format("prepare_send_delta1~n", []),
    LeaseList = dht_node_state:get(State, lease_list),
    Interval = slide_op:get_interval(OldSlideOp),
    Pred = fun (L) ->
                   intervals:is_subset(Interval, l_on_cseq:get_range(L))
                       andalso intervals:is_continuous(intervals:intersection(Interval, l_on_cseq:get_range(L)))
           end,
    io:format("lease list: ~p~n", [LeaseList]),
    io:format("interval  : ~p~n", [Interval]),
    case lists:filter(Pred, LeaseList) of
        [Lease] ->
            Id = l_on_cseq:id(l_on_cseq:get_range(Lease)),
            io:format("found lease ~p~n", [Lease]),
            io:format("slide interval ~p~n", [Interval]),
            % check slide direction
            {R1, R2} = case intervals:in(Id, Interval) of
                           true ->
                               % ->
                               {intervals:minus(l_on_cseq:get_range(Lease), Interval), Interval};
                           false ->
                               % <-
                               {Interval, intervals:minus(l_on_cseq:get_range(Lease), Interval)}
                       end,
            io:format("R1 ~p ~n", [R1]),
            io:format("R2 ~p ~n", [R2]),
            l_on_cseq:lease_split(Lease, R1, R2, ReplyPid),
            {ok, State, OldSlideOp};
        _ ->
            error
    end.

-spec prepare_send_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                          EmbeddedMsg::any())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_delta2(State, SlideOp, Msg) ->
    % check that split has been done
    case Msg of
        {split, success, Lease} ->
            % disable new lease
            State1 = locally_disable_lease(State, SlideOp),
            % @todo
            io:format("prepare_send_delta2 ~p~n", [Msg]),
            {ok, State1, SlideOp};
        _ ->
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec finish_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta1(State, OldSlideOp, ReplyPid) ->
    % do nothing
    io:format("finish_delta1~n", []),
    send_continue_msg(ReplyPid),
    {ok, State, OldSlideOp}.

-spec finish_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    EmbeddedMsg::{continue}) -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta2(State, SlideOp, {continue}) ->
    % do nothing
    io:format("finish_delta2~n", []),
    {ok, State, SlideOp}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% handover
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec finish_delta_ack1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                        NextOpMsg::dht_node_move:next_op_msg(),
                        ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta_ack1(State, OldSlideOp, NextOpMsg, ReplyPid) ->
    % handover lease to succ
    % notify succ
    % @todo
    send_continue_msg(ReplyPid),
    io:format("finish_delta_ack1~n", []),
    {ok, State, OldSlideOp}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec finish_delta_ack2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(), NextOpMsg)
        -> {ok, dht_node_state:state(), slide_op:slide_op(), NextOpMsg}
        when is_subtype(NextOpMsg, dht_node_move:next_op_msg()).
finish_delta_ack2(State, SlideOp, NextOpMsg) ->
    % do nothing
    io:format("finish_delta_ack2~n", []),
    {ok, State, SlideOp, NextOpMsg}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec send_continue_msg(Pid::comm:erl_local_pid()) -> ok.
send_continue_msg(Pid) ->
    ?TRACE_SEND(Pid, {continue}),
    comm:send_local(Pid, {continue}).

-spec locally_disable_lease(State::dht_node_state:state(),
                            SlideOp::slide_op:slide_op()) -> dht_node_state:state().
locally_disable_lease(State, SlideOp) ->
    State.
