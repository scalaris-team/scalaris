%  @copyright 2013 Zuse Institute Berlin

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
%% @doc    Behaviour for rm-dependant slide functionality.
%% @end
%% @version $Id$
-module(slide_beh).
-author('kruber@zib.de').
-vsn('$Id$ ').

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-callback prepare_join_send(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback prepare_rcv_data(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback prepare_send_data1(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback prepare_send_data2(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback update_rcv_data1(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback update_rcv_data2(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            EmbeddedMsg::comm:message())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback prepare_send_delta1(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback prepare_send_delta2(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            EmbeddedMsg::comm:message())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback finish_delta1(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback finish_delta2(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            EmbeddedMsg::comm:message())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback finish_delta_ack1(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback finish_delta_ack2(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(), NextOpMsg,
            EmbeddedMsg::comm:message())
        -> {ok, dht_node_state:state(), slide_op:slide_op(), NextOpMsg} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}
        when is_subtype(NextOpMsg, dht_node_move:next_op_msg()).
-callback abort_slide(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                      Reason::dht_node_move:abort_reason(), MoveMsgTag::atom())
        -> dht_node_state:state().
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {prepare_join_send, 2},
     {prepare_rcv_data, 2},
     {prepare_send_data1, 3},
     {prepare_send_data2, 3},
     {update_rcv_data1, 3},
     {update_rcv_data2, 3},
     {prepare_send_delta1, 3},
     {prepare_send_delta2, 3},
     {finish_delta1, 3},
     {finish_delta2, 3},
     {finish_delta_ack1, 3},
     {finish_delta_ack2, 4},
     {abort_slide, 4}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
