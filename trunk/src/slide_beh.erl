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
            ChangedData::dht_node_state:slide_delta(), ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-callback finish_delta2(
            State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
            EmbeddedMsg::comm:message())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, AbortReason::dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {prepare_send_delta1, 3}
     {prepare_send_delta2, 3}
     {finish_delta1, 4}
     {finish_delta2, 3}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
