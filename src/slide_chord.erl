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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Slide protocol used for Chord ring maintenance (also see the
%%         dht_node_move module).
%%         Note: assumes that the neighborhood does not change during the
%%         handling of a message.
%% @end
%% @version $Id$
-module(slide_chord).
-author('kruber@zib.de').
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
         finish_delta_ack1/3, finish_delta_ack2/4]).

-spec prepare_join_send(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_join_send(State, SlideOp) ->
    % in ordinary slides, the DB range is extended right before the
    % other node is instructed to change the ID - during a join this is
    % the case after receiving the initial join_request -> set it now!
    State1 = dht_node_state:add_db_range(
               State, slide_op:get_interval(SlideOp),
               slide_op:get_id(SlideOp)),
    {ok, State1, SlideOp}.

-spec prepare_rcv_data(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_rcv_data(State, SlideOp) ->
    Type = slide_op:get_type(SlideOp),
    % message forward on succ is set before the pred can change its ID;
    % on pred in ordinary slides, the message forward will be set right before
    % the ID is going to be changed;
    % during a join we already set the new ID -> set message forward now, too!
    SlideOp1 = case (slide_op:get_sendORreceive(Type) =:= 'rcv' andalso
                         slide_op:get_predORsucc(Type) =:= pred)
                        orelse slide_op:is_join(Type, 'rcv') of
                   true ->
                       slide_op:set_msg_fwd(SlideOp);
                   _ -> SlideOp
               end,
    {ok, State, SlideOp1}.
    
%% @doc Change the local node's ID to the given TargetId by calling the ring
%%      maintenance and sending a continue message when the node is up-to-date.
-spec change_my_id(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                   ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
change_my_id(State, SlideOp, ReplyPid) ->
    case slide_op:get_sendORreceive(SlideOp) of
        'send' ->
            State1 = dht_node_state:add_db_range(
                       State, slide_op:get_interval(SlideOp),
                       slide_op:get_id(SlideOp)),
            SlideOp2 = SlideOp;
        'rcv'  ->
            State1 = State,
            SlideOp2 = slide_op:set_msg_fwd(SlideOp)
    end,
    case slide_op:is_leave(SlideOp2) of
        true ->
            rm_loop:leave(),
            % de-activate processes not needed anymore:
            dht_node_reregister:deactivate(),
            % note: do not deactivate gossip, vivaldi or dc_clustering -
            % their values are still valid and still count!
%%             gossip:deactivate(),
%%             dc_clustering:deactivate(),
%%             vivaldi:deactivate(),
            cyclon:deactivate(),
            rt_loop:deactivate(),
            case pid_groups:get_my(dht_node) of
                failed -> ok;
                Pid -> service_per_vm:deregister_dht_node(Pid)
            end,
            {ok, State1, SlideOp2};
        _ ->
            % note: subscribe with fully qualified function names, i.e. module:fun/arity
            % (a so created fun seems to be the same no matter where created)
            TargetId = slide_op:get_target_id(SlideOp2),
            rm_loop:subscribe(
              ReplyPid, {move, slide_op:get_id(SlideOp2)},
              fun(_OldN, NewN, _IsSlide) ->
                      nodelist:nodeid(NewN) =:= TargetId
              % note: no need to check the id version
              end,
              fun(Pid, {move, _RMSlideId}, _RMOldNeighbors, _RMNewNeighbors) ->
                      send_continue_msg(Pid)
              end, 1),
            rm_loop:update_id(TargetId),
            {ok, State1, SlideOp2}
    end.

%% @doc Prepares sending data for the given (existing!) slide operation and
%%      changes the own ID if necessary.
%% @see prepare_send_data2/3
%% @see dht_node_move:prepare_send_data1/2
-spec prepare_send_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_data1(State, SlideOp, ReplyPid) ->
    case slide_op:get_predORsucc(SlideOp) of
        'succ' -> change_my_id(State, SlideOp, ReplyPid);
        'pred' -> State1 = dht_node_state:add_db_range(
                             State, slide_op:get_interval(SlideOp),
                             slide_op:get_id(SlideOp)),
                  send_continue_msg(ReplyPid),
                  {ok, State1, SlideOp}
    end.

%% @doc Cleans up after prepare_send_data1/3 once the RM is up-to-date, (no-op here).
%% @see prepare_send_data1/3
%% @see dht_node_move:prepare_send_data2/3
-spec prepare_send_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_data2(State, SlideOp, {continue}) ->
    {ok, State, SlideOp}.

%% @doc Accepts data received during the given (existing!) slide operation,
%%      writes it to the DB and changes the own ID if necessary.
%% @see update_rcv_data2/3
%% @see dht_node_move:update_rcv_data1/5
-spec update_rcv_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
update_rcv_data1(State, SlideOp, ReplyPid) ->
    case slide_op:get_predORsucc(SlideOp) of
        'succ' -> change_my_id(State, SlideOp, ReplyPid);
        'pred' -> send_continue_msg(ReplyPid),
                  {ok, State, SlideOp}
    end.

%% @doc Cleans up after update_rcv_data1/3 once the RM is up-to-date, (no-op here).
%% @see update_rcv_data1/3
%% @see dht_node_move:update_rcv_data2/3
-spec update_rcv_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
update_rcv_data2(State, SlideOp, {continue}) ->
    {ok, State, SlideOp}.

-spec send_continue_msg(Pid::comm:erl_local_pid()) -> ok.
send_continue_msg(Pid) ->
    ?TRACE_SEND(Pid, {continue}),
    comm:send_local(Pid, {continue}).

-spec send_continue_msg_when_pred_ok(
        State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
        ReplyPid::comm:erl_local_pid()) -> ok.
send_continue_msg_when_pred_ok(State, SlideOp, ReplyPid) ->
    ExpPredId = slide_op:get_target_id(SlideOp),
    case dht_node_state:get(State, pred_id) of
        ExpPredId ->
            send_continue_msg(ReplyPid);
        _ ->
            OldPred = slide_op:get_node(SlideOp),
            rm_loop:subscribe(
              ReplyPid, {move, slide_op:get_id(SlideOp)},
              fun(RMOldN, RMNewN, _IsSlide) ->
                      RMNewPred = nodelist:pred(RMNewN),
                      RMOldPred = nodelist:pred(RMOldN),
                      RMOldPred =/= RMNewPred orelse
                          node:id(RMNewPred) =:= ExpPredId orelse
                          RMNewPred =/= OldPred
              end,
              fun(Pid, {move, _RMSlideId}, _RMOldNeighbors, _RMNewNeighbors) ->
                      send_continue_msg(Pid)
              end, 1)
    end.

%% @doc Accepts data_ack received during the given (existing!) slide operation
%%      and continues be sending a message to ReplyPid (if sending to pred,
%%      right after the RM is up-to-date).
%% @see prepare_send_delta2/3
%% @see dht_node_move:prepare_send_delta1/2
-spec prepare_send_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                          ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_delta1(State, OldSlideOp, ReplyPid) ->
    case slide_op:get_predORsucc(OldSlideOp) of
        succ -> send_continue_msg(ReplyPid);
        pred -> send_continue_msg_when_pred_ok(State, OldSlideOp, ReplyPid)
    end,
    {ok, State, OldSlideOp}.

%% @doc Cleans up after prepare_send_delta1/3 once the RM is up-to-date, e.g.
%%      removes temporary additional db_range entries.
%% @see prepare_send_delta1/3
%% @see dht_node_move:prepare_send_delta2/3
-spec prepare_send_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                          EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_delta2(State, SlideOp, {continue}) ->
    MoveFullId = slide_op:get_id(SlideOp),
    {ok, dht_node_state:rm_db_range(State, MoveFullId), SlideOp}.

%% @doc Removes the dht_node's message forward for the slide operation's
%%      interval and continues by sending a message to ReplyPid
%%      (if receiving from pred, right after the RM is up-to-date).
%% @see finish_delta2/3
%% @see dht_node_move:finish_delta1/3
-spec finish_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta1(State, OldSlideOp, ReplyPid) ->
    SlideOp = slide_op:remove_msg_fwd(OldSlideOp),
    case slide_op:get_predORsucc(SlideOp) of
        succ ->
            send_continue_msg(ReplyPid),
            {ok, State, SlideOp};
        pred ->
            send_continue_msg_when_pred_ok(State, SlideOp, ReplyPid),
            % optimization: until we know about the new id of our pred (or a
            % new pred or the continue message), add the range to the db_range
            % so our node already responds to such messages
            {ok, dht_node_state:add_db_range(
               State, slide_op:get_interval(SlideOp), slide_op:get_id(SlideOp)),
             SlideOp}
    end.

%% @doc Cleans up after finish_delta1/4 once the RM is up-to-date, e.g. removes
%%      temporary additional db_range entries.
%% @see finish_delta1/4
%% @see dht_node_move:finish_delta2/3
-spec finish_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    EmbeddedMsg::{continue}) -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta2(State, SlideOp, {continue}) ->
    MoveFullId = slide_op:get_id(SlideOp),
    {ok, dht_node_state:rm_db_range(State, MoveFullId), SlideOp}.

%% @doc No-op with chord RT.
%% @see finish_delta_ack2/3
%% @see dht_node_move:finish_delta_ack1/2
-spec finish_delta_ack1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                        ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta_ack1(State, OldSlideOp, ReplyPid) ->
    send_continue_msg(ReplyPid),
    {ok, State, OldSlideOp}.

%% @doc No-op with chord RT.
%% @see finish_delta_ack1/3
%% @see dht_node_move:finish_delta_ack2/3
-spec finish_delta_ack2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                        NextOpMsg, EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op(), NextOpMsg}
        when is_subtype(NextOpMsg, dht_node_move:next_op_msg()).
finish_delta_ack2(State, SlideOp, NextOpMsg, {continue}) ->
    {ok, State, SlideOp, NextOpMsg}.
