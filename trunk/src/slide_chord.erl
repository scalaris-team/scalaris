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

-export([change_my_id/2, send_data/2, accept_data/3,
         try_send_delta_to_pred/2, send_delta/2, finish_delta1/4, finish_delta2/3]).

%% @doc Change the local node's ID to the given TargetId by calling the ring
%%      maintenance and changing the slide operation's phase to
%%      wait_for_node_update. 
-spec change_my_id(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> dht_node_state:state().
change_my_id(State, SlideOp) ->
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
            dht_node_state:set_slide(
              State1, succ, slide_op:set_phase(SlideOp2, wait_for_node_update));
        _ ->
            % note: subscribe with fully qualified function names, i.e. module:fun/arity
            % (a so created fun seems to be the same no matter where created)
            TargetId = slide_op:get_target_id(SlideOp2),
            RMSubscrTag = {move, slide_op:get_id(SlideOp2)},
            rm_loop:subscribe(self(), RMSubscrTag,
                              fun(_OldN, NewN, _IsSlide) ->
                                      nodelist:nodeid(NewN) =:= TargetId
                                      % note: no need to check the id version
                              end,
                              fun dht_node_move:rm_send_node_change/4, 1),
            rm_loop:update_id(TargetId),
            dht_node_state:set_slide(
              State1, succ, slide_op:set_phase(SlideOp2, wait_for_node_update))
    end.

%% @doc Gets all data in the slide operation's interval from the DB and sends
%%      it to the target node. Also sets the DB to record changes in this
%%      interval and changes the slide operation's phase to wait_for_data_ack.
-spec send_data(State::dht_node_state:state(), SlideOp::slide_op:slide_op()) -> dht_node_state:state().
send_data(State, SlideOp) ->
    % last part of a leave? -> transfer all DB entries!
    % since in this case there is no other slide, we can safely use intervals:all()
    MovingInterval =
        case slide_op:is_leave(SlideOp) andalso not slide_op:is_jump(SlideOp)
                 andalso slide_op:get_next_op(SlideOp) =:= {none} of
            true  -> intervals:all();
            false -> slide_op:get_interval(SlideOp)
        end,
    {State_NewDB, MovingData} = dht_node_state:slide_get_data_start_record(State, MovingInterval),
    NewSlideOp = slide_op:set_phase(SlideOp, wait_for_data_ack),
    Msg = {move, data, MovingData, slide_op:get_id(NewSlideOp),
           slide_op:get_target_id(NewSlideOp),
           slide_op:get_next_op(NewSlideOp)},
    dht_node_move:send2(State_NewDB, NewSlideOp, Msg).

%% @doc Accepts data received during the given (existing!) slide operation and
%%      writes it to the DB.
-spec accept_data(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                  Data::dht_node_state:slide_data()) -> dht_node_state:state().
accept_data(State, SlideOp, Data) ->
    State1 = dht_node_state:slide_add_data(State, Data),
    case slide_op:get_predORsucc(SlideOp) of
        'succ' -> dht_node_move:change_my_id(State1, SlideOp);
        'pred' -> dht_node_move:send_data_ack(State1, SlideOp)
    end.

%% @doc Tries to send the delta to the predecessor. If a slide is sending
%%      data to its predecessor, we need to take care that the delta is not
%%      send before the predecessor has changed its ID and our node knows
%%      about it.
-spec try_send_delta_to_pred(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> dht_node_state:state().
try_send_delta_to_pred(State, SlideOp) ->
    ExpPredId = slide_op:get_target_id(SlideOp),
    Pred = dht_node_state:get(State, pred),
    case node:id(Pred) of
        ExpPredId ->
            send_delta(State, SlideOp);
        _ ->
            SlideOp1 = slide_op:set_phase(SlideOp, wait_for_pred_update_data_ack),
            RMSubscrTag = {move, slide_op:get_id(SlideOp)},
            rm_loop:subscribe(
              self(), RMSubscrTag,
              fun(RMOldN, RMNewN, _IsSlide) ->
                      RMNewPred = nodelist:pred(RMNewN),
                      RMOldPred = nodelist:pred(RMOldN),
                      RMOldPred =/= RMNewPred orelse
                          node:id(RMNewPred) =:= ExpPredId
              end,
              fun dht_node_move:rm_notify_new_pred/4, 1),
            dht_node_state:set_slide(State, pred, SlideOp1)
    end.

%% @doc Gets changed data in the slide operation's interval from the DB and
%%      sends as a delta to the target node. Also sets the DB to stop recording
%%      changes in this interval and delete any such entries. Changes the slide
%%      operation's phase to wait_for_delta_ack.
-spec send_delta(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> dht_node_state:state().
send_delta(State, SlideOp) ->
    % last part of a leave? -> transfer all DB entries!
    % since in this case there is no other slide, we can safely use intervals:all()
    SlideOpInterval =
        case slide_op:is_leave(SlideOp) andalso not slide_op:is_jump(SlideOp)
                 andalso slide_op:get_next_op(SlideOp) =:= {none} of
            true  -> intervals:all();
            false -> slide_op:get_interval(SlideOp)
        end,
    % send delta (values of keys that have changed during the move)
    {State1, ChangedData} = dht_node_state:slide_take_delta_stop_record(State, SlideOpInterval),
    State2 = dht_node_state:rm_db_range(State1, slide_op:get_id(SlideOp)),
    SlOp1 = slide_op:set_phase(SlideOp, wait_for_delta_ack),
    Msg = {move, delta, ChangedData, slide_op:get_id(SlOp1)},
    dht_node_move:send2(State2, SlOp1, Msg).

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

%% @doc Accepts delta received during the given (existing!) slide operation and
%%      writes it to the DB. Then removes the dht_node's message forward for 
%%      the slide operation's interval and continues be sending a message to
%%      ReplyPid (if receiving from pred, right after the RM is up-to-date).
%% @see finish_delta2/3
%% @see dht_node_move:finish_delta1/3
-spec finish_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    ChangedData::dht_node_state:slide_delta(),
                    ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta1(State, OldSlideOp, ChangedData, ReplyPid) ->
    State1 = dht_node_state:slide_add_delta(State, ChangedData),
    SlideOp = slide_op:remove_msg_fwd(OldSlideOp),
    case slide_op:get_predORsucc(SlideOp) of
        succ -> send_continue_msg(ReplyPid);
        pred -> send_continue_msg_when_pred_ok(State1, SlideOp, ReplyPid)
    end,
    % optimization: until we know about the new id of our pred (or a
    % new pred or the continue message), add the range to the db_range so our
    % node already responds to such messages
    {ok, dht_node_state:add_db_range(State1, slide_op:get_interval(SlideOp),
                                     slide_op:get_id(SlideOp)), SlideOp}.

%% @doc Cleans up after finish_delta1/4 once the RM is up-to-date, e.g. removes
%%      temporary additional db_range entries.
%% @see finish_delta1/4
%% @see dht_node_move:finish_delta2/3
-spec finish_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    EmbeddedMsg::{continue}) -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta2(State, SlideOp, {continue}) ->
    MoveFullId = slide_op:get_id(SlideOp),
    {ok, dht_node_state:rm_db_range(State, MoveFullId), SlideOp}.

%% TODO: add handling of received delta_ack messages
