%% @copyright 2010-2014 Zuse Institute Berlin

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
-behaviour(slide_beh).

-include("scalaris.hrl").

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p~n", [self(), Pid, Msg])).

-export([prepare_join_send/2, prepare_rcv_data/2,
         prepare_send_data1/3, prepare_send_data2/3,
         update_rcv_data1/3, update_rcv_data2/3,
         prepare_send_delta1/3, prepare_send_delta2/3,
         finish_delta1/3, finish_delta2/3,
         finish_delta_ack1/3, finish_delta_ack2/4,
         abort_slide/4]).

-export([rm_exec/5]).

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
    TargetId = slide_op:get_target_id(SlideOp2),
    case slide_op:is_leave(SlideOp2) andalso
             slide_op:get_next_op(SlideOp2) =:= {none} of
        true ->
            Tag = ?IIF(slide_op:is_jump(SlideOp2), jump, leave),
            rm_loop:leave(Tag),
            % de-activate processes not needed anymore:
            dht_node_reregister:deactivate(),
            % note: do not deactivate gossip, vivaldi or dc_clustering -
            % their values are still valid and still count!
            dn_cache:unsubscribe(),
            rt_loop:deactivate(),
            service_per_vm:deregister_dht_node(comm:this()),
            {ok, State1, SlideOp2};
        _ ->
            rm_loop:subscribe(
              ReplyPid, {move, slide_op:get_id(SlideOp2)},
              fun(_OldN, NewN, Reason) ->
                      nodelist:nodeid(NewN) =:= TargetId orelse
                          Reason =:= {update_id_failed}
              % note: no need to check the id version
              end,
              fun ?MODULE:rm_exec/5,
              1),
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
        succ -> change_my_id(State, SlideOp, ReplyPid);
        pred -> State1 =
                    case slide_op:get_type(SlideOp) of
                        {join, 'send'} -> % already set
                            State;
                        _ ->
                            dht_node_state:add_db_range(
                              State, slide_op:get_interval(SlideOp),
                              slide_op:get_id(SlideOp))
                    end,
                send_continue_msg(ReplyPid),
                {ok, State1, SlideOp}
    end.

%% @doc Cleans up after prepare_send_data1/3 once the RM is up-to-date, (no-op here).
%% @see prepare_send_data1/3
%% @see dht_node_move:prepare_send_data2/3
-spec prepare_send_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()};
                        (State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         EmbeddedMsg::{abort})
        -> {abort, dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
prepare_send_data2(State, SlideOp, {continue}) ->
    {ok, State, SlideOp};
prepare_send_data2(State, SlideOp, {abort}) ->
    {abort, target_id_not_in_range, State, SlideOp}.

%% @doc Accepts data received during the given (existing!) slide operation,
%%      writes it to the DB and changes the own ID if necessary.
%% @see update_rcv_data2/3
%% @see dht_node_move:update_rcv_data1/5
-spec update_rcv_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
update_rcv_data1(State, SlideOp, ReplyPid) ->
    case slide_op:get_predORsucc(SlideOp) of
        succ -> change_my_id(State, SlideOp, ReplyPid);
        pred -> send_continue_msg(ReplyPid),
                {ok, State, SlideOp}
    end.

%% @doc Cleans up after update_rcv_data1/3 once the RM is up-to-date, (no-op here).
%% @see update_rcv_data1/3
%% @see dht_node_move:update_rcv_data2/3
-spec update_rcv_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()};
                      (State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       EmbeddedMsg::{abort})
        -> {abort, dht_node_move:abort_reason(), dht_node_state:state(), slide_op:slide_op()}.
update_rcv_data2(State, SlideOp, {continue}) ->
    {ok, State, SlideOp};
update_rcv_data2(State, SlideOp, {abort}) ->
    {abort, target_id_not_in_range, State, SlideOp}.

-spec send_continue_msg(Pid::comm:erl_local_pid()) -> ok.
send_continue_msg(Pid) ->
    ?TRACE_SEND(Pid, {continue}),
    comm:send_local(Pid, {continue}).

-spec send_abort_msg(Pid::comm:erl_local_pid()) -> ok.
send_abort_msg(Pid) ->
    ?TRACE_SEND(Pid, {abort}),
    comm:send_local(Pid, {abort}).

-spec send_continue_msg_when_pred_ok(
        State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
        ReplyPid::comm:erl_local_pid()) -> ok.
send_continue_msg_when_pred_ok(State, SlideOp, ReplyPid) ->
    ExpPredId = slide_op:get_target_id(SlideOp),
    case dht_node_state:get(State, pred_id) of
        ExpPredId ->
            send_continue_msg(ReplyPid);
        _OldPredId ->
            OldPred = dht_node_state:get(State, pred),
            rm_loop:subscribe(
              ReplyPid, {move, slide_op:get_id(SlideOp)},
              fun(_RMOldN, RMNewN, _Reason) ->
                      RMNewPred = nodelist:pred(RMNewN),
                      % new pred pid or same pid but (updated) ID
                      PredChanged = RMNewPred =/= OldPred,
                      ?DBG_ASSERT2(not (PredChanged andalso
                                            node:pidX(RMNewPred) =:= node:pidX(OldPred)) orelse
                                       node:id(RMNewPred) =:= ExpPredId,
                                   {"unexpected pred ID change", _OldPredId,
                                    node:id(RMNewPred), ExpPredId}),
                      PredChanged
              end,
              fun ?MODULE:rm_exec/5,
              1)
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

%% @doc Executed when aborting the given slide operation (assumes the SlideOp
%%      has already been set up).
-spec abort_slide(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                  Reason::dht_node_move:abort_reason(), MoveMsgTag::atom())
        -> dht_node_state:state().
abort_slide(State, SlideOp, Reason, MoveMsgTag) ->
    case slide_op:get_phase(SlideOp) of
        {wait_for_continue, Phase} -> ok;
        Phase -> ok
    end,
    SendOrRcv = slide_op:get_sendORreceive(SlideOp),

    % revert ID change?
    case slide_op:get_predORsucc(SlideOp) of
        succ ->
            % try to change ID back (if not the first receiving join slide)
            case SendOrRcv of
                'rcv' when Reason =:= target_down ->
                    % if ID already changed, keep it; as well as the already incorporated data
                    ok;
                _ ->
                    MyId = dht_node_state:get(State, node_id),
                    case slide_op:get_my_old_id(SlideOp) of
                        null -> ok;
                        MyId -> ok;
                        _ when Phase =:= wait_for_delta_ack -> ok;
                        MyOldId ->
                            log:log(warn, "[ dht_node_move ~.0p ] trying to revert ID change from ~p to ~p~n",
                                    [comm:this(), MyOldId, MyId]),
                            rm_loop:update_id(MyOldId),
                            ok
                    end
            end;
        pred ->
            % nothing we can do on this side
            ok
    end,

    % remove incomplete/useless data?
    case SendOrRcv of
        'rcv' when Reason =:= target_down ->
            % nothing to do
            State;
        'rcv' when (Phase =/= wait_for_delta orelse MoveMsgTag =/= delta_ack) ->
            % remove incomplete data (note: this function will be called again
            % as part of dht_node_move:abort_slide/4 but without deleting items!)
            dht_node_state:slide_stop_record(State, slide_op:get_interval(SlideOp), true);
        _ ->
            State
    end.

-spec rm_exec(pid(), term(),
              OldNeighbors::nodelist:neighborhood(),
              NewNeighbors::nodelist:neighborhood(),
              Reason::rm_loop:reason()) -> ok.
rm_exec(Pid, {move, _RMSlideId}, _RMOldNeighbors, _RMNewNeighbors, Reason) ->
    case Reason of
        {update_id_failed} -> send_abort_msg(Pid);
        _ -> send_continue_msg(Pid)
    end.
