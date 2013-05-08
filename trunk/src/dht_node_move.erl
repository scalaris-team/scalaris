%  @copyright 2010-2013 Zuse Institute Berlin

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
%% @doc    dht_node move procedure
%%         Note: assumes that the neighborhood does not change during the
%%         handling of a message.
%% @end
%% @version $Id$
-module(dht_node_move).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).
-define(TRACE_SEND(Pid, Msg), ?TRACE("[ ~.0p ] to ~.0p: ~.0p~n", [self(), Pid, Msg])).
-define(TRACE1(Msg, State),
        ?TRACE("[ ~.0p ]~n  Msg: ~.0p~n"
               "  State: pred: ~.0p~n"
               "         node: ~.0p~n"
               "         succ: ~.0p~n"
               "   slide_pred: ~.0p~n"
               "   slide_succ: ~.0p~n"
               "      msg_fwd: ~.0p~n"
               "     db_range: ~.0p~n",
               [self(), Msg, dht_node_state:get(State, pred),
                dht_node_state:get(State, node), dht_node_state:get(State, succ),
                dht_node_state:get(State, slide_pred), dht_node_state:get(State, slide_succ),
                dht_node_state:get(State, msg_fwd), dht_node_state:get(State, db_range)])).

-export([process_move_msg/2,
         can_slide_succ/3, can_slide_pred/3,
         make_slide/5,
         make_slide_leave/2, make_jump/4,
         crashed_node/3,
         check_config/0]).
% for dht_node_join, slide_chord:
-export([send/3, send_no_slide/3, send2/3, finish_slide/2,
         notify_other/2, check_setup_slide_not_found/5, exec_setup_slide_not_found/10,
         use_incremental_slides/0, get_max_transport_entries/0]).

-ifdef(with_export_type_support).
-export_type([move_message/0, next_op_msg/0, abort_reason/0]).
-endif.

-type abort_reason() ::
    ongoing_slide |
    target_id_not_in_range |
    % moving node and its succ or pred did not share the same knowledge about
    % each other or the pred/succ changed during a move:
    wrong_pred_succ_node |
    % received slide_succ with different parameters than previously set up before sending slide_pred to succ:
    changed_parameters |
    % node received data but its interval is not adjacent to the range of the node: (should never occur - error in the protocoll!) or
    % node received data_ack but the send interval is not correct anymore, i.e. at either end of our range
    {wrong_interval, MyRange::intervals:interval(), MovingDataInterval::intervals:interval()} |
    % node received data but previous data still exists (should never occur - error in the protocoll!)
    existing_data |
    target_down | % target node reported down by fd
    scheduled_leave | % tried to continue a slide but a leave was already scheduled
    leave_no_partner_found | % tried to do a graceful leave but no successor to move the data to
    next_op_mismatch |
    {protocol_error, string()}.

-type result_message() :: {move, result, Tag::any(), Reason::abort_reason() | ok}.

-type next_op_msg() ::
    {none} |
    {continue, NewSlideId::slide_op:id()}.

-type move_message1() ::
    {move, start_slide, pred | succ, TargetId::?RT:key(), Tag::any(), SourcePid::comm:erl_local_pid() | null} |
    {move, start_jump, TargetId::?RT:key(), Tag::any(), SourcePid::comm:erl_local_pid() | null} |
    {move, slide, OtherType::slide_op:type(), MoveFullId::slide_op:id(),
     InitNode::node:node_type(), TargetNode::node:node_type(), TargetId::?RT:key(),
     Tag::any(), NextOp::slide_op:next_op(), MaxTransportEntries::unknown | pos_integer()} |
    {move, slide_abort, pred | succ, MoveFullId::slide_op:id(), Reason::abort_reason()} |
    {move, data, MovingData::dht_node_state:slide_data(), MoveFullId::slide_op:id(), TargetId::?RT:key(), NextOp::slide_op:next_op()} |
    {move, data_ack, MoveFullId::slide_op:id()} |
    {move, delta, ChangedData::dht_node_state:slide_delta(), MoveFullId::slide_op:id()} |
    {move, delta_ack, MoveFullId::slide_op:id(), next_op_msg()} |
    {move, rm_db_range, MoveFullId::slide_op:id()} |
    {move, done, MoveFullId::slide_op:id()} |
    {move, timeout, MoveFullId::slide_op:id()} % sent a message to the succ/pred but no reply yet
.

-ifdef(forward_or_recursive_types_are_not_allowed).
-type move_message() ::
    move_message1() |
    {move, {send_error, Target::comm:mypid(), Message::comm:message(), Reason::atom()}, {timeouts, Timeouts::non_neg_integer()}} |
    {move, {send_error, Target::comm:mypid(), Message::comm:message(), Reason::atom()}, MoveFullId::slide_op:id()}.
-else.
-type move_message() ::
    move_message1() |
    {move, {send_error, Target::comm:mypid(), Message::move_message1(), Reason::atom()}, {timeouts, Timeouts::non_neg_integer()}} |
    {move, {send_error, Target::comm:mypid(), Message::move_message1(), Reason::atom()}, MoveFullId::slide_op:id()}.
-endif.

%% @doc Processes move messages for the dht_node and implements the node move
%%      protocol.
-spec process_move_msg(move_message(), dht_node_state:state()) -> dht_node_state:state().
process_move_msg({move, start_slide, PredOrSucc, TargetId, Tag, SourcePid} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    make_slide(State, PredOrSucc, TargetId, Tag, SourcePid);

process_move_msg({move, start_jump, TargetId, Tag, SourcePid} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    make_jump(State, TargetId, Tag, SourcePid);

% notification from predecessor/successor that it wants to slide with our node
% (maybe incremental)
process_move_msg({move, slide, MyType, MoveFullId, OtherNode,
                  OtherTargetNode, TargetId, Tag, NextOp, MaxTransportEntries} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    setup_slide(State, MyType, MoveFullId,
                OtherTargetNode, OtherNode, TargetId, Tag, MaxTransportEntries,
                null, slide, NextOp);

% notification from predecessor/successor that the move is a noop, i.e. already
% finished
process_move_msg({move, done, MoveFullId} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    WorkerFun =
        fun(SlideOp, State) ->
                SlideOp1 = slide_op:cancel_timer(SlideOp), % cancel previous timer
                notify_source_pid(slide_op:get_source_pid(SlideOp1),
                                  {move, result, slide_op:get_tag(SlideOp1), ok}),
                dht_node_state:set_slide(State, slide_op:get_predORsucc(SlideOp1), null)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, [wait_for_other], done);

% notification from pred/succ that he could not aggree on a slide with us
process_move_msg({move, slide_abort, PredOrSucc, MoveFullId, Reason} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    SlideOp = case PredOrSucc of
                  pred -> dht_node_state:get(State, slide_pred);
                  succ -> dht_node_state:get(State, slide_succ)
              end,
    case SlideOp =/= null andalso slide_op:get_id(SlideOp) =:= MoveFullId of
        true ->
            abort_slide(State, SlideOp, Reason, false);
        _ ->
            log:log(warn, "[ dht_node_move ~.0p ] slide_abort (~.0p) received with no "
                          "matching slide operation (ID: ~.0p, slide_~.0p: ~.0p)~n",
                    [comm:this(), PredOrSucc, MoveFullId, PredOrSucc, SlideOp]),
            State
    end;

process_move_msg({move, node_leave} = _Msg, State) ->
    ?TRACE1(_Msg, State),
    % note: can't use safe_operation/6 - there is no slide op id
    SlideOp = dht_node_state:get(State, slide_succ),
    % only handle node_update in wait_for_continue phase
    case SlideOp =/= null andalso slide_op:is_leave(SlideOp) andalso
             slide_op:get_phase(SlideOp) =:= wait_for_continue andalso
             slide_op:get_sendORreceive(SlideOp) =:= 'send' of
        true ->
            prepare_send_data2(State, SlideOp, {continue});
        _ ->
            % we should not receive node update messages unless we are waiting for them
            % (node id updates should only be triggered by this module anyway)
            log:log(warn, "[ dht_node_move ~w ] received rm node leave message with no "
                          "matching slide operation (slide_succ: ~w)~n",
                    [comm:this(), SlideOp]),
            State % ignore unrelated node leave messages
    end;

% data from a neighbor
process_move_msg({move, data, MovingData, MoveFullId, TargetId, NextOp} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    WorkerFun =
        fun(SlideOp, State) ->
                SlideOp1 = slide_op:cancel_timer(SlideOp), % cancel previous timer
                update_rcv_data1(State, SlideOp1, MovingData, TargetId, NextOp)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, [wait_for_data, wait_for_other], data);

% acknowledgement from neighbor that its node received data for the slide op with the given id
process_move_msg({move, data_ack, MoveFullId} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    WorkerFun =
        fun(SlideOp, State) ->
                SlideOp1 = slide_op:cancel_timer(SlideOp), % cancel previous timer
                prepare_send_delta1(State, SlideOp1)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, [wait_for_data_ack], data_ack);

% delta from neighbor
process_move_msg({move, delta, ChangedData, MoveFullId} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    WorkerFun =
        fun(SlideOp, State) ->
                SlideOp1 = slide_op:cancel_timer(SlideOp), % cancel previous timer
                finish_delta1(State, SlideOp1, ChangedData)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, [wait_for_delta], delta);

% acknowledgement from neighbor that its node received delta for the slide op
% with the given id and information about how to continue
process_move_msg({move, delta_ack, MoveFullId, NextOpMsg} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    WorkerFun =
        fun(SlideOp, State) ->
                SlideOp1 = slide_op:cancel_timer(SlideOp), % cancel previous timer
                finish_delta_ack1(State, SlideOp1, NextOpMsg)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, [wait_for_delta_ack], delta_ack);

process_move_msg({move, {send_error, Target, Message, _Reason}, {timeouts, Timeouts}} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    NewTimeouts = Timeouts + 1,
    MaxRetries = get_send_msg_retries(),
    % TODO: keep references to target nodes? (could stop sending messages if fd reports down, otherwise send indefinitely)
    case NewTimeouts =< MaxRetries of
        true -> send_no_slide(Target, Message, NewTimeouts);
        _    -> log:log(warn,
                        "[ dht_node_move ~.0p ] giving up to send ~.0p to ~p (~p unsuccessful retries)",
                        [comm:this(), Message, Target, NewTimeouts])
    end,
    MyState;

process_move_msg({move, {send_error, Target, Message, _Reason}, MoveFullId} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    % delay the actual re-try (it may be a crash or a temporary failure)
    comm:send_local_after(config:read(move_send_msg_retry_delay), self(),
                          {move, {send_error_retry, Target, Message, _Reason}, MoveFullId}),
    MyState;

process_move_msg({move, {send_error_retry, Target, Message, _Reason}, MoveFullId} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    WorkerFun =
        fun(SlideOp, State) ->
                NewSlideOp = slide_op:inc_timeouts(SlideOp),
                MaxRetries = get_send_msg_retries(),
                PredOrSucc = slide_op:get_predORsucc(SlideOp),
                case slide_op:get_timeouts(SlideOp) of
                    T when T =< MaxRetries -> ok;
                    T    ->
                        log:log(warn,
                                "[ dht_node_move ~.0p ] slide with ~p: ~p unsuccessful retries to send message ~.0p",
                                [comm:this(), PredOrSucc, T, Message])
                end,
                send(Target, Message, MoveFullId),
                dht_node_state:set_slide(State, PredOrSucc, NewSlideOp)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, all, send_error_retry);

% no reply from the target node within get_wait_for_reply_timeout() ms
process_move_msg({move, timeout, MoveFullId} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    WorkerFun =
        fun(SlideOp, State) ->
                PredOrSucc = slide_op:get_predORsucc(SlideOp),
                log:log(warn,
                        "[ dht_node_move ~.0p ] slide with ~p: no reply received within ~pms",
                        [comm:this(), PredOrSucc, get_wait_for_reply_timeout()]),
                SlideOp1 = slide_op:set_timer(
                             SlideOp, get_wait_for_reply_timeout(),
                             {move, timeout, slide_op:get_id(SlideOp)}),
                dht_node_state:set_slide(State, PredOrSucc, SlideOp1)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, all, timeout);

% no reply from the target node within get_wait_for_reply_timeout() ms
process_move_msg({move, continue, MoveFullId, Operation, EmbeddedMsg} = _Msg, MyState) ->
    ?TRACE1(_Msg, MyState),
    WorkerFun = fun(SlideOp, State) ->
                        % although Operation matches the function name, verify
                        % validity here and call functions manually to avoid
                        % having to export them
                        case Operation of
                            prepare_send_data2 ->
                                prepare_send_data2(State, SlideOp, EmbeddedMsg);
                            update_rcv_data2 ->
                                update_rcv_data2(State, SlideOp, EmbeddedMsg);
                            prepare_send_delta2 ->
                                prepare_send_delta2(State, SlideOp, EmbeddedMsg);
                            finish_delta2 ->
                                finish_delta2(State, SlideOp, EmbeddedMsg);
                            finish_delta_ack2 ->
                                finish_delta_ack2(State, SlideOp, EmbeddedMsg)
                        end
                end,
    safe_operation(WorkerFun, MyState, MoveFullId, [wait_for_continue], continue).

% misc.

%% @doc Sends a move message using the dht_node as the shepherd to handle
%%      broken connections.
-spec send(Pid::comm:mypid(), Message::comm:message(), MoveFullId::slide_op:id()) -> ok.
send(Pid, Message, MoveFullId) ->
    Shepherd = comm:reply_as(self(), 2, {move, '_', MoveFullId}),
    ?TRACE_SEND(Pid, Message),
    comm:send(Pid, Message, [{shepherd, Shepherd}]).

%% @doc Sends a move message using the dht_node as the shepherd to handle
%%      broken connections. This does not require a slide_op being set up.
%%      The error message handler can count the number of timeouts using the
%%      provided cookie.
-spec send_no_slide(Pid::comm:mypid(), Message::comm:message(), Timeouts::non_neg_integer()) -> ok.
send_no_slide(Pid, Message, Timeouts) ->
    Shepherd = comm:reply_as(self(), 2, {move, '_', {timeouts, Timeouts}}),
    ?TRACE_SEND(Pid, Message),
    comm:send(Pid, Message, [{shepherd, Shepherd}]).

%% @doc Sends a move message using the dht_node as the shepherd to handle
%%      broken connections. The target node is determined from the SlideOp.
%%      A timeout counter in the SlideOp is reset and the dht_node_state is
%%      updated with the (new) slide operation.
-spec send2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(), Message::comm:message()) -> dht_node_state:state().
send2(State, SlideOp, Message) ->
    MoveFullId = slide_op:get_id(SlideOp),
    Target = node:pidX(slide_op:get_node(SlideOp)),
    PredOrSucc = slide_op:get_predORsucc(SlideOp),
    SlideOp1 = slide_op:set_timer(
                 SlideOp, get_wait_for_reply_timeout(),
                 {move, timeout, slide_op:get_id(SlideOp)}),
    send(Target, Message, MoveFullId),
    dht_node_state:set_slide(State, PredOrSucc, SlideOp1).

%% @doc Notifies the successor or predecessor about a (new) slide operation.
%%      Assumes that the information in the slide operation is still correct
%%      (check with safe_operation/6!).
%%      Will also set the appropriate slide_phase and update the slide op in
%%      the dht_node.
-spec notify_other(SlideOp::slide_op:slide_op(), State::dht_node_state:state())
        -> dht_node_state:state().
notify_other(SlideOp, State) ->
    null = slide_op:get_phase(SlideOp), % just to check
    Type = slide_op:get_type(SlideOp),
    SetupAtOther = slide_op:is_setup_at_other(SlideOp),
    SendOrReceive = slide_op:get_sendORreceive(Type),
    UseIncrSlides = use_incremental_slides(),
    if SendOrReceive =:= 'rcv' ->
           IncrSlide = slide_op:is_incremental(SlideOp),
           {MTE, NextOp} =
               if IncrSlide     -> {unknown, slide_op:get_next_op(SlideOp)};
                  UseIncrSlides -> {get_max_transport_entries(), {none}};
                  true          -> {unknown, {none}}
               end,
           SlideOp1 = slide_op:set_phase(SlideOp, wait_for_data),
           send2(State, SlideOp1,
                 {move, slide, slide_op:other_type_to_my_type(Type),
                  slide_op:get_id(SlideOp1), dht_node_state:get(State, node),
                  slide_op:get_node(SlideOp), slide_op:get_target_id(SlideOp1),
                  slide_op:get_tag(SlideOp1), NextOp, MTE});
       not SetupAtOther -> % beware: overlap with 1st
           SlideOp1 = slide_op:set_phase(SlideOp, wait_for_other),
           case slide_op:is_join(Type, 'send') of
               true ->
                   % first message here is a join_request which will be answered
                   % by code in dht_node_join! (we simply set the timeout here)
                   SlideOp2 = slide_op:set_timer(
                                SlideOp1, get_wait_for_reply_timeout(),
                                {move, timeout, slide_op:get_id(SlideOp1)}),
                   PredOrSucc = slide_op:get_predORsucc(Type),
                   dht_node_state:set_slide(State, PredOrSucc, SlideOp2);
               false ->
                   IncrSlide = slide_op:is_incremental(SlideOp1),
                   {MTE, NextOp} =
                       if IncrSlide     -> {unknown, slide_op:get_next_op(SlideOp1)};
                          UseIncrSlides -> {get_max_transport_entries(), {none}};
                          true          -> {unknown, {none}}
                       end,
                   send2(State, SlideOp1,
                         {move, slide, slide_op:other_type_to_my_type(Type),
                          slide_op:get_id(SlideOp1), dht_node_state:get(State, node),
                          slide_op:get_node(SlideOp), slide_op:get_target_id(SlideOp1),
                          slide_op:get_tag(SlideOp1), NextOp, MTE})
           end
    end.

%% @doc Sets up a new slide operation with the node's successor or predecessor
%%      after a request for a slide has been received.
-spec setup_slide(State::dht_node_state:state(), Type::slide_op:type(),
                  MoveFullId::slide_op:id(), MyNode::node:node_type(),
                  TargetNode::node:node_type(), TargetId::?RT:key(),
                  Tag::any(), MaxTransportEntries::unknown | pos_integer(),
                  SourcePid::comm:erl_local_pid() | null,
                  MsgTag::nomsg | slide,
                  NextOp::slide_op:next_op())
        -> dht_node_state:state().
setup_slide(State, Type, MoveFullId, MyNode, TargetNode, TargetId, Tag,
            MaxTransportEntries, SourcePid, MsgTag, NextOp) ->
    case get_slide(State, MoveFullId) of
        {ok, _PredOrSucc, SlideOp} ->
            % there could already be a slide operation because
            % a) a second message was send after an unsuccessful send-message
            %    on the other node
            % -> ignore this message
            % b) we are waiting for the second node's MaxTransportEntries
            % -> re-create the slide (TargetId or NextOp may have changed)
            case slide_op:get_phase(SlideOp) of
                wait_for_other ->
                    WorkerFun =
                        fun(SlideOp0, State0) ->
                                % during a join, everything was already set up
                                % by join_request
                                % -> don't re-create the slide!!
                                % (the interval may be wrong since the joining
                                % node may already be known to the rm)
                                case slide_op:is_join(SlideOp0, 'send') of
                                    true ->
                                        prepare_send_data1(State0, SlideOp);
                                    false ->
                                        recreate_existing_slide(
                                          SlideOp0, State0, TargetId,
                                          MaxTransportEntries, MsgTag, NextOp)
                                end
                        end,
                    safe_operation(WorkerFun, State, MoveFullId, [wait_for_other], slide);
                _ ->
                    State
            end;
        not_found ->
            Command = check_setup_slide_not_found(
                        State, Type, MyNode, TargetNode, TargetId),
            exec_setup_slide_not_found(
              Command, State, MoveFullId, TargetNode, TargetId, Tag,
              MaxTransportEntries, SourcePid, MsgTag, NextOp);
        {wrong_neighbor, _PredOrSucc, SlideOp} -> % wrong pred or succ
            abort_slide(State, SlideOp, wrong_pred_succ_node, true)
    end.

-type command() :: {abort, abort_reason(), OrigType::slide_op:type()} |
                   {ok, NewType::slide_op:type() | move_done}.

%% @doc Checks whether a new slide operation with the node's successor or
%%      predecessor and the given parameters can be set up.
-spec check_setup_slide_not_found(State::dht_node_state:state(),
        Type::slide_op:type(), MyNode::node:node_type(),
        TargetNode::node:node_type(), TargetId::?RT:key())
        -> Command::command().
check_setup_slide_not_found(State, Type, MyNode, TNode, TId) ->
    PredOrSucc = slide_op:get_predORsucc(Type),
    CanSlide = case PredOrSucc of
                   pred -> can_slide_pred(State, TId, Type);
                   succ -> can_slide_succ(State, TId, Type)
               end,
    % correct pred/succ info? did pred/succ know our current ID? -> compare node info
    Neighbors = dht_node_state:get(State, neighbors),
    NodesCorrect = MyNode =:= nodelist:node(Neighbors) andalso
                       (TNode =:= nodelist:PredOrSucc(Neighbors) orelse Type =:= {join, 'send'}),
    MoveDone = (PredOrSucc =:= pred andalso node:id(TNode) =:= TId andalso Type =/= {join, 'send'}) orelse
               (PredOrSucc =:= succ andalso node:id(MyNode) =:= TId),
    Command =
        case CanSlide andalso NodesCorrect andalso not MoveDone of
            true ->
                SendOrReceive = slide_op:get_sendORreceive(Type),
                case slide_op:is_leave(Type) of
                    true when SendOrReceive =:= 'send' ->
                        % graceful leave (slide with succ, send all data)
                        % note: may also be a jump operation!
                        % TODO: check for running slide, abort it if possible, eventually extend it
                        case slide_op:is_jump(Type) of
                            true ->
                                TIdInRange = intervals:in(TId, nodelist:node_range(Neighbors)),
                                HasLeft = dht_node_state:has_left(State),
                                TIdInSuccRange =
                                    intervals:in(TId, nodelist:succ_range(Neighbors)),
                                if % convert jump to slide?
                                    TIdInRange     -> {ok, {slide, succ, 'send'}};
                                    TIdInSuccRange -> {ok, {slide, succ, 'rcv'}};
                                    HasLeft        -> {abort, target_id_not_in_range, Type};
                                    true ->
                                        SlideSucc =
                                            dht_node_state:get(State, slide_succ),
                                        SlidePred =
                                            dht_node_state:get(State, slide_pred),
                                        case SlideSucc =/= null andalso
                                                 SlidePred =/= null of
                                            true -> {ok, {jump, 'send'}};
                                            _    -> {abort, ongoing_slide, Type}
                                        end
                                end;
                            _ -> {ok, {leave, 'send'}}
                        end;
                    true when SendOrReceive =:= 'rcv' ->
                        {ok, {leave, SendOrReceive}};
                    false when SendOrReceive =:= 'rcv' -> % no leave/jump
                        {ok, {slide, PredOrSucc, SendOrReceive}};
                    false when SendOrReceive =:= 'send' -> % no leave/jump
                        TIdInRange = intervals:in(TId, nodelist:node_range(Neighbors)) andalso
                                         not dht_node_state:has_left(State),
                        case not TIdInRange of
                            true -> {abort, target_id_not_in_range, Type};
                            _    -> {ok, Type}
                        end
                end;
            _ when not CanSlide -> {abort, ongoing_slide, Type};
            _ when not NodesCorrect -> {abort, wrong_pred_succ_node, Type};
            _ ->
                case slide_op:is_leave(Type) of
                    false -> {ok, move_done}; % MoveDone, i.e. target id already reached (noop)
                    true  -> {abort, leave_no_partner_found, Type}
                end
        end,
    Command.

%% @doc Creates a new slide operation with the node's successor or
%%      predecessor and the given parameters according to the command created
%%      by check_setup_slide_not_found/5.
%%      Note: assumes that such a slide does not already exist if command is not abort.
-spec exec_setup_slide_not_found(
        Command::command(),
        State::dht_node_state:state(), MoveFullId::slide_op:id(),
        TargetNode::node:node_type(), TargetId::?RT:key(), Tag::any(),
        OtherMaxTransportEntries::unknown | pos_integer(),
        SourcePid::comm:erl_local_pid() | null,
        MsgTag::nomsg | slide | delta_ack,
        NextOp::slide_op:next_op()) -> dht_node_state:state().
exec_setup_slide_not_found(Command, State, MoveFullId, TargetNode,
                           TargetId, Tag, OtherMTE, SourcePid, MsgTag, NextOp) ->
    Neighbors = dht_node_state:get(State, neighbors),
    % note: NewType (inside the command) may be different than the initially planned type
    case Command of
        {abort, Reason, OrigType} ->
            abort_slide(State, TargetNode, MoveFullId, null, SourcePid, Tag,
                        OrigType, Reason, MsgTag =/= nomsg);
        {ok, {join, 'send'}} -> % similar to {slide, pred, 'send'}
            fd:subscribe([node:pidX(TargetNode)], {move, MoveFullId}),
            % TODO: implement step-wise join
            SlideOp = slide_op:new_sending_slide_join(
                        MoveFullId, TargetNode, join, Neighbors),
            % in ordinary slides, the DB range is extended right before the
            % other node is instructed to change the ID - during a join this is
            % the case after receiving the initial join_request -> set it now!
            State1 = dht_node_state:add_db_range(
                       State, slide_op:get_interval(SlideOp),
                       slide_op:get_id(SlideOp)),
            % note: phase will be set by notify_other/2 and needs to remain null here
            case MsgTag of
                nomsg -> notify_other(SlideOp, State1);
                slide -> prepare_send_data1(State1, SlideOp)
            end;
        {ok, {slide, pred, 'send'} = NewType} ->
            fd:subscribe([node:pidX(TargetNode)], {move, MoveFullId}),
            UseIncrSlides = use_incremental_slides(),
            case MsgTag of
                nomsg -> % first inform other node:
                    SlideOp = slide_op:new_slide(
                                MoveFullId, NewType, TargetId, Tag, SourcePid,
                                OtherMTE, NextOp, Neighbors),
                    notify_other(SlideOp, State);
                Y when (Y =:= slide orelse Y =:= delta_ack) ->
                    SlideOp =
                        if UseIncrSlides orelse OtherMTE =/= unknown->
                               IncTargetKey = find_incremental_target_id(
                                                Neighbors, State,
                                                TargetId, NewType, OtherMTE),
                               slide_op:new_slide_i(
                                 MoveFullId, NewType, IncTargetKey, TargetId,
                                 Tag, SourcePid, OtherMTE, Neighbors);
                           true ->
                               slide_op:new_slide(
                                 MoveFullId, NewType, TargetId, Tag, SourcePid,
                                 OtherMTE, NextOp, Neighbors)
                        end,
                    % note: phase will be set by prepare_send_data1/2 and needs to remain null here
                    State1 = dht_node_state:add_db_range(
                               State, slide_op:get_interval(SlideOp), MoveFullId),
                    prepare_send_data1(State1, SlideOp)
            end;
        {ok, {join, 'rcv'}} -> % similar to {slide, succ, 'rcv'}
            fd:subscribe([node:pidX(TargetNode)], {move, MoveFullId}),
            SlideOp = slide_op:new_receiving_slide_join(MoveFullId, TargetId, join, Neighbors),
            % note: phase will be set by notify_other/2 and needs to remain null here
            SlideOp1 = slide_op:set_setup_at_other(SlideOp), % we received a join_response before
            SlideOp2 = slide_op:set_next_op(SlideOp1, NextOp),
            case slide_chord:prepare_rcv_data(State, SlideOp2) of
                {ok, State1, SlideOp3} ->
                    notify_other(SlideOp3, State1);
                {abort, Reason, State1, SlideOp3} ->
                    abort_slide(State1, SlideOp3, Reason, true)
            end;
        {ok, {jump, 'send'}} -> % similar to {ok, {slide, succ, 'send'}}
            fd:subscribe([node:pidX(TargetNode)], {move, MoveFullId}),
            % TODO: activate incremental jump:
%%             IncTargetKey = find_incremental_target_id(Neighbors, State, TargetId, NewType, OtherMTE),
%%             SlideOp = slide_op:new_sending_slide_jump(MoveFullId, IncTargetKey, TargetId, Tag, Neighbors),
            SlideOp = slide_op:new_sending_slide_jump(MoveFullId, TargetId, Tag, Neighbors),
            case MsgTag of
                nomsg ->
                    notify_other(SlideOp, State);
                X when (X =:= slide orelse X =:= delta_ack) ->
                    prepare_send_data1(State, SlideOp)
            end;
        {ok, {leave, 'send'}} -> % similar to {ok, {slide, succ, 'send'}}
            fd:subscribe([node:pidX(TargetNode)], {move, MoveFullId}),
            % TODO: activate incremental leave:
%%             IncTargetKey = find_incremental_target_id(Neighbors, State, TargetId, NewType, OtherMTE),
%%             SlideOp = slide_op:new_sending_slide_leave(MoveFullId, IncTargetKey, leave, Neighbors),
            SlideOp = slide_op:new_sending_slide_leave(MoveFullId, leave, SourcePid, Neighbors),
            case MsgTag of
                nomsg ->
                    notify_other(SlideOp, State);
                X when (X =:= slide orelse X =:= delta_ack) ->
                    prepare_send_data1(State, SlideOp)
            end;
        {ok, {slide, succ, 'send'} = NewType} ->
            fd:subscribe([node:pidX(TargetNode)], {move, MoveFullId}),
            UseIncrSlides = use_incremental_slides(),
            case MsgTag of
                nomsg -> % first inform other node:
                    SlideOp = slide_op:new_slide(
                                MoveFullId, NewType, TargetId, Tag, SourcePid,
                                OtherMTE, NextOp, Neighbors),
                    notify_other(SlideOp, State);
                X when (X =:= slide orelse X =:= delta_ack) ->
                    SlideOp =
                        if UseIncrSlides orelse OtherMTE =/= unknown->
                               IncTargetKey = find_incremental_target_id(
                                                Neighbors, State,
                                                TargetId, NewType, OtherMTE),
                               slide_op:new_slide_i(
                                 MoveFullId, NewType, IncTargetKey, TargetId,
                                 Tag, SourcePid, OtherMTE, Neighbors);
                           true ->
                               slide_op:new_slide(
                                 MoveFullId, NewType, TargetId, Tag, SourcePid,
                                 OtherMTE, NextOp, Neighbors)
                        end,
                    prepare_send_data1(State, SlideOp)
            end;
        {ok, NewType} when NewType =:= {slide, pred, 'rcv'} orelse
                               NewType =:= {leave, 'rcv'} orelse
                               NewType =:= {slide, succ, 'rcv'} ->
            fd:subscribe([node:pidX(TargetNode)], {move, MoveFullId}),
            SlideOp = slide_op:new_slide(MoveFullId, NewType, TargetId, Tag,
                                         SourcePid, OtherMTE, NextOp, Neighbors),
            % note: phase will be set by notify_other/2 and needs to remain null here
            case slide_chord:prepare_rcv_data(State, SlideOp) of
                {ok, State1, SlideOp1} when MsgTag =:= nomsg ->
                    notify_other(SlideOp1, State1);
                {ok, State1, SlideOp1} when MsgTag =:= slide ->
                    notify_other(slide_op:set_setup_at_other(SlideOp1), State1);
                {abort, Reason, State1, SlideOp1} ->
                    abort_slide(State1, SlideOp1, Reason, MsgTag =/= nomsg)
            end;
        {ok, move_done} ->
            notify_source_pid(SourcePid, {move, result, Tag, ok}),
            case MsgTag of
                nomsg -> ok;
                _     -> Msg = {move, done, MoveFullId},
                         send_no_slide(node:pidX(TargetNode), Msg, 0)
            end,
            State
    end.

%% @doc On the sending node: looks into the DB and selects a key in the slide
%%      op's range which involves at most OtherMaxTransportEntries DB entries
%%      to be moved.
-spec find_incremental_target_id(Neighbors::nodelist:neighborhood(),
        State::dht_node_state:state(), FinalTargetId::?RT:key(), Type::slide_op:type(),
        OtherMaxTransportEntries::unknown | pos_integer()) -> ?RT:key().
find_incremental_target_id(Neighbors, State, FinalTargetId, Type, OtherMTE) ->
    'send' = slide_op:get_sendORreceive(Type), % just in case
    MTE = case OtherMTE of
              unknown -> get_max_transport_entries();
              _       -> erlang:min(OtherMTE, get_max_transport_entries())
          end,
    PredId = node:id(nodelist:pred(Neighbors)),
    % TODO: optimise here - if the remaining interval has no data, return FinalTargetId
    case slide_op:get_predORsucc(Type) of
        pred -> BeginId = PredId, Dir = forward;
        succ -> BeginId = nodelist:nodeid(Neighbors), Dir = backward
    end,
    case dht_node_state:get_split_key(State, BeginId, FinalTargetId, MTE, Dir) of
        {SplitKey, MTE} -> SplitKey;
        {_SplitKey, MTEX} when MTEX < MTE -> FinalTargetId
    end.

%% @doc Change the local node's ID to the given TargetId and progresses to the
%%      next phase, e.g. wait_for_continue. 
%% @see slide_chord:prepare_send_data1/3
-spec prepare_send_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> dht_node_state:state().
prepare_send_data1(State, SlideOp) ->
    MoveFullId = slide_op:get_id(SlideOp),
    SlideOp1 = slide_op:set_setup_at_other(SlideOp),
    ReplyPid = comm:reply_as(self(), 5, {move, continue, MoveFullId, prepare_send_data2, '_'}),
    SlideOp2 = slide_op:set_phase(SlideOp1, wait_for_continue),
    case slide_chord:prepare_send_data1(State, SlideOp2, ReplyPid) of
        {ok, State1, SlideOp3} ->
            PredOrSucc = slide_op:get_predORsucc(SlideOp3),
            dht_node_state:set_slide(State1, PredOrSucc, SlideOp3);
        {abort, Reason, State1, SlideOp3} ->
            abort_slide(State1, SlideOp3, Reason, true)
    end.

%% @doc Gets all data in the slide operation's interval from the DB and sends
%%      it to the target node. Also sets the DB to record changes in this
%%      interval and changes the slide operation's phase to wait_for_data_ack.
%% @see slide_chord:prepare_send_data2/3
-spec prepare_send_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         EmbeddedMsg::comm:message()) -> dht_node_state:state().
prepare_send_data2(State, SlideOp, EmbeddedMsg) ->
    case slide_chord:prepare_send_data2(State, SlideOp, EmbeddedMsg) of
        {ok, State1, SlideOp1} ->
            % last part of a leave? -> transfer all DB entries!
            % since in this case there is no other slide, we can safely use intervals:all()
            MovingInterval =
                case slide_op:is_leave(SlideOp1) andalso not slide_op:is_jump(SlideOp1)
                         andalso slide_op:get_next_op(SlideOp1) =:= {none} of
                    true  -> intervals:all();
                    false -> slide_op:get_interval(SlideOp1)
                end,
            {State2, MovingData} = dht_node_state:slide_get_data_start_record(
                                          State1, MovingInterval),
            SlideOp2 = slide_op:set_phase(SlideOp1, wait_for_data_ack),
            Msg = {move, data, MovingData, slide_op:get_id(SlideOp2),
                   slide_op:get_target_id(SlideOp2),
                   slide_op:get_next_op(SlideOp2)},
            dht_node_move:send2(State2, SlideOp2, Msg);
        {abort, Reason, State1, SlideOp1} ->
            abort_slide(State1, SlideOp1, Reason, true)
    end.

%% @doc Accepts data received during the given (existing!) slide operation and
%%      writes it to the DB.
%% @see slide_chord:update_rcv_data1/2
-spec update_rcv_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       Data::dht_node_state:slide_data(), TargetId::?RT:key(),
                       NextOp::slide_op:next_op()) -> dht_node_state:state().
update_rcv_data1(State, SlideOp, Data, TargetId, NextOp) ->
    MoveFullId = slide_op:get_id(SlideOp),
    PredOrSucc = slide_op:get_predORsucc(SlideOp),
    State1 = update_target_on_existing_slide(
               SlideOp, State, TargetId, NextOp),
    case dht_node_state:get_slide(State1, MoveFullId) of
        {PredOrSucc, SlideOp1} ->
            MoveFullId = slide_op:get_id(SlideOp1),
            ReplyPid = comm:reply_as(self(), 5, {move, continue, MoveFullId, update_rcv_data2, '_'}),
            SlideOp2 = slide_op:set_phase(SlideOp1, wait_for_continue),
            State2 = dht_node_state:slide_add_data(State1, Data),
            case slide_chord:update_rcv_data1(State2, SlideOp2, ReplyPid) of
                {ok, State3, SlideOp3} ->
                    PredOrSucc = slide_op:get_predORsucc(SlideOp3),
                    dht_node_state:set_slide(State3, PredOrSucc, SlideOp3);
                {abort, Reason, State3, SlideOp3} ->
                    abort_slide(State3, SlideOp3, Reason, true)
            end;
        not_found ->
            State1 % if aborted
    end.

%% @doc Sends data_ack message and progresses to the next phase, i.e. wait_for_delta.
%% @see slide_chord:prepare_send_delta2/3
-spec update_rcv_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       EmbeddedMsg::comm:message()) -> dht_node_state:state().
update_rcv_data2(State, SlideOp, EmbeddedMsg) ->
    case slide_chord:update_rcv_data2(State, SlideOp, EmbeddedMsg) of
        {ok, State1, SlideOp1} ->
            SlideOp2 = slide_op:set_phase(SlideOp1, wait_for_delta),
            Msg = {move, data_ack, slide_op:get_id(SlideOp2)},
            dht_node_move:send2(State1, SlideOp2, Msg);
        {abort, Reason, State1, SlideOp1} ->
            abort_slide(State1, SlideOp1, Reason, true)
    end.

%% @doc Prepares to send a delta message for the given (existing!) slide operation and
%%      continues.
%% @see slide_chord:prepare_send_delta1/3
-spec prepare_send_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> dht_node_state:state().
prepare_send_delta1(State, OldSlideOp) ->
    MoveFullId = slide_op:get_id(OldSlideOp),
    ReplyPid = comm:reply_as(self(), 5, {move, continue, MoveFullId, prepare_send_delta2, '_'}),
    SlideOp1 = slide_op:set_phase(OldSlideOp, wait_for_continue),
    case slide_chord:prepare_send_delta1(State, SlideOp1, ReplyPid) of
        {ok, State1, SlideOp2} ->
            PredOrSucc = slide_op:get_predORsucc(SlideOp2),
            dht_node_state:set_slide(State1, PredOrSucc, SlideOp2);
        {abort, Reason, State1, SlideOp2} ->
            abort_slide(State1, SlideOp2, Reason, true)
    end.

%% @doc Gets changed data in the slide operation's interval from the DB and
%%      sends it as a delta to the target node. Also sets the DB to stop
%%      recording changes in this interval and delete any such entries. Changes
%%      the slide operation's phase to wait_for_delta_ack.
%% @see slide_chord:prepare_send_delta2/3
-spec prepare_send_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                          EmbeddedMsg::comm:message()) -> dht_node_state:state().
prepare_send_delta2(State, SlideOp, EmbeddedMsg) ->
    case slide_chord:prepare_send_delta2(State, SlideOp, EmbeddedMsg) of
        {ok, State1, SlideOp1} ->
            % last part of a leave? -> transfer all DB entries!
            % since in this case there is no other slide, we can safely use intervals:all()
            SlideOpInterval =
                case slide_op:is_leave(SlideOp1) andalso not slide_op:is_jump(SlideOp1)
                         andalso slide_op:get_next_op(SlideOp1) =:= {none} of
                    true  -> intervals:all();
                    false -> slide_op:get_interval(SlideOp1)
                end,
            {State2, ChangedData} = dht_node_state:slide_take_delta_stop_record(
                                      State1, SlideOpInterval),
            % send delta (values of keys that have changed during the move)
            SlideOp2 = slide_op:set_phase(SlideOp1, wait_for_delta_ack),
            Msg = {move, delta, ChangedData, slide_op:get_id(SlideOp2)},
            dht_node_move:send2(State2, SlideOp2, Msg);
        {abort, Reason, State1, SlideOp1} ->
            abort_slide(State1, SlideOp1, Reason, true)
    end.

%% @doc Accepts delta received during the given (existing!) slide operation and
%%      continues.
%% @see slide_chord:finish_delta1/4
-spec finish_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                   ChangedData::dht_node_state:slide_delta())
        -> dht_node_state:state().
finish_delta1(State, OldSlideOp, ChangedData) ->
    MoveFullId = slide_op:get_id(OldSlideOp),
    ReplyPid = comm:reply_as(self(), 5, {move, continue, MoveFullId, finish_delta2, '_'}),
    SlideOp1 = slide_op:set_phase(OldSlideOp, wait_for_continue),
    case slide_chord:finish_delta1(State, SlideOp1, ChangedData, ReplyPid) of
        {ok, State1, SlideOp2} ->
            PredOrSucc = slide_op:get_predORsucc(SlideOp2),
            dht_node_state:set_slide(State1, PredOrSucc, SlideOp2);
        {abort, Reason, State1, SlideOp2} ->
            abort_slide(State1, SlideOp2, Reason, true)
    end.

-spec send_delta_ack(SlideOp::slide_op:slide_op()) -> ok.
send_delta_ack(SlideOp) ->
    Pid = node:pidX(slide_op:get_node(SlideOp)),
    Msg = {move, delta_ack, slide_op:get_id(SlideOp), {none}},
    send_no_slide(Pid, Msg, 0).

% similar to notify_other/2:
% pre: incremental slide in slide op on this node
-spec notify_other_in_delta_ack(OldMoveFullId::slide_op:id(),
        NextSlideOp::slide_op:slide_op(), State::dht_node_state:state())
            -> dht_node_state:state().
notify_other_in_delta_ack(OldMoveFullId, NextSlideOp, State) ->
    Msg = {move, delta_ack, OldMoveFullId, {continue, slide_op:get_id(NextSlideOp)}},
    send2(State, NextSlideOp, Msg).

-spec finish_slide(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> dht_node_state:state().
finish_slide(State, SlideOp) ->
    Pid = node:pidX(slide_op:get_node(SlideOp)),
    MoveFullId = slide_op:get_id(SlideOp),
    fd:unsubscribe([Pid], {move, MoveFullId}),
    notify_source_pid(slide_op:get_source_pid(SlideOp),
                      {move, result, slide_op:get_tag(SlideOp), ok}),
    PredOrSucc = slide_op:get_predORsucc(SlideOp),
    rm_loop:notify_slide_finished(PredOrSucc),
    dht_node_state:set_slide(State, PredOrSucc, null).

-spec finish_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    EmbeddedMsg::comm:message()) -> dht_node_state:state().
finish_delta2(State, SlideOp, EmbeddedMsg) ->
    case slide_chord:finish_delta2(State, SlideOp, EmbeddedMsg) of
        {ok, State1, SlideOp1} ->
            % continue with the next planned operation:
            case slide_op:is_incremental(SlideOp1) of
                true ->
                    Type = slide_op:get_type(SlideOp1),
                    PredOrSucc = slide_op:get_predORsucc(SlideOp1),
                    % TODO: support other types
                    case slide_op:get_next_op(SlideOp1) of
                        {slide, continue, NewTargetId} when Type =:= {slide, PredOrSucc, 'rcv'} ->
                            State2 = dht_node_state:set_slide(State1, PredOrSucc, null),
                            MyNode = dht_node_state:get(State2, node),
                            TargetNode = dht_node_state:get(State2, PredOrSucc),
                            MoveFullId = slide_op:get_id(SlideOp1),
                            Tag = slide_op:get_tag(SlideOp1),
                            SourcePid = slide_op:get_source_pid(SlideOp1),
                            OtherMTE = slide_op:get_other_max_entries(SlideOp1),
                            Command = check_setup_slide_not_found(
                                        State2, Type, MyNode, TargetNode, NewTargetId),
                            case Command of
                                {ok, {slide, _, 'rcv'} = NewType} ->
                                    Neighbors = dht_node_state:get(State2, neighbors),
                                    % continued slide with pred/succ, receive data
                                    % -> reserve slide_op with pred/succ
                                    NewMoveFullId = uid:get_global_uid(),
                                    fd:subscribe([node:pidX(TargetNode)], {move, NewMoveFullId}),
                                    NextSlideOp =
                                        slide_op:new_slide(
                                          NewMoveFullId, NewType, NewTargetId, Tag,
                                          SourcePid, OtherMTE, {none}, Neighbors),
                                    NextSlideOp1 = slide_op:set_phase(NextSlideOp, wait_for_data),
                                    notify_other_in_delta_ack(MoveFullId, NextSlideOp1, State2);
                                {abort, Reason, _Type} -> % note: the type returned here is the same as Type 
                                    abort_slide(State2, TargetNode, MoveFullId, null, SourcePid, Tag,
                                                Type, Reason, true)
                            end;
                        {jump, continue, _NewTargetId} ->
                            % TODO
                            send_delta_ack(SlideOp1),
                            finish_slide(State1, SlideOp1);
                        {leave, continue} ->
                            % TODO
                            send_delta_ack(SlideOp1),
                            finish_slide(State1, SlideOp1)
                    end;
                _ ->
                    % note: send delta_ack and a potential new slide setup in two
                    %       messages so new op suggestions also have a chance to be
                    %       setup instead
                    send_delta_ack(SlideOp1),
                    State2 = finish_slide(State1, SlideOp1),
                    PredOrSucc = slide_op:get_predORsucc(SlideOp1),
                    case slide_op:get_next_op(SlideOp1) of
                        {none} -> State2;
                        {join, _NewTargetId} ->
                            log:log(warn, "[ dht_node_move ~.0p ] ignoring scheduled join after "
                                        "receiving data (this doesn't make any sense!)",
                                    [comm:this()]),
                            State2;
                        {slide, PredOrSucc, NewTargetId, NewTag, NewSourcePid} ->
                            % continue operation with the same node previously sliding with
                            make_slide(State2, PredOrSucc, NewTargetId, NewTag, NewSourcePid);
                        {slide, PredOrSucc2, NewTargetId, NewTag, NewSourcePid} ->
                            % try setting up a slide with the other node
                            make_slide(State2, PredOrSucc2, NewTargetId, NewTag, NewSourcePid);
                        {jump, NewTargetId, NewTag, NewSourcePid} ->
                            % finish current slide, then set up jump
                            make_jump(State2, NewTargetId, NewTag, NewSourcePid);
                        {leave, NewSourcePid} ->
                            % finish current slide, then set up leave
                            % (this is not a continued operation!)
                            make_slide_leave(State2, NewSourcePid)
                    end
            end;
        {abort, Reason, State1, SlideOp1} ->
            % an abort at this stage is really useless (data has been fully integrated!)
            % nevertheless at least the source can be notified... 
            abort_slide(State1, SlideOp1, Reason, true)
    end.

%% @doc Accepts delta_ack received during the given (existing!) slide operation and
%%      continues.
%% @see slide_chord:finish_delta_ack1/4
-spec finish_delta_ack1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                        NextOpMsg::next_op_msg())
        -> dht_node_state:state().
finish_delta_ack1(State, OldSlideOp, NextOpMsg) ->
    MoveFullId = slide_op:get_id(OldSlideOp),
    ReplyPid = comm:reply_as(self(), 5, {move, continue, MoveFullId, finish_delta_ack2, '_'}),
    SlideOp1 = slide_op:set_phase(OldSlideOp, wait_for_continue),
    case slide_chord:finish_delta_ack1(State, SlideOp1, NextOpMsg, ReplyPid) of
        {ok, State1, SlideOp2} ->
            PredOrSucc = slide_op:get_predORsucc(SlideOp2),
            dht_node_state:set_slide(State1, PredOrSucc, SlideOp2);
        {abort, Reason, State1, SlideOp2} ->
            abort_slide(State1, SlideOp2, Reason, true)
    end.

-spec finish_delta_ack2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                        NextOp::next_op_msg()) -> dht_node_state:state().
finish_delta_ack2(State, SlideOp, NextOpMsg) ->
    case slide_chord:finish_delta_ack2(State, SlideOp, NextOpMsg) of
        {ok, State1, SlideOp1, NextOpMsg1} ->
            NextOpMsg2 =
                case slide_op:is_leave(SlideOp1) andalso not slide_op:is_jump(SlideOp1) of
                    true  -> {finish_leave};
                    false -> NextOpMsg1
                end,
            finish_delta_ack2B(State1, SlideOp1, NextOpMsg2);
        {abort, Reason, State1, SlideOp1} ->
            abort_slide(State1, SlideOp1, Reason, true)
    end.

%% Pre: SlideOp is no finished leaving slide (see finish_delta_ack2/3)
-spec finish_delta_ack2B(
        State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
        NextOpMsg::next_op_msg() |
          {finish_leave} |
          {NextOpType::slide_op:type(), NewSlideId::slide_op:id(),
          InitNode::node:node_type(), TargetNode::node:node_type(),
          TargetId::?RT:key(), Tag::any(), SourcePid::comm:erl_local_pid() | null})
        -> dht_node_state:state().
finish_delta_ack2B(State, SlideOp, {finish_leave}) ->
    State1 = finish_slide(State, SlideOp),
    SupDhtNodeId = erlang:get(my_sup_dht_node_id),
    SupDhtNode = pid_groups:get_my(sup_dht_node),
    comm:send_local(pid_groups:find_a(service_per_vm),
                    {delete_node, SupDhtNode, SupDhtNodeId}),
    % note: we will be killed soon but need to be removed from the supervisor first
    % -> do not kill this process
    State1;
finish_delta_ack2B(State, SlideOp, {none}) ->
    State1 = finish_slide(State, SlideOp),
    % continue with the next planned operation:
    PredOrSucc = slide_op:get_predORsucc(SlideOp),
    case slide_op:get_next_op(SlideOp) of
        {none} -> State1;
        {join, NewTargetId} ->
            OldIdVersion = node:id_version(dht_node_state:get(State1, node)),
            dht_node_join:join_as_other(NewTargetId, OldIdVersion + 1, [{skip_psv_lb}]);
        {slide, PredOrSucc, NewTargetId, NewTag, NewSourcePid} ->
            make_slide(State1, PredOrSucc, NewTargetId, NewTag, NewSourcePid);
        {jump, NewTargetId, NewTag, NewSourcePid} ->
            make_jump(State1, NewTargetId, NewTag, NewSourcePid);
        {leave, NewSourcePid} ->
            make_slide_leave(State1, NewSourcePid)
    end;
finish_delta_ack2B(State, SlideOp, {continue, NewSlideId}) ->
    MyNode = dht_node_state:get(State, node),
    Type = slide_op:get_type(SlideOp),
    PredOrSucc = slide_op:get_predORsucc(Type),
    TargetNode = dht_node_state:get(State, PredOrSucc),
    Tag = slide_op:get_tag(SlideOp),
    SourcePid = slide_op:get_source_pid(SlideOp),
    % TODO: support other types
    case slide_op:get_next_op(SlideOp) of
        {slide, continue, NewTargetId} when Type =:= {slide, PredOrSucc, 'send'} ->
            finish_delta_ack2B(
              State, SlideOp, {Type, NewSlideId, MyNode,
                               TargetNode, NewTargetId, Tag, SourcePid});
        {jump, continue, _NewTargetId} ->
            % TODO
            finish_delta_ack2B(State, SlideOp, {none});
        {leave, continue} ->
            % TODO
            finish_delta_ack2B(State, SlideOp, {none});
        _ -> % our next op is different from the other node's next op
            % TODO
            abort_slide(State, SlideOp, next_op_mismatch, true)
    end;
finish_delta_ack2B(State, SlideOp, {MyNextOpType, NewSlideId, MyNode,
                                   TargetNode, TargetId, Tag, SourcePid}) ->
    fd:unsubscribe([node:pidX(slide_op:get_node(SlideOp))], {move, slide_op:get_id(SlideOp)}),
    % Always prefer the other node's next_op over ours as it is almost
    % set up. Unless our scheduled op is a leave operation which needs
    % to be favoured.
    case slide_op:get_next_op(SlideOp) of
        {leave, NewSourcePid} when NewSourcePid =/= continue ->
            State1 = abort_slide(State, TargetNode, NewSlideId, null, SourcePid,
                                 Tag, MyNextOpType, scheduled_leave, true),
            make_slide_leave(State1, NewSourcePid);
        MyNextOp ->
            % TODO: check if warnings are generated in all cases
            case MyNextOp of
                {none} -> ok;
                {slide, continue, TargetId} -> ok;
                {jump, continue, TargetId} -> ok;
                {leave, continue} -> ok;
                _ ->
                    log:log(info, "[ dht_node_move ~.0p ] removing "
                                "scheduled next op ~.0p, "
                                "got next op: ~.0p",
                            [comm:this(), MyNextOp, MyNextOpType])
            end,
            State1 = dht_node_state:set_slide(
                       State, slide_op:get_predORsucc(SlideOp), null),
            Command = check_setup_slide_not_found(
                        State1, MyNextOpType, MyNode, TargetNode, TargetId),
            exec_setup_slide_not_found(
              Command, State1, NewSlideId, TargetNode, TargetId,
              Tag, slide_op:get_other_max_entries(SlideOp), SourcePid,
              delta_ack, {none})
    end.

%% @doc Checks if a slide operation with the given MoveFullId exists and
%%      executes WorkerFun if everything is ok. If the successor/predecessor
%%      information in the slide operation is incorrect, the slide is aborted
%%      (a message to the pred/succ is send, too). An exception is made for a
%%      crashed_node message which would naturally result in a wrong_neighbor!
-spec safe_operation(
    WorkerFun::fun((SlideOp::slide_op:slide_op(), State::dht_node_state:state())
                    -> dht_node_state:state()),
    State::dht_node_state:state(), MoveFullId::slide_op:id(),
    WorkPhases::[slide_op:phase(),...] | all,
    MoveMsgTag::atom()) -> dht_node_state:state().
safe_operation(WorkerFun, State, MoveFullId, WorkPhases, MoveMsgTag) ->
    case get_slide(State, MoveFullId) of
        {_, _PredOrSucc, SlideOp} when MoveMsgTag =:= crashed_node ->
            WorkerFun(SlideOp, State);
        {ok, _PredOrSucc, SlideOp} ->
            case WorkPhases =:= all orelse lists:member(slide_op:get_phase(SlideOp), WorkPhases) of
                true -> WorkerFun(SlideOp, State);
                _    ->
                    log:log(info, "[ dht_node_move ~.0p ] unexpected message ~.0p received in phase ~.0p",
                            [comm:this(), MoveMsgTag, slide_op:get_phase(SlideOp)]),
                    State
            end;
        not_found when MoveMsgTag =:= send_error_retry ->
            State;
        not_found ->
            log:log(warn,"[ dht_node_move ~.0p ] ~.0p received with no matching "
                   "slide operation (ID: ~.0p, slide_pred: ~.0p, slide_succ: ~.0p)~n",
                    [comm:this(), MoveMsgTag, MoveFullId,
                     dht_node_state:get(State, slide_pred),
                     dht_node_state:get(State, slide_succ)]),
            State;
        {wrong_neighbor, PredOrSucc, SlideOp} -> % wrong pred or succ
            case WorkPhases =:= all orelse lists:member(slide_op:get_phase(SlideOp), WorkPhases) of
                true ->
                    log:log(warn,"[ dht_node_move ~.0p ] ~.0p received but ~s "
                           "changed during move (ID: ~.0p, node(slide): ~.0p, new_~s: ~.0p)~n",
                            [comm:this(), MoveMsgTag, PredOrSucc, MoveFullId,
                             slide_op:get_node(SlideOp),
                             PredOrSucc, dht_node_state:get(State, PredOrSucc)]),
                    abort_slide(State, SlideOp, wrong_pred_succ_node, true);
                _ -> State
            end
    end.

%% @doc Tries to find a slide operation with the given MoveFullId and returns
%%      it including its type (pred or succ) if successful and its pred/succ
%%      info is correct (a wrong predecessor is tolerated if the slide
%%      operation is a leave since the node will leave and thus we will get
%%      a new predecessor). Otherwise returns {fail, wrong_pred} if the
%%      predecessor info is wrong (slide with pred) and {fail, wrong_succ} if
%%      the successor info is wrong (slide with succ). If not found,
%%      {fail, not_found} is returned.
-spec get_slide(State::dht_node_state:state(), MoveFullId::slide_op:id()) ->
        {Result::ok, Type::pred | succ, SlideOp::slide_op:slide_op()} |
        {Result::wrong_neighbor, Type::pred | succ, SlideOp::slide_op:slide_op()} |
        not_found.
get_slide(State, MoveFullId) ->
    case dht_node_state:get_slide(State, MoveFullId) of
        not_found -> not_found;
        {PredOrSucc, SlideOp} ->
            Node = dht_node_state:get(State, PredOrSucc),
            NodeSlOp = slide_op:get_node(SlideOp),
            % - allow changed pred during a leave if the new pred is not between
            %   the leaving node and the current node!
            % - allow outdated pred during join (as an existing node) if the new
            %   pred is in the current range
            case node:same_process(Node, NodeSlOp) orelse
                     (PredOrSucc =:= pred andalso
                          (slide_op:is_leave(SlideOp) orelse
                               slide_op:is_join(SlideOp)) andalso
                          intervals:in(node:id(NodeSlOp), dht_node_state:get(State, my_range))) of
                true -> {ok,             PredOrSucc, SlideOp};
                _    -> {wrong_neighbor, PredOrSucc, SlideOp}
            end
    end.

%% @doc Returns whether a slide with the successor is possible for the given
%%      target id.
%% @see can_slide/3
-spec can_slide_succ(State::dht_node_state:state(), TargetId::?RT:key(), Type::slide_op:type()) -> boolean().
can_slide_succ(State, TargetId, Type) ->
    SlidePred = dht_node_state:get(State, slide_pred),
    DBRange = dht_node_state:get(State, db_range),
    dht_node_state:get(State, slide_succ) =:= null andalso
        % 1) TargetId not interfering with SlidePred?
        (SlidePred =:= null orelse
             (not intervals:in(TargetId, slide_op:get_interval(SlidePred)) andalso
                  not (slide_op:is_leave(Type) andalso slide_op:is_leave(SlidePred)))
        ) andalso
        % 2) no left-over DBRange (from sending data to successor) waiting for RM-update
        % -> we need to integrate this range into my_range first in order to proceed
        %    (most code does not look at db_range and only uses my_range to create intervals!)
        (DBRange =:= [] orelse
             (SlidePred =/= null andalso
                  element(2, hd(DBRange)) =:= slide_op:get_id(SlidePred))
        ).

%% @doc Returns whether a slide with the predecessor is possible for the given
%%      target id.
%% @see can_slide/3
-spec can_slide_pred(State::dht_node_state:state(), TargetId::?RT:key(), Type::slide_op:type()) -> boolean().
can_slide_pred(State, TargetId, _Type) ->
    SlideSucc = dht_node_state:get(State, slide_succ),
    DBRange = dht_node_state:get(State, db_range),
    dht_node_state:get(State, slide_pred) =:= null andalso
        % 1) TargetId not interfering with SlideSucc?
        % 2) no left-over DBRange (from sending data to successor) waiting for RM-update
        % -> we need to integrate this range into my_range first in order to proceed
        %    (most code does not look at db_range and only uses my_range to create intervals!)
        (SlideSucc =:= null orelse
             (not intervals:in(TargetId, slide_op:get_interval(SlideSucc)) andalso
                  not slide_op:is_leave(SlideSucc))
        ) andalso
        % 2) no left-over DBRange (from receiving data from predecessor) waiting for RM-update
        % -> we need to integrate this range into my_range first in order to proceed
        %    (most code does not look at db_range and only uses my_range to create intervals!)
        (DBRange =:= [] orelse
             (SlideSucc =/= null andalso tl(DBRange) =:= [] andalso % only one DBRange element
                  element(2, hd(DBRange)) =:= slide_op:get_id(SlideSucc))
        ).

%% @doc Sends the source pid the given message if it is not 'null'.
-spec notify_source_pid(SourcePid::comm:erl_local_pid() | null, Message::result_message()) -> ok.
notify_source_pid(SourcePid, Message) ->
    case SourcePid of
        null -> ok;
        _ -> ?TRACE_SEND(SourcePid, Message),
             comm:send_local(SourcePid, Message)
    end.

%% @doc Updates TargetId and NextOp after receiving it along with a data message.
-spec update_target_on_existing_slide(
        OldSlideOp::slide_op:slide_op(), State::dht_node_state:state(),
        TargetId::?RT:key(), NextOp::slide_op:next_op()) -> dht_node_state:state().
update_target_on_existing_slide(OldSlideOp, State, TargetId, NextOp) ->
            PredOrSucc = slide_op:get_predORsucc(OldSlideOp),
    case slide_op:get_target_id(OldSlideOp) of
        TargetId ->
            SlideOp1 = slide_op:set_next_op(OldSlideOp, NextOp),
            dht_node_state:set_slide(State, PredOrSucc, SlideOp1);
        _ ->
            SendOrReceive = slide_op:get_sendORreceive(OldSlideOp),
            AllowedI =
                if PredOrSucc =:= succ andalso SendOrReceive =:= 'rcv' ->
                       % new target ID can only be between my current ID and the old target ID!
                       OldTargetId = slide_op:get_target_id(OldSlideOp),
                       MyId = dht_node_state:get(State, node_id),
                       node:mk_interval_between_ids(MyId, OldTargetId);
                   PredOrSucc =:= succ andalso SendOrReceive =:= 'send' ->
                       % new target ID can only be between the old target ID and my current ID!
                       OldTargetId = slide_op:get_target_id(OldSlideOp),
                       MyId = dht_node_state:get(State, node_id),
                       node:mk_interval_between_ids(OldTargetId, MyId);
                   PredOrSucc =:= pred ->
                       % we cannot really check anything here with chord as the pred may have already changed
                       intervals:all()
                end,
            case intervals:in(TargetId, AllowedI) of
                true ->
                    % TODO: if there is any other NextOp planned, abort that!
                    % (currently there is no mechanism to add NextOp's other than
                    % incremental slides, so it is ok to just remove the old one for now)
                    SlideOp1 = slide_op:update_target_id(
                                 OldSlideOp, TargetId, NextOp,
                                 dht_node_state:get(State, neighbors)),
                    
                    MoveFullId = slide_op:get_id(SlideOp1),
                    MySlideDBRange = [1 || {_, Id} <- dht_node_state:get(State, db_range),
                                           Id =:= MoveFullId],
                    State1 = case MySlideDBRange of
                                 []    -> State;
                                 [_|_] -> dht_node_state:add_db_range(
                                            dht_node_state:rm_db_range(State, MoveFullId),
                                            slide_op:get_interval(SlideOp1), MoveFullId)
                             end,
                    dht_node_state:set_slide(State1, PredOrSucc, SlideOp1);
                false ->
                    log:log(warn,"[ dht_node_move ~.0p ] new TargetId and NextOp received "
                                "but not in allowed range (ID: ~.0p, node(slide): ~.0p, "
                                "my_id: ~.0p, target_id: ~.0p, new_target_id: ~.0p)~n",
                            [comm:this(), slide_op:get_id(OldSlideOp),
                             dht_node_state:get(State, node_id),
                             slide_op:get_target_id(OldSlideOp), TargetId]),
                    abort_slide(State, OldSlideOp, changed_parameters, true)
            end
    end.

%% @doc Re-creates a slide operation with the given (updated) parameters.
-spec recreate_existing_slide(
        OldSlideOp::slide_op:slide_op(), State::dht_node_state:state(),
        TargetId::?RT:key(), OtherMaxTransportEntries::unknown | pos_integer(),
        MsgTag::nomsg | slide | delta_ack,
        NextOp::slide_op:next_op()) -> dht_node_state:state().
recreate_existing_slide(OldSlideOp, State, TargetId, OtherMTE, MsgTag, NextOp) ->
    % TODO: if there is any other NextOp planned, abort that!
    % (currently there is no mechanism to add NextOp's other than
    % incremental slides, so it is ok to just remove the old one for now)
    PredOrSucc = slide_op:get_predORsucc(OldSlideOp),
    MoveFullId = slide_op:get_id(OldSlideOp),
    SlideOp1 = slide_op:cancel_timer(OldSlideOp), % cancel previous timer
    % simply re-create the slide (TargetId or NextOp have changed)
    % note: fd:subscribe/2 will be called by exec_setup_slide_not_found
    fd:unsubscribe([node:pidX(slide_op:get_node(SlideOp1))], {move, MoveFullId}),
    State1 = dht_node_state:set_slide(State, PredOrSucc, null), % just in case
    % note: msg_fwd are stored in the slide and do not require additional removal
    State2 = dht_node_state:rm_db_range(State1, MoveFullId),
    Command = {ok, slide_op:get_type(SlideOp1)},
    exec_setup_slide_not_found(
      Command, State2, MoveFullId, slide_op:get_node(SlideOp1), TargetId,
      slide_op:get_tag(SlideOp1), OtherMTE, slide_op:get_source_pid(SlideOp1),
      MsgTag, NextOp).

%% @doc Aborts the given slide operation. Assume the SlideOp has already been
%%      set in the dht_node and resets the according slide in its state to
%%      null.
%% @see abort_slide/8
-spec abort_slide(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
        Reason::abort_reason(), NotifyNode::boolean()) -> dht_node_state:state().
abort_slide(State, SlideOp, Reason, NotifyNode) ->
    % write to log when aborting an already set-up slide:
    case slide_op:is_setup_at_other(SlideOp) of
        true ->
            log:log(warn, "[ dht_node_move ~.0p ] abort_slide(op: ~.0p, reason: ~.0p)~n",
                    [comm:this(), SlideOp, Reason]);
        _ -> ok
    end,
    SlideOp1 = slide_op:cancel_timer(SlideOp), % cancel previous timer
    % potentially set up for joining nodes (slide with pred) or
    % nodes sending data to their predecessor:
    RMSubscrTag = {move, slide_op:get_id(SlideOp1)},
    rm_loop:unsubscribe(self(), RMSubscrTag),
    State2 = dht_node_state:rm_db_range(State, slide_op:get_id(SlideOp1)),
    % set a 'null' slide_op if there was an old one with the given ID
    Type = slide_op:get_type(SlideOp1),
    PredOrSucc = slide_op:get_predORsucc(Type),
    Node = slide_op:get_node(SlideOp1),
    Id = slide_op:get_id(SlideOp1),
    fd:unsubscribe([node:pidX(Node)], {move, Id}),
    State3 = dht_node_state:set_slide(State2, PredOrSucc, null),
    State4 = dht_node_state:slide_stop_record(State3, slide_op:get_interval(SlideOp), false),
    abort_slide(State4, Node, Id, slide_op:get_phase(SlideOp1),
                slide_op:get_source_pid(SlideOp1), slide_op:get_tag(SlideOp1),
                Type, Reason, NotifyNode).

%% @doc Like abort_slide/5 but does not need a slide operation in order to
%%      work. Note: prefer using abort_slide/5 when a slide operation is
%%      available as this also resets all its timers!
-spec abort_slide(State::dht_node_state:state(), Node::node:node_type(),
                  SlideOpId::slide_op:id(), Phase::slide_op:phase(),
                  SourcePid::comm:erl_local_pid() | null,
                  Tag::any(), Type::slide_op:type(), Reason::abort_reason(),
                  NotifyNode::boolean()) -> dht_node_state:state().
abort_slide(State, Node, SlideOpId, _Phase, SourcePid, Tag, Type, Reason, NotifyNode) ->
    PredOrSucc = slide_op:get_predORsucc(Type),
    NodePid = node:pidX(Node),
    % abort slide on the (other) node:
    case NotifyNode of
        true ->
            PredOrSuccOther = case PredOrSucc of
                                  pred -> succ;
                                  succ -> pred
                              end,
            Msg = {move, slide_abort, PredOrSuccOther, SlideOpId, Reason},
            send_no_slide(NodePid, Msg, 0);
        _ -> ok
    end,
    % re-start a leaving slide on the leaving node if it hasn't left the ring yet:
    case Reason =/= leave_no_partner_found andalso
             slide_op:is_leave(Type, 'send') andalso
             not slide_op:is_jump(Type) of
        true -> comm:send_local(self(), {leave, SourcePid}),
                State;
        _    -> notify_source_pid(SourcePid, {move, result, Tag, Reason}),
                State
    end.

% failure detector reported dead node
-spec crashed_node(State::dht_node_state:state(), DeadPid::comm:mypid(), Cookie::{move, MoveFullId::slide_op:id()}) -> dht_node_state:state().
crashed_node(MyState, _DeadPid, {move, MoveFullId} = _Cookie) ->
    ?TRACE1({crash, _DeadPid, _Cookie}, MyState),
    WorkerFun =
        fun(SlideOp, State) ->
                abort_slide(State, SlideOp, target_down, false)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, all, crashed_node).

%% @doc Creates a slide with the node's successor or predecessor. TargetId will
%%      become the ID between the two nodes, i.e. either the current node or
%%      the other node will change its ID to TargetId. SourcePid will be
%%      notified about the result.
-spec make_slide(State::dht_node_state:state(), pred | succ, TargetId::?RT:key(),
        Tag::any(), SourcePid::comm:erl_local_pid() | null) -> dht_node_state:state().
make_slide(State, PredOrSucc, TargetId, Tag, SourcePid) ->
    % slide with PredOrSucc possible? if so, receive or send data?
    Neighbors = dht_node_state:get(State, neighbors),
    SendOrReceive =
        case PredOrSucc of
            succ ->
                case intervals:in(TargetId, nodelist:succ_range(Neighbors)) of
                    true -> 'rcv';
                    _    -> 'send'
                end;
            pred ->
                case intervals:in(TargetId, nodelist:node_range(Neighbors)) of
                    true -> 'send';
                    _    -> 'rcv'
                end
        end,
    MoveFullId = uid:get_global_uid(),
    MyNode = nodelist:node(Neighbors),
    TargetNode = nodelist:PredOrSucc(Neighbors),
    setup_slide(State, {slide, PredOrSucc, SendOrReceive},
                MoveFullId, MyNode, TargetNode, TargetId, Tag,
                unknown, SourcePid, nomsg, {none}).

%% @doc Creates a slide with the node's predecessor. The predecessor will
%%      change its ID to TargetId, SourcePid will be notified about the result.
-spec make_jump(State::dht_node_state:state(), TargetId::?RT:key(),
                Tag::any(), SourcePid::comm:erl_local_pid() | null)
    -> dht_node_state:state().
make_jump(State, TargetId, Tag, SourcePid) ->
    MoveFullId = uid:get_global_uid(),
    MyNode = dht_node_state:get(State, node),
    TargetNode = dht_node_state:get(State, succ),
    log:log(info, "[ Node ~.0p ] starting jump (succ: ~.0p, TargetId: ~.0p)~n",
            [MyNode, TargetNode, TargetId]),
    setup_slide(State, {jump, 'send'},
                MoveFullId, MyNode, TargetNode, TargetId, Tag,
                unknown, SourcePid, nomsg, {none}).

%% @doc Creates a slide that will move all data to the successor and leave the
%%      ring. Note: Will re-try (forever) to successfully start a leaving slide
%%      if anything causes an abort!
-spec make_slide_leave(State::dht_node_state:state(), SourcePid::comm:erl_local_pid() | null)
        -> dht_node_state:state().
make_slide_leave(State, SourcePid) ->
    MoveFullId = uid:get_global_uid(),
    InitNode = dht_node_state:get(State, node),
    OtherNode = dht_node_state:get(State, succ),
    PredNode = dht_node_state:get(State, pred),
    log:log(info, "[ Node ~.0p ] starting leave (succ: ~.0p)~n", [InitNode, OtherNode]),
    setup_slide(State, {leave, 'send'}, MoveFullId, InitNode,
                OtherNode, node:id(PredNode), leave,
                unknown, SourcePid, nomsg, {none}).

%% @doc Checks whether config parameters regarding dht_node moves exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(move_max_transport_entries) and
    config:cfg_is_greater_than(move_max_transport_entries, 0) and

    config:cfg_is_integer(move_wait_for_reply_timeout) and
    config:cfg_is_greater_than(move_wait_for_reply_timeout, 0) and

    config:cfg_is_integer(move_send_msg_retries) and
    config:cfg_is_greater_than(move_send_msg_retries, 0) and

    config:cfg_is_integer(move_send_msg_retry_delay) and
    config:cfg_is_greater_than_equal(move_send_msg_retry_delay, 0) and

    config:cfg_is_bool(move_use_incremental_slides).
    
%% @doc Gets the max number of DB entries per data move operation (set in the
%%      config files).
-spec get_max_transport_entries() -> pos_integer().
get_max_transport_entries() ->
    config:read(move_max_transport_entries).

%% @doc Gets the max number of ms to wait for the other node's reply until
%%      logging a warning (set in the config files).
-spec get_wait_for_reply_timeout() -> pos_integer().
get_wait_for_reply_timeout() ->
    config:read(move_wait_for_reply_timeout).

%% @doc Gets the max number of retries to send a message to the other node
%%      until logging a warning (set in the config files).
-spec get_send_msg_retries() -> pos_integer().
get_send_msg_retries() ->
    config:read(move_send_msg_retries).

%% @doc Checks whether incremental slides are to be used
%%      (set in the config files).
-spec use_incremental_slides() -> boolean().
use_incremental_slides() ->
    config:read(move_use_incremental_slides).
