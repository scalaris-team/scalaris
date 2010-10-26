%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @end
%% @version $Id$
-module(dht_node_move).
-author('kruber@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([process_move_msg/2,
         can_slide_succ/3, can_slide_pred/3,
         rm_pred_changed/2, rm_notify_new_pred/3,
         make_slide_leave/1,
         check_config/0]).

-ifdef(with_export_type_support).
-export_type([move_message/0]).
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
    notify_succ_timeout | % tried to notify my successor of a move
    notify_pred_timeout | % tried to notify my predecessor of a move
    send_data_timeout | % sent data to succ/pred but no reply yet
    rcv_data_timeout | % sent req_data to succ/pred but no reply yet
    data_ack_timeout | % sent data_ack to succ/pred but no reply yet
    send_delta_timeout | % sent delta data to succ/pred but no reply yet
    {protocol_error, string()}.

-type result_message() :: {move, result, Tag::any(), Reason::abort_reason() | ok}.

-type move_message() ::
    {move, slide_succ, TargetId::?RT:key(), Tag::any(), SourcePid::comm:erl_local_pid() | null} |
    {move, slide_pred, TargetId::?RT:key(), Tag::any(), SourcePid::comm:erl_local_pid() | null} |
    {move, jump, TargetId::?RT:key(), Tag::any(), SourcePid::comm:erl_local_pid() | null} |
    {move, slide_pred, SendOrReceive::'send' | 'rcv', MoveFullId::slide_op:id(), InitNode::node:node_type(), TargetNode::node:node_type(), TargetId::?RT:key(), Tag::any(), MaxTransportBytes::pos_integer()} |
    {move, slide_succ, SendOrReceive::'send' | 'rcv', MoveFullId::slide_op:id(), InitNode::node:node_type(), TargetNode::node:node_type(), TargetId::?RT:key(), Tag::any(), MaxTransportBytes::pos_integer()} |
    {move, slide_pred_abort, MoveFullId::slide_op:id(), Reason::abort_reason()} |
    {move, slide_succ_abort, MoveFullId::slide_op:id(), Reason::abort_reason()} |
    {move, node_update, NewNode::node:node_type()} |
    {move, rm_new_pred, NewPred::node:node_type()} | % only for nodes which join at our node
    {move, req_data, MoveFullId::slide_op:id(), MaxTransportBytes::pos_integer()} |
    {move, data, MovingData::?DB:db_as_list(), MoveFullId::slide_op:id()} |
    {move, data_ack, MoveFullId::slide_op:id()} |
    {move, delta, ChangedData::?DB:db_as_list(), DeletedKeys::[?RT:key()], MoveFullId::slide_op:id()} |
    {move, delta_ack, MoveFullId::slide_op:id()} |
    % timeout messages:
    {move, notify_succ_timeout, MoveFullId::slide_op:id()} | % tried to notify my successor of a move
    {move, notify_pred_timeout, MoveFullId::slide_op:id()} | % tried to notify my predecessor of a move
    {move, send_data_timeout, MoveFullId::slide_op:id()} | % sent data to succ/pred but no reply yet
    {move, rcv_data_timeout, MoveFullId::slide_op:id()} | % sent req_data to succ/pred but no reply yet
    {move, data_ack_timeout, MoveFullId::slide_op:id()} | % sent data_ack to succ/pred but no reply yet
    {move, send_delta_timeout, MoveFullId::slide_op:id(), ChangedData::?DB:db_as_list(), DeletedKeys::[?RT:key()]} % sent delta data to succ/pred but no reply yet
.

%% @doc Processes move messages for the dht_node and implements the node move
%%      protocol.
-spec process_move_msg(move_message(), dht_node_state:state()) -> dht_node_state:state().
process_move_msg({move, slide_succ, TargetId, Tag, SourcePid}, State) ->
    % slide with successor possible? if so -> receive or send data?
    {IsSlideSucc, SendOrReceive} =
        case intervals:in(TargetId, dht_node_state:get(State, succ_range)) of
            true -> {true, 'rcv'};
            _ ->
                TargetIdInMyRange = dht_node_state:is_responsible(TargetId, State),
                {TargetIdInMyRange, 'send'}
        end,
    
    if
        IsSlideSucc ->
            MoveFullId = util:get_global_uid(),
            MyNode = dht_node_state:get(State, node),
            TargetNode = dht_node_state:get(State, succ),
            setup_slide_with(succ, State, SendOrReceive, MoveFullId, MyNode,
                             TargetNode, TargetId, Tag,
                             get_max_transport_bytes(), SourcePid, true);
        true ->
            notify_source_pid(SourcePid,
                              {move, result, Tag, wrong_pred_succ_node}),
            State
    end;

process_move_msg({move, slide_pred, TargetId, Tag, SourcePid}, State) ->
    % Note: we can not check everything whether this is a valid slide_pred
    % -> our predecessor has to check that
    SendOrReceive = case dht_node_state:is_responsible(TargetId, State) of
                        true -> 'rcv';
                        _    -> 'send'
                    end,
    
    MoveFullId = util:get_global_uid(),
    MyNode = dht_node_state:get(State, node),
    TargetNode = dht_node_state:get(State, pred),
    setup_slide_with(pred, State, SendOrReceive, MoveFullId, MyNode,
                     TargetNode, TargetId, Tag, get_max_transport_bytes(),
                     SourcePid, true);

process_move_msg({move, jump, TargetId, Tag, SourcePid}, State) ->
    % check whether this is a slide_succ -> if so, do that instead
    case dht_node_state:is_responsible(TargetId, State) orelse
             intervals:in(TargetId, dht_node_state:get(State, succ_range)) of
        true ->
            process_move_msg({move, slide_succ, TargetId, Tag, SourcePid},
                             State);
        _ ->
            case dht_node_state:get(State, slide_succ) =/= null andalso
                     dht_node_state:get(State, slide_pred) =/= null of
                true ->
                    % leave the ring and join somewhere else
                    % TODO: send data to successor (andalso predecessor?) and afterwards, leave!
                    OldIdVersion = node:id_version(dht_node_state:get(State, node)),
                    idholder:set_id(TargetId, OldIdVersion + 1),
                    comm:send_local(self(), {kill}),
                    rm_loop:leave(),
                    State;
                _ ->
                    notify_source_pid(SourcePid,
                                      {move, result, Tag, ongoing_slide}),
                    State
            end
    end;

process_move_msg({move, notify_succ_timeout, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, succ, State) ->
                case (slide_op:get_timeouts(SlideOp) < get_notify_succ_retries()) of
                    true ->
                        NewSlideOp = slide_op:inc_timeouts(SlideOp),
                        notify_other_slide(succ, NewSlideOp, State);
                    _ ->
                        % if the message was received by succ, it will itself run
                        % into a timeout since we do not reply to it anymore
                        % -> do not send abort message
                        abort_slide(State, SlideOp, succ, notify_succ_timeout, false)
                end
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_succ_ack, succ, notify_succ_timeout);

process_move_msg({move, notify_pred_timeout, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, pred, State) ->
                case (slide_op:get_timeouts(SlideOp) < get_notify_pred_retries()) of
                    true ->
                        NewSlideOp = slide_op:inc_timeouts(SlideOp),
                        notify_other_slide(pred, NewSlideOp, State);
                    _ ->
                        % if the message was received by pred, it will itself run
                        % into a timeout since we do not reply to it anymore
                        % -> do not send abort message
                        abort_slide(State, SlideOp, pred, notify_pred_timeout, false)
                end
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, [wait_for_req_data, wait_for_data], pred, notify_pred_timeout);

% notification from predecessor that it wants to slide with our node
process_move_msg({move, slide_pred, SendOrReceive, MoveFullId, OtherNode,
                  OtherTargetNode, TargetId, Tag, MaxTransportBytes}, State) ->
    setup_slide_with(pred, State, SendOrReceive, MoveFullId, OtherTargetNode,
                     OtherNode, TargetId, Tag, MaxTransportBytes, null, false);

% notification from successor that it wants to slide with our node
process_move_msg({move, slide_succ, SendOrReceive, MoveFullId, OtherNode,
                  OtherTargetNode, TargetId, Tag, MaxTransportBytes}, State) ->
    setup_slide_with(succ, State, SendOrReceive, MoveFullId, OtherTargetNode,
                     OtherNode, TargetId, Tag, MaxTransportBytes, null, false);

% notification from pred that he could not aggree on a slide with us
process_move_msg({move, slide_pred_abort, MoveFullId, Reason}, State) ->
    SlidePred = dht_node_state:get(State, slide_pred),
    case SlidePred =/= null andalso slide_op:get_id(SlidePred) =:= MoveFullId of
        true ->
            abort_slide(State, SlidePred, pred, Reason, false);
        _ ->
            log:log(warn, "[ dht_node_move ~.0p ] slide_pred_abort received with no "
                          "matching slide operation (ID: ~.0p, slide_pred: ~.0p)~n",
                    [comm:this(), MoveFullId, dht_node_state:get(State, slide_pred)]),
            State
    end;

% notification from succ that he could not aggree on a slide with us
process_move_msg({move, slide_succ_abort, MoveFullId, Reason}, State) ->
    SlideSucc = dht_node_state:get(State, slide_succ),
    case SlideSucc =/= null andalso slide_op:get_id(SlideSucc) =:= MoveFullId of
        true ->
            abort_slide(State, SlideSucc, succ, Reason, false);
        _ ->
            log:log(warn, "[ dht_node_move ~.0p ] slide_succ_abort received with no "
                          "matching slide operation (ID: ~.0p, slide_succ: ~.0p)~n",
                    [comm:this(), MoveFullId, dht_node_state:get(State, slide_succ)]),
            State
    end;

process_move_msg({move, node_update, NewNode}, State) ->
    % note: can't use safe_operation/6 - there is no slide op id
    SlideOp = dht_node_state:get(State, slide_succ),
    % only handle node_update in wait_for_node_update phase
    case SlideOp =/= null andalso
             slide_op:get_phase(SlideOp) =:= wait_for_node_update of
        true ->
            rm_loop:unsubscribe(self(), fun rm_loop:subscribe_node_change_filter/2,
                                fun send_node_change/3),
            TargetId = slide_op:get_target_id(SlideOp),
            case node:id(NewNode) =:= TargetId of
                true ->
                    case slide_op:get_type(SlideOp) of
                        'send' ->
                            State1 = dht_node_state:add_db_range(
                                       State, slide_op:get_interval(SlideOp)),
                            send_data(State1, succ, SlideOp);
                        'rcv'  -> request_data(State, succ, SlideOp)
                    end;
                _ ->
                    % there should not be any other node id change when we are waiting for one!
                    log:log(fatal, "[ dht_node_move ~.0p ] received node id update "
                                   "with no matching slide operation "
                                   "(NewNode: ~.0p, slide_succ: ~.0p)~n",
                            [comm:this(), NewNode, SlideOp]),
                    State
            end;
        _ ->
            % we should not receive node update messages unless we are waiting for them
            % (node id updates should only be triggered by this module anyway)
            log:log(warn, "[ dht_node_move ~.0p ] received node id update with no "
                          "matching slide operation (NewNode: ~.0p, slide_succ: ~.0p)~n",
                    [comm:this(), NewNode, SlideOp]),
            State % ignore unrelated node updates
    end;

process_move_msg({move, node_leave}, State) ->
    % note: can't use safe_operation/6 - there is no slide op id
    SlideOp = dht_node_state:get(State, slide_succ),
    % only handle node_update in wait_for_node_update phase
    case SlideOp =/= null andalso slide_op:is_leave(SlideOp) andalso
             slide_op:get_phase(SlideOp) =:= wait_for_node_update andalso
             slide_op:get_type(SlideOp) =:= 'send' of
        true ->
            State1 = dht_node_state:add_db_range(
                       State, slide_op:get_interval(SlideOp)),
            send_data(State1, succ, SlideOp);
        _ ->
            % we should not receive node update messages unless we are waiting for them
            % (node id updates should only be triggered by this module anyway)
            log:log(warn, "[ dht_node_move ~w ] received rm node leave message with no "
                          "matching slide operation (slide_succ: ~w)~n",
                    [comm:this(), SlideOp]),
            State % ignore unrelated node leave messages
    end;

% wait for the joining node to appear in the rm-process -> got ack from rm:
% (see dht_node_join.erl)
process_move_msg({move, rm_new_pred, NewPred}, State) ->
    % note: can't use safe_operation/6 - there is no slide op id
    SlideOp = dht_node_state:get(State, slide_pred),
    % only handle rm_new_pred in wait_for_pred_update phase
    case SlideOp =/= null andalso
             slide_op:get_phase(SlideOp) =:= wait_for_pred_update andalso
             node:same_process(NewPred, slide_op:get_node(SlideOp)) of
        true ->
            rm_loop:unsubscribe(self(), fun dht_node_move:rm_pred_changed/2,
                                fun dht_node_move:rm_notify_new_pred/3),
            NewSlideOp = slide_op:set_phase(SlideOp, wait_for_req_data),
            dht_node_state:set_slide(State, pred, NewSlideOp);
        _ ->
            State % ignore unrelated rm_new_pred messages
    end;

% timeout waiting for data
process_move_msg({move, rcv_data_timeout, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, succ, State) ->
                case (slide_op:get_timeouts(SlideOp) < get_rcv_data_retries()) of
                    true ->
                        NewSlideOp = slide_op:inc_timeouts(SlideOp),
                        request_data(State, succ, NewSlideOp);
                    _ ->
                        abort_slide(State, SlideOp, succ, rcv_data_timeout, true)
                end
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_data, succ, rcv_data_timeout);

% request for data from a neighbor
process_move_msg({move, req_data, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, PredOrSucc, State) ->
                SlideOp1 = slide_op:reset_timer(SlideOp), % reset previous timeouts
                send_data(State, PredOrSucc, SlideOp1)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_req_data, pred, req_data);

% request for data from a neighbor
process_move_msg({move, send_data_timeout, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, PredOrSucc, State) ->
                case (slide_op:get_timeouts(SlideOp) < get_send_data_retries()) of
                    true ->
                        NewSlideOp = slide_op:inc_timeouts(SlideOp),
                        send_data(State, PredOrSucc, NewSlideOp);
                    _ ->
                        abort_slide(State, SlideOp, PredOrSucc,
                                    send_data_timeout, true)
                end
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_data_ack, both, send_data_timeout);

% data from a neighbor
process_move_msg({move, data, MovingData, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, PredOrSucc, State) ->
                SlideOp1 = slide_op:reset_timer(SlideOp), % reset previous timeouts
                accept_data(State, PredOrSucc, SlideOp1, MovingData)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_data, both, data);

% request for data from a neighbor
process_move_msg({move, data_ack_timeout, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, PredOrSucc, State) ->
                case (slide_op:get_timeouts(SlideOp) < get_data_ack_retries()) of
                    true ->
                        NewSlideOp = slide_op:inc_timeouts(SlideOp),
                        send_data_ack(State, PredOrSucc, NewSlideOp);
                    _ ->
                        abort_slide(State, SlideOp, PredOrSucc,
                                    data_ack_timeout, true)
                end
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_delta, both, data_ack_timeout);

% acknowledgement from neighbor that its node received data for the slide op with the given id
process_move_msg({move, data_ack, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, PredOrSucc, State) ->
                SlideOp1 = slide_op:reset_timer(SlideOp), % reset previous timeouts
                send_delta(State, PredOrSucc, SlideOp1)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_data_ack, both, data_ack);

% request for data from a neighbor
process_move_msg({move, send_delta_timeout, MoveFullId, ChangedData, DeletedKeys}, MyState) ->
    WorkerFun =
        fun(SlideOp, PredOrSucc, State) ->
                case (slide_op:get_timeouts(SlideOp) < get_send_delta_retries()) of
                    true ->
                        NewSlideOp = slide_op:inc_timeouts(SlideOp),
                        send_delta2(State, PredOrSucc, NewSlideOp, ChangedData, DeletedKeys);
                    _ ->
                        % abort slide but no need to tell the other node here
                        abort_slide(State, SlideOp, PredOrSucc,
                                    data_ack_timeout, false)
                end
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_delta_ack, both, send_delta_timeout);

% delta from neighbor
process_move_msg({move, delta, ChangedData, DeletedKeys, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, PredOrSucc, State) ->
                SlideOp1 = slide_op:reset_timer(SlideOp), % reset previous timeouts
                accept_delta(State, PredOrSucc, SlideOp1, ChangedData, DeletedKeys)
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_delta, both, delta);

% acknowledgement from neighbor that its node received delta for the slide op with the given id
process_move_msg({move, delta_ack, MoveFullId}, MyState) ->
    WorkerFun =
        fun(SlideOp, PredOrSucc, State) ->
                SlideOp1 = slide_op:reset_timer(SlideOp), % reset previous timeouts
                notify_source_pid(slide_op:get_source_pid(SlideOp1),
                                  {move, result, slide_op:get_tag(SlideOp1), ok}),
                case slide_op:is_leave(SlideOp1) of
                    true ->
                        SupDhtNodeId = erlang:get(my_sup_dht_node_id),
                        ok = supervisor:terminate_child(main_sup, SupDhtNodeId),
                        ok = supervisor:delete_child(main_sup, SupDhtNodeId),
                        kill;
                    _    ->
                        dht_node_state:set_slide(State, PredOrSucc, null)
                end
        end,
    safe_operation(WorkerFun, MyState, MoveFullId, wait_for_delta_ack, both, delta_ack).

% misc.

%% @doc Notifies the successor or predecessor about a (new) slide operation and
%%      sets a timeout timer if the information in the slide operation is still
%%      correct, i.e. its target node is still the node's successor. No timer
%%      should have been set on NewSlideOp. Will also set the appropriate
%%      slide_phase and update the slide op in the dht_node.
-spec notify_other_slide(PredOrSucc::pred | succ, NewSlideOp::slide_op:slide_op(), State::dht_node_state:state()) -> dht_node_state:state().
notify_other_slide(succ, NewSlideOp, State) ->
    OtherNode = dht_node_state:get(State, succ),
    SlOpNodePid = slide_op:get_node(NewSlideOp),
    case node:same_process(OtherNode, SlOpNodePid) of
        true ->
            Timeout_ms = get_notify_succ_timeout(),
            Timeout_msgtag = notify_succ_timeout,
            Notify_msgtag = slide_pred,
            NewPhase = wait_for_succ_ack,
            notify_other_slide2(succ, NewSlideOp, State, OtherNode,
                                Timeout_ms, Timeout_msgtag, Notify_msgtag,
                                NewPhase);
        _ ->
            dht_node_state:set_slide(State, succ, null)
    end;
notify_other_slide(pred, NewSlideOp, State) ->
    OtherNode = dht_node_state:get(State, pred),
    SlOpNodePid = slide_op:get_node(NewSlideOp),
    case node:same_process(OtherNode, SlOpNodePid) of
        true ->
            Timeout_ms = get_notify_pred_timeout(),
            Timeout_msgtag = notify_pred_timeout,
            Notify_msgtag = slide_succ,
            NewPhase =
                case slide_op:get_type(NewSlideOp) of % note: type = my slide_op's type
                    'rcv'  -> wait_for_data;
                    'send' -> wait_for_req_data
                end,
            notify_other_slide2(pred, NewSlideOp, State, OtherNode,
                                Timeout_ms, Timeout_msgtag, Notify_msgtag,
                                NewPhase);
        _ ->
            dht_node_state:set_slide(State, pred, null)
    end.

%% @doc Helper for notify_other_slide/4. Sets the timer, sends the message and
%%      finally sets the new slide op in the dht_node state.
-spec notify_other_slide2
        (succ, NewSlideOp::slide_op:slide_op(),
         State::dht_node_state:state(), OtherNode::node:node_type(),
         Timeout_ms::pos_integer(), Timeout_msgtag::notify_succ_timeout,
         Notify_msgtag::slide_pred, NewPhase::wait_for_succ_ack)
            -> dht_node_state:state();
        (pred, NewSlideOp::slide_op:slide_op(),
         State::dht_node_state:state(), OtherNode::node:node_type(),
         Timeout_ms::pos_integer(), Timeout_msgtag::notify_pred_timeout,
         Notify_msgtag::slide_succ, NewPhase::wait_for_req_data | wait_for_data)
            -> dht_node_state:state().
notify_other_slide2(PredOrSucc, NewSlideOp, State, OtherNode,
                    Timeout_ms, Timeout_msgtag, Notify_msgtag, NewPhase) ->
    SlOpNodePid = node:pidX(OtherNode),
    SlideOp = slide_op:set_timer(NewSlideOp, Timeout_ms,
                                {move, Timeout_msgtag, slide_op:get_id(NewSlideOp)}),
    OtherSlideType = case slide_op:get_type(SlideOp) of
                         'send' -> 'rcv';
                         'rcv'  -> 'send'
                     end,
    comm:send(SlOpNodePid, {move, Notify_msgtag, OtherSlideType,
                            slide_op:get_id(SlideOp),
                            dht_node_state:get(State, node),
                            OtherNode, slide_op:get_target_id(SlideOp),
                            slide_op:get_tag(SlideOp),
                            get_max_transport_bytes()}),
    dht_node_state:set_slide(State, PredOrSucc,
                             slide_op:set_phase(SlideOp, NewPhase)).

%% @doc Sets up a new slide operation with the node's successor or predecessor
%%      after a request for a slide has been received.
-spec setup_slide_with(PredOrSucc::pred | succ, State::dht_node_state:state(),
                       SendOrReceive::'send' | 'rcv', MoveFullId::slide_op:id(),
                       MyNode::node:node_type(), TargetNode::node:node_type(),
                       TargetId::?RT:key(), Tag::any(),
                       MaxTransportBytes::pos_integer(),
                       SourcePid::comm:erl_local_pid() | null, First::boolean())
        -> dht_node_state:state().
setup_slide_with(PredOrSucc, State, SendOrReceive, MoveFullId, MyNode,
                 TargetNode, TargetId, Tag, MaxTransportBytes, SourcePid, First) ->
    case get_slide_op(State, MoveFullId) of
        {ok, pred, SlideOp} ->
            % there should not be any previous slide_pred with the given ID!
            log:log(warn,"[ dht_node_move ~.0p ] slide_~.0p received but found previous slide_pred with the same ID (ID: ~.0p, slide_pred: ~.0p)~n", [comm:this(), PredOrSucc, MoveFullId, dht_node_state:get(State, slide_pred)]),
            abort_slide(State, SlideOp, pred, changed_parameters, true);
        {ok, succ, SlideOp} ->
            % the predecessor can already have a slide operation if it
            % initiated the slide and contacted the successor first
            % -> continue now
            % (for now, abort the slide if the parameters are not the same)
            Phase = slide_op:get_phase(SlideOp),
            case SendOrReceive =:= slide_op:get_type(SlideOp) andalso
                     node:same_process(TargetNode, slide_op:get_node(SlideOp)) andalso
                     Tag =:= slide_op:get_tag(SlideOp) andalso
                     TargetId =:= slide_op:get_target_id(SlideOp) andalso
                     First =:= false of
                true when Phase =:= wait_for_succ_ack ->
                    State1 =
                        case SendOrReceive of
                            'rcv'  -> dht_node_state:add_msg_fwd(
                                        State, slide_op:get_interval(SlideOp),
                                        slide_op:get_node(SlideOp));
                            'send' -> dht_node_state:add_db_range(
                                       State, slide_op:get_interval(SlideOp))
                        end,
                    change_my_id(State1, slide_op:reset_timer(SlideOp), TargetId);
                true -> State; % ignore already processed message
                _    -> abort_slide(State, SlideOp, succ, changed_parameters, true)
            end;
        not_found ->
            setup_slide_with2_not_found(
              PredOrSucc, State, SendOrReceive, MoveFullId, MyNode,
              TargetNode, TargetId, Tag, MaxTransportBytes, SourcePid, First);
        {wrong_neighbor, Type, SlideOp} -> % wrong pred or succ
            abort_slide(State, SlideOp, Type, wrong_pred_succ_node, true)
    end.
    
%% @doc Sets up a new slide operation with the node's successor or predecessor
%%      after a request for a slide has been received and setup_slide_with/9
%%      checked that the slide to create did not exist yet.
-spec setup_slide_with2_not_found(PredOrSucc::pred | succ, State::dht_node_state:state(),
                       SendOrReceive::'send' | 'rcv', MoveFullId::slide_op:id(),
                       MyNode::node:node_type(), TargetNode::node:node_type(),
                       TargetId::?RT:key(), Tag::any(),
                       MaxTransportBytes::pos_integer(),
                       SourcePid::comm:erl_local_pid() | null, First::boolean())
        -> dht_node_state:state().
setup_slide_with2_not_found(PredOrSucc, State, SendOrReceive, MoveFullId,
                            MyNode, TargetNode, TargetId, Tag,
                            MaxTransportBytes, SourcePid, First) ->
    CanSlide = case PredOrSucc of
                   pred -> can_slide_pred(State, TargetId, Tag);
                   succ -> can_slide_succ(State, TargetId, Tag)
               end,
    % correct pred/succ info? did pred/succ know our current ID (compare node info)
    NodesCorrect = MyNode =:= dht_node_state:get(State, node) andalso
                       TargetNode =:= dht_node_state:get(State, PredOrSucc),
    MoveDone = (PredOrSucc =:= pred andalso node:id(TargetNode) =:= TargetId) orelse
               (PredOrSucc =:= succ andalso node:id(MyNode) =:= TargetId),
    case CanSlide andalso NodesCorrect andalso not MoveDone of
        true ->
            TargetIdInRange = dht_node_state:is_responsible(TargetId, State),
            case SendOrReceive of
                'send' when PredOrSucc =:= succ andalso Tag =:= '$leave$' andalso First =:= true ->
                    % graceful leave (slide with succ, send all data)
                    SlideOp = slide_op:new_sending_slide_leave(
                                MoveFullId, State),
                    notify_other_slide(succ, SlideOp, State);
                'send' when TargetIdInRange andalso PredOrSucc =:= pred ->
                    % slide with pred, send data
                    SlideOp = slide_op:new_sending_slide(
                                MoveFullId, pred, TargetId, Tag,
                                SourcePid, State),
                    State1 = dht_node_state:add_db_range(
                               State, slide_op:get_interval(SlideOp)),
                    notify_other_slide(pred, SlideOp, State1);
                'send' when TargetIdInRange andalso PredOrSucc =:= succ ->
                    % slide with succ, send data
                    SlideOp = slide_op:new_sending_slide(
                                MoveFullId, succ, TargetId, Tag,
                                SourcePid, State),
                    case First of
                        true -> notify_other_slide(succ, SlideOp, State);
                        _    ->
                            State1 = dht_node_state:add_db_range(
                                       State, slide_op:get_interval(SlideOp)),
                            change_my_id(State1, SlideOp, TargetId)
                    end;
                'send' -> % can not send if TargetId is not in my range!
                    abort_slide(State, node:pidX(TargetNode), MoveFullId, null, SourcePid, Tag,
                                PredOrSucc, target_id_not_in_range, not First);
                'rcv' when PredOrSucc =:= pred ->
                    % slide with pred, receive data
                    SlideOp = slide_op:new_receiving_slide(
                                MoveFullId, pred, TargetId, Tag,
                                SourcePid, State),
                    State1 = dht_node_state:add_msg_fwd(
                               State, slide_op:get_interval(SlideOp),
                               slide_op:get_node(SlideOp)),
                    notify_other_slide(pred, SlideOp, State1);
                'rcv' when PredOrSucc =:= succ ->
                    % slide with succ, receive data
                    SlideOp = slide_op:new_receiving_slide(
                                MoveFullId, succ, TargetId, Tag,
                                SourcePid, State),
                    case First of
                        true -> notify_other_slide(succ, SlideOp, State);
                        _    ->
                            State1 = dht_node_state:add_msg_fwd(
                                       State, slide_op:get_interval(SlideOp),
                                       slide_op:get_node(SlideOp)),
                            change_my_id(State1, SlideOp, TargetId)
                    end
            end;
        _ when not CanSlide ->
            abort_slide(State, node:pidX(TargetNode), MoveFullId, null, SourcePid, Tag,
                        PredOrSucc, ongoing_slide, not First);
        _ when not NodesCorrect ->
            abort_slide(State, node:pidX(TargetNode), MoveFullId, null, SourcePid, Tag,
                        PredOrSucc, wrong_pred_succ_node, not First);
        _ -> % MoveDone, i.e. target id already reached (noop)
            notify_source_pid(SourcePid, {move, result, Tag, ok}),
            State
    end.

%% @doc Change the local node's ID to the given TargetId by calling the ring
%%      maintenance and changing the slide operation's phase to
%%      wait_for_node_update. 
-spec change_my_id(State::dht_node_state:state(), NewSlideOp::slide_op:slide_op(), TargetId::?RT:key()) -> dht_node_state:state().
change_my_id(State, NewSlideOp, TargetId) ->
    % TODO: implement step-wise slide/leave
    case slide_op:is_leave(NewSlideOp) of
        true ->
            rm_loop:leave(),
            % de-activate processes not needed anymore:
            cyclon:deactivate(),
            % note: do not deactivate gossip or vivaldi - their values are still valid and still count!
%%             gossip:deactivate(),
%%             vivaldi:deactivate(),
            dht_node_state:set_slide(
              State, succ, slide_op:set_phase(NewSlideOp, wait_for_node_update));
        _ ->
            rm_loop:subscribe(self(), fun rm_loop:subscribe_node_change_filter/2, fun send_node_change/3),
            rm_loop:update_id(TargetId),
            dht_node_state:set_slide(
              State, succ, slide_op:set_phase(NewSlideOp, wait_for_node_update))
    end.

%% @doc Requests data from the node of the given slide operation, sets a
%%      rcv_data_timeout timeout, sets the slide operation's phase to
%%      wait_for_data and sets the slide operation in the dht node's state.
-spec request_data(State::dht_node_state:state(), PredOrSucc::succ, SlideOp::slide_op:slide_op()) -> dht_node_state:state().
request_data(State, PredOrSucc = succ, SlideOp) ->
    SlOp1 = slide_op:set_timer(SlideOp, get_rcv_data_timeout(),
                              {move, rcv_data_timeout, slide_op:get_id(SlideOp)}),
    NewSlideOp = slide_op:set_phase(SlOp1, wait_for_data),
    comm:send(slide_op:get_node(NewSlideOp),
              {move, req_data, slide_op:get_id(NewSlideOp)}),
    dht_node_state:set_slide(State, PredOrSucc, NewSlideOp).

%% @doc Gets all data in the slide operation's interval from the DB and sends
%%      it to the target node. Also sets the DB to record changes in this
%%      interval, changes the slide operation's phase to wait_for_data_ack and
%%      sets a send_data_timeout.
-spec send_data(State::dht_node_state:state(), PredOrSucc::pred | succ, SlideOp::slide_op:slide_op()) -> dht_node_state:state().
send_data(State, PredOrSucc, SlideOp) ->
    MovingInterval = slide_op:get_interval(SlideOp),
    OldDB = dht_node_state:get(State, db),
    MovingData = ?DB:get_entries(OldDB, MovingInterval),
    NewDB = ?DB:record_changes(OldDB, MovingInterval),
    SlOp2 = slide_op:set_timer(SlideOp, get_send_data_timeout(),
                              {move, send_data_timeout, slide_op:get_id(SlideOp)}),
    comm:send(slide_op:get_node(SlOp2),
              {move, data, MovingData, slide_op:get_id(SlOp2)}),
    NewSlideOp = slide_op:set_phase(SlOp2, wait_for_data_ack),
    State_NewDB = dht_node_state:set_db(State, NewDB),
    dht_node_state:set_slide(State_NewDB, PredOrSucc, NewSlideOp).

%% @doc Accepts data received during the given (existing!) slide operation and
%%      writes it to the DB. Then calls send_data_ack/3.
%% @see send_data_ack/3
-spec accept_data(State::dht_node_state:state(), PredOrSucc::pred | succ,
                  SlideOp::slide_op:slide_op(), Data::?DB:db_as_list()) -> dht_node_state:state().
accept_data(State, PredOrSucc, SlideOp, Data) ->
    NewDB = ?DB:add_data(dht_node_state:get(State, db), Data),
    State1 = dht_node_state:set_db(State, NewDB),
    send_data_ack(State1, PredOrSucc, SlideOp).

%% @doc Sets a data_ack message for the given slide operation, sets its phase
%%      to wait_for_delta and sets a data_ack_timeout timeout.
-spec send_data_ack(State::dht_node_state:state(), PredOrSucc::pred | succ, SlideOp::slide_op:slide_op()) -> dht_node_state:state().
send_data_ack(State, PredOrSucc, SlideOp) ->
    SlOp1 = slide_op:set_timer(SlideOp, get_data_ack_timeout(),
                              {move, data_ack_timeout, slide_op:get_id(SlideOp)}),
    NewSlideOp = slide_op:set_phase(SlOp1, wait_for_delta),
    comm:send(slide_op:get_node(NewSlideOp),
              {move, data_ack, slide_op:get_id(NewSlideOp)}),
    dht_node_state:set_slide(State, PredOrSucc, NewSlideOp).

%% @doc Gets changed data in the slide operation's interval from the DB and
%%      sends as a delta to the target node. Also sets the DB to stop recording
%%      changes in this interval and delete any such entries. Changes the slide
%%      operation's phase to wait_for_delta_ack and calls send_delta2/5.
%% @see send_delta2/5
-spec send_delta(State::dht_node_state:state(), PredOrSucc::pred | succ, SlideOp::slide_op:slide_op()) -> dht_node_state:state().
send_delta(State, PredOrSucc, SlideOp) ->
    SlideOpInterval = slide_op:get_interval(SlideOp),
    % send delta (values of keys that have changed during the move)
    OldDB = dht_node_state:get(State, db),
    {ChangedData, DeletedKeys} = ?DB:get_changes(OldDB, SlideOpInterval),
    NewDB1 = ?DB:stop_record_changes(OldDB, SlideOpInterval),
    NewDB = ?DB:delete_entries(NewDB1, SlideOpInterval),
    State1 = dht_node_state:set_db(State, NewDB),
    State2 = dht_node_state:rm_db_range(State1, SlideOpInterval),
    SlOp1 = slide_op:set_phase(SlideOp, wait_for_delta_ack),
    send_delta2(State2, PredOrSucc, SlOp1, ChangedData, DeletedKeys).

%% @doc Sets a delta message with the given data and sets a send_delta_timeout
%%      timeout.
-spec send_delta2(State::dht_node_state:state(), PredOrSucc::pred | succ, SlideOp::slide_op:slide_op(), ChangedData::?DB:db_as_list(), DeletedKeys::[?RT:key()]) -> dht_node_state:state().
send_delta2(State, PredOrSucc, SlideOp, ChangedData, DeletedKeys) ->
    SlOp1 = slide_op:set_timer(SlideOp, get_send_delta_timeout(),
                              {move, send_delta_timeout, slide_op:get_id(SlideOp),
                               ChangedData, DeletedKeys}),
    comm:send(slide_op:get_node(SlOp1),
              {move, delta, ChangedData, DeletedKeys, slide_op:get_id(SlOp1)}),
    dht_node_state:set_slide(State, PredOrSucc, SlOp1).

%% @doc Accepts delta received during the given (existing!) slide operation and
%%      writes it to the DB. Then removes the dht_node's message forward for 
%%      the slide operation's interval, sends a delta_ack message, notifies
%%      the source pid (if it exists) and removes the slide operation from the
%%      dht_node_state.
-spec accept_delta(State::dht_node_state:state(), PredOrSucc::pred | succ,
                   SlideOp::slide_op:slide_op(), ChangedData::?DB:db_as_list(),
                   DeletedKeys::[?RT:key()]) -> dht_node_state:state().
accept_delta(State, PredOrSucc, SlideOp, ChangedData, DeletedKeys) ->
    NewDB1 = ?DB:add_data(dht_node_state:get(State, db), ChangedData),
    NewDB2 = ?DB:delete_entries(NewDB1, intervals:from_elements(DeletedKeys)),
    State1 = dht_node_state:set_db(State, NewDB2),
    State2 = dht_node_state:rm_msg_fwd(
               State1, slide_op:get_interval(SlideOp)),
    comm:send(slide_op:get_node(SlideOp),
              {move, delta_ack, slide_op:get_id(SlideOp)}),
    notify_source_pid(slide_op:get_source_pid(SlideOp),
                      {move, result, slide_op:get_tag(SlideOp), ok}),
    dht_node_state:set_slide(State2, PredOrSucc, null).

%% @doc Checks if a slide operation with the given MoveFullId exists and
%%      executes WorkerFun if everything is ok. If the successor/predecessor
%%      information in the slide operation is incorrect, the slide is aborted
%%      (a message to the pred/succ is send, too).
-spec safe_operation(
    WorkerFun::fun((SlideOp::slide_op:slide_op(), PredOrSucc::pred | succ,
                    State::dht_node_state:state()) -> dht_node_state:state()),
    State::dht_node_state:state(), MoveFullId::slide_op:id(),
    WorkPhases::slide_op:slide_phase() | [slide_op:slide_phase(),...],
    PredOrSuccExp::pred | succ | both, MoveMsgTag::atom())
        -> dht_node_state:state().
safe_operation(WorkerFun, State, MoveFullId, WorkPhase, PredOrSuccExp, MoveMsgTag) when not is_list(WorkPhase) ->
    safe_operation(WorkerFun, State, MoveFullId, [WorkPhase], PredOrSuccExp, MoveMsgTag);
safe_operation(WorkerFun, State, MoveFullId, WorkPhases, PredOrSuccExp, MoveMsgTag) ->
    case get_slide_op(State, MoveFullId) of
        {ok, PredOrSucc, SlideOp} ->
            case PredOrSuccExp =:= both orelse PredOrSucc =:= PredOrSuccExp of
                true ->
                    case lists:member(slide_op:get_phase(SlideOp), WorkPhases) of
                        true -> WorkerFun(SlideOp, PredOrSucc, State);
                        _    -> State
                    end;
                _ ->
                    % abort slide but do not notify the other node
                    % (this message should not have been received anyway!)
                    ErrorMsg = io_lib:format("~.0p received for a slide with ~s, but only expected for slides with~s~n",
                                             [MoveMsgTag, PredOrSucc, PredOrSuccExp]),
                    NewState = abort_slide(State, SlideOp, PredOrSucc, {protocol_error, ErrorMsg}, false),
                    log:log(warn, "[ dht_node_move ~.0p ] ~.0p received for a slide with my ~s, but only expected for slides with~s~n"
                                      "(operation: ~.0p)~n", [comm:this(), MoveMsgTag, PredOrSucc, PredOrSuccExp, SlideOp]),
                    NewState
            end;
        not_found ->
            log:log(warn,"[ dht_node_move ~.0p ] ~.0p received with no matching slide operation (ID: ~.0p, slide_pred: ~.0p, slide_succ: ~.0p)~n",
                    [comm:this(), MoveMsgTag, MoveFullId, dht_node_state:get(State, slide_pred), dht_node_state:get(State, slide_succ)]),
            State;
        {wrong_neighbor, Type, SlideOp} -> % wrong pred or succ
            case lists:member(slide_op:get_phase(SlideOp), WorkPhases) of
                true ->
                    NewState = abort_slide(State, SlideOp, Type, wrong_pred_succ_node, true),
                    log:log(warn,"[ dht_node_move ~.0p ] ~.0p received but ~s changed during move (ID: ~.0p, node(slide): ~.0p, new_~s: ~.0p)~n",
                            [comm:this(), MoveMsgTag, Type, MoveFullId, slide_op:get_node(SlideOp), Type, dht_node_state:get(State, Type)]),
                    NewState;
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
-spec get_slide_op(State::dht_node_state:state(), MoveFullId::slide_op:id()) ->
        {Result::ok, Type::pred | succ, SlideOp::slide_op:slide_op()} |
        {Result::wrong_neighbor, Type::pred | succ, SlideOp::slide_op:slide_op()} |
        not_found.
get_slide_op(State, MoveFullId) ->
    case dht_node_state:get_slide_op(State, MoveFullId) of
        not_found -> not_found;
        {PredOrSucc, SlideOp} ->
            Node = dht_node_state:get(State, PredOrSucc),
            case node:same_process(Node, slide_op:get_node(SlideOp)) orelse
                     (slide_op:is_leave(SlideOp) andalso PredOrSucc =:= pred) of
                true -> {ok,             PredOrSucc, SlideOp};
                _    -> {wrong_neighbor, PredOrSucc, SlideOp}
            end
    end.

%% @doc Returns whether a slide with the successor is possible for the given
%%      target id.
%% @see can_slide/3
-spec can_slide_succ(State::dht_node_state:state(), TargetId::?RT:key(), Tag::any()) -> boolean().
can_slide_succ(State, TargetId, Tag) ->
    SlidePred = dht_node_state:get(State, slide_pred),
    dht_node_state:get(State, slide_succ) =:= null andalso
        (SlidePred =:= null orelse
             (not intervals:in(TargetId, slide_op:get_interval(SlidePred)) andalso
                  not (Tag =:= '$leave$' andalso slide_op:is_leave(SlidePred)))
        ).

%% @doc Returns whether a slide with the predecessor is possible for the given
%%      target id.
%% @see can_slide/3
-spec can_slide_pred(State::dht_node_state:state(), TargetId::?RT:key(), Tag::any()) -> boolean().
can_slide_pred(State, TargetId, _Tag) ->
    SlideSucc = dht_node_state:get(State, slide_succ),
    dht_node_state:get(State, slide_pred) =:= null andalso
        (SlideSucc =:= null orelse
             (not intervals:in(TargetId, slide_op:get_interval(SlideSucc)) andalso
                  not slide_op:is_leave(SlideSucc))
        ).

%% @doc Sends the source pid the given message if it is not 'null'.
-spec notify_source_pid(SourcePid::comm:erl_local_pid() | null, Message::result_message()) -> ok.
notify_source_pid(SourcePid, Message) ->
    case comm:is_valid(SourcePid) of
        true -> comm:send_local(SourcePid, Message);
        _    -> ok
    end.

%% @doc Aborts the given slide operation. Pred_or_Succ determines whether the
%%      SlideOp is a slide with the predecessor or successor of our node. We
%%      can thus notify the proper node of the abort.
%% @see abort_slide/8
-spec abort_slide(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                  Pred_or_Succ::pred | succ, Reason::abort_reason(),
                  NotifyNode::boolean()) -> dht_node_state:state().
abort_slide(State, SlideOp, Pred_or_Succ, Reason, NotifyNode) ->
    % write to log when aborting an already set-up slide:
    log:log(warn, "abort_slide(op: ~.0p, ~.0p, reason: ~.0p)~n",
            [SlideOp, Pred_or_Succ, Reason]),
    slide_op:reset_timer(SlideOp), % reset previous timeouts
    % potentially set up for joining nodes (slide with pred):
    case Pred_or_Succ =:= pred andalso slide_op:is_join(SlideOp) of
        true -> rm_loop:unsubscribe(self(),
                                    fun dht_node_move:rm_pred_changed/2,
                                    fun dht_node_move:rm_notify_new_pred/3);
        _ -> ok
    end,
    State1 = dht_node_state:rm_db_range(State, slide_op:get_interval(SlideOp)),
    State2 = dht_node_state:rm_msg_fwd(State1, slide_op:get_interval(SlideOp)),
    abort_slide(State2, slide_op:get_node(SlideOp), slide_op:get_id(SlideOp),
                slide_op:get_phase(SlideOp),
                slide_op:get_source_pid(SlideOp), slide_op:get_tag(SlideOp),
                Pred_or_Succ, Reason, NotifyNode).

%% @doc Like abort_slide/5 but does not need a slide operation in order to
%%      work. Note: prefer using abort_slide/5 when a slide operation is
%%      available as this also resets all its timers!
-spec abort_slide(State::dht_node_state:state(), Node::comm:mypid(),
                  SlideOpId::slide_op:id(), Phase::slide_op:slide_phase(),
                  SourcePid::comm:erl_local_pid() | null,
                  Tag::any(), Pred_or_Succ::pred | succ, Reason::abort_reason(),
                  NotifyNode::boolean()) -> dht_node_state:state().
abort_slide(State, Node, SlideOpId, Phase, SourcePid, Tag, Pred_or_Succ, Reason, NotifyNode) ->
    % abort slide on the (other) node:
    case NotifyNode of
        true when Pred_or_Succ =:= pred ->
            comm:send(Node, {move, slide_succ_abort, SlideOpId, Reason});
        true when Pred_or_Succ =:= succ ->
            comm:send(Node, {move, slide_pred_abort, SlideOpId, Reason});
        _ -> ok
    end,
    notify_source_pid(SourcePid, {move, result, Tag, Reason}),
    % set a 'null' slide_op if there was an old one with the given ID
    State2 =
        case dht_node_state:get_slide_op(State, SlideOpId) of
            not_found -> State;
            {Pred_or_Succ, _} ->
                State1 = dht_node_state:set_slide(State, Pred_or_Succ, null),
                dht_node_state:set_db(State1, ?DB:stop_record_changes(
                                        dht_node_state:get(State1, db)))
        end,
    % re-start a leaving slide on the leaving node if it hasn't left the ring yet:
    case Tag =:= '$leave$' andalso Pred_or_Succ =:= succ andalso
             lists:member(Phase, [null, wait_for_succ_ack]) of
        true -> make_slide_leave(State2);
        _    -> State2
    end.

%% @doc Creates a slide that will move all data to the successor and leave the
%%      ring. Note: Will re-try (forever) to successfully start a leaving slide
%%      if anything causes an abort!
-spec make_slide_leave(State::dht_node_state:state()) -> dht_node_state:state().
make_slide_leave(State) ->
    % TODO: check for running slide, abort that if possible, eventually extend it
    MoveFullId = util:get_global_uid(),
    InitNode = dht_node_state:get(State, node),
    OtherNode = dht_node_state:get(State, succ),
    log:log(info, "[ Node ~.0p ] starting leave (succ: ~.0p)~n", [InitNode, OtherNode]),
    setup_slide_with(succ, State, 'send', MoveFullId, InitNode,
                     OtherNode, node:id(OtherNode), '$leave$',
                     get_max_transport_bytes(), null, true).

%% @doc Send a  node change update message to this module inside the dht_node.
%%      Will be registered with the dht_node as a node change subscriber.
%% @see dht_node_state:add_nc_subscr/3
-spec send_node_change(Pid::comm:erl_local_pid(),
                       OldNeighbors::nodelist:neighborhood(),
                       NewNeighbors::nodelist:neighborhood()) -> ok.
send_node_change(Pid, _OldNeighbors, NewNeighbors) ->
    NewNode = nodelist:node(NewNeighbors),
    comm:send_local(Pid, {move, node_update, NewNode}).

%% @doc Checks whether the predecessor changed. Used in the rm-subscription
%%      during a node join - see dht_node_join.erl.
-spec rm_pred_changed(OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood()) -> boolean().
rm_pred_changed(OldNeighbors, NewNeighbors) ->
    nodelist:pred(OldNeighbors) =/= nodelist:pred(NewNeighbors).

%% @doc Sends a rm_new_pred message to the dht_node_move module when a new
%%      predecessor appears at the rm-process. Used in the rm-subscription
%%      during a node join - see dht_node_join.erl.
-spec rm_notify_new_pred(Pid::comm:erl_local_pid(),
        OldNeighbors::nodelist:neighborhood(), NewNeighbors::nodelist:neighborhood()) -> ok.
rm_notify_new_pred(Pid, _OldNeighbors, NewNeighbors) ->
    comm:send_local(Pid, {move, rm_new_pred, nodelist:pred(NewNeighbors)}).

%% @doc Checks whether config parameters regarding dht_node moves exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(move_max_transport_bytes) and
    config:is_greater_than(move_max_transport_bytes, 0) and

    config:is_integer(move_notify_succ_timeout) and
    config:is_greater_than(move_notify_succ_timeout, 0) and

    config:is_integer(move_notify_succ_retries) and
    config:is_greater_than(move_notify_succ_retries, 0) and

    config:is_integer(move_notify_pred_timeout) and
    config:is_greater_than(move_notify_pred_timeout, 0) and

    config:is_integer(move_notify_pred_retries) and
    config:is_greater_than(move_notify_pred_retries, 0) and

    config:is_integer(move_send_data_timeout) and
    config:is_greater_than(move_send_data_timeout, 0) and

    config:is_integer(move_send_data_retries) and
    config:is_greater_than(move_send_data_retries, 0) and

    config:is_integer(move_send_delta_timeout) and
    config:is_greater_than(move_send_delta_timeout, 0) and

    config:is_integer(move_send_delta_retries) and
    config:is_greater_than(move_send_delta_retries, 0) and

    config:is_integer(move_rcv_data_timeout) and
    config:is_greater_than(move_rcv_data_timeout, 0) and

    config:is_integer(move_rcv_data_retries) and
    config:is_greater_than(move_rcv_data_retries, 0) and

    config:is_integer(move_data_ack_timeout) and
    config:is_greater_than(move_data_ack_timeout, 0) and

    config:is_integer(move_data_ack_retries) and
    config:is_greater_than(move_data_ack_retries, 0).
    
%% @doc Gets the max number of bytes per data move operation (set in the config
%%      files).
-spec get_max_transport_bytes() -> pos_integer().
get_max_transport_bytes() ->
    config:read(move_max_transport_bytes).

%% @doc Gets the max number of ms to wait for the successor's reply when
%%      initiating a move (set in the config files).
-spec get_notify_succ_timeout() -> pos_integer().
get_notify_succ_timeout() ->
    config:read(move_notify_succ_timeout).

%% @doc Gets the max number of retries to notify the successor when
%%      initiating a move (set in the config files).
-spec get_notify_succ_retries() -> pos_integer().
get_notify_succ_retries() ->
    config:read(move_notify_succ_retries).

%% @doc Gets the max number of ms to wait for the predecessor's reply when
%%      initiating or acknowledging a slide op (set in the config files).
-spec get_notify_pred_timeout() -> pos_integer().
get_notify_pred_timeout() ->
    config:read(move_notify_pred_timeout).

%% @doc Gets the max number of retries to notify the predecessor when
%%      initiating or acknowledging a slide op (set in the config files).
-spec get_notify_pred_retries() -> pos_integer().
get_notify_pred_retries() ->
    config:read(move_notify_pred_retries).

%% @doc Gets the max number of ms to wait for the succ/pred's reply after
%%      sending data to it (set in the config files).
-spec get_send_data_timeout() -> pos_integer().
get_send_data_timeout() ->
    config:read(move_send_data_timeout).

%% @doc Gets the max number of retries to send data to the succ/pred
%%      (set in the config files).
-spec get_send_data_retries() -> pos_integer().
get_send_data_retries() ->
    config:read(move_send_data_retries).

%% @doc Gets the max number of ms to wait for the succ/pred's reply after
%%      sending delta data to it (set in the config files).
-spec get_send_delta_timeout() -> pos_integer().
get_send_delta_timeout() ->
    config:read(move_send_delta_timeout).

%% @doc Gets the max number of retries to send delta to the succ/pred
%%      (set in the config files).
-spec get_send_delta_retries() -> pos_integer().
get_send_delta_retries() ->
    config:read(move_send_delta_retries).

%% @doc Gets the max number of ms to wait for the succ/pred's reply after
%%      requesting data from it (set in the config files).
-spec get_rcv_data_timeout() -> pos_integer().
get_rcv_data_timeout() ->
    config:read(move_rcv_data_timeout).

%% @doc Gets the max number of retries to send a data request to the succ/pred
%%      (set in the config files).
-spec get_rcv_data_retries() -> pos_integer().
get_rcv_data_retries() ->
    config:read(move_rcv_data_retries).

%% @doc Gets the max number of ms to wait for the succ/pred's reply after
%%      sending data_ack to it (set in the config files).
-spec get_data_ack_timeout() -> pos_integer().
get_data_ack_timeout() ->
    config:read(move_data_ack_timeout).

%% @doc Gets the max number of retries to send a data_ack to the succ/pred
%%      (set in the config files).
-spec get_data_ack_retries() -> pos_integer().
get_data_ack_retries() ->
    config:read(move_data_ack_retries).
