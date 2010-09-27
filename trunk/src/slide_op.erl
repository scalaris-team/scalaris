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
%% @doc    Slide operation structure for node moves, joins and leaves.
%% @end
%% @version $Id$
-module(slide_op).
-author('kruber@zib.de').
-vsn('$Id$').

-export([new_receiving_slide/7, new_sending_slide/7,
         new_receiving_slide_join/4,
         get_id/1, get_node/1, get_interval/1, get_target_id/1,
         get_source_pid/1, get_tag/1, get_type/1,
         get_timer/1, set_timer/3, reset_timer/1,
         get_timeouts/1, inc_timeouts/1, reset_timeouts/1,
         get_phase/1, set_phase/2]).

-include("scalaris.hrl").
-include("record_helpers.hrl").

-ifdef(with_export_type_support).
-export_type([slide_op/0, id/0, slide_phase/0]).
-endif.

-type id() :: {LocalUUID::pos_integer(), ProcessPid::comm:mypid()}.

-type slide_phase() ::
        null | % should only occur as an intermediate state, otherwise equal to "no slide op"
        wait_for_succ_ack | % pred initiated a slide and has notified its succ
        wait_for_node_update | % pred changing its id
        wait_for_pred_update | % wait for the local rm process to know about a joining node (pred)
        wait_for_req_data | wait_for_data_ack | wait_for_delta_ack | % sending node
        wait_for_data | wait_for_delta. % receiving node

-record(slide_op, {type       = ?required(slide_op, type)      :: 'send' | 'rcv',
                   id         = ?required(slide_op, id)        :: id(),
                   node       = ?required(slide_op, node)      :: comm:mypid(), % the node, data is sent to/received from
                   interval   = ?required(slide_op, interval)  :: intervals:interval(), % send/receive data in this range
                   target_id  = ?required(slide_op, target_id) :: ?RT:key(), % ID to move the predecessor of the two participating nodes to
                   tag        = ?required(slide_op, tag)       :: any(),
                   source_pid = null          :: comm:erl_local_pid() | null, % pid of the process that requested the move (and will thus receive a message about its state)
                   timer      = {null, nomsg} :: {reference(), comm:message()} | {null, nomsg}, % timeout timer
                   timeouts   = 0             :: non_neg_integer(),
                   phase      = null          :: slide_phase()
                  }).
-opaque slide_op() :: #slide_op{}.

%% @doc Sets up a new slide operation for the special use during the join
%%      protocol. MyKey is the joining nodes new Id and will be used as the
%%      target id of the slide operation.
-spec new_receiving_slide_join(MoveId::id(), Pred::node:node_type(),
                               Succ::node:node_type(), MyKey::?RT:key())
        -> slide_op().
new_receiving_slide_join(MoveId, Pred, Succ, MyKey) ->
    IntervalToReceive = node:mk_interval_between_ids(node:id(Pred), MyKey),
    #slide_op{type = 'rcv',
              id = MoveId,
              node = node:pidX(Succ),
              interval = IntervalToReceive,
              target_id = MyKey,
              tag = join,
              source_pid = null}.

%% @doc Sets up a slide operation where the current node is receiving data from
%%      the given target node. One of the nodes will change its ID to TargetId.
-spec new_receiving_slide(MoveId::id(),
                          TargetNodePid::comm:mypid(),
                          PredOrSucc::pred | succ, TargetId::?RT:key(), Tag::any(),
                          SourcePid::comm:mypid() | null,
                          State::dht_node_state:state()) -> slide_op().
new_receiving_slide(MoveId, TargetNodePid, PredOrSucc, TargetId, Tag, SourcePid, State) ->
    IntervalToReceive = case PredOrSucc of
                            pred -> node:mk_interval_between_ids(
                                      TargetId,
                                      dht_node_state:get(State, pred_id));
                            succ -> node:mk_interval_between_ids(
                                      dht_node_state:get(State, node_id),
                                      TargetId)
                        end,
    #slide_op{type = 'rcv',
              id = MoveId,
              node = TargetNodePid,
              interval = IntervalToReceive,
              target_id = TargetId,
              tag = Tag,
              source_pid = SourcePid}.

%% @doc Sets up a slide operation where the current node is sending data to
%%      the given target node. One of the nodes will change its ID to TargetId.
-spec new_sending_slide(MoveId::id(),
                        TargetNodePid::comm:mypid(),
                        PredOrSucc::pred | succ, TargetId::?RT:key(), Tag::any(),
                        SourcePid::comm:mypid() | null,
                        State::dht_node_state:state()) -> slide_op().
new_sending_slide(MoveId, TargetNodePid, PredOrSucc, TargetId, Tag, SourcePid, State) ->
    IntervalToSend = case PredOrSucc of
                         pred -> node:mk_interval_between_ids(
                                   dht_node_state:get(State, pred_id),
                                   TargetId);
                         succ -> node:mk_interval_between_ids(
                                   TargetId,
                                   dht_node_state:get(State, node_id))
                     end,
    #slide_op{type = 'send',
              id = MoveId,
              node = TargetNodePid,
              interval = IntervalToSend,
              target_id = TargetId,
              tag = Tag,
              source_pid = SourcePid}.

%% @doc Returns the id of a receiving or sending slide operation.
-spec get_id(SlideOp::slide_op()) -> id().
get_id(#slide_op{id=Id}) -> Id.

%% @doc Returns the pid of the node to exchange data with.
-spec get_node(SlideOp::slide_op()) -> comm:mypid().
get_node(#slide_op{node=Node}) -> Node.

%% @doc Returns the interval of data to receive or send.
-spec get_interval(SlideOp::slide_op()) -> intervals:interval().
get_interval(#slide_op{interval=Interval}) -> Interval.

%% @doc Returns the target id a node participating in a receiving or sending
%%      slide operation moves to (note: this may be the other node).
-spec get_target_id(SlideOp::slide_op()) -> ?RT:key().
get_target_id(#slide_op{target_id=TargetId}) -> TargetId.

%% @doc Gets the pid of the (local) process that requested the move or null if
%%      no local process initiated it.
-spec get_source_pid(SlideOp::slide_op()) -> comm:erl_local_pid() | null.
get_source_pid(#slide_op{source_pid=Pid}) -> Pid.

%% @doc Returns the tag of a slide operation. This will be send to the
%%      originating process (along with the result message).
%% @see get_source_pid/1
-spec get_tag(SlideOp::slide_op()) -> any().
get_tag(#slide_op{tag=Tag}) -> Tag.

%% @doc Returns whether the given slide operation sends or receives data.
-spec get_type(SlideOp::slide_op()) -> 'send' | 'rcv'.
get_type(#slide_op{type=Type}) -> Type.

%% @doc Returns the timer of the slide operation or {null, nomsg} if no timer
%%      is set.
-spec get_timer(SlideOp::slide_op()) -> {reference(), comm:message()} | {null, nomsg}.
get_timer(#slide_op{timer=Timer}) -> Timer.

%% @doc Sets a timer that will send the given Message in Timeout ms. If Timeout
%%      is null, no timer will be send and the stored timer will be set to
%%      {null, nomsg}.
-spec set_timer(SlideOp::slide_op(), TimeOut::pos_integer(), Message::comm:message()) -> slide_op();
               (SlideOp::slide_op(), Timer::null, Message::nomsg) -> slide_op().
set_timer(SlideOp, Timeout, Message) ->
    TimerRef = case Timeout =/= null of
                   %TODO: switch to msg_delay (need to implement timer canceling first - see reset_timer/1)
%%                    true -> msg_delay:send_local(Timeout / 1000, self(), Message);
                   true -> comm:send_local_after(Timeout, self(), Message);
                   _    -> null
               end,
    SlideOp#slide_op{timer = {TimerRef, Message}}.

%% @doc Resets the timer of the given SlideOp, consumes any of its timeout
%%      messages and resets the timeout counter.
-spec reset_timer(SlideOp::slide_op()) -> slide_op().
reset_timer(SlideOp) ->
    {TimerRef, Msg} = get_timer(SlideOp),
    SlOp1 = case (TimerRef =/= null) of
                true ->
                    erlang:cancel_timer(TimerRef),
                    % consume potential timeout message
                    receive Msg -> ok
                    after 0 -> ok
                    end,
                    set_timer(SlideOp, null, nomsg);
                false when Msg =:= nomsg ->
                    SlideOp;
                false ->
                    set_timer(SlideOp, null, nomsg)
            end,
    reset_timeouts(SlOp1).

%% @doc Returns the number of timeouts received by a timer.
-spec get_timeouts(SlideOp::slide_op()) -> non_neg_integer().
get_timeouts(#slide_op{timeouts=Timeouts}) -> Timeouts.

%% @doc Increases the number of timeouts received by a timer by 1.
-spec inc_timeouts(SlideOp::slide_op()) -> slide_op().
inc_timeouts(SlideOp = #slide_op{timeouts=Timeouts}) ->
    SlideOp#slide_op{timeouts = Timeouts + 1}.

%% @doc Resets the number of timeouts to 0.
-spec reset_timeouts(SlideOp::slide_op()) -> slide_op().
reset_timeouts(SlideOp) ->
    SlideOp#slide_op{timeouts = 0}.

%% @doc Returns the current phase of the slide operation.
-spec get_phase(SlideOp::slide_op()) -> slide_phase().
get_phase(#slide_op{phase=Phase}) -> Phase.

%% @doc Sets the slide operation's current phase.
-spec set_phase(SlideOp::slide_op(), NewPhase::slide_phase()) -> slide_op().
set_phase(SlideOp, NewPhase) ->
    SlideOp#slide_op{phase = NewPhase}.
