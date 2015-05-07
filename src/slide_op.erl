%  @copyright 2010-2014 Zuse Institute Berlin

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

-export([new_slide/8, new_slide_i/8,
         new_receiving_slide_join/5,
         new_sending_slide_join/4, new_sending_slide_join_i/5,
         new_sending_slide_leave/5,
         new_sending_slide_jump/6,
         update_target_id/4,
         other_type_to_my_type/1,
         is_slide/1,
         is_join/1, is_join/2, is_leave/1, is_leave/2, is_jump/1, is_jump/2,
         is_incremental/1,
         get_id/1, get_node/1, get_interval/1,
         get_my_old_id/1, get_target_id/1, get_jump_target_id/1,
         get_source_pid/1, get_tag/1, get_sendORreceive/1, get_type/1,
         get_predORsucc/1,
         get_time_last_send/1, get_time_next_warn/1,
         set_time_last_send/2, set_time_next_warn/2,
         get_send_errors/1, inc_send_errors/1, reset_send_errors/1,
         get_phase/1, set_phase/2,
         is_setup_at_other/1, set_setup_at_other/1,
         get_next_op/1, set_next_op/2,
         get_other_max_entries/1, set_other_max_entries/2,
         get_msg_fwd/1, set_msg_fwd/1, remove_msg_fwd/1]).

-include("scalaris.hrl").
-include("record_helpers.hrl").

%-define(TRACE(X,Y), log:pal(X,Y)).
-define(TRACE(X,Y), ok).

-export_type([slide_op/0, id/0, phase/0, type/0, next_op/0]).

-type id() :: uid:global_uid().

-type type() ::
        {slide, pred | succ, 'send' | 'rcv'} |
        {join, 'send' | 'rcv'} |
        {leave, 'send' | 'rcv'} |
        {jump, 'send' | 'rcv'}.

-type phase_plain() ::
        null | % should only occur as an intermediate state, otherwise equal to "no slide op"
        wait_for_other | % a node initiated a slide but needs more info from its partner
        wait_for_data_ack | wait_for_delta_ack | % sending node
        wait_for_data | wait_for_delta. % receiving node
-type phase() ::
        phase_plain() |
        {wait_for_continue, phase_plain()}. % async (local) slide message to rm-specific implementation

-type next_op() ::
        {slide, continue, Id::?RT:key()} |
        {jump, continue, Id::?RT:key()} |
        {leave, continue} |
        {slide, pred | succ, Id::?RT:key(), Tag::any(), SourcePid::comm:mypid() | null} |
        {jump, Id::?RT:key(), Tag::any(), SourcePid::comm:mypid() | null} |
        {leave, SourcePid::comm:mypid() | null} |
        {none}.

-record(slide_op,
        {type              = ?required(slide_op, type)      :: type(),
         id                = ?required(slide_op, id)        :: id(),
         node              = ?required(slide_op, node)      :: node:node_type(), % the node, data is sent to/received from
         interval          = ?required(slide_op, interval)  :: intervals:interval(), % send/receive data in this range
         my_old_id         = ?required(slide_op, my_old_id) :: ?RT:key() | null, % ID before changing the own ID (null in case of receiving joins)
         target_id         = ?required(slide_op, target_id) :: ?RT:key(), % ID to move the predecessor of the two participating nodes to
         jump_target_id    = null                           :: ?RT:key() | null, % ID to jump to in case of a jump operation which is preceeded by a slide to leave
         tag               = ?required(slide_op, tag)       :: any(),
         source_pid        = null              :: comm:mypid() | null, % pid of the process that requested the move (and will thus receive a message about its state)
         time_last_send    = never             :: erlang_timestamp() | never,
         time_next_warn    = never             :: erlang_timestamp() | never,
         send_errors       = 0                 :: non_neg_integer(),
         phase             = null              :: phase(),
         setup_at_other    = false             :: boolean(),
         % note: use a format which does not require conversion when read
         % -> the pid is already contained in node, but this should be faster
         msg_fwd           = []                :: [{intervals:interval(), comm:mypid()}],
         next_op           = {none}            :: next_op(),
         other_max_entries = unknown           :: unknown | pos_integer()
        }).
-opaque slide_op() :: #slide_op{}.

%% @doc Sets up a slide operation of the given type. One of the nodes will
%%      change its ID to TargetId.
-spec new_slide(MoveId::uid:global_uid(), Type::type(), CurTargetId::?RT:key(),
                Tag::any(), SourcePid::comm:mypid() | null,
                OtherMTE::unknown | pos_integer(), NextOp::next_op(),
                Neighbors::nodelist:neighborhood())
        -> slide_op().
new_slide(MoveId, Type, CurTargetId, Tag, SourcePid, OtherMTE, NextOp, Neighbors) ->
    {PredOrSucc, SendOrReceive} =
        case Type of
            {slide, PoS, SoR} -> {PoS, SoR};
            % do not handle "join" here -> use the specialized new_*_slide_join methods!
            % creating slides for the leaving/jumping node must be done with new_slide_leave/jump!
            {leave, 'rcv'} -> {pred, 'rcv'};
            {jump, 'rcv'} ->  {pred, 'rcv'}
        end,
    {Interval, TargetNode} =
        get_interval_tnode(PredOrSucc, SendOrReceive, CurTargetId, Neighbors),
    #slide_op{type = Type,
              id = MoveId,
              node = TargetNode,
              interval = Interval,
              my_old_id = nodelist:nodeid(Neighbors),
              target_id = CurTargetId,
              tag = Tag,
              source_pid = SourcePid,
              next_op = NextOp,
              other_max_entries = OtherMTE}.

%% @doc Sets up an incremental slide operation of the given type. One of the
%%      nodes will change its ID to CurTargetId and finally FinalTargetId.
-spec new_slide_i(MoveId::uid:global_uid(), Type::type(),
                CurTargetId::?RT:key(), FinalTargetId::?RT:key(),
                Tag::any(), SourcePid::comm:erl_local_pid() | null,
                OtherMTE::unknown | pos_integer(), Neighbors::nodelist:neighborhood())
        -> slide_op().
new_slide_i(MoveId, Type, CurTargetId, FinalTargetId, Tag, SourcePid, OtherMTE, Neighbors) ->
    NextOp = case FinalTargetId of
                 CurTargetId -> {none};
                 _           -> {slide, continue, FinalTargetId}
             end,
    new_slide(MoveId, Type, CurTargetId, Tag, SourcePid, OtherMTE, NextOp, Neighbors).

-spec get_interval_tnode(PredOrSucc::pred | succ, SendOrReceive::'send' | 'rcv',
                         TargetId::?RT:key(), Neighbors::nodelist:neighborhood())
        -> {intervals:interval(), node:node_type()}.
get_interval_tnode(PredOrSucc, SendOrReceive, TargetId, Neighbors) ->
    case PredOrSucc of
        pred ->
            Pred = nodelist:pred(Neighbors),
            PredId = node:id(Pred),
            I = case SendOrReceive of
                    'rcv'  -> node:mk_interval_between_ids(TargetId, PredId);
                    'send' -> node:mk_interval_between_ids(PredId, TargetId)
                end,
            {I, Pred};
        succ ->
            NodeId = nodelist:nodeid(Neighbors),
            I = case SendOrReceive of
                    'rcv'  -> node:mk_interval_between_ids(NodeId, TargetId);
                    'send' -> node:mk_interval_between_ids(TargetId, NodeId)
                end,
            {I, nodelist:succ(Neighbors)}
    end.

%% @doc Sets up a new slide operation for a joining node (see
%%      dht_node_join.erl). MyKey is the joining node's new Id and will be used
%%      as the target id of the slide operation.
-spec new_receiving_slide_join(MoveId::uid:global_uid(), TargetId::?RT:key(),
        Tag::any(), SourcePid::comm:mypid(), Neighbors::nodelist:neighborhood()) -> slide_op().
new_receiving_slide_join(MoveId, TargetId, Tag, SourcePid, Neighbors) ->
    Pred = nodelist:pred(Neighbors),
    TargetNode = nodelist:succ(Neighbors),
    IntervalToReceive = node:mk_interval_between_ids(node:id(Pred), TargetId),
    #slide_op{type = {join, 'rcv'},
              id = MoveId,
              node = TargetNode,
              interval = IntervalToReceive,
              my_old_id = null,
              target_id = TargetId,
              tag = Tag,
              source_pid = SourcePid}.

%% @doc Sets up a new slide operation for a node which sends a joining node
%%      some of its data.
%%      Throws 'throw:not_responsible' if the current node is not responsible
%%      for the ID of JoiningNode.
-spec new_sending_slide_join(MoveId::uid:global_uid(), JoiningNode::node:node_type(),
        Tag::any(), Neighbors::nodelist:neighborhood()) -> slide_op().
new_sending_slide_join(MoveId, JoiningNode, Tag, Neighbors) ->
    JoiningNodeId = node:id(JoiningNode),
    new_sending_slide_join(MoveId, JoiningNode, JoiningNodeId, Tag, Neighbors).

%% @doc Sets up an incremental slide operation of the given type. One of the
%%      nodes will change its ID to CurTargetId and finally FinalTargetId.
-spec new_sending_slide_join_i(
        MoveId::uid:global_uid(), JoiningNode::node:node_type(),
        CurTargetId::?RT:key(), Tag::any(), Neighbors::nodelist:neighborhood())
        -> slide_op().
new_sending_slide_join_i(MoveId, JoiningNode, CurTargetId, Tag, Neighbors) ->
    FinalTargetId = node:id(JoiningNode),
    NextOp = case FinalTargetId of
                 CurTargetId -> {none};
                 _           -> {slide, continue, FinalTargetId}
             end,
    Slide = new_sending_slide_join(MoveId, JoiningNode, CurTargetId, Tag, Neighbors),
    Slide#slide_op{next_op = NextOp}.

-spec new_sending_slide_join(
        MoveId::uid:global_uid(), JoiningNode::node:node_type(),
        TargetId::?RT:key(), Tag::any(), Neighbors::nodelist:neighborhood()) -> slide_op().
new_sending_slide_join(MoveId, JoiningNode, JoiningNodeId, Tag, Neighbors) ->
    case intervals:in(JoiningNodeId, nodelist:node_range(Neighbors)) of
        false -> erlang:throw(not_responsible);
        _ ->
            IntervalToSend = node:mk_interval_between_ids(
                               node:id(nodelist:pred(Neighbors)), JoiningNodeId),
            #slide_op{type = {join, 'send'},
                      id = MoveId,
                      node = JoiningNode,
                      interval = IntervalToSend,
                      my_old_id = nodelist:nodeid(Neighbors),
                      target_id = JoiningNodeId,
                      tag = Tag,
                      source_pid = null}
    end.

%% @doc Sets up a new slide operation for a node which is about to leave its
%%      position in the ring incrementally (current step is to move to
%%      CurTargetId) and transfer its data to its successor.
-spec new_sending_slide_leave(MoveId::id(), CurTargetId::?RT:key(), Tag::any(),
        SourcePid::comm:erl_local_pid() | null,
        Neighbors::nodelist:neighborhood()) -> slide_op().
new_sending_slide_leave(MoveId, CurTargetId, Tag, SourcePid, Neighbors) ->
    {Interval, TargetNode} =
        get_interval_tnode('succ', 'send', CurTargetId, Neighbors),
    NextOp = case node:id(nodelist:pred(Neighbors)) of
                 CurTargetId -> {none};
                 _           -> {leave, continue}
             end,
    #slide_op{type = {leave, 'send'},
              id = MoveId,
              node = TargetNode,
              interval = Interval,
              my_old_id = nodelist:nodeid(Neighbors),
              target_id = CurTargetId,
              tag = Tag,
              source_pid = SourcePid,
              next_op = NextOp}.

%% @doc Sets up a new slide operation for a node which is about to leave its
%%      position in the ring, transfer its data to its successor
%%      incrementally (current step is to move to CurTargetId) and afterwards
%%      join somewhere else.
-spec new_sending_slide_jump(MoveId::id(), CurTargetId::?RT:key(),
        JumpTargetId::?RT:key(), SourcePid::comm:erl_local_pid(),
                              Tag::any(), Neighbors::nodelist:neighborhood())
        -> slide_op().
new_sending_slide_jump(MoveId, CurTargetId, JumpTargetId, SourcePid, Tag, Neighbors) ->
    {Interval, TargetNode} =
        get_interval_tnode('succ', 'send', CurTargetId, Neighbors),
    NextOp =
         case node:id(nodelist:pred(Neighbors)) of
             CurTargetId -> {none};
             _           -> {leave, continue}
         end,
    #slide_op{type = {jump, 'send'},
              id = MoveId,
              node = TargetNode,
              interval = Interval,
              my_old_id = nodelist:nodeid(Neighbors),
              target_id = CurTargetId,
              jump_target_id = JumpTargetId,
              tag = Tag,
              source_pid = SourcePid,
              next_op = NextOp}.

%% @doc Updates the slide op with a new TargetId and NextOp adapting message
%%      forwards and intervals accordingly.
-spec update_target_id(slide_op(), TargetId::?RT:key(), NextOp::next_op(),
                       Neighbors::nodelist:neighborhood()) -> slide_op().
update_target_id(SlideOp = #slide_op{target_id=TargetId}, TargetId, NextOp, _Neighbors) ->
    SlideOp#slide_op{next_op = NextOp};
update_target_id(SlideOp = #slide_op{type=Type, node=TargetNode, msg_fwd=OldMsgFwd},
                 TargetId, NextOp, Neighbors) ->
    PredOrSucc = get_predORsucc(Type),
    SendOrReceive = get_sendORreceive(Type),
    {Interval, TargetNode2} =
        case is_join(Type, 'rcv') of
            false ->
                Pred = nodelist:pred(Neighbors),
                case SendOrReceive =:= 'rcv' andalso
                         node:id(Pred) =:= TargetId of
                    false ->
                        get_interval_tnode(PredOrSucc, SendOrReceive, TargetId, Neighbors);
                    true ->
                        % the leaving node may not be in Neighbors any more!
                        % in this case, the TargetId is the same ID as the pred ID!
                        % -> use the ID of the TargetNode instead (should be up-to-date!)
                        {node:mk_interval_between_ids(TargetId, node:id(TargetNode)),
                         TargetNode}
                end;
            true ->
                % note: we always need to transfer everything from Pred to TargetId
                {node:mk_interval_between_ids(node:id(nodelist:pred(Neighbors)), TargetId),
                 nodelist:succ(Neighbors)}
        end,
    false = intervals:is_all(Interval),
    % TargetNode2 may be more up to date - check that this is the same node though
    true = (node:same_process(TargetNode, TargetNode2) orelse is_leave(SlideOp, 'rcv')),
    SlideOp1 = SlideOp#slide_op{interval = Interval,
                                target_id = TargetId,
                                next_op = NextOp},
    case OldMsgFwd of
        []  -> SlideOp1;
        [_] -> set_msg_fwd(SlideOp1)
    end.

%% @doc Returns the id of a receiving or sending slide operation.
-spec get_id(SlideOp::slide_op()) -> id().
get_id(#slide_op{id=Id}) -> Id.

%% @doc Returns the node to exchange data with.
-spec get_node(SlideOp::slide_op()) -> node:node_type().
get_node(#slide_op{node=Node}) -> Node.

%% @doc Returns the interval of data to receive or send.
-spec get_interval(SlideOp::slide_op()) -> intervals:interval().
get_interval(#slide_op{interval=Interval}) -> Interval.

%% @doc Returns the old ID before any changes, i.e. at the start of the slide
%%      (null in case of receiving joins).
-spec get_my_old_id(SlideOp::slide_op()) -> ?RT:key() | null.
get_my_old_id(#slide_op{my_old_id=MyOldId}) -> MyOldId.

%% @doc Returns the target id a node participating in a receiving or sending
%%      slide operation moves to (note: this may be the other node).
-spec get_target_id(SlideOp::slide_op()) -> ?RT:key().
get_target_id(#slide_op{target_id=TargetId}) -> TargetId.

-spec get_jump_target_id(SlideOp::slide_op()) -> ?RT:key().
get_jump_target_id(#slide_op{jump_target_id=TargetId}) -> TargetId.

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
-spec get_sendORreceive(SlideOp::slide_op() | type()) -> 'send' | 'rcv'.
get_sendORreceive(#slide_op{type=Type}) -> get_sendORreceive(Type);
get_sendORreceive({slide, _, SendOrReceive}) -> SendOrReceive;
get_sendORreceive({_TypeTag, SendOrReceive}) -> SendOrReceive.

%% @doc Returns whether the given slide operation works with the successor or
%%      predecessor.
-spec get_predORsucc(SlideOp::slide_op() | type()) -> pred | succ.
get_predORsucc(#slide_op{type=Type}) -> get_predORsucc(Type);
get_predORsucc({slide, PredOrSucc, _}) -> PredOrSucc;
get_predORsucc({join, 'send'}) -> pred;
get_predORsucc({join, 'rcv'}) -> succ;
get_predORsucc({leave, 'send'}) -> succ;
get_predORsucc({leave, 'rcv'}) -> pred;
get_predORsucc({jump, 'send'}) -> succ;
get_predORsucc({jump, 'rcv'}) -> pred.

%% @doc Returns the given slide operation's (full) type.
-spec get_type(SlideOp::slide_op()) -> type().
get_type(#slide_op{type=Type}) -> Type.

%% @doc Converts the given slide type to the type the other participating node
%%      can use.
-spec other_type_to_my_type(type()) -> type().
other_type_to_my_type({slide, pred, SendOrReceive}) ->
    {slide, succ, switch_sendORreceive2(SendOrReceive)};
other_type_to_my_type({slide, succ, SendOrReceive}) ->
    {slide, pred, switch_sendORreceive2(SendOrReceive)};
other_type_to_my_type({TypeTag, SendOrReceive}) ->
    {TypeTag, switch_sendORreceive2(SendOrReceive)}.

%% @doc Helper to change 'send' to 'rcv' and the other way around.
-spec switch_sendORreceive2('send') -> 'rcv';
                           ('rcv') -> 'send'.
switch_sendORreceive2('send') -> 'rcv';
switch_sendORreceive2('rcv') -> 'send'.

%% @doc Returns whether the given term is a slide op or not.
-spec is_slide(any()) -> boolean().
is_slide(#slide_op{}) -> true;
is_slide(_) -> false.

%% @doc Returns whether the given slide op or type is a join operation.
-spec is_join(SlideOp::slide_op() | type()) -> boolean().
is_join(#slide_op{type=Type}) -> is_join(Type);
is_join(Type) -> element(1, Type) =:= join.

%% @doc Returns whether the given slide op or type is a join operation sending
%%      or receiving data as provided.
-spec is_join(SlideOp::slide_op() | type(), 'send' | 'rcv') -> boolean().
is_join(#slide_op{type=Type}, SendOrReceive) -> is_join(Type, SendOrReceive);
is_join({join, SendOrReceive}, SendOrReceive) -> true;
is_join(_Type, _SendOrReceive) -> false.

%% @doc Returns whether the given slide op or type is a leave operation.
-spec is_leave(SlideOp::slide_op() | type()) -> boolean().
is_leave(#slide_op{type=Type}) -> is_leave(Type);
is_leave({leave, _}) -> true;
is_leave({jump, _}) -> true;
is_leave(_Type) -> false.

%% @doc Returns whether the given slide op or type is a leave operation sending
%%      or receiving data as provided.
-spec is_leave(SlideOp::slide_op() | type(), 'send' | 'rcv') -> boolean().
is_leave(#slide_op{type=Type}, SendOrReceive) -> is_leave(Type, SendOrReceive);
is_leave({leave, SendOrReceive}, SendOrReceive) -> true;
is_leave({jump, SendOrReceive}, SendOrReceive) -> true;
is_leave(_Type, _SendOrReceive) -> false.

%% @doc Returns whether the given slide op or type is a jump operation.
-spec is_jump(SlideOp::slide_op() | type()) -> boolean().
is_jump(#slide_op{type=Type}) -> is_jump(Type);
is_jump({jump, _SendOrReceive}) -> true;
is_jump(_Type) -> false.

%% @doc Returns whether the given slide op or type is a jump operation sending
%%      or receiving data as provided.
-spec is_jump(SlideOp::slide_op() | type(), 'send' | 'rcv') -> boolean().
is_jump(#slide_op{type=Type}, SendOrReceive) -> is_jump(Type, SendOrReceive);
is_jump({jump, SendOrReceive}, SendOrReceive) -> true;
is_jump(_Type, _SendOrReceive) -> false.

%% @doc Returns whether the given slide op is part of an incremental slide.
-spec is_incremental(SlideOp::slide_op()) -> boolean().
is_incremental(#slide_op{next_op={slide, continue, _Id}}) -> true;
is_incremental(#slide_op{next_op={jump, continue, _Id}}) -> true;
is_incremental(#slide_op{next_op={leave, continue}}) -> true;
is_incremental(_) -> false.

%% @doc Returns the time of the last send operation.
-spec get_time_last_send(SlideOp::slide_op()) -> erlang_timestamp() | never.
get_time_last_send(#slide_op{time_last_send = X}) -> X.

%% @doc Returns the time the next warning should be emitted if no further send
%%      operation occurs.
-spec get_time_next_warn(SlideOp::slide_op()) -> erlang_timestamp() | never.
get_time_next_warn(#slide_op{time_next_warn = X}) -> X.

%% @doc Sets the time of the last send operation.
-spec set_time_last_send(SlideOp::slide_op(), erlang_timestamp()) -> slide_op().
set_time_last_send(SlideOp, X) -> SlideOp#slide_op{time_last_send = X}.

%% @doc Sets the time the next warning should be emitted if no further send
%%      operation occurs.
-spec set_time_next_warn(SlideOp::slide_op(), erlang_timestamp()) -> slide_op().
set_time_next_warn(SlideOp, X) -> SlideOp#slide_op{time_next_warn = X}.

%% @doc Returns the number of send errors from messages with shepherd.
-spec get_send_errors(SlideOp::slide_op()) -> non_neg_integer().
get_send_errors(#slide_op{send_errors = X}) -> X.

%% @doc Increases the number of send_errors from messages with shepherd by 1.
-spec inc_send_errors(SlideOp::slide_op()) -> slide_op().
inc_send_errors(SlideOp = #slide_op{send_errors = X}) ->
    SlideOp#slide_op{send_errors = X + 1}.

%% @doc Resets the number of send_errors from messages with shepherd to 0.
-spec reset_send_errors(SlideOp::slide_op()) -> slide_op().
reset_send_errors(SlideOp) ->
    SlideOp#slide_op{send_errors = 0}.

%% @doc Returns the current phase of the slide operation.
-spec get_phase(SlideOp::slide_op()) -> phase().
get_phase(#slide_op{phase=Phase}) -> Phase.

%% @doc Sets the slide operation's current phase.
-spec set_phase(SlideOp::slide_op(), NewPhase::phase()) -> slide_op().
set_phase(SlideOp, NewPhase) -> SlideOp#slide_op{phase = NewPhase}.

%% @doc Returns wether the current slide op has already been set up at the
%%      other node.
-spec is_setup_at_other(SlideOp::slide_op()) -> boolean().
is_setup_at_other(#slide_op{setup_at_other=SetupAtOther}) -> SetupAtOther.

%% @doc Sets that the current slide op has already been set up at the
%%      other node.
-spec set_setup_at_other(SlideOp::slide_op()) -> slide_op().
set_setup_at_other(SlideOp) -> SlideOp#slide_op{setup_at_other = true}.

-spec get_next_op(SlideOp::slide_op()) -> next_op().
get_next_op(#slide_op{next_op=Op}) -> Op.

-spec set_next_op(SlideOp::slide_op(), NextOp::next_op()) -> slide_op().
set_next_op(SlideOp, NextOp) -> SlideOp#slide_op{next_op = NextOp}.

-spec get_other_max_entries(SlideOp::slide_op()) -> unknown | pos_integer().
get_other_max_entries(#slide_op{other_max_entries=OtherMTE}) -> OtherMTE.

-spec set_other_max_entries(SlideOp::slide_op(), OtherMTE::pos_integer()) -> slide_op().
set_other_max_entries(SlideOp, OtherMTE) -> SlideOp#slide_op{other_max_entries = OtherMTE}.

-spec get_msg_fwd(SlideOp::slide_op() | null) -> [{intervals:interval(), comm:mypid()}].
get_msg_fwd(null) -> [];
get_msg_fwd(#slide_op{msg_fwd=MsgFwd}) -> MsgFwd.

-spec set_msg_fwd(SlideOp::slide_op()) -> slide_op().
set_msg_fwd(SlideOp = #slide_op{interval=Interval}) ->
    ?TRACE("[ ~.0p ] set_msg_fwd: ~.0p~n", [self(), Interval]),
    SlideOp#slide_op{msg_fwd = [{Interval, node:pidX(get_node(SlideOp))}]}.

-spec remove_msg_fwd(SlideOp::slide_op()) -> slide_op().
remove_msg_fwd(SlideOp) ->
    ?TRACE("[ ~.0p ] remove_msg_fwd: ~.0p~n", [self(), SlideOp#slide_op.msg_fwd]),
    SlideOp#slide_op{msg_fwd = []}.
