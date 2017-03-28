%% @copyright 2010-2017 Zuse Institute Berlin

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

-spec prepare_join_send(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_join_send(State, SlideOp) ->
    %log:log("prepare_join_send", []),
    % can be ignored for leases
    {ok, State, SlideOp}.

-spec prepare_rcv_data(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_rcv_data(State, SlideOp) ->
    % do nothing
    %io:format("prepare_rcv_data~n", []),
    {ok, State, SlideOp}.

-spec prepare_send_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_data1(State, SlideOp, ReplyPid) ->
    % do nothing
    %io:format("prepare_send_data1~n", []),
    send_continue_msg(ReplyPid),
    {ok, State, SlideOp}.

-spec prepare_send_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                         EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_data2(State, SlideOp, {continue}) ->
    % do nothing
    %io:format("prepare_send_data2~n", []),
    {ok, State, SlideOp}.

-spec update_rcv_data1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
update_rcv_data1(State, SlideOp, ReplyPid) ->
    % do nothing
    %io:format("update_rcv_data1~n", []),
    send_continue_msg(ReplyPid),
    {ok, State, SlideOp}.

-spec update_rcv_data2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                       EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
update_rcv_data2(State, SlideOp, {continue}) ->
    % do nothing
    %io:format("update_rcv_data2~n", []),
    {ok, State, SlideOp}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% split lease and disable lease
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prepare_send_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                          ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, Reason::{protocol_error, EmbeddedMsg::any()}, State::dht_node_state:state(), SlideOp1::slide_op:slide_op()}.
prepare_send_delta1(State, OldSlideOp, ReplyPid) ->
    % start to split own range
    %log:log("prepare_send_delta1 ~p~n", [slide_op:get_type(OldSlideOp)]),
    case find_lease(State, OldSlideOp, active) of
        {ok, Lease} ->
            Id = l_on_cseq:id(l_on_cseq:get_range(Lease)),
            % check slide direction
            Interval = slide_op:get_interval(OldSlideOp),
            case Interval =:= l_on_cseq:get_range(Lease) of
                false ->
                    case intervals:in(Id, Interval) of
                        true ->
                                                % ->
                            log:log("unsupported slide direction in prepare_send_delta1~n"),
                            log:log("~p in ~p~n == false", [Id, Interval]),
                            log:log("~p~n", [Lease]),
                            {abort, {protocol_error, unsupported_slide_direction}, State, OldSlideOp};
                        false ->
                                                % <-
                            {R1, R2} =  {Interval, intervals:minus(l_on_cseq:get_range(Lease), Interval)},
                            NewOwner = node:pidX(slide_op:get_node(OldSlideOp)),
                            l_on_cseq:lease_split_and_change_owner(Lease, R1, R2, NewOwner, ReplyPid),
                            {ok, State, OldSlideOp}
                    end;
                true ->
                    log:log("only change owner instead of split and change owner", []),
                    %% @todo
                    %log:log("Id ~p ~n", [Id]),
                    %log:log("Interval     ~p", [Interval]),
                    %log:log("intervals:in ~p", [intervals:in(Id, Interval)]),
                    %log:log("lease        ~p", [Lease]),
                    %log:log("lease range  ~p", [l_on_cseq:get_range(Lease)]),
                    %log:log("bounds       ~p", [intervals:get_bounds(l_on_cseq:get_range(Lease))]),
                    %log:log("id           ~p", [l_on_cseq:id(l_on_cseq:get_range(Lease))]),
                    NewOwner = node:pidX(slide_op:get_node(OldSlideOp)),
                    l_on_cseq:lease_handover(Lease, NewOwner, ReplyPid),
                    {ok, State, OldSlideOp}
            end;
        error ->
            % @todo
            LeaseList = dht_node_state:get(State, lease_list),
            ActiveLease = lease_list:get_active_lease(LeaseList),
            PassiveLeaseList = lease_list:get_passive_leases(LeaseList),
            Interval = slide_op:get_interval(OldSlideOp),
            log:log("unknown lease in prepare_send_delta1~n"),
            log:log("~p:~p~n", [ActiveLease, PassiveLeaseList]),
            log:log("~p~n", [Interval]),
            {abort, {protocol_error, unknown_lease_in_prepare_send_delta}, State, OldSlideOp}
    end.


-spec prepare_send_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                          EmbeddedMsg::any())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} |
           {abort, Reason::{protocol_error, EmbeddedMsg::any()}, State::dht_node_state:state(), SlideOp1::slide_op:slide_op()}.
prepare_send_delta2(State, SlideOp, Msg) ->
    % check that split has been done
    case Msg of
        {handover, success, _NewLease} ->
            % disable new lease
            %log:log("prepare_send_delta2 ~p~n", [Msg]),
            %State1 = locally_disable_lease(State, NewLease),
            {ok, State, SlideOp};
        {split, fail, _Lease} ->
            log:log("prepare_send_delta2: split failed~n", []),
            {abort, {protocol_error, Msg}, State, SlideOp}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec finish_delta1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta1(State, OldSlideOp, ReplyPid) ->
    % do nothing
    send_continue_msg(ReplyPid),
    {ok, State, OldSlideOp}.

-spec finish_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                    EmbeddedMsg::{continue}) -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta2(State, SlideOp, {continue}) ->
    % do nothing
    {ok, State, SlideOp}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% handover
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec finish_delta_ack1(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                        ReplyPid::comm:erl_local_pid())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
finish_delta_ack1(State, OldSlideOp, ReplyPid) ->
    % handover lease to succ
    comm:send_local(ReplyPid, {continue}),
    %log:log("finish_delta_ack1~n", []),
    {ok, State, OldSlideOp}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec finish_delta_ack2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                        NextOpMsg, EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op(), NextOpMsg}
        when is_subtype(NextOpMsg, dht_node_move:next_op_msg()).
finish_delta_ack2(State, SlideOp, NextOpMsg, Msg) ->
    % notify neighbor on successful handover
    %log:log("finish_delta_ack2 ~p~n", [_Msg]),
    % notify succ
    case find_lease(State, SlideOp, passive) of
        {ok, Lease} ->
            Owner = l_on_cseq:get_owner(Lease),
            l_on_cseq:lease_send_lease_to_node(Owner, Lease, active),
            State1 = lease_list:remove_lease_from_dht_node_state(Lease, l_on_cseq:get_id(Lease),
                                                                 State, passive),
            {ok, State1, SlideOp, NextOpMsg};
        error ->
            log:log("error in finish_delta_ack2"),
            {abort, {protocol_error, Msg}, State, SlideOp}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Executed when aborting the given slide operation (assumes the SlideOp
%%      has already been set up).
-spec abort_slide(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                  Reason::dht_node_move:abort_reason(), MoveMsgTag::atom())
        -> dht_node_state:state().
abort_slide(State, _SlideOp, _Reason, _MoveMsgTag) ->
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec send_continue_msg(Pid::comm:erl_local_pid()) -> ok.
send_continue_msg(Pid) ->
    ?TRACE_SEND(Pid, {continue}),
    comm:send_local(Pid, {continue}).

find_lease(State, SlideOp, Mode) ->
    LeaseList = dht_node_state:get(State, lease_list),
    Interval = slide_op:get_interval(SlideOp),
    Pred = fun (L) ->
                   intervals:is_subset(Interval, l_on_cseq:get_range(L))
                       andalso intervals:is_continuous(
                                   intervals:intersection(Interval,l_on_cseq:get_range(L)))
           end,
    ActiveLease = lease_list:get_active_lease(LeaseList),
    PassiveLeases = lease_list:get_passive_leases(LeaseList),
    case Mode of
        active ->
            case Pred(ActiveLease) of
                true ->
                    {ok, ActiveLease};
                false ->
                    error
            end;
        passive ->
            case lists:filter(Pred, PassiveLeases) of
                [Lease] ->
                    {ok, Lease};
                _ ->
                    log:log("did not found requested lease in passive list:~n~w~n~w~n~w~n",
                            [Interval, ActiveLease, PassiveLeases]),
                    error
            end
    end.
