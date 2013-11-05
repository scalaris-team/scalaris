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

%% @author Thorsten Schütt <schuett@zib.de>
%% @doc    Slide protocol used for lease-based ring maintenance (also see the
%%         dht_node_move module).
%% @end
%% @version $Id$
-module(slide_leases).
-author('schuett@zib.de').
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

% for tester
-export([tester_create_dht_node_state/0]).

-spec prepare_join_send(State::dht_node_state:state(), SlideOp::slide_op:slide_op())
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_join_send(State, SlideOp) ->
    log:log("prepare_join_send", []),
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
        -> {ok, dht_node_state:state(), slide_op:slide_op()}.
prepare_send_delta1(State, OldSlideOp, ReplyPid) ->
    % start to split own range
    log:log("prepare_send_delta1 ~p~n", [slide_op:get_type(OldSlideOp)]),
    case find_lease(State, OldSlideOp, active) of
        {ok, Lease} ->
            Id = l_on_cseq:id(l_on_cseq:get_range(Lease)),
            % check slide direction
            Interval = slide_op:get_interval(OldSlideOp),
            case Interval =:= l_on_cseq:get_range(Lease) of
                false ->
                    {R1, R2} = case intervals:in(Id, Interval) of
                                   true ->
                                                % ->
                                       true = false, % TS: not tested
                                       {intervals:minus(l_on_cseq:get_range(Lease), Interval), Interval};
                                   false ->
                                                % <-
                                       {Interval, intervals:minus(l_on_cseq:get_range(Lease), Interval)}
                               end,
                    NewOwner = node:pidX(slide_op:get_node(OldSlideOp)),
                    l_on_cseq:lease_split_and_change_owner(Lease, R1, R2, second, NewOwner, ReplyPid),
                    {ok, State, OldSlideOp};
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
            {ActiveLeaseList, PassiveLeaseList} = dht_node_state:get(State, lease_list),
            Interval = slide_op:get_interval(OldSlideOp),
            log:log("unknown lease in prepare_send_delta1~n"),
            log:log("~p:~p~n", [ActiveLeaseList, PassiveLeaseList]),
            log:log("~p~n", [Interval]),
            error
    end.


-spec prepare_send_delta2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                          EmbeddedMsg::any())
        -> {ok, dht_node_state:state(), slide_op:slide_op()} | error.
prepare_send_delta2(State, SlideOp, Msg) ->
    % check that split has been done
    case Msg of
        {handover, success, _NewLease} ->
            % disable new lease
            log:log("prepare_send_delta2 ~p~n", [Msg]),
            %State1 = locally_disable_lease(State, NewLease),
            {ok, State, SlideOp};
        {split, fail, _Lease} ->
            log:log("prepare_send_delta2: split failed~n", []),
            % @todo
            error
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
    log:log("finish_delta_ack1~n", []),
    {ok, State, OldSlideOp}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec finish_delta_ack2(State::dht_node_state:state(), SlideOp::slide_op:slide_op(),
                        NextOpMsg, EmbeddedMsg::{continue})
        -> {ok, dht_node_state:state(), slide_op:slide_op(), NextOpMsg}
        when is_subtype(NextOpMsg, dht_node_move:next_op_msg()).
finish_delta_ack2(State, SlideOp, NextOpMsg, Msg) ->
    % notify neighbor on successful handover
    log:log("finish_delta_ack2 ~p~n", [Msg]),
    % notify succ
    case find_lease(State, SlideOp, passive) of
        {ok, Lease} ->
            Owner = l_on_cseq:get_owner(Lease),
            l_on_cseq:lease_send_lease_to_node(Owner, Lease),
            State1 = lease_list:remove_lease_from_dht_node_state(Lease, State, passive),
            {ok, State1, SlideOp, NextOpMsg};
        error ->
            log:log("error in finish_delta_ack2"),
            % @todo
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec send_continue_msg(Pid::comm:erl_local_pid()) -> ok.
send_continue_msg(Pid) ->
    ?TRACE_SEND(Pid, {continue}),
    comm:send_local(Pid, {continue}).

-spec locally_disable_lease(State::dht_node_state:state(),
                            Lease::l_on_cseq:lease_t()) -> dht_node_state:state().
locally_disable_lease(State, Lease) ->
    l_on_cseq:disable_lease(State, Lease).

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
                    log:log("did not found requested lease in passive list: ~w ~w",
                            [Interval, PassiveLeases]),
                    error
            end
    end.

% @doc create dht_node state for tester
-spec tester_create_dht_node_state() -> dht_node_state:state().
tester_create_dht_node_state() ->
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {get_state, comm:this()}),
    receive
        {get_state_response, State} ->
            State
    end.
