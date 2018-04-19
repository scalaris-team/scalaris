%  @copyright 2007-2018 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Bulk owner operations (for now only broadcasting).

%% @version $Id$
%% @reference Ali Ghodsi, <em>Distributed k-ary System: Algorithms for Distributed Hash Tables</em>, PhD Thesis, page 129.
-module(bulkowner).
-author('schuett@zib.de').
-vsn('$Id$').

-define(TRACE(X, Y), ok).
%% -define(TRACE(X, Y), io:format(X, Y)).

-include("scalaris.hrl").

% public API:
-export([issue_bulk_owner/3, issue_send_reply/4,
         issue_bulk_distribute/5, issue_bulk_distribute/6,
         send_reply/5, send_reply_failed/6]).

% only use inside the dht_node process:
-export([on/2]).

-export_type([bulkowner_msg/0]).

-type bulkowner_req() ::
      {bulkowner, start,
       Id::uid:global_uid(), I::intervals:interval(), Msg::comm:message()}
      | {bulkowner, Id::uid:global_uid(), I::intervals:interval(),
         Msg::comm:message(), Parents::[comm:mypid(),...]}
      | {bulkowner, deliver, Id::uid:global_uid(), Range::intervals:interval(),
         Msg::comm:message(), Parents::[comm:mypid(),...]}
      | {bulkowner, reply, Id::uid:global_uid(), Target::comm:mypid(),
         Msg::comm:message(), Parents::[comm:mypid()]}
      | {bulkowner, reply_process_all}
      | {bulkowner, gather, Id::uid:global_uid(), Target::comm:mypid(),
         [comm:message()], Parents::[comm:mypid()]}.

-type bulkowner_send_error() ::
        {send_error, comm:mypid(), bulkowner_req(), atom() }.

-type bulkowner_msg() :: bulkowner_req() | bulkowner_send_error().

%% @doc Start a bulk owner operation to send the message to all nodes in the
%%      given interval.
-spec issue_bulk_owner(Id::uid:global_uid(), I::intervals:interval(), Msg::comm:message()) -> ok.
issue_bulk_owner(Id, I, Msg) ->
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {bulkowner, start, Id, I, Msg}).

-spec issue_send_reply(Id::uid:global_uid(), Target::comm:mypid(), Msg::comm:message(),
                       Parents::[comm:mypid()]) -> ok.
issue_send_reply(Id, Target, Msg, Parents) ->
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {bulkowner, reply, Id, Target, Msg, Parents}).

-spec issue_bulk_distribute(ID::uid:global_uid(), atom(), pos_integer(),
                           comm:message(), [{?RT:key(), string(), term()}] | {ets, ets:tab()}) -> ok.
issue_bulk_distribute(Id, Proc, Pos, Msg, Data) ->
    issue_bulk_distribute(Id, Proc, Pos, Msg, Data, intervals:all()).

-spec issue_bulk_distribute(ID::uid:global_uid(), atom(), pos_integer(),
                           comm:message(), [{?RT:key(), string(), term()}] | {ets, ets:tab()},
                           intervals:interval()) -> ok.
issue_bulk_distribute(Id, Proc, Pos, Msg, Data, Interval) ->
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {bulkowner, start, Id, Interval, {bulk_distribute, Proc,
                                                        Pos, Msg, Data}}).

-spec send_reply(Id::uid:global_uid(), Target::comm:mypid(), Msg::comm:message() | comm:group_message(),
                 Parents::[comm:mypid()], Shepherd::comm:erl_local_pid()) -> ok.
send_reply(Id, Target, {?send_to_group_member, Proc, Msg}, [], Shepherd) ->
    comm:send(Target, {bulkowner, reply, Id, Msg}, [{shepherd, Shepherd}, {group_member, Proc}]);
send_reply(Id, Target, {?send_to_registered_proc, Proc, Msg}, [], Shepherd) ->
    comm:send(Target, {bulkowner, reply, Id, Msg}, [{shepherd, Shepherd}, {registered_proc, Proc}]);
send_reply(Id, Target, Msg, [], Shepherd) ->
    comm:send(Target, {bulkowner, reply, Id, Msg}, [{shepherd, Shepherd}]);
send_reply(Id, Target, Msg, [Parent | Rest], Shepherd) ->
    comm:send(Parent, {bulkowner, reply, Id, Target, Msg, Rest}, [{shepherd, Shepherd}]).

-spec send_reply_failed(Id::uid:global_uid(), Target::comm:mypid(), Msg::comm:message(),
                        Parents::[comm:mypid()], Shepherd::comm:erl_local_pid(),
                        FailedPid::comm:mypid()) -> ok.
send_reply_failed(_Id, Target, Msg, [], _Shepherd, Target) ->
    log:log(warn, "[ ~p ] cannot send bulkowner_reply with message ~p (target node not available)",
            [pid_groups:pid_to_name(self()), Msg]);
send_reply_failed(Id, Target, Msg, Parents, Shepherd, _FailedPid) ->
    send_reply(Id, Target, Msg, Parents, Shepherd).

%% @doc main routine. It spans a broadcast tree over the nodes in I
-spec bulk_owner(State::dht_node_state:state(), Id::uid:global_uid(),
                 I::intervals:interval(), Msg::comm:message(),
                 Parents::[comm:mypid()]) -> ok.
bulk_owner(State, Id, I, Msg, Parents) ->
%%     log:pal("bulk_owner:~n self:~p,~n int :~p,~n rt  :~p~n", [dht_node_state:get(State, node), I, ?RT:to_list(State)]),
    Neighbors = dht_node_state:get(State, neighbors),
    SuccIntI = intervals:intersection(I, nodelist:succ_range(Neighbors)),
    case intervals:is_empty(SuccIntI) of
        true  -> ok;
        false ->
            case Msg of
                {bulk_distribute, Proc, N, Env, Data} ->
                    SuccData = get_range_data(Data, SuccIntI),
                    comm:send(node:pidX(nodelist:succ(Neighbors)),
                              {bulkowner, deliver, Id, SuccIntI,
                              {bulk_distribute, Proc, N, Env, SuccData}, Parents});
                _ ->
                    comm:send(node:pidX(nodelist:succ(Neighbors)),
                              {bulkowner, deliver, Id, SuccIntI, Msg, Parents})
            end
    end,
    case I =:= SuccIntI of
        true  -> ok;
        false ->
            MyNode = nodelist:node(Neighbors),
            NewParents = [node:pidX(MyNode) | Parents],
            RTList = lists:reverse(?RT:to_list(State)),
            RemainingInterval = intervals:minus(I, SuccIntI),
            bulk_owner_iter(RTList, Id, RemainingInterval, Msg, node:id(MyNode),
                            NewParents)
    end.

%% @doc Iterates through the list of (unique) nodes in the routing table and
%%      sends them the according bulkowner messages for sub-intervals of I.
%%      The first call should have Limit=Starting_nodeid. The method will
%%      then go through the ReverseRTList (starting with the longest finger,
%%      ending with the node's successor) and send each node a bulk_owner
%%      message for the interval it is responsible for:
%%      I \cap (id(Node_in_reversertlist), Limit], e.g.
%%      node Nl from the longest finger is responsible for
%%      I \cap (id(Nl), id(Starting_node)].
%%      Note that the range (id(Starting_node), id(Succ_of_starting_node)]
%%      has already been covered by bulk_owner/3.
-spec bulk_owner_iter(ReverseRTList::[{Id::?RT:key(), Pid::comm:mypid()}],
                      Id::uid:global_uid(),
                      I::intervals:interval(), Msg::comm:message(),
                      Limit::?RT:key(), Parents::[comm:mypid(),...]) -> ok.
bulk_owner_iter([], _Id, _I, _Msg, _Limit, _Parents) ->
    ok;
bulk_owner_iter([{HeadId, HeadPid} | Tail], Id, I, Msg, Limit, Parents) ->
    case intervals:is_empty(I) of
        false ->
            Interval_Head_Limit =
                node:mk_interval_between_ids(HeadId, Limit),
            Range = intervals:intersection(I, Interval_Head_Limit),
            %%     log:pal("send_bulk_owner_if: ~p ~p ~n", [I, Range]),
            NewLimit =
                case intervals:is_empty(Range) of
                    false ->
                        case Msg of
                            {bulk_distribute, Proc, N, Env, Data} ->
                                RangeData = get_range_data(Data, Range),
                                comm:send(HeadPid,
                                          {bulkowner, Id, Range,
                                           {bulk_distribute, Proc, N, Env, RangeData}, Parents},
                                         [{group_member, dht_node}]);
                            _ ->
                                comm:send(HeadPid,
                                          {bulkowner, Id, Range, Msg, Parents},
                                          [{group_member, dht_node}])
                        end,
                        HeadId;
                    true  -> Limit
                end,
            RemainingInterval = intervals:minus(I, Range),
            bulk_owner_iter(Tail, Id, RemainingInterval, Msg, NewLimit, Parents);
        true -> ok
    end.

%% protocol implementation / called inside the dht_node on handler
-spec on(bulkowner_msg(), dht_node_state:state()) -> dht_node_state:state().
on({bulkowner, start, Id, I, Msg}, State) ->
    bulk_owner(State, Id, I, Msg, []),
    State;

on({bulkowner, Id, I, Msg, Parents}, State) ->
    bulk_owner(State, Id, I, Msg, Parents),
    State;

on({bulkowner, deliver, Id, Range, Msg, Parents}, State) ->
    MsgFwd = dht_node_state:get(State, msg_fwd),

    F = fun({FwdInt, FwdPid}, AccI) ->
                case intervals:is_subset(FwdInt, AccI) of
                    true ->
                        FwdRange = intervals:intersection(AccI, FwdInt),
                        ?TRACE("~p forwarding msg to ~p~nbecause of fwdInt ~p~n",
                               [self(), FwdPid, FwdInt]),
                        case Msg of
                            %% when Msg = bulk_distribute only forward the
                            %% FwdInt part of the data. otherwise we get duplicate
                            %% data
                            {bulk_distribute, Proc, N, Msg1, Data} ->
                                RangeData = get_range_data(Data, FwdRange),
                                comm:send(FwdPid,
                                      {bulkowner, deliver, Id, FwdRange,
                                      {bulk_distribute, Proc, N, Msg1, RangeData},
                                      Parents});
                            _ ->
                                comm:send(FwdPid, {bulkowner, deliver, Id, FwdRange, Msg, Parents})
                        end,
                        intervals:minus(AccI, FwdRange);
                    _    -> AccI
                end
        end,
    % we are responsible for the intersection of the BulkOwner range and our
    % ring maintenance range (plus DB range from ongoing slides).
    DBRange = lists:foldl(fun({I, _SlideId}, AccI) ->
                                  intervals:union(AccI, I)
                          end, intervals:empty(),
                          dht_node_state:get(State, db_range)),
    MyDBRange = intervals:union(dht_node_state:get(State, my_range), DBRange),
    MyRange0 = lists:foldl(F, Range, MsgFwd),
    MyRange = intervals:intersection(MyRange0, MyDBRange),
    NewState = case intervals:is_empty(MyRange) of
        true -> State;
        _ ->
            %% check if node has left the ring via a slide
            %% if rm_loop:has_left/1 is true this message arrived through a
            %% msg_fwd and needs to be passed back into the system
            case rm_loop:has_left(dht_node_state:get(State, rm_state)) of
                false ->
                    handle_delivery(Msg, MyRange, Id, Parents, State);
                true ->
                    ?TRACE("~p bulkowner: delivering to leaving node...
                            forwarding msg ~p to succ ~p~n",
                           [self(), Msg, nodelist:succ(dht_node_state:get(State,
                                                                          neighbors))]),
                    Neighbors = dht_node_state:get(State, neighbors),
                    comm:send(node:pidX(nodelist:succ(Neighbors)),
                              {bulkowner, Id, Range, Msg, Parents}),
                    State
            end
    end,
    RestRange = intervals:minus(MyRange0, MyRange),
    % no need to check whether non-empty - this is a no-op anyway
    bulk_owner(State, Id, RestRange, Msg, Parents),
    NewState;

on({bulkowner, reply, Id, Target, Msg, Parents}, State) ->
    % target information should be the same
    PrevMsgs =
        case erlang:get({'$bulkowner_reply_msg', Id}) of
            undefined ->
                _ = comm:send_local_after(100, self(),
                                          {bulkowner, reply_process_all, Id}),
                [];
            {Id, Target, X = [_|_], _PrevParents} ->
                X
        end,
    % Parents may be different if the node is twice in the tree -> use the latest:
    _ = erlang:put({'$bulkowner_reply_msg', Id},
                   {Id, Target, [Msg | PrevMsgs], Parents}),
    State;

on({bulkowner, reply_process_all, Id}, State) ->
    {Id, Target, Msgs, Parents} = erlang:erase({'$bulkowner_reply_msg', Id}),
    case Msgs of
        [] -> ok;
        [{bulk_read_entry_response, _HRange, _HData} | _] ->
            %% all messages must have the same type:
            ?DBG_ASSERT([] =:= [Msg1 || Msg1 <- Msgs,
                                        element(1, Msg1) =/= bulk_read_entry_response]),
            comm:send_local(self(),
                            {bulkowner, gather, Id, Target, Msgs, Parents});
        [{?send_to_group_member, Proc, _Msg} | _] ->
            %% all messages must have the same type:
            ?DBG_ASSERT([] =:= [Msg1 || Msg1 <- Msgs,
                                        element(1, Msg1) =/= ?send_to_group_member orelse
                                            element(2, Msg1) =/= Proc]),
            Msgs1 = [Msg1 || {?send_to_group_member, _Proc, Msg1} <- Msgs],
            comm:forward_to_group_member(
              Proc, {bulkowner, gather, Id, Target, Msgs1, Parents});
        [{?send_to_registered_proc, Proc, _Msg} | _] ->
            %% all messages must have the same type:
            ?DBG_ASSERT([] =:= [Msg1 || Msg1 <- Msgs,
                                        element(1, Msg1) =/= ?send_to_registered_proc orelse
                                            element(2, Msg1) =/= Proc]),
            Msgs1 = [Msg1 || {?send_to_registered_proc, _Proc, Msg1} <- Msgs],
            comm:forward_to_registered_proc(
              Proc, {bulkowner, gather, Id, Target, Msgs1, Parents})
    end,
    State;

on({bulkowner, gather, Id, Target, [H = {bulk_read_entry_response, _HRange, _HData} | T], Parents}, State) ->
    Msg = lists:foldl(
            fun({bulk_read_entry_response, Range, Data},
                {bulk_read_entry_response, AccRange, AccData}) ->
                    {bulk_read_entry_response,
                     intervals:union(Range, AccRange),
                     lists:append(Data, AccData)}
            end, H, T),
    send_reply(Id, Target, Msg, Parents, self()),
    State;

on({send_error, FailedTarget, {bulkowner, reply, Id, Target, Msg, Parents}, _Reason}, State) ->
    bulkowner:send_reply_failed(Id, Target, Msg, Parents, self(), FailedTarget),
    State.

handle_delivery({bulk_read_entry, Issuer}, MyRange, Id, _Parents, State) ->
    Data = db_dht:get_entries(dht_node_state:get(State, db), MyRange),
    ReplyMsg = {bulk_read_entry_response, MyRange, Data},
    % for aggregation using a tree, activate this instead:
    % issue_send_reply(Id, Issuer, ReplyMsg, Parents);
    comm:send(Issuer, {bulkowner, reply, Id, ReplyMsg}),
    State;
handle_delivery({bulk_distribute, Proc, N, Msg1, Data}, MyRange, Id, Parents, State) ->
    ?TRACE("~p bulkowner: delivering {~p, ~p} locally to ~p",
           [self(),
            element(1, Msg1),
            element(2, Msg1),
            MyRange]),
    RangeData = get_range_data(Data, MyRange),
    %% only deliver data in MyRange as data outside of it was
    %% forwarded
    case Proc of
        dht_node ->
            gen_component:post_op({bulk_distribute, Id, MyRange,
                                   setelement(N, Msg1, RangeData), Parents},
                                  State);
        _ ->
            comm:forward_to_group_member(
              Proc, {bulk_distribute, Id, MyRange,
                     setelement(N, Msg1, RangeData), Parents}),
            State
    end;
handle_delivery({?send_to_group_member, Proc, Msg1}, MyRange, Id, Parents, State) ->
    comm:forward_to_group_member(
      Proc, {bulkowner, deliver, Id, MyRange, Msg1, Parents}),
    State;
handle_delivery({?send_to_registered_proc, Proc, Msg1}, MyRange, Id, Parents, State) ->
    comm:forward_to_registered_proc(
      Proc, {bulkowner, deliver, Id, MyRange, Msg1, Parents}),
    State;
handle_delivery({do_snapshot, _SnapNo, _Leader} = Msg, _MyRange, _Id, _Parents, State) ->
    gen_component:post_op(Msg, State);
handle_delivery(MrMsg , MyRange, _Id, _Parents, State) when mr =:= element(1, MrMsg) ->
    gen_component:post_op(erlang:append_element(MrMsg, MyRange), State);
handle_delivery({deliver_ping, Pid, Msg}, _MyRange, _Id, _Parents, State) ->
    gen_component:post_op({ping, Pid, {pong, _MyRange, _Id, Msg}}, State);
handle_delivery(Msg, _MyRange, _Id, _Parents, State) ->
    log:log(warn, "[ ~p ] cannot deliver message ~p (don't know how to handle)",
            [pid_groups:pid_to_name(self()), Msg]),
    State.

-spec get_range_data({ets, db_ets:db()} | [{?RT:key(), nonempty_string(), term()},...],
                     intervals:interval()) -> [{?RT:key(), nonempty_string(), term()}].
get_range_data({ets, ETS}, Range) ->
    lists:foldl(fun(Interval, Acc1) ->
                    db_ets:foldl(ETS,
                                 fun(E, Acc) -> [db_ets:get(ETS,E) | Acc] end,
                                 Acc1, Interval)
                end,
                [], intervals:get_simple_intervals(Range));
get_range_data(Data, Range) ->
    lists:filter(
      fun(Entry) ->
              intervals:in(element(1, Entry),
                           Range)
      end, Data).
