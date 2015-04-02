% @copyright 2012-2015 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Thorsten Schuett <schuett@zib.de>
%% @doc lease store based on rbrcseq.
%% @end
%% @version $Id$
-module(l_on_cseq).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("record_helpers.hrl").

-define(WARN(BOOL, BOOL2, TEXT),
        if
            (BOOL) andalso not (BOOL2) ->
                case util:is_unittest() of
                    true ->
                        ct:fail("loncq: " ++ TEXT);
                    false ->
                        log:log("loncq: " ++ TEXT)
                end;
            true ->
                ok
        end).

-export([read/1, read/2]).
%%-export([write/2]).

-export([on/2]).

-export([lease_renew/2, lease_renew/3]).
-export([lease_handover/3]).
-export([lease_takeover/2]).
-export([lease_takeover_after/3]).
-export([lease_split/5]).
-export([lease_merge/3]).
-export([lease_send_lease_to_node/3]).
-export([lease_split_and_change_owner/6]).
-export([disable_lease/2]).

-export([id/1]).

% for unit tests
-export([unittest_lease_update/3]).
-export([unittest_create_lease/1]).
-export([unittest_create_lease_with_range/3]).
-export([unittest_clear_lease_list/1]).
-export([unittest_get_delta/0]).

-export([get_db_for_id/1]).

% lease accessors
-export([get_version/1,set_version/2,
         get_epoch/1, set_epoch/2,
         new_timeout/0, set_timeout/1, get_timeout/1, get_pretty_timeout/1,
         get_id/1,
         get_owner/1, set_owner/2,
         get_aux/1, set_aux/2, is_live_aux_field/1,
         get_range/1, set_range/2,
         split_range/1,
         is_valid/1, has_timed_out/1,
         invalid_lease/0]).

-export([add_first_lease_to_db/2]).

-ifdef(with_export_type_support).
-export_type([lease_t/0, lease_id/0]).
-endif.

%% filters and checks for rbr_cseq operations
%% consistency

-type lease_id() :: ?RT:key().
-type lease_aux() ::
        empty
      | {change_owner, comm:mypid()}
      | {invalid, split, intervals:interval(), intervals:interval()}
      | {valid,   split, intervals:interval(), intervals:interval()}
      | {invalid, merge, intervals:interval(), intervals:interval()}
      | {invalid, merge, no_renew}
      | {valid,   merge, intervals:interval(), intervals:interval()}.

-record(lease, {
          id      = ?required(lease, id     ) :: lease_id(),
          epoch   = ?required(lease, epoch  ) :: non_neg_integer(),
          owner   = ?required(lease, owner  ) :: comm:mypid_plain() | nil,
          range   = ?required(lease, range  ) :: intervals:interval(),
          aux     = ?required(lease, aux    ) :: lease_aux(),
          version = ?required(lease, version) :: non_neg_integer(),
          timeout = ?required(lease, timeout) :: erlang_timestamp()}).
-type lease_t() :: #lease{}.

-type generic_failed_reason() :: lease_does_not_exist
                               | unexpected_id
                               | unexpected_owner
                               | unexpected_aux
                               | unexpected_range
                               | unexpected_timeout
                               | unexpected_epoch
                               | unexpected_version
                               | timeout_is_not_newer_than_current_lease.

-type update_failed_reason() :: lease_does_not_exist
                              | epoch_or_version_mismatch.

-type split_step1_failed_reason() :: lease_already_exists.

-type content_check_t() :: fun ((Current::lease_t() | prbr_bottom, 
                                 WriteFilter::prbr:write_filter(), 
                                 Next::lease_t()) ->
          {Result::boolean(), {Reason::generic_failed_reason(), Current::lease_t() | prbr_bottom, 
                               Next::lease_t()} | null}).

-spec delta() -> pos_integer().
delta() -> 10.

-spec unittest_get_delta() -> pos_integer().
unittest_get_delta() -> 
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    delta().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Public API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec lease_renew(lease_t(), active | passive) -> ok.
lease_renew(Lease, Mode) ->
    lease_renew(pid_groups:get_my(dht_node), Lease, Mode).

-spec lease_renew(comm:erl_local_pid(), lease_t(), active | passive) -> ok.
lease_renew(Pid, Lease, Mode) ->
    comm:send_local(Pid,
                    {l_on_cseq, renew, Lease, Mode}),
    ok.

-spec lease_handover(lease_t(), comm:mypid(), comm:erl_local_pid()) -> ok.
lease_handover(Lease, NewOwner, ReplyTo) ->
    % @todo precondition: i am owner of Lease
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, handover, Lease, NewOwner, ReplyTo}),
    ok.

-spec lease_takeover(lease_t(), comm:erl_local_pid()) -> ok.
lease_takeover(Lease, ReplyTo) ->
    % @todo precondition: Lease has timeouted
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, takeover, Lease, ReplyTo}),
    ok.

-spec lease_takeover_after(non_neg_integer(), lease_t(), comm:erl_local_pid()) -> ok.
lease_takeover_after(Delay, Lease, ReplyTo) ->
    % @todo precondition: Lease has timeouted
    msg_delay:send_local(Delay, pid_groups:get_my(dht_node),
                         {l_on_cseq, takeover, Lease, ReplyTo}),
    ok.

-spec lease_split(lease_t(), intervals:interval(),
                  intervals:interval(), first | second,
                  comm:erl_local_pid()) -> ok.
lease_split(Lease, R1, R2, Keep, ReplyTo) ->
    % @todo precondition: i am owner of Lease and id(R2) == id(Lease)
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, split, Lease, R1, R2, Keep, ReplyTo, empty}),
    ok.

-spec lease_merge(lease_t(), lease_t(), comm:erl_local_pid()) -> ok.
lease_merge(Lease1, Lease2, ReplyTo) ->
    % @todo precondition: i am owner of Lease1 and Lease2
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, merge, Lease1, Lease2, ReplyTo}),
    ok.

-spec lease_send_lease_to_node(Pid::comm:mypid(), Lease::lease_t(), active | passive) -> ok.
lease_send_lease_to_node(Pid, Lease, Mode) ->
    % @todo precondition: Pid is a dht_node
    comm:send(Pid, {l_on_cseq, send_lease_to_node, Lease, Mode}),
    ok.

-spec lease_split_and_change_owner(lease_t(),
                                   intervals:interval(), intervals:interval(),
                                   first | second,
                                   comm:mypid(), comm:erl_local_pid()) -> ok.
lease_split_and_change_owner(Lease, R1, R2, Keep, NewOwner, ReplyPid) ->
    % @todo precondition: i am owner of Lease and id(R2) == id(Lease)
    % @todo precondition: i am owner of Lease
    DHTNode = pid_groups:get_my(dht_node),
    SplitReply = comm:reply_as(DHTNode, 6,
                               {l_on_cseq, split_and_change_owner, Lease,
                                NewOwner, ReplyPid, '_'}),
    comm:send_local(DHTNode,
                    {l_on_cseq, split, Lease, R1, R2, Keep, SplitReply,
                     {change_owner, NewOwner}}),
    ok.

-spec disable_lease(State::dht_node_state:state(), Lease::lease_t()) -> dht_node_state:state().
disable_lease(State, Lease) ->
    lease_list:remove_lease_from_dht_node_state(Lease, State, passive).

% for unit tests
-spec unittest_lease_update(lease_t(), lease_t(), active | passive) -> ok | failed.
unittest_lease_update(Old, New, Mode) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, unittest_update, Old, New, Mode, self()}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {l_on_cseq, unittest_update_success, Old, New}, %% ->
            ok);
        ?SCALARIS_RECV(
            {l_on_cseq, unittest_update_failed, Old, New}, %% ->
            failed
          )
    end.

-spec unittest_clear_lease_list(Pid::comm:erl_local_pid()) -> ok.
unittest_clear_lease_list(Pid) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    comm:send_local(Pid,
                    {l_on_cseq, unittest_clear_lease_list, comm:this()}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {l_on_cseq, unittest_clear_lease_list_success}, %% ->
            ok
          )
    end.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_component
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec on(comm:message(), dht_node_state:state()) -> dht_node_state:state() | kill.
on({l_on_cseq, split_and_change_owner, _Lease, NewOwner, ReplyPid, SplitResult}, State) ->
    case SplitResult of
        {split, success, L2, _L1} ->
            gen_component:post_op({l_on_cseq, handover, L2, NewOwner, ReplyPid},
                                  State);
        {split, fail, L1} ->
            comm:send_local(ReplyPid, {split, fail, L1}),
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease renewal
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, renew, Old = #lease{owner=Owner,epoch=OldEpoch,version=OldVersion}, Mode},
   State) ->
    %log:pal("on renew ~w (~w, ~w)~n", [Old, Mode, self()]),
    Self = comm:this(),
    {New, Renew} = 
        case get_aux(Old) of
            % change owner to self -> remove aux
            {change_owner, Self} ->
                {Old#lease{aux=empty,version=OldVersion+1, timeout=new_timeout()}, renew};
            _ ->
                case comm:this() of
                    Owner ->
                        {Old#lease{version=OldVersion+1, timeout=new_timeout()}, renew};
                    _ ->
                        % we are trying to recover
                        log:log("trying to recover: owner=~p id=~p, self=~p", 
                                [Owner, get_id(Old), comm:this()]),
                        {Old#lease{owner = comm:this(), epoch = OldEpoch+1, version=0, 
                                  timeout=new_timeout()}, renew_recover}
                end
          end,
    ContentCheck = generic_content_check(Old, New, Renew),
%% @todo New passed for debugging only:
    ReplyTo = comm:reply_as(self(), 3, {l_on_cseq, renew_reply, '_', New, Mode, Renew}),
    update_lease(ReplyTo, ContentCheck, Old, New, State),
    State;

on({l_on_cseq, renew_reply, {qwrite_done, _ReqId, Round, Value}, _New, Mode, _Renew}, State) ->
    %% log:pal("successful renew~n~w~n~w~n", [Value, l_on_cseq:get_id(Value)]),
    lease_list:update_lease_in_dht_node_state(Value,
                                              lease_list:update_next_round(l_on_cseq:get_id(Value),
                                                                           Round, State),
                                              Mode, renew);

on({l_on_cseq, renew_reply,
    {qwrite_deny, _ReqId, Round, Value, {content_check_failed, {Reason, _Current, _Next}}}, 
    _New, Mode, Renew}, State) ->
    % @todo retry
    ?TRACE("renew denied: ~p~nVal: ~p~nNew: ~p~n~p~n", [Reason, Value, _New, Mode]),
    ?TRACE("id: ~p~n", [dht_node_state:get(State, node_id)]),
    ?TRACE("lease list: ~p~n", [dht_node_state:get(State, lease_list)]),
    ?TRACE("timeout: ~p~n", [calendar:now_to_local_time(get_timeout(Value))]),
    case Reason of
        lease_does_not_exist ->
            case Value of %@todo is this necessary?
                prbr_bottom ->
                    State;
                _ ->
                    lease_list:remove_lease_from_dht_node_state(Value, State,
                                                                Mode)
            end;
        unexpected_owner   ->
            CurrentOwner = get_owner(Value),
            case {comm:this(), Renew} of
                {CurrentOwner, renew_recover} ->
                    % the owner was already changed in a recover
                    State;
                _ ->
                    lease_list:remove_lease_from_dht_node_state(Value, State, Mode)
            end;
        unexpected_aux     ->
            case get_aux(Value) of
                empty                  ->
                    renew_and_update_round(Value, Round, Mode, State);
                {invalid, split, _, _} ->
                    renew_and_update_round(Value, Round, Mode, State);
                {invalid, merge, _, _} ->
                    renew_and_update_round(Value, Round, Mode, State);
                {invalid, merge, no_renew} ->
                    lease_list:remove_lease_from_dht_node_state(Value, State, Mode);
                {valid, split, _, _}   ->
                    renew_and_update_round(Value, Round, Mode, State);
                {valid, merge, _, _}   ->
                    renew_and_update_round(Value, Round, Mode, State);
                {change_owner, _Pid}  ->
                    renew_and_update_round(Value, Round, Mode, State)
            end;
        unexpected_range   ->
            renew_and_update_round(Value, Round, Mode, State);
        unexpected_timeout ->
            renew_and_update_round(Value, Round, Mode, State);
        unexpected_epoch   ->
            renew_and_update_round(Value, Round, Mode, State);
        unexpected_version ->
            renew_and_update_round(Value, Round, Mode, State);
        timeout_is_not_newer_than_current_lease ->
                    renew_and_update_round(Value, Round, Mode, State)
    end;

on({l_on_cseq, send_lease_to_node, Lease, Mode}, State) ->
    % @todo do we need any checks?
    % @todo do i need to notify rm about the new range?
    ?TRACE("send_lease_to_node ~p ~p~n", [self(), Lease]),
    lease_list:update_lease_in_dht_node_state(Lease, State, Mode, received_lease);


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease update (only for unit tests)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, unittest_update,
    Old = #lease{id=Id, epoch=OldEpoch,version=OldVersion},
    New, Mode, Caller}, State) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    %% io:format("renew ~p~n", [Old]),
    ContentCheck = is_valid_update(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    %% @todo New passed for debugging only:
    Self = comm:reply_as(self(), 3, {l_on_cseq, unittest_update_reply, '_',
                                     Old, New, Mode, Caller}),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, New),
    State;

on({l_on_cseq, unittest_update_reply, {qwrite_done, _ReqId, _Round, Value},
    Old, New, Mode, Caller}, State) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    %% io:format("successful update~n", []),
    comm:send_local(Caller, {l_on_cseq, unittest_update_success, Old, New}),
    lease_list:update_lease_in_dht_node_state(Value, State, Mode, unittest);

on({l_on_cseq, unittest_update_reply,
    {qwrite_deny, _ReqId, _Round, _Value, {content_check_failed, _Reason}},
    Old, New, _Mode, Caller}, State) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    comm:send_local(Caller, {l_on_cseq, unittest_update_failed, Old, New}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% clear lease list (only for unit tests)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, unittest_clear_lease_list, Pid}, State) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    comm:send(Pid, {l_on_cseq, unittest_clear_lease_list_success}),
    dht_node_state:set_lease_list(State, lease_list:empty());

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease handover
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, handover, Old = #lease{epoch=OldEpoch},
    NewOwner, ReplyTo}, State) ->
    %log:log("handover with aux= ~p", [Old]),
    New = case get_aux(Old) of
              empty ->
                  Old#lease{epoch   = OldEpoch + 1,
                            owner   = NewOwner,
                            version = 0,
                            timeout = new_timeout()};
              {change_owner, NewOwner} ->
                  Old#lease{epoch   = OldEpoch + 1,
                            owner   = NewOwner,
                            aux     = empty,
                            version = 0,
                            timeout = new_timeout()}
          end,
    ContentCheck = generic_content_check(Old, New, handover),
    Self = comm:reply_as(self(), 3, {l_on_cseq, handover_reply, '_', ReplyTo,
                                     NewOwner, New}),
    update_lease(Self, ContentCheck, Old, New, State),
    State;


on({l_on_cseq, handover_reply, {qwrite_done, _ReqId, _Round, Value}, ReplyTo,
    _NewOwner, _New}, State) ->
    % @todo if success update lease in State
    ?TRACE("successful handover ~p~n", [Value]),
    comm:send_local(ReplyTo, {handover, success, Value}),
    lease_list:update_lease_in_dht_node_state(Value, State, passive, handover);

on({l_on_cseq, handover_reply, {qwrite_deny, _ReqId, _Round, Value,
                                {content_check_failed, {Reason, _Current, _Next}}},
    ReplyTo, NewOwner, _New}, State) ->
    ?TRACE("handover denied: ~p ~p ~p~n", [Reason, Value, _New]),
    case Reason of
        lease_does_not_exist ->
            comm:send_local(ReplyTo, {handover, failed, Value}),
            case Value of %@todo is this necessary?
                prbr_bottom ->
                    State;
                _ ->
                    lease_list:remove_lease_from_dht_node_state(Value, State, any)
            end;
        unexpected_owner   ->
            comm:send_local(ReplyTo, {handover, failed, Value}),
            lease_list:remove_lease_from_dht_node_state(Value, State, any);
        unexpected_aux     ->
            comm:send_local(ReplyTo, {handover, failed, Value}), State;
        unexpected_range   ->
            comm:send_local(ReplyTo, {handover, failed, Value}), State;
        unexpected_timeout -> lease_handover(Value, NewOwner, ReplyTo), State;
        unexpected_epoch   -> lease_handover(Value, NewOwner, ReplyTo), State;
        unexpected_version -> lease_handover(Value, NewOwner, ReplyTo), State;
        timeout_is_not_newer_than_current_lease ->
            lease_handover(Value, NewOwner, ReplyTo),
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease takeover
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, takeover, Old = #lease{epoch=OldEpoch}, 
    ReplyTo}, State) ->
    New = Old#lease{epoch   = OldEpoch + 1,
                    version = 0,
                    owner   = comm:this(),
                    timeout = new_timeout()},
    ContentCheck = generic_content_check(Old, New, takeover),
    Self = comm:reply_as(self(), 4, {l_on_cseq, takeover_reply, ReplyTo, '_'}),
    update_lease(Self, ContentCheck, Old, New, State),
    State;


on({l_on_cseq, takeover_reply, ReplyTo,
    {qwrite_done, _ReqId, _Round, Value}}, State) ->
    %% log:log("takeover success ~p~n", [Value]),
    comm:send_local(ReplyTo, {takeover, success, Value}),
    lease_list:update_lease_in_dht_node_state(Value, State, passive, takeover);

on({l_on_cseq, takeover_reply, ReplyTo,
    {qwrite_deny, _ReqId, _Round, Value, 
     {content_check_failed, {Reason, _Current, _Next}}}}, State) ->
    ?TRACE("takeover failed ~p ~p~n", [Value, Reason]),
    comm:send_local(ReplyTo, {takeover, failed, Value, Reason}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step1)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge, _L1 = #lease{}, _L2 = empty, _ReplyTo}, State) ->
    ?TRACE("trying to merge with empty lease ?!?", []),
    State;

on({l_on_cseq, merge, L1 = #lease{epoch=OldEpoch}, L2, ReplyTo}, State) ->
    New = L1#lease{epoch    = OldEpoch + 1,
                   version = 0,
                   aux     = {invalid, merge, get_range(L1), get_range(L2)},
                   timeout = new_timeout()},
    ContentCheck = generic_content_check(L1, New, merge_step1),
    Self = comm:reply_as(self(), 5, {l_on_cseq, merge_reply_step1,
                                     L2, ReplyTo, '_'}),
    update_lease(Self, ContentCheck, L1, New, State),
    State;

on({l_on_cseq, merge_reply_step1, L2, ReplyTo,
    {qwrite_deny, _ReqId, Round, L1, {content_check_failed, 
                                      {Reason, _Current, _Next}}}}, State) ->
    % @todo if success update lease in State
    ?TRACE("merge step1 failed~n~w~n~w~n~w~n", [Reason, L1, L2]),
    % retry?
    case Reason of
        %lease_does_not_exist ->
        %  % cannot happen
        %  State;
        %unexpected_id ->
        %  % cannot happen
        %    State;
        unexpected_owner ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_aux ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_range ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_timeout ->
            % retry
            NextState = lease_list:update_next_round(l_on_cseq:get_id(L1),
                                                     Round, State),
            gen_component:post_op({l_on_cseq, merge, L1, L2, ReplyTo}, NextState);
        %unexpected_epoch ->
        %    % cannot happen
        %    gen_component:post_op({l_on_cseq, merge_reply_step1, L2, ReplyTo,
        %                           {qwrite_done, fake_reqid, fake_round, L1}},
        %                          lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State));
        %unexpected_version ->
        %    % cannot happen
        %    gen_component:post_op({l_on_cseq, merge_reply_step1, L2, ReplyTo,
        %                           {qwrite_done, fake_reqid, fake_round, L1}},
        %                          lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State));
        timeout_is_not_newer_than_current_lease ->
            % retry
            NextState = lease_list:update_next_round(l_on_cseq:get_id(L1),
                                                     Round, State),
            gen_component:post_op({l_on_cseq, merge, L1, L2, ReplyTo}, NextState)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step2)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step1, L2 = #lease{epoch=OldEpoch}, ReplyTo,
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    % @todo if success update lease in State
    New = L2#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   range   = intervals:union(L1#lease.range, L2#lease.range),
                   aux     = {valid, merge, get_range(L1), get_range(L2)},
                   timeout = new_timeout()},
    ContentCheck = generic_content_check(L2, New, merge_step2),
    Self = comm:reply_as(self(), 5, {l_on_cseq, merge_reply_step2,
                                     L1, ReplyTo, '_'}),
    update_lease(Self, ContentCheck, L2, New, State),
    lease_list:update_lease_in_dht_node_state(L1, State, passive,
                                              merge_reply_step1);



on({l_on_cseq, merge_reply_step2, L1, ReplyTo,
    {qwrite_deny, _ReqId, Round, L2,
     {content_check_failed, {Reason, _Current, _Next}}}}, State) ->
    % @todo if success update lease in State
    ?TRACE("merge step2 failed~n~w~n~w~n~w~n~w~n~w~n", [Reason, L1, L2, _Current, _Next]),
    case Reason of
        %lease_does_not_exist ->
        %    % cannot happen
        %    State;
        %unexpected_id ->
        %    % cannot happen
        %    State;
        unexpected_owner ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_aux ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_range ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_timeout ->
            % retry
            gen_component:post_op({l_on_cseq, merge_reply_step1, L2, ReplyTo,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State));
        %unexpected_epoch ->
        %    % cannot happen
        %    gen_component:post_op({l_on_cseq, merge_reply_step1, L2, ReplyTo,
        %                           {qwrite_done, fake_reqid, fake_round, L1}},
        %                          lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State));
        %unexpected_version ->
        %    % cannot happen
        %    gen_component:post_op({l_on_cseq, merge_reply_step1, L2, ReplyTo,
        %                           {qwrite_done, fake_reqid, fake_round, L1}},
        %                          lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State));
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op({l_on_cseq, merge_reply_step1, L2, ReplyTo,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State))
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step3)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step2, L1 = #lease{epoch=OldEpoch}, ReplyTo,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    % @todo if success update lease in State
    %log:pal("merge step3~n~w~n~w", [L1, L2]),
    New = L1#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   aux     = {invalid, merge, no_renew},
                   timeout = new_timeout()},
    ContentCheck = generic_content_check(L1, New, merge_step3),
    Self = comm:reply_as(self(), 5, {l_on_cseq, merge_reply_step3,
                                     L2, ReplyTo, '_'}),
    update_lease(Self, ContentCheck, L1, New, State),
    lease_list:update_lease_in_dht_node_state(L2, State, active,
                                              merge_reply_step2);

on({l_on_cseq, merge_reply_step3, L2, ReplyTo,
    {qwrite_deny, _ReqId, Round, L1, {content_check_failed, 
                                      {Reason, _Current, _Next}}}}, State) ->
    % @todo if success update lease in State
    ?TRACE("merge step3 failed~n~w~n~w~n~w~n", [Reason, L1, L2]),
    case Reason of
        %lease_does_not_exist ->
        %  % cannot happen
        %  State;
        %unexpected_id ->
        %  % cannot happen
        %    State;
        unexpected_owner ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_aux ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_range ->
            % give up, there was probably a concurrent merge
            State;
        unexpected_timeout ->
            % retry
            NextState = lease_list:update_next_round(l_on_cseq:get_id(L1),
                                                     Round, State),
            gen_component:post_op({l_on_cseq, merge, L1, L2, ReplyTo}, NextState);
        %unexpected_epoch ->
        %    % cannot happen
        %    gen_component:post_op({l_on_cseq, merge_reply_step1, L2, ReplyTo,
        %                           {qwrite_done, fake_reqid, fake_round, L1}},
        %                          lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State));
        %unexpected_version ->
        %    % cannot happen
        %    gen_component:post_op({l_on_cseq, merge_reply_step1, L2, ReplyTo,
        %                           {qwrite_done, fake_reqid, fake_round, L1}},
        %                          lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State));
        timeout_is_not_newer_than_current_lease ->
            % retry
            NextState = lease_list:update_next_round(l_on_cseq:get_id(L1),
                                                     Round, State),
            gen_component:post_op({l_on_cseq, merge, L1, L2, ReplyTo}, NextState)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step4)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step3, L2 = #lease{epoch=OldEpoch}, ReplyTo,
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    % @todo if success update lease in State
    ?TRACE("successful merge step3 ~p~n", [L1]),
    New = L2#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   aux     = empty,
                   timeout = new_timeout()},
    ContentCheck = generic_content_check(L2, New, merge_step4),
    Self = comm:reply_as(self(), 5, {l_on_cseq, merge_reply_step4,
                                     L1, ReplyTo, '_'}),
    update_lease(Self, ContentCheck, L2, New, State),
    lease_list:remove_lease_from_dht_node_state(L1, State, passive);

on({l_on_cseq, merge_reply_step4, L1, ReplyTo,
    {qwrite_done, _ReqId, Round, L2}}, State) ->
    ?TRACE("successful merge ~p~p~n", [ReplyTo, L2]),
    comm:send_local(ReplyTo, {merge, success, L2, L1}),
    lease_list:update_lease_in_dht_node_state(L2,
                                              lease_list:update_next_round(l_on_cseq:get_id(L2),
                                                                           Round, State),
                                              active,
                                              merge_reply_step3);

on({l_on_cseq, merge_reply_step4, L1, ReplyTo,
    {qwrite_deny, _ReqId, Round, L2, {content_check_failed, 
                                      {Reason, _Current, _Next}}}}, State) ->
    % @todo if success update lease in State
    ?TRACE("merge step4 failed~n~w~n~w~n~w~n", [Reason, L1, L2]),
    % retry?
    case Reason of
        unexpected_timeout ->
            % retry
            gen_component:post_op({l_on_cseq, merge_reply_step3, L2, ReplyTo,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State));
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op({l_on_cseq, merge_reply_step3, L2, ReplyTo,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  lease_list:update_next_round(l_on_cseq:get_id(L2), Round, State))
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step1)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split, Lease, R1, R2, Keep, ReplyTo, PostAux}, State) ->
    Id = id(R1),
    ?TRACE("split first step: creating new lease L1(~w) (~p)~n", [self(), Id]),
    _Active = get_active_lease(State),
    ?TRACE("going to split(~w):~n~w~n~w~n", [self(), _Active, Lease]),
    New = #lease{id      = id(R1),
                 epoch   = 1,
                 owner   = comm:this(),
                 range   = R1,
                 aux     = {invalid, split, R1, R2},
                 version = 0,
                 timeout = new_timeout()},
    ContentCheck = is_valid_split_step1(),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 9, {l_on_cseq, split_reply_step1, Lease, R1, R2,
                                     Keep, ReplyTo, PostAux, '_'}),
    %log:log("self in split firststep: ~w", [Self]),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, New),
    State;

on({l_on_cseq, split_reply_step1, _Lease, _R1, _R2, _Keep, ReplyTo, _PostAux,
    {qwrite_deny, _ReqId, _Round, Lease, {content_check_failed, Reason}}}, State) ->
    ?TRACE("split first step failed: ~p~n", [Reason]),
    case Reason of
        lease_already_exists ->
            comm:send_local(ReplyTo, {split, fail, Lease}),
            State
            %lease_list:remove_lease_from_dht_node_state(Lease, State)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step2)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split_reply_step1, L2=#lease{id=_Id,epoch=OldEpoch}, R1, R2,
    Keep, ReplyTo, PostAux, {qwrite_done, _ReqId, _Round, L1}}, State) ->
    ?TRACE("split second step(~w): updating L2 (~p)~n", [self(), _Id]),
    _Active = get_active_lease(State),
    ?TRACE("split second step(~w):~n~w~n~w~n~w~n", [self(), _Active, L1, L2]),
    New = L2#lease{
            epoch   = OldEpoch + 1,
            range   = R2,
            aux     = {valid, split, R1, R2},
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(L2, New, split_reply_step1),
    Self = comm:reply_as(self(), 9, {l_on_cseq, split_reply_step2,
                                     L1, R1, R2, Keep, ReplyTo, PostAux, '_'}),
    update_lease(Self, ContentCheck, L2, New, State),
    case Keep of
        first -> ?ASSERT2(false, nyi), % TS: not supported at the moment
                 lease_list:update_lease_in_dht_node_state(L1, State, active,
                                                           split_reply_step1);
        second -> lease_list:update_lease_in_dht_node_state(L1, State, passive,
                                                            split_reply_step1)

    end;

on({l_on_cseq, split_reply_step2, L1, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_deny, _ReqId, _Round, L2, {content_check_failed, 
                                       {Reason, _Current, _Next}}}}, State) ->
    ?TRACE("split second step failed: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send_local(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_owner     -> comm:send_local(ReplyTo, {split, fail, L2}),
                                lease_list:remove_lease_from_dht_node_state(L2, State, any); %@todo
        unexpected_range     -> comm:send_local(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_aux       -> comm:send_local(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_timeout ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step1, L2, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  State);
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step1, L2, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  State);
        unexpected_epoch ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step1, L2, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  State);
        unexpected_version ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step1, L2, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  State)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step3)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split_reply_step2,
    L1 = #lease{id=_Id,epoch=OldEpoch}, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    ?TRACE("split third step(~w): renew L1 ~p~n", [self(), _Id]),
    _Active = get_active_lease(State),
    ?TRACE("split_reply_step2(~w):~n~w~n~w~n~w~n", [self(), _Active, L1, L2]),
    New = L1#lease{
            epoch   = OldEpoch + 1,
            aux     = PostAux,
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(L1, New, split_reply_step2),
    Self = comm:reply_as(self(), 9, {l_on_cseq, split_reply_step3, L2, R1, R2,
                                     Keep, ReplyTo, PostAux, '_'}),
    update_lease(Self, ContentCheck, L1, New, State),
    lease_list:update_lease_in_dht_node_state(L2, State, active, split_reply_step2);

on({l_on_cseq, split_reply_step3, L2, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_deny, _ReqId, _Round, L1, {content_check_failed, 
                                       {Reason, _Current, _Next}}}}, State) ->
    % @todo
    ?TRACE("split third step failed: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send_local(ReplyTo, {split, fail, L1}), State; %@todo
        unexpected_owner     -> comm:send_local(ReplyTo, {split, fail, L1}),
                                lease_list:remove_lease_from_dht_node_state(L1, State, any); %@todo
        unexpected_range     -> comm:send_local(ReplyTo, {split, fail, L1}), State; %@todo
        unexpected_aux       -> comm:send_local(ReplyTo, {split, fail, L1}), State; %@todo
        unexpected_timeout ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step2, L1, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L2}},
                                  State);
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step2, L1, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L2}},
                                  State);
        unexpected_epoch ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step2, L1, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L2}},
                                  State);
        unexpected_version ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step2, L1, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L2}},
                                  State)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step4)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split_reply_step3,
    L2 = #lease{id=_Id,epoch=OldEpoch}, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    ?TRACE("split fourth step: renew L2 ~p ~p ~p ~p~n", [R1, R2, _Id, PostAux]),
    New = L2#lease{
            epoch   = OldEpoch + 1,
            aux     = empty,
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(L2, New, split_reply_step3),
    Self = comm:reply_as(self(), 9, {l_on_cseq, split_reply_step4, L1, R1, R2,
                                     Keep, ReplyTo, PostAux, '_'}),
    update_lease(Self, ContentCheck, L2, New, State),
    lease_list:update_lease_in_dht_node_state(L1, State, passive, split_reply_step3);

on({l_on_cseq, split_reply_step4, L1, _R1, _R2, _Keep, ReplyTo, _PostAux,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    ?TRACE("successful split~n", []),
    ?TRACE("successful split ~p~n", [ReplyTo]),
    _Active = get_active_lease(State),
    ?TRACE("split_reply_step4(~w):~n~w~n~w~n~w~n", [self(), _Active, L1, L2]),
    comm:send_local(ReplyTo, {split, success, L1, L2}),
    lease_list:update_lease_in_dht_node_state(L2, State, active, split_reply_step4);

on({l_on_cseq, split_reply_step4, L1, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_deny, _ReqId, _Round, L2, {content_check_failed, 
                                       {Reason, _Current, _Next}}}}, State) ->
    % @todo
    ?TRACE("split fourth step: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send_local(ReplyTo, {split, fail, L2}), State;
        unexpected_owner     -> comm:send_local(ReplyTo, {split, fail, L2}),
                                lease_list:remove_lease_from_dht_node_state(L2, State, active);
        unexpected_range     -> comm:send_local(ReplyTo, {split, fail, L2}), State;
        unexpected_aux       -> comm:send_local(ReplyTo, {split, fail, L2}), State;
        unexpected_timeout ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step3, L2, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  State);
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step3, L2, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  State);
        unexpected_epoch ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step3, L2, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  State);
        unexpected_version ->
            % retry
            gen_component:post_op({l_on_cseq, split_reply_step3, L2, R1, R2,
                                   Keep, ReplyTo, PostAux,
                                   {qwrite_done, fake_reqid, fake_round, L1}},
                                  State)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% garbage collector results
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, garbage_collector, {merge, success, _, _}}, State) ->
    log:pal("garbage collector: success~n"),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% renew all local leases
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, renew_leases}, State) ->
    LeaseList = dht_node_state:get(State, lease_list),
    ActiveLease = lease_list:get_active_lease(LeaseList),
    PassiveLeaseList = lease_list:get_passive_leases(LeaseList),
    % log:pal("renewing local leases: ~p~n", [ActiveLease]),
    case ActiveLease of
        empty -> ok;
        _ ->
            lease_renew(self(), ActiveLease, active)
    end,
    _ = [lease_renew(self(), L, passive) ||
            L <- PassiveLeaseList, get_aux(L) =/= {invalid, merge, no_renew}],
    msg_delay:send_trigger(delta() div 2, {l_on_cseq, renew_leases}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% content checks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% % @doc generic content check. used for almost all qwrite operations
%% -spec generic_content_check_(lease_t(), lease_t(), atom()) -> 
%%                                    content_check_t(). %% content check
%% generic_content_check_(#lease{id=OldId,owner=_OldOwner,aux = OldAux,range=OldRange,
%%                              epoch=OldEpoch,version=OldVersion,timeout=OldTimeout} = Old,
%%                      New, Writer) ->
%%     fun(prbr_bottom, _WriteFilter, Next) ->
%%             {false, {lease_does_not_exist, prbr_bottom, Next}};
%%         (Current, _WriteFilter, Next) when Current =:= New ->
%%             log:pal("re-write in CC:~n~w~n~w~n~w~n~w~n~w~n", [Current, Next, Old, New, Writer]),
%%             {true, null};
%%         (#lease{id = Id0} = Current, _, Next)    when Id0 =/= OldId->
%%             {false, {unexpected_id, Current, Next}};
%%         %(#lease{owner = O0} = Current, _, Next)    when O0 =/= OldOwner->
%%         %    {false, {unexpected_owner, Current, Next}};
%%         (#lease{aux = Aux0} = Current, _, Next)    when Aux0 =/= OldAux->
%%             {false, {unexpected_aux, Current, Next}};
%%         (#lease{range = R0} = Current, _, Next)    when R0 =/= OldRange->
%%             {false, {unexpected_range, Current, Next}};
%%         (#lease{timeout = T0} = Current, _, Next)                   when T0 =/= OldTimeout->
%%             {false, {unexpected_timeout, Current, Next}};
%%         (#lease{epoch = E0} = Current, _, Next)                     when E0 =/= OldEpoch ->
%%             {false, {unexpected_epoch, Current, Next}};
%%         (#lease{version = V0} = Current, _, Next)                   when V0 =/= OldVersion->
%%             {false, {unexpected_version, Current, Next}};
%%         (#lease{timeout = T0} = Current, _, #lease{timeout = T1} = Next)  when not (T0 < T1)->
%%             {false, {timeout_is_not_newer_than_current_lease, Current, Next}};
%%         (_, _, _) ->
%%             {true, null}
%%     end.

% @doc generic content check. used for almost all qwrite operations
-spec generic_content_check(lease_t(), lease_t(), atom()) -> 
                                   content_check_t(). %% content check
generic_content_check(#lease{id=OldId,owner=OldOwner,aux = OldAux,range=OldRange,
                             epoch=OldEpoch,version=OldVersion,timeout=OldTimeout} = Old,
                     #lease{id=NewId,owner=NewOwner,aux = NewAux,range=NewRange,
                             epoch=NewEpoch,version=NewVersion,timeout=NewTimeout} = New, Writer) ->
    fun(Current, _WriteFilter, Next) ->
            case Current of
                % check for prbr_bottom 
%                {prbr_bottom, _WriteFilter, _Next} ->
                prbr_bottom ->
                    {false, {lease_does_not_exist, prbr_bottom, Next}};
                % check for re-write
                New -> % Current =:= New
                    log:pal("re-write in CC:~n~w~n~w~n~w~n~w~n~w~n", 
                            [Current, Next, Old, New, Writer]),
                    {true, null};
                % special case for renew after crash-recovery
                #lease{epoch = E0, owner = O0, version = V0} 
                  when E0 =:= OldEpoch andalso V0 =:= OldVersion andalso O0 =:= OldOwner
                       andalso NewOwner =/= OldOwner andalso Writer =:= renew_recover ->
                       % after a crash the logical owner should not
                       % have changed. however its pid will have
                       % changed. this special case checks that epoch
                       % and version are correctly guessed, but the
                       % owner is wrong. in addition, we require that
                       % this is a renew.
                    ?TRACE("loncq: this has to be a renew after a recovery", []),
                    {true, null};
                % check that epoch and version match with Old
                % we only warn/fail if the remaining fields do not match
                #lease{epoch = E0, version = V0} when  E0 =:= OldEpoch andalso V0 =:= OldVersion ->
                    % @todo sanity check with warnings: protocol was implemented correctly
                    EpochUpdate = (NewEpoch =:= OldEpoch + 1) andalso (NewVersion =:= 0),
                    VersionUpdate = (NewEpoch =:= OldEpoch) andalso (NewVersion =:= OldVersion + 1),
                    % check for correct epoch/version handling
                    ?WARN(OldEpoch =:= NewEpoch, 
                          OldVersion + 1 =:= NewVersion,
                         "the version has to increase by one"),
                    ?WARN(OldEpoch + 1 =:= NewEpoch, 
                          0 =:= NewVersion,
                         "the version has to be zero after an epoch update: " 
                             ++ atom_to_list(Writer)),
                    % check that the id does not change
                    ?WARN(OldId =/= NewId, 
                          false, 
                          "the id may never change: " 
                              ++ atom_to_list(Writer)),
                    % check for correct behavior for the remaining fields
                    ?WARN(OldOwner =/= NewOwner, 
                          EpochUpdate, 
                          "the owner changed without an epoch update: " 
                              ++ atom_to_list(Writer)),
                    ?WARN(OldRange =/= NewRange, 
                         EpochUpdate,
                         "the range changed without an epoch update: " 
                             ++ atom_to_list(Writer)),
                    ?WARN(OldAux =/= NewAux, 
                         EpochUpdate,
                         "the aux changed without an epoch update: " 
                             ++ atom_to_list(Writer)),
                    ?WARN(OldTimeout =/= NewTimeout, 
                         VersionUpdate orelse EpochUpdate,
                         "the timeout changed without a version update: " 
                             ++ atom_to_list(Writer)),
                    ?WARN(OldTimeout =/= NewTimeout, 
                         OldTimeout < NewTimeout,
                         "the new timeout is not newer than the old: " 
                             ++ atom_to_list(Writer)),
                    {true, null};
                % epoch and/or version did not match: give useful error message
                #lease{id = Id0, epoch = E0, owner = O0, range = R0, aux = Aux0, 
                       version = V0, timeout = T0} ->
                    if
                        Id0 =/= OldId ->
                            {false, {unexpected_id, Current, Next}};
                        O0 =/= OldOwner ->
                            {false, {unexpected_owner, Current, Next}};
                        Aux0 =/= OldAux ->
                            {false, {unexpected_aux, Current, Next}};
                        R0 =/= OldRange ->
                            {false, {unexpected_range, Current, Next}};
                        T0 =/= OldTimeout ->
                            {false, {unexpected_timeout, Current, Next}};
                        E0 =/= OldEpoch ->
                            {false, {unexpected_epoch, Current, Next}};
                        V0 =/= OldVersion ->
                            {false, {unexpected_version, Current, Next}}%;
                        %not (T0 < NewTimeout) ->
                        %    {false, {timeout_is_not_newer_than_current_lease, Current, Next}}
                    end
            end
    end.
                            

% @doc only for unittests
-spec is_valid_update(non_neg_integer(), non_neg_integer()) ->
    fun ((Current::lease_t(), WriteFilter::prbr:write_filter(), Next::lease_t()) -> 
                {Result::boolean(), update_failed_reason() | null}). %% content check
is_valid_update(CurrentEpoch, CurrentVersion) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    fun (#lease{epoch = E0}, _, _)                     when E0 =/= CurrentEpoch ->
            %% log:pal("is_valid_update: expected ~p, got ~p", [CurrentEpoch, E0]),
            {false, epoch_or_version_mismatch};
        (#lease{version = V0}, _, _)                   when V0 =/= CurrentVersion->
            %% log:pal("is_valid_update: expected ~p, got ~p", [CurrentVersion, V0]),
            {false, epoch_or_version_mismatch};
        (_Current, _WriteFilter, _Next) ->
            {true, null}
    end.

%@doc only content check which allows to create a new lease
-spec is_valid_split_step1() -> 
        fun ((Current::lease_t(), WriteFilter::prbr:write_filter(), Next::lease_t()) -> 
                     {Result::boolean(), split_step1_failed_reason() | null}). %% content check
is_valid_split_step1() ->
    fun (Current, _WriteFilter, _Next) ->
            case Current == prbr_bottom of
                true ->
                    {true, null};
                false ->
                    {false, lease_already_exists}
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% util
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec read(lease_id()) -> api_tx:read_result().
read(Key) ->
    read(Key, self()),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({qread_done, _ReqId, _Round, Value},
                       case Value of
                           no_value_yet -> {fail, not_found};
                           _ -> {ok, Value}
                       end
                      )
        end.

-spec read(lease_id(), comm:erl_local_pid()) -> ok.
read(Key, Pid) ->
    %% decide which lease db is responsible, ie. if the key is from
    %% the first quarter of the ring, use lease_db1, if from 2nd
    %% quarter -> use lease_db2, ...
    DB = get_db_for_id(Key),
    %% perform qread
    rbrcseq:qread(DB, Pid, Key).

%% write(Key, Value, ContentCheck) ->
%%     %% decide which lease db is responsible, ie. if the key is from
%%     %% the first quarter of the ring, use lease_db1, if from 2nd
%%     %% quarter -> use lease_db2, ...
%%     DB = get_db_for_id(Key),
%%     rbrcseq:qwrite(DB, self(), Key, ContentCheck, Value),
%%     trace_mpath:thread_yield(),
%%     receive
%%         ?SCALARIS_RECV({qwrite_done, _ReqId, _Round, _Value}, {ok} ) %%;
%%         %% ?SCALARIS_RECV({qwrite_deny, _ReqId, _Round, _Value, Reason}, {fail, timeout} )
%%         end.

%% -spec write(lease_id(), lease_t()) -> api_tx:write_result().
%% write(Key, Value) ->
%%     write(Key, Value, fun l_on_cseq:is_valid_state_change/3).

-spec add_first_lease_to_db(?RT:key(), dht_node_state:state()) ->
                                  dht_node_state:state().
add_first_lease_to_db(Id, State) ->
    DB = get_db_for_id(Id),
    Lease = #lease{id      = Id, %% set to 0 in dht_node
                   epoch   = 1,
                   owner   = comm:this(),
                   range   = intervals:all(),
                   aux     = empty,
                   version = 1,
                   timeout = new_timeout()
                  },
    DBHandle = dht_node_state:get(State, DB),
    _ = [ begin
              Entry = prbr:new(X, Lease),
              prbr:set_entry(Entry, DBHandle)
          end || X <- ?RT:get_replica_keys(Id) ],
    dht_node_state:set_lease_list(State,
                                  lease_list:make_lease_list(Lease, [], [])).

-spec unittest_create_lease(?RT:key()) -> lease_t().
unittest_create_lease(Id) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    #lease{id      = Id,
           epoch   = 1,
           owner   = comm:this(),
           range   = intervals:all(),
           aux     = empty,
           version = 1,
           timeout = new_timeout()
          }.

-spec unittest_create_lease_with_range(?RT:key(), ?RT:key(), comm:mypid_plain()) -> lease_t().
unittest_create_lease_with_range(From, To, Owner) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    Range = node:mk_interval_between_ids(From, To),
    Id = id(Range),
    #lease{id      = Id,
           epoch   = 1,
           owner   = Owner,
           range   = Range,
           aux     = empty,
           version = 1,
           timeout = new_timeout()
          }.

-spec get_db_for_id(?RT:key()) -> atom().
get_db_for_id(Id) ->
    erlang:list_to_existing_atom(
      lists:flatten(
        io_lib:format("lease_db~p", [?RT:get_key_segment(Id)]))).

-spec new_timeout() -> erlang_timestamp().
new_timeout() ->
    util:time_plus_s(os:timestamp(), delta()).

-spec get_version(lease_t()) -> non_neg_integer().
get_version(#lease{version=Version}) -> Version.

-spec set_version(lease_t(), non_neg_integer()) -> lease_t().
set_version(Lease, Version) -> Lease#lease{version=Version}.

-spec get_epoch(lease_t()) -> non_neg_integer().
get_epoch(#lease{epoch=Epoch}) -> Epoch.

-spec set_epoch(lease_t(), non_neg_integer()) -> lease_t().
set_epoch(Lease, Epoch) -> Lease#lease{epoch=Epoch}.

-spec set_timeout(lease_t()) -> lease_t().
set_timeout(Lease) -> Lease#lease{timeout=new_timeout()}.

-spec get_timeout(lease_t()) -> erlang_timestamp().
get_timeout(#lease{timeout=Timeout}) -> Timeout.

-spec get_pretty_timeout(lease_t()) -> string().
get_pretty_timeout(L) ->
    format_utc_timestamp(get_timeout(L)).

-spec get_id(lease_t()) -> ?RT:key().
get_id(#lease{id=Id}) -> Id.

-spec get_owner(lease_t()) -> comm:mypid_plain() | nil.
get_owner(#lease{owner=Owner}) -> Owner.

-spec set_owner(lease_t(), comm:mypid_plain() | nil) -> lease_t().
set_owner(L, NewOwner) -> L#lease{owner=NewOwner}.

-spec get_aux(lease_t()) -> lease_aux().
get_aux(#lease{aux=Aux}) -> Aux.

-spec set_aux(lease_t(), lease_aux()) -> lease_t().
set_aux(L, Aux) -> L#lease{aux=Aux}.

-spec get_range(lease_t()) -> intervals:interval().
get_range(#lease{range=Range}) -> Range.

-spec set_range(lease_t(), intervals:interval()) -> lease_t().
set_range(L, Range) -> L#lease{range=Range}.

-spec is_valid(lease_t()) -> boolean().
is_valid(L) ->
    case L of
        empty ->
            %% should not happen but it does
            log:log("loncq: you are calling is_valid on an empty lease~n"),
            false;
        _ ->
            os:timestamp() <  L#lease.timeout
    end.

-spec has_timed_out(lease_t()) -> boolean().
has_timed_out(L) ->
    not is_valid(L).

-spec is_live_aux_field(lease_t()) -> boolean().
is_live_aux_field(L) ->
    {invalid, merge, no_renew} =/= get_aux(L).

-spec invalid_lease() -> lease_t().
invalid_lease() ->
    {A, B, C} = os:timestamp(),
    #lease{
       id      = ?RT:get_random_node_id(),
       epoch   = 0,
       owner   = nil,
       range   = intervals:empty(),
       aux     = {invalid, merge, no_renew},
       version = 0,
       timeout = {A+1, B, C}}.

-spec id(intervals:interval()) -> ?RT:key().
id([all]) -> ?MINUS_INFINITY;
id(X) ->
    {_, _, Id, _} = intervals:get_bounds(X),
    Id.

-spec split_range(intervals:interval()) ->
                         {ok, intervals:interval(), intervals:interval()}.
split_range(Range) ->
    {_, Low, Hi, _} = intervals:get_bounds(Range),
    Key = ?RT:get_split_key(Low, Hi, {1,2}),
    R1 = node:mk_interval_between_ids(Low, Key),
    R2 = node:mk_interval_between_ids(Key, Hi),
    {ok, R1, R2}.

-spec get_active_lease(dht_node_state:state()) -> lease_list:active_lease_t().
get_active_lease(State) ->
    LeaseList = dht_node_state:get(State, lease_list),
    lease_list:get_active_lease(LeaseList).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% qwrite_done messages should always be received by the current owner
% of the message. The message handler can directly change the lease
% list in the dht_node. The next message will than be handled under
% the new lease list.
%
% If we would update the lease of another node and the qwrite_done is
% received by us, the system assumes that the lease was updated but
% the owner will still work according to the old version of the
% lease. In some cases such a remote-modify is acceptable and correct.
% E.g. if we use a remote-modify to extend the lease's range, the
% remote node can continue to work with his old lease until the next
% renewal. On renewal, he will notice that it fails because the lease
% changed and additionally he will get notified of the range
% extension.
%
% The goal is to limited remote-modifies to correct operations,
% i.e. renewals, aux updates and range extensions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Lease agreement
%
% Rule 16: The lease's id never changes.
% Rule 17: Every content check has to perform the checks in the same order.
%          1. Does the lease exist?
%          2. Is the value of the owner field as expected?
%          3. Is the value of the aux   field as expected?
%          4. Is the value of the range field as expected?
%          5. Are the values of the epoch and version fields as expected?
%          6. Is the proposed timeout newer than the current one and is it in
%             the future (for debugging only)?
%          7. Only, now it may check for debug purposes whether the proposed
%             changes are acceptable.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% TODO
%
% - fix merge protocol
% - improve error handling for deny in renewal
% - do i need to check for timeout_is_not_in_the_future?
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%        (_Current, _WriteFilter, Next) ->
%            case (os:timestamp() <  Next#lease.timeout) of
%                false ->
%                    {false, timeout_is_not_in_the_future};
%                true ->
%                    {true, null}
%            end

-spec format_utc_timestamp(erlang_timestamp()) -> string().
format_utc_timestamp({_,_,Micro} = TS) ->
    {{Year,Month,Day},{Hour,Minute,Second}} = calendar:now_to_local_time(TS),
    Mstr = element(Month,{"Jan","Feb","Mar","Apr","May","Jun","Jul", "Aug","Sep",
                          "Oct","Nov","Dec"}),
    lists:flatten(io_lib:format("~2w ~s ~4w ~2w:~2..0w:~2..0w.~6..0w",
                  [Day,Mstr,Year,Hour,Minute,Second,Micro])).

% @doc updates lease and tries to use qwrite_fast whenever
%      possible. almost all leases updates use this routine
-spec update_lease(ReplyTo::comm:erl_local_pid(),
                   ContentCheck::content_check_t(),
                   Old::lease_t(), New::lease_t(), dht_node_state:state()) -> ok.
update_lease(ReplyTo, ContentCheck, Old, New, State) ->
    ?DBG_ASSERT(get_id(Old) =:= get_id(New)), % the lease id may not be changed
    LeaseId = get_id(New),
    DB = get_db_for_id(LeaseId),
    case lease_list:get_next_round(LeaseId, State) of
        failed ->
            rbrcseq:qwrite     (DB, ReplyTo, LeaseId, ContentCheck, New);
        NextRound ->
            rbrcseq:qwrite_fast(DB, ReplyTo, LeaseId, ContentCheck, New, NextRound, Old)
    end.

% triggers renew of lease and updates known round number for the lease
-spec renew_and_update_round(lease_t(), pr:pr(), active | passive, dht_node_state:state()) ->
                                    dht_node_state:state().
renew_and_update_round(Lease, Round, Mode, State) ->
    lease_renew(self(), Lease, Mode),
    lease_list:update_next_round(l_on_cseq:get_id(Lease), Round, State).
