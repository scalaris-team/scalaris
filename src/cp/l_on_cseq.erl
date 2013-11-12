% @copyright 2012-2013 Zuse Institute Berlin,

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

-export([read/1, read/2]).
%%-export([write/2]).

-export([on/2]).

-export([lease_renew/2]).
-export([lease_handover/3]).
-export([lease_takeover/2]).
-export([lease_split/5]).
-export([lease_merge/3]).
-export([lease_send_lease_to_node/2]).
-export([lease_split_and_change_owner/6]).
-export([disable_lease/2]).

-export([id/1]).

% for unit tests
-export([unittest_lease_update/3]).
-export([unittest_create_lease/1]).

-export([get_db_for_id/1]).

% lease accessors
-export([get_version/1,set_version/2,
         get_epoch/1, set_epoch/2,
         new_timeout/0, set_timeout/1, get_pretty_timeout/1,
         get_id/1,
         get_owner/1, set_owner/2,
         get_aux/1, set_aux/2,
         get_range/1, set_range/2,
         split_range/1,
         is_valid/1,
         invalid_lease/0]).

-export([add_first_lease_to_db/2]).

-ifdef(with_export_type_support).
-export_type([lease_t/0]).
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
      | {invalid, merge, stopped}
      | {valid,   merge, intervals:interval(), intervals:interval()}.

-record(lease, {
          id      = ?required(lease, id     ) :: lease_id(),
          epoch   = ?required(lease, epoch  ) :: non_neg_integer(),
          owner   = ?required(lease, owner  ) :: comm:mypid() | nil,
          range   = ?required(lease, range  ) :: intervals:interval(),
          aux     = ?required(lease, aux    ) :: lease_aux(),
          version = ?required(lease, version) :: non_neg_integer(),
          timeout = ?required(lease, timeout) :: erlang_timestamp()}).
-type lease_t() :: #lease{}.

-type generic_failed_reason() :: lease_does_not_exist
                               | unexpected_owner
                               | unexpected_aux
                               | unexpected_range
                               | unexpected_timeout
                               | unexpected_epoch
                               | unexpected_version
                               | timeout_is_not_newer_than_current_lease.

%-type renewal_failed_reason() :: lease_does_not_exist
%                               | unexpected_owner
%                               | unexpected_aux
%                               | unexpected_range
%                               | unexpected_epoch
%                               | unexpected_version
%                               | timeout_is_not_newer_than_current_lease.

-type update_failed_reason() :: lease_does_not_exist
                              | epoch_or_version_mismatch.

-type split_step1_failed_reason() :: lease_already_exists.
%-type split_step2_failed_reason() :: lease_does_not_exist
%                                   | epoch_or_version_mismatch
%                                   | owner_changed
%                                   | range_unchanged
%                                   | aux_unchanged
%                                   | timeout_is_not_newer_than_current_lease
%                                   | timeout_is_not_in_the_future.
%
%-type split_step3_failed_reason() :: lease_does_not_exist
%                                   | epoch_or_version_mismatch
%                                   | owner_changed
%                                   | range_changed
%                                   | aux_unchanged
%                                   | timeout_is_not_newer_than_current_lease
%                                   | timeout_is_not_in_the_future.
%
%-type split_step4_failed_reason() :: lease_does_not_exist
%                                   | epoch_or_version_mismatch
%                                   | owner_changed
%                                   | range_changed
%                                   | aux_unchanged
%                                   | timeout_is_not_newer_than_current_lease
%                                   | timeout_is_not_in_the_future.

-spec delta() -> pos_integer().
delta() -> 10.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Public API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec lease_renew(lease_t(), active | passive) -> ok.
lease_renew(Lease, Mode) ->
    comm:send_local(pid_groups:get_my(dht_node),
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

-spec lease_send_lease_to_node(Pid::comm:mypid(), Lease::lease_t()) -> ok.
lease_send_lease_to_node(Pid, Lease) ->
    % @todo precondition: Pid is a dht_node
    comm:send(Pid, {l_on_cseq, send_lease_to_node, Lease}),
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
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, unittest_update, Old, New, Mode, self()}),
    receive
        ?SCALARIS_RECV(
            {l_on_cseq, unittest_update_success, Old, New}, %% ->
            ok);
        ?SCALARIS_RECV(
            {l_on_cseq, unittest_update_failed, Old, New}, %% ->
            failed
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
            gen_component:post_op(State,
                                  {l_on_cseq, handover, L2, NewOwner, ReplyPid});
        {split, fail, L1} ->
            comm:send_local(ReplyPid, {split, fail, L1}),
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease renewal
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, renew, Old = #lease{id=Id,version=OldVersion}, Mode},
   State) ->
    %log:pal("on renew ~w (~w)~n", [Old, Mode]),
    New = Old#lease{version=OldVersion+1, timeout=new_timeout()},
    ContentCheck = generic_content_check(Old),
    DB = get_db_for_id(Id),
%% @todo New passed for debugging only:
    Self = comm:reply_as(self(), 3, {l_on_cseq, renew_reply, '_', New, Mode}),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, New),
    State;

on({l_on_cseq, renew_reply, {qwrite_done, _ReqId, _Round, Value}, _New, Mode}, State) ->
    %% log:pal("successful renew~n", []),
    lease_list:update_lease_in_dht_node_state(Value, State, Mode, renew);

on({l_on_cseq, renew_reply,
    {qwrite_deny, _ReqId, _Round, Value, {content_check_failed, Reason}}, New, Mode},
   State) ->
    % @todo retry
    log:pal("renew denied: ~p~nVal: ~p~nNew: ~p~n~p~n", [Reason, Value, New, Mode]),
    log:pal("id: ~p~n", [dht_node_state:get(State, node_id)]),
    log:pal("lease list: ~p~n", [dht_node_state:get(State, lease_list)]),
    log:pal("timeout: ~p~n", [calendar:now_to_local_time(get_timeout(Value))]),
    case Reason of
        lease_does_not_exist ->
            case Value of %@todo is this necessary?
                prbr_bottom ->
                    State;
                _ ->
                    lease_list:remove_lease_from_dht_node_state(Value, State, Mode)
            end;
        unexpected_owner   -> lease_list:remove_lease_from_dht_node_state(Value, State, Mode);
        unexpected_aux     ->
            case get_aux(Value) of
                empty                  -> lease_renew(Value, Mode), State;
                {invalid, split, _, _} -> lease_renew(Value, Mode), State;
                {invalid, merge, _, _} -> lease_renew(Value, Mode), State;
                {invalid, merge, stopped} ->
                    lease_list:remove_lease_from_dht_node_state(Value, State, Mode);
                {valid, split, _, _}   -> lease_renew(Value, Mode), State;
                {valid, merge, _, _}   -> lease_renew(Value, Mode), State
            end;
        unexpected_range   -> lease_renew(Value, Mode), State;
        unexpected_timeout -> lease_renew(Value, Mode), State;
        unexpected_epoch   -> lease_renew(Value, Mode), State;
        unexpected_version -> lease_renew(Value, Mode), State;
        timeout_is_not_newer_than_current_lease ->
            lease_renew(Value, Mode),
            State
    end;

on({l_on_cseq, send_lease_to_node, Lease}, State) ->
    % @todo do we need any checks?
    % @todo do i need to notify rm about the new range?
    log:log("send_lease_to_node ~p ~p~n", [self(), Lease]),
    lease_list:update_lease_in_dht_node_state(Lease, State, active, received_lease);


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease update (only for unit tests)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, unittest_update,
    Old = #lease{id=Id, epoch=OldEpoch,version=OldVersion},
    New, Mode, Caller}, State) ->
    %% io:format("renew ~p~n", [Old]),
    ContentCheck = is_valid_update(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    %% @todo New passed for debugging only:
    Self = comm:reply_as(self(), 3, {l_on_cseq, unittest_update_reply, '_', Old, New, Mode, Caller}),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, New),
    State;

on({l_on_cseq, unittest_update_reply, {qwrite_done, _ReqId, _Round, Value},
    Old, New, Mode, Caller}, State) ->
    %% io:format("successful update~n", []),
    comm:send_local(Caller, {l_on_cseq, unittest_update_success, Old, New}),
    lease_list:update_lease_in_dht_node_state(Value, State, Mode, unittest);

on({l_on_cseq, unittest_update_reply,
    {qwrite_deny, _ReqId, _Round, _Value, {content_check_failed, _Reason}},
    Old, New, _Mode, Caller}, State) ->
   comm:send_local(Caller, {l_on_cseq, unittest_update_failed, Old, New}),
   State;


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease handover
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, handover, Old = #lease{id=Id, epoch=OldEpoch},
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
    ContentCheck = generic_content_check(Old),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 3, {l_on_cseq, handover_reply, '_', ReplyTo,
                                     NewOwner, New}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, handover_reply, {qwrite_done, _ReqId, _Round, Value}, ReplyTo,
    _NewOwner, _New}, State) ->
    % @todo if success update lease in State
    log:log("successful handover ~p~n", [Value]),
    comm:send_local(ReplyTo, {handover, success, Value}),
    lease_list:update_lease_in_dht_node_state(Value, State, passive, handover);

on({l_on_cseq, handover_reply, {qwrite_deny, _ReqId, _Round, Value,
                                {content_check_failed, Reason}},
    ReplyTo, NewOwner, New}, State) ->
    log:log("handover denied: ~p ~p ~p~n", [Reason, Value, New]),
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
on({l_on_cseq, takeover, Old = #lease{id=Id, epoch=OldEpoch,version=OldVersion}, 
    ReplyTo}, State) ->
    New = Old#lease{epoch   = OldEpoch + 1,
                    version = 0,
                    owner   = comm:this(),
                    timeout = new_timeout()},
    ContentCheck = is_valid_takeover(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 4, {l_on_cseq, takeover_reply, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, takeover_reply, ReplyTo, {qwrite_done, _ReqId, _Round, Value}}, State) ->
    %% log:log("takeover success ~p~n", [Value]),
    comm:send_local(ReplyTo, {takeover, success, Value}),
    lease_list:update_lease_in_dht_node_state(Value, State, passive, takeover);

on({l_on_cseq, takeover_reply, ReplyTo, {qwrite_deny, _ReqId, _Round, Value, _Reason}}, State) ->
    log:log("takeover failed ~p ~p~n", [Value, _Reason]),
    comm:send_local(ReplyTo, {takeover, failed, Value}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step1)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge, L1 = #lease{id=Id, epoch=OldEpoch,version=OldVersion},
    L2, ReplyTo}, State) ->
    New = L1#lease{epoch    = OldEpoch + 1,
                    version = 0,
                    aux     = {invalid, merge, get_range(L1), get_range(L2)},
                    timeout = new_timeout()},
    ContentCheck = is_valid_merge_step1(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 5, {l_on_cseq, merge_reply_step1, L2, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, merge_reply_step1, _L2, _ReplyTo, {qwrite_deny, _ReqId, _Round, _L1, _Reason}}, State) ->
    % @todo if success update lease in State
    % retry?
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step2)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step1,
    L2 = #lease{id=Id,epoch=OldEpoch,version=OldVersion}, ReplyTo,
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    % @todo if success update lease in State
    New = L2#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   range   = intervals:union(L1#lease.range, L2#lease.range),
                   aux     = {valid, merge, get_range(L1), get_range(L2)},
                   timeout = new_timeout()},
    ContentCheck = is_valid_merge_step2(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 5, {l_on_cseq, merge_reply_step2, L1, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    lease_list:update_lease_in_dht_node_state(L1, State, passive, merge_reply_step1);


on({l_on_cseq, merge_reply_step2, _L1, _ReplyTo, {qwrite_deny, _ReqId, _Round, _L2, _Reason}}, State) ->
    % @todo if success update lease in State
    % retry?
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step3)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step2,
    L1 = #lease{id=Id,epoch=OldEpoch,version=OldVersion}, ReplyTo,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    % @todo if success update lease in State
    New = L1#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   aux     = {invalid, merge, stopped}},
    ContentCheck = is_valid_merge_step3(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 5, {l_on_cseq, merge_reply_step3, L2, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    lease_list:update_lease_in_dht_node_state(L2, State, active, merge_reply_step2);

on({l_on_cseq, merge_reply_step3, _L2, _ReplyTo, {qwrite_deny, _ReqId, _Round, _L1, _Reason}}, State) ->
    % @todo if success update lease in State
    % retry?
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step4)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step3,
    L2 = #lease{id=Id,epoch=OldEpoch,version=OldVersion}, ReplyTo,
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    % @todo if success update lease in State
    log:pal("successful merge step3 ~p~n", [L1]),
    New = L2#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   aux     = empty,
                   timeout = new_timeout()},
    ContentCheck = is_valid_merge_step4(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 5, {l_on_cseq, merge_reply_step4, L1, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    lease_list:remove_lease_from_dht_node_state(L1, State, passive);

on({l_on_cseq, merge_reply_step4, L1, ReplyTo,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    log:pal("successful merge ~p~p~n", [ReplyTo, L2]),
    comm:send_local(ReplyTo, {merge, success, L2, L1}),
    lease_list:update_lease_in_dht_node_state(L2, State, active, merge_reply_step3);

on({l_on_cseq, merge_reply_step4, _L1, _ReplyTo, {qwrite_deny, _ReqId, _Round, _L2, _Reason}}, State) ->
    % @todo if success update lease in State
    % retry?
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step1)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split, Lease, R1, R2, Keep, ReplyTo, PostAux}, State) ->
    Id = id(R1),
    log:pal("split first step: creating new lease L1(~w) (~p)~n", [self(), Id]),
    Active = get_active_lease(State),
    log:log("going to split(~w):~n~w~n~w~n", [self(), Active, Lease]),
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
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, split_reply_step1, _Lease, _R1, _R2, _Keep, ReplyTo, _PostAux,
    {qwrite_deny, _ReqId, _Round, Lease, {content_check_failed, Reason}}}, State) ->
    log:pal("split first step failed: ~p~n", [Reason]),
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
on({l_on_cseq, split_reply_step1, L2=#lease{id=Id,epoch=OldEpoch}, R1, R2,
    Keep, ReplyTo, PostAux, {qwrite_done, _ReqId, _Round, L1}}, State) ->
    log:pal("split second step(~w): updating L2 (~p)~n", [self(), Id]),
    Active = get_active_lease(State),
    log:log("split second step(~w):~n~w~n~w~n~w~n", [self(), Active, L1, L2]),
    New = L2#lease{
            epoch   = OldEpoch + 1,
            range   = R2,
            aux     = {valid, split, R1, R2},
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(L2),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 9, {l_on_cseq, split_reply_step2, L1, R1, R2, Keep, ReplyTo, PostAux, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    case Keep of
        first -> true = false, % TS: not supported at the moment
                 lease_list:update_lease_in_dht_node_state(L1, State, active, split_reply_step1);
        second -> lease_list:update_lease_in_dht_node_state(L1, State, passive, split_reply_step1)

    end;

on({l_on_cseq, split_reply_step2, L1, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_deny, _ReqId, _Round, L2, {content_check_failed, Reason}}}, State) ->
    log:pal("split second step failed: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send_local(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_owner     -> comm:send_local(ReplyTo, {split, fail, L2}),
                                lease_list:remove_lease_from_dht_node_state(L2, State, any); %@todo
        unexpected_range     -> comm:send_local(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_aux       -> comm:send_local(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_timeout ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step1, L2, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step1, L2, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        unexpected_epoch ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step1, L2, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        unexpected_version ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step1, L2, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L1}})
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step3)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split_reply_step2,
    L1 = #lease{id=Id,epoch=OldEpoch}, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    log:pal("split third step(~w): renew L1 ~p~n", [self(), Id]),
    Active = get_active_lease(State),
    log:log("split_reply_step2(~w):~n~w~n~w~n~w~n", [self(), Active, L1, L2]),
    New = L1#lease{
            epoch   = OldEpoch + 1,
            aux     = PostAux,
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(L1),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 9, {l_on_cseq, split_reply_step3, L2, R1, R2, Keep, ReplyTo, PostAux, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    lease_list:update_lease_in_dht_node_state(L2, State, active, split_reply_step2);

on({l_on_cseq, split_reply_step3, L2, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_deny, _ReqId, _Round, L1, {content_check_failed, Reason}}}, State) ->
    % @todo
    log:pal("split third step failed: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send_local(ReplyTo, {split, fail, L1}), State; %@todo
        unexpected_owner     -> comm:send_local(ReplyTo, {split, fail, L1}),
                                lease_list:remove_lease_from_dht_node_state(L1, State, any); %@todo
        unexpected_range     -> comm:send_local(ReplyTo, {split, fail, L1}), State; %@todo
        unexpected_aux       -> comm:send_local(ReplyTo, {split, fail, L1}), State; %@todo
        unexpected_timeout ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step2, L1, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step2, L1, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        unexpected_epoch ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step2, L1, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        unexpected_version ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step2, L1, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L2}})
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step4)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split_reply_step3,
    L2 = #lease{id=Id,epoch=OldEpoch}, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    log:pal("split fourth step: renew L2 ~p ~p ~p ~p~n", [R1, R2, Id, PostAux]),
    New = L2#lease{
            epoch   = OldEpoch + 1,
            aux     = empty,
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(L2),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 9, {l_on_cseq, split_reply_step4, L1, R1, R2, Keep, ReplyTo, PostAux, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    lease_list:update_lease_in_dht_node_state(L1, State, passive, split_reply_step3);

on({l_on_cseq, split_reply_step4, L1, _R1, _R2, Keep, ReplyTo, _PostAux,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    log:pal("successful split~n", []),
    log:pal("successful split ~p~n", [ReplyTo]),
    Active = get_active_lease(State),
    log:log("split_reply_step4(~w):~n~w~n~w~n~w~n", [self(), Active, L1, L2]),
    comm:send_local(ReplyTo, {split, success, L1, L2}),
    lease_list:update_lease_in_dht_node_state(L2, State, active, split_reply_step4);

on({l_on_cseq, split_reply_step4, L1, R1, R2, Keep, ReplyTo, PostAux,
    {qwrite_deny, _ReqId, _Round, L2, {content_check_failed, Reason}}}, State) ->
    % @todo
    log:pal("split fourth step: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send_local(ReplyTo, {split, fail, L2}), State;
        unexpected_owner     -> comm:send_local(ReplyTo, {split, fail, L2}),
                                lease_list:remove_lease_from_dht_node_state(L2, State, active);
        unexpected_range     -> comm:send_local(ReplyTo, {split, fail, L2}), State;
        unexpected_aux       -> comm:send_local(ReplyTo, {split, fail, L2}), State;
        unexpected_timeout ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step3, L2, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step3, L2, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        unexpected_epoch ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step3, L2, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        unexpected_version ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step3, L2, R1, R2,
                                          Keep, ReplyTo, PostAux,
                                          {qwrite_done, fake_reqid, fake_round, L1}})
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
    %io:format("renewing all local leases: ~p~n", [length(LeaseList)]),
    lease_renew(ActiveLease, active),
    _ = [lease_renew(L, passive) || L <- PassiveLeaseList, get_aux(L) =/= {invalid, merge, stopped}],
    msg_delay:send_local(delta() div 2, self(), {l_on_cseq, renew_leases}),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% content checks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec generic_content_check(lease_t()) ->
    fun ((any(), any(), any()) -> {boolean(), generic_failed_reason() | null}). %% content check
generic_content_check(#lease{owner=OldOwner,aux = OldAux,range=OldRange,
                             epoch=OldEpoch,version=OldVersion,timeout=OldTimeout}) ->
    fun (prbr_bottom, _WriteFilter, _Next) ->
            {false, lease_does_not_exist};
        (#lease{owner = O0}, _, _)    when O0 =/= OldOwner->
            {false, unexpected_owner};
        (#lease{aux = Aux0}, _, _)    when Aux0 =/= OldAux->
            {false, unexpected_aux};
        (#lease{range = R0}, _, _)    when R0 =/= OldRange->
            {false, unexpected_range};
        (#lease{timeout = T0}, _, _)                   when T0 =/= OldTimeout->
            {false, unexpected_timeout};
        (#lease{epoch = E0}, _, _)                     when E0 =/= OldEpoch ->
            {false, unexpected_epoch};
        (#lease{version = V0}, _, _)                   when V0 =/= OldVersion->
            {false, unexpected_version};
        (#lease{timeout = T0}, _, #lease{timeout = T1})  when not (T0 < T1)->
            {false, timeout_is_not_newer_than_current_lease};
        (_, _, _) ->
            {true, null}
    end.

%-spec is_valid_renewal(lease_t()) ->
%    fun ((any(), any(), any()) -> {boolean(), renewal_failed_reason() | null}). %% content check
%is_valid_renewal(#lease{owner=OldOwner,aux = OldAux,range=OldRange,
%                        epoch=OldEpoch,version=OldVersion}) ->
%    fun (prbr_bottom, _WriteFilter, _Next) ->
%            {false, lease_does_not_exist};
%        (#lease{owner = O0}, _, _)    when O0 =/= OldOwner->
%            {false, unexpected_owner};
%        (#lease{aux = Aux0}, _, _)    when Aux0 =/= OldAux->
%            {false, unexpected_aux};
%        (#lease{range = R0}, _, _)    when R0 =/= OldRange->
%            {false, unexpected_range};
%        (#lease{epoch = E0}, _, _)                     when E0 =/= OldEpoch ->
%            {false, unexpected_epoch};
%        (#lease{version = V0}, _, _)                   when V0 =/= OldVersion->
%            {false, unexpected_version};
%        (#lease{timeout = T0}, _, #lease{timeout = T1})  when not (T0 < T1)->
%            {false, timeout_is_not_newer_than_current_lease}
%    end.

-spec is_valid_update(non_neg_integer(), non_neg_integer()) ->
    fun ((any(), any(), any()) -> {boolean(), update_failed_reason() | null}). %% content check
is_valid_update(CurrentEpoch, CurrentVersion) ->
    fun (#lease{epoch = E0}, _, _)                     when E0 =/= CurrentEpoch ->
            %% log:pal("is_valid_update: expected ~p, got ~p", [CurrentEpoch, E0]),
            {false, epoch_or_version_mismatch};
        (#lease{version = V0}, _, _)                   when V0 =/= CurrentVersion->
            %% log:pal("is_valid_update: expected ~p, got ~p", [CurrentVersion, V0]),
            {false, epoch_or_version_mismatch};
        (_Current, _WriteFilter, _Next) ->
            {true, null}
    end.

%(prbr_bottom, _WriteFilter, _Next) ->
%            {false, lease_does_not_exist};


%-spec is_valid_handover(non_neg_integer(), non_neg_integer()) ->
%    fun ((any(), any(), any()) -> {boolean(), null}). %% content check
%is_valid_handover(Epoch, Version) ->
%    fun (Current, _WriteFilter, Next) ->
%            Res = standard_check(Current, Next, Epoch, Version)
%            %% checks for debugging
%                andalso (Current#lease.epoch+1 == Next#lease.epoch)
%                andalso (Current#lease.owner == comm:make_global(pid_groups:get_my(dht_node)))
%                andalso (Current#lease.owner =/= Next#lease.owner)
%                andalso (Current#lease.range == Next#lease.range)
%                andalso (Current#lease.aux == Next#lease.aux)
%                andalso (Current#lease.timeout < Next#lease.timeout)
%                andalso (os:timestamp() <  Next#lease.timeout),
%            {Res, null}
%    end.

-spec is_valid_takeover(non_neg_integer(), non_neg_integer()) ->
    fun ((any(), any(), any()) -> {boolean(), null}). %% content check
is_valid_takeover(Epoch, Version) ->
    MyDHTNode = comm:make_global(pid_groups:get_my(dht_node)),
    % standard_check: serialization
    fun (#lease{epoch=Value}, _, _) when Value =/= Epoch ->
            {false, epoch_or_version_mismatch};
        (#lease{version=Value}, _, _) when Value =/= Version ->
            {false, epoch_or_version_mismatch};
    % standard_check: update epoch or version
        (#lease{epoch=CurrentEpoch, version=CurrentVersion},
         _,
         #lease{epoch=NextEpoch, version=NextVersion})
          when not (((CurrentEpoch + 1 =:= NextEpoch) andalso (NextVersion =:= 0)) orelse
                    ((CurrentEpoch =:= NextEpoch) andalso (CurrentVersion+1 =:= NextVersion))) ->
            {false, epoch_or_version_mismatch};
    % check that epoch increases
        (#lease{epoch=CurrentEpoch},
         _,
         #lease{epoch=NextEpoch}) when CurrentEpoch + 1 =/= NextEpoch ->
            {false, epoch_or_version_mismatch};
   % check that next owner is my dht_node
        (_, _, #lease{owner=NextOwner}) when NextOwner =/= MyDHTNode ->
            {false, unexpected_new_owner};
   % check that the owner actually changed
        (#lease{owner=CurrentOwner}, _, #lease{owner=NextOwner}) when CurrentOwner =:= NextOwner ->
            {false, unexpected_new_owner};
   % check that the range didn't change
        (#lease{range=CurrentRange}, _, #lease{range=NextRange}) when CurrentRange =/= NextRange ->
            {false, unexpected_new_range};
   % check that aux didn't change
        (#lease{aux=CurrentAux}, _, #lease{aux=NextAux}) when CurrentAux =/= NextAux ->
            {false, unexpected_new_aux};
   % check that aux timeout increased
        (#lease{timeout=CurrentTimeout}, _, #lease{timeout=NextTimeout})
          when CurrentTimeout >= NextTimeout ->
            {false, unexpected_new_timeout};
        (Current, _WriteFilter, #lease{timeout=NextTimeout}) ->
            Timestamp = os:timestamp(),
            IsValid = is_valid(Current),
            if
                Timestamp >= NextTimeout ->
                    {false, unexpected_new_timeout};
                IsValid ->
                    {false, lease_is_still_valid};
                true ->
                    {true, null}
            end
    end.
%
%    fun (Current, _WriteFilter, Next) ->
%            log:log("is_valid_takeover~n~w~n~w~n", [Current, Next]),
%            Res = standard_check(Current, Next, Epoch, Version)
%            %% checks for debugging
%                andalso (Current#lease.epoch+1 == Next#lease.epoch)
%                andalso (Next#lease.owner == comm:make_global(pid_groups:get_my(dht_node)))
%                andalso (Current#lease.owner =/= Next#lease.owner)
%                andalso (Current#lease.range == Next#lease.range)
%                andalso (Current#lease.aux == Next#lease.aux)
%                andalso (Current#lease.timeout < Next#lease.timeout)
%                andalso (os:timestamp() <  Next#lease.timeout)
%                andalso not is_valid(Current), % Current has to be invalid!
%            {Res, null}
%    end.

-spec is_valid_merge_step1(non_neg_integer(), non_neg_integer()) ->
    fun ((any(), any(), any()) -> {boolean(), null}). %% content check
is_valid_merge_step1(Epoch, Version) ->
    fun (Current, _WriteFilter, Next) ->
            Res = standard_check(Current, Next, Epoch, Version)
            %% checks for debugging
                andalso (Current#lease.epoch+1 == Next#lease.epoch)
                andalso (Current#lease.owner == Next#lease.owner)
                andalso (Current#lease.range == Next#lease.range)
                andalso (Current#lease.aux =/= Next#lease.aux)
                andalso (Current#lease.timeout < Next#lease.timeout)
                andalso (os:timestamp() <  Next#lease.timeout),
            {Res, null}
    end.

-spec is_valid_merge_step2(non_neg_integer(), non_neg_integer()) ->
    fun ((any(), any(), any()) -> {boolean(), null}). %% content check
is_valid_merge_step2(Epoch, Version) ->
    fun (Current, _WriteFilter, Next) ->
            Res = standard_check(Current, Next, Epoch, Version)
            %% checks for debugging
                andalso (Current#lease.epoch+1 == Next#lease.epoch)
                andalso (Current#lease.owner == Next#lease.owner)
                andalso (Current#lease.range =/= Next#lease.range)
                andalso (Current#lease.aux =/= Next#lease.aux)
                andalso (Current#lease.timeout < Next#lease.timeout)
                andalso (os:timestamp() <  Next#lease.timeout),
            {Res, null}
    end.

-spec is_valid_merge_step3(non_neg_integer(), non_neg_integer()) ->
    fun ((any(), any(), any()) -> {boolean, null}). %% content check
is_valid_merge_step3(Epoch, Version) ->
    fun (Current, _WriteFilter, Next) ->
            Res = standard_check(Current, Next, Epoch, Version)
            %% checks for debugging
                andalso (Current#lease.epoch+1 == Next#lease.epoch)
                andalso (Current#lease.owner == Next#lease.owner)
                andalso (Current#lease.range == Next#lease.range)
                andalso (Current#lease.aux =/= Next#lease.aux)
                andalso (Current#lease.timeout == Next#lease.timeout),
            {Res, null}
    end.

-spec is_valid_merge_step4(non_neg_integer(), non_neg_integer()) ->
    fun ((any(), any(), any()) -> {boolean(), null}). %% content check
is_valid_merge_step4(Epoch, Version) ->
    fun (Current, _WriteFilter, Next) ->
            Res = standard_check(Current, Next, Epoch, Version)
            %% checks for debugging
                andalso (Current#lease.epoch+1 == Next#lease.epoch)
                andalso (Current#lease.owner == Next#lease.owner)
                andalso (Current#lease.range == Next#lease.range)
                andalso (Next#lease.aux == empty)
                andalso (Current#lease.aux =/= Next#lease.aux)
                andalso (Current#lease.timeout =/= Next#lease.timeout)
                andalso (os:timestamp() <  Next#lease.timeout),
            {Res, null}
    end.

-spec is_valid_split_step1() ->
    fun ((any(), any(), any()) -> {boolean(), split_step1_failed_reason() | null}). %% content check
is_valid_split_step1() ->
    fun (Current, _WriteFilter, _Next) ->
            case Current == prbr_bottom of
                true ->
                    {true, null};
                false ->
                    {false, lease_already_exists}
            end
    end.

%-spec is_valid_split_step2(non_neg_integer(), non_neg_integer()) ->
%    fun ((any(), any(), any()) -> {boolean(), split_step2_failed_reason() | null}). %% content check
%is_valid_split_step2(CurrentEpoch, CurrentVersion) ->
%    This = comm:make_global(pid_groups:get_my(dht_node)),
%    fun (prbr_bottom, _WriteFilter, _Next) ->
%            {false, lease_does_not_exist};
%        (#lease{owner = O0}, _, #lease{owner = O1})    when O0 =/= O1->
%            {false, owner_changed};
%        (#lease{owner = O0}, _, _)                     when O0 =/= This->
%            {false, owner_changed};
%        (#lease{aux = Aux0}, _, #lease{aux = Aux1})    when Aux0 =:= Aux1->
%            {false, aux_unchanged};
%        (#lease{range = R0}, _, #lease{range = R1})    when R0 =:= R1->
%            {false, range_unchanged};
%        (#lease{epoch = E0}, _, _)                     when E0 =/= CurrentEpoch ->
%            {false, epoch_or_version_mismatch};
%        (#lease{version = V0}, _, _)                   when V0 =/= CurrentVersion->
%            {false, epoch_or_version_mismatch};
%        (#lease{timeout = T0}, _, #lease{timeout = T1})  when not (T0 < T1)->
%            {false, timeout_is_not_newer_than_current_lease};
%        (_Current, _WriteFilter, Next) ->
%            case (os:timestamp() <  Next#lease.timeout) of
%                false ->
%                    {false, timeout_is_not_in_the_future};
%                true ->
%                    {true, null}
%            end
%    end.
%
%-spec is_valid_split_step3(lease_t()) ->
%    fun ((any(), any(), any()) -> {boolean(), split_step3_failed_reason() | null}). %% content check
%is_valid_split_step3(#lease{owner=OldOwner,aux = OldAux,range=OldRange,
%                        epoch=OldEpoch,version=OldVersion}) ->
%    This = comm:make_global(pid_groups:get_my(dht_node)),
%    fun  (prbr_bottom, _WriteFilter, _Next) ->
%            {false, lease_does_not_exist};
%        (#lease{owner = O0}, _, #lease{owner = O1})    when O0 =/= O1->
%            {false, owner_changed};
%        (#lease{owner = O0}, _, _)                     when O0 =/= This->
%            {false, owner_changed};
%        (#lease{aux = Aux0}, _, #lease{aux = Aux1})    when Aux0 =:= Aux1->
%            {false, aux_unchanged};
%        (#lease{range = R0}, _, #lease{range = R1})    when R0 =/= R1->
%            {false, range_changed};
%        (#lease{epoch = E0}, _, _)                     when E0 =/= OldEpoch ->
%            {false, epoch_or_version_mismatch};
%        (#lease{version = V0}, _, _)                   when V0 =/= OldVersion->
%            {false, epoch_or_version_mismatch};
%        (#lease{timeout = T0}, _, #lease{timeout = T1})  when not (T0 < T1)->
%            {false, timeout_is_not_newer_than_current_lease};
%        (_Current, _WriteFilter, Next) ->
%            case (os:timestamp() <  Next#lease.timeout) of
%                false ->
%                    {false, timeout_is_not_in_the_future};
%                true ->
%                    {true, null}
%            end
%    end.
%
%-spec is_valid_split_step4(non_neg_integer(), non_neg_integer()) ->
%    fun ((any(), any(), any()) -> {boolean(), split_step4_failed_reason() | null}). %% content check
%is_valid_split_step4(CurrentEpoch, CurrentVersion) ->
%    This = comm:make_global(pid_groups:get_my(dht_node)),
%    fun (prbr_bottom, _WriteFilter, _Next) ->
%            {false, lease_does_not_exist};
%        (#lease{owner = O0}, _, #lease{owner = O1})    when O0 =/= O1->
%            {false, owner_changed};
%        (#lease{owner = O0}, _, _)                     when O0 =/= This->
%            {false, owner_changed};
%        (#lease{aux = Aux0}, _, #lease{aux = Aux1})    when Aux0 =:= Aux1->
%            {false, aux_unchanged};
%        (#lease{range = R0}, _, #lease{range = R1})    when R0 =/= R1->
%            {false, range_changed};
%        (#lease{epoch = E0}, _, _)                     when E0 =/= CurrentEpoch ->
%            {false, epoch_or_version_mismatch};
%        (#lease{version = V0}, _, _)                   when V0 =/= CurrentVersion->
%            {false, epoch_or_version_mismatch};
%        (#lease{timeout = T0}, _, #lease{timeout = T1})  when not (T0 < T1)->
%            {false, timeout_is_not_newer_than_current_lease};
%        (_Current, _WriteFilter, Next) ->
%            case (os:timestamp() <  Next#lease.timeout) of
%                false ->
%                    {false, timeout_is_not_in_the_future};
%                true ->
%                    {true, null}
%            end
%    end.

-spec standard_check(prbr_bottom | lease_t(), lease_t(),
                     non_neg_integer(), non_neg_integer()) -> boolean().
standard_check(prbr_bottom, _, _, _) ->
    %% do not create leases from thin air. There are only two
    %% situations where a lease DB entry can be created:
    %% 1. when a first node starts a new Scalaris system
    %%    (this lease is directly put to the database)
    %% 2. when a split is performed (this has to use another check)
    false;
standard_check(Current, Next, CurrentEpoch, CurrentVersion) ->
%% this serializes all operations on leases
%% additional checks only for debugging the protocol and ensuring
%% valid maintenance of the lease data structure and state machine

%% for serialization, we check whether epoch and version match
    (Current#lease.epoch == CurrentEpoch)
        andalso (Current#lease.version == CurrentVersion)

%% normal operation incs version
        andalso ( ( (Current#lease.version+1 == Next#lease.version)
                    andalso (Current#lease.epoch == Next#lease.epoch))
%% reset version counter on epoch change
                  orelse ((0 == Next#lease.version)
                          andalso (Current#lease.epoch+1 == Next#lease.epoch))
                 )
%% debugging test
        andalso (Current#lease.id == Next#lease.id).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% util
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec read(lease_id()) -> api_tx:read_result().
read(Key) ->
    read(Key, self()),
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
                                  lease_list:make_lease_list(Lease, [])).

-spec unittest_create_lease(?RT:key()) -> lease_t().
unittest_create_lease(Id) ->
    #lease{id      = Id,
           epoch   = 1,
           owner   = comm:this(),
           range   = intervals:all(),
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

-spec get_owner(lease_t()) -> comm:mypid() | nil.
get_owner(#lease{owner=Owner}) -> Owner.

-spec set_owner(lease_t(), comm:mypid()) -> lease_t().
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
    os:timestamp() <  L#lease.timeout.

-spec invalid_lease() -> lease_t().
invalid_lease() ->
    {A, B, C} = os:timestamp(),
    #lease{
       id      = ?RT:get_random_node_id(),
       epoch   = 0,
       owner   = nil,
       range   = intervals:empty(),
       aux     = {invalid, merge, stopped},
       version = 0,
       timeout = {A+1, B, C}}.

-spec id(intervals:interval()) -> ?RT:key().
id([all]) -> ?MINUS_INFINITY;
id(X) ->
    {_, _, Id, _} = intervals:get_bounds(X),
    Id.

-spec split_range(intervals:interval()) -> {ok, intervals:interval(), intervals:interval()}.
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
