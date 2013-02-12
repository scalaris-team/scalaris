% @copyright 2012 Zuse Institute Berlin,

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

-export([read/1]).
%%-export([write/2]).

-export([on/2]).

-export([lease_renew/1]).
-export([lease_handover/3]).
-export([lease_takeover/1]).
-export([lease_split/4]).
-export([lease_merge/2]).
% for unit tests
-export([lease_update/2]).
-export([create_lease/1]).
-export([get_db_for_id/1]).

% lease accessors
-export([get_version/1,set_version/2,
         get_epoch/1, set_epoch/2,
         new_timeout/0, set_timeout/1,
         get_id/1,
         get_owner/1, set_owner/2,
         get_aux/1, set_aux/2,
         get_range/1, set_range/2]).

-export([add_first_lease_to_db/2]).

-ifdef(with_export_type_support).
-export_type([lease_list/0]).
-endif.

%% filters and checks for rbr_cseq operations
%% consistency

-type lease_id() :: ?RT:key().
-type lease_aux() ::
        empty
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
-type lease_list() :: [lease_t()].

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

-spec lease_renew(lease_t()) -> ok.
lease_renew(Lease) ->
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, renew, Lease}),
    ok.

-spec lease_handover(lease_t(), comm:mypid(), comm:mypid()) -> ok.
lease_handover(Lease, NewOwner, ReplyTo) ->
    % @todo precondition: i am owner of Lease
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, handover, Lease, NewOwner, ReplyTo}),
    ok.

-spec lease_takeover(lease_t()) -> ok.
lease_takeover(Lease) ->
    % @todo precondition: Lease has timeouted
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, takeover, Lease}),
    ok.

-spec lease_split(lease_t(), intervals:interval(),
                  intervals:interval(), comm:mypid()) -> ok.
lease_split(Lease, R1, R2, ReplyTo) ->
    % @todo precondition: i am owner of Lease and id(R2) == id(Lease)
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, split, Lease, R1, R2, ReplyTo}),
    ok.

-spec lease_merge(lease_t(), lease_t()) -> ok.
lease_merge(Lease1, Lease2) ->
    % @todo precondition: i am owner of Lease1 and Lease2
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, merge, Lease1, Lease2}),
    ok.

% for unit tests
-spec lease_update(lease_t(), lease_t()) -> ok.
lease_update(Old, New) ->
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, update, Old, New, self()}),
    receive
        ?SCALARIS_RECV(
            {l_on_cseq, update_success, Old, New}, %% ->
            ok
          )
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_component
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease renewal
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(any(), dht_node_state:state()) -> dht_node_state:state() | kill.
on({l_on_cseq, renew, Old = #lease{id=Id,version=OldVersion}},
   State) ->
    ct:pal("renew ~p~n", [Old]),
    New = Old#lease{version=OldVersion+1, timeout=new_timeout()},
    ContentCheck = generic_content_check(Old),
    DB = get_db_for_id(Id),
%% @todo New passed for debugging only:
    Self = comm:reply_as(self(), 3, {l_on_cseq, renew_reply, '_', New}),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, New),
    State;

on({l_on_cseq, renew_reply, {qwrite_done, _ReqId, _Round, Value}, _New}, State) ->
    ct:pal("successful renew~n", []),
    update_lease_in_dht_node_state(Value, State);

on({l_on_cseq, renew_reply,
    {qwrite_deny, _ReqId, _Round, Value, {content_check_failed, Reason}}, New},
   State) ->
    % @todo retry
    ct:pal("renew denied: ~p ~p ~p~n", [Reason, Value, New]),
    case Reason of
        lease_does_not_exist ->
            case Value of %@todo is this necessary?
                prbr_bottom ->
                    State;
                _ ->
                    remove_lease_from_dht_node_state(Value, State)
            end;
        unexpected_owner   -> remove_lease_from_dht_node_state(Value, State);
        unexpected_aux     ->
            case get_aux(Value) of
                empty                  -> lease_renew(Value), State;
                {invalid, split, _, _} -> lease_renew(Value), State;
                {invalid, merge, _, _} -> lease_renew(Value), State;
                {invalid, merge, stopped} ->
                    remove_lease_from_dht_node_state(Value, State);
                {valid, split, _, _}   -> lease_renew(Value), State;
                {valid, merge, _, _}   -> lease_renew(Value), State
            end;
        unexpected_range   -> lease_renew(Value), State;
        unexpected_timeout -> lease_renew(Value), State;
        unexpected_epoch   -> lease_renew(Value), State;
        unexpected_version -> lease_renew(Value), State;
        timeout_is_not_newer_than_current_lease ->
            lease_renew(Value),
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease update (only for unit tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, update, Old = #lease{id=Id, epoch=OldEpoch,version=OldVersion},
    New, Caller}, State) ->
    %io:format("renew ~p~n", [Old]),
    ContentCheck = is_valid_update(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    %% @todo New passed for debugging only:
    Self = comm:reply_as(self(), 3, {l_on_cseq, update_reply, '_', Old, New, Caller}),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, New),
    State;

on({l_on_cseq, update_reply, {qwrite_done, _ReqId, _Round, Value},
    Old, New, Caller}, State) ->
    io:format("successful update~n", []),
    comm:send_local(Caller, {l_on_cseq, update_success, Old, New}),
    update_lease_in_dht_node_state(Value, State);

on({l_on_cseq, update_reply,
    {qwrite_deny, _ReqId, _Round, Value, {content_check_failed, Reason}}, New},
   State) ->
    % @todo retry
    ct:pal("update denied: ~p ~p ~p~n", [Reason, Value, New]),
    case Reason of
        lease_does_not_exist ->
            % @todo log message
            State;
        epoch_or_version_mismatch ->
            lease_renew(Value),
            io:format("trying again~n", []),
            State
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease handover
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, handover, Old = #lease{id=Id, epoch=OldEpoch},
    NewOwner, ReplyTo}, State) ->
    New = Old#lease{epoch   = OldEpoch + 1,
                    owner   = NewOwner,
                    version = 0,
                    timeout = new_timeout()},
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
    ct:pal("successful handover~n", []),
    comm:send(ReplyTo, {handover, success, Value}),
    update_lease_in_dht_node_state(Value, State);

on({l_on_cseq, handover_reply, {qwrite_deny, _ReqId, _Round, Value,
                                {content_check_failed, Reason}},
    ReplyTo, NewOwner, New}, State) ->
    ct:pal("handover denied: ~p ~p ~p~n", [Reason, Value, New]),
    case Reason of
        lease_does_not_exist ->
            comm:send(ReplyTo, {handover, failed, Value}),
            case Value of %@todo is this necessary?
                prbr_bottom ->
                    State;
                _ ->
                    remove_lease_from_dht_node_state(Value, State)
            end;
        unexpected_owner   ->
            comm:send(ReplyTo, {handover, failed, Value}),
            remove_lease_from_dht_node_state(Value, State);
        unexpected_aux     ->
            %ct:pal("sending {handover, failed, Value}"),
            comm:send(ReplyTo, {handover, failed, Value}), State;
        unexpected_range   ->
            comm:send(ReplyTo, {handover, failed, Value}), State;
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
on({l_on_cseq, takeover, Old = #lease{id=Id, epoch=OldEpoch,version=OldVersion}},
   State) ->
    New = Old#lease{epoch   = OldEpoch + 1,
                    version = 0,
                    owner   = comm:this(),
                    timeout = new_timeout()},
    ContentCheck = is_valid_takeover(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 3, {l_on_cseq, takeover_reply, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, takeover_reply, {qwrite_done, _ReqId, _Round, _Value}}, State) ->
    % @todo if success update lease in State
    State;

on({l_on_cseq, takeover_reply, {qwrite_deny, _ReqId, _Round, _Value, _Reason}}, State) ->
    % @todo if success update lease in State
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step1)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge, L1 = #lease{id=Id, epoch=OldEpoch,version=OldVersion},
    L2}, State) ->
    New = L1#lease{epoch    = OldEpoch + 1,
                    version = 0,
                    aux     = {invalid, merge, get_range(L1), get_range(L2)},
                    timeout = new_timeout()},
    ContentCheck = is_valid_merge_step1(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 4, {l_on_cseq, merge_reply_step1, L2, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, merge_reply_step1, _L2, {qwrite_deny, _ReqId, _Round, _L1, _Reason}}, State) ->
    % @todo if success update lease in State
    % retry?
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step2)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step1,
    L2 = #lease{id=Id,epoch=OldEpoch,version=OldVersion},
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    % @todo if success update lease in State
    New = L2#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   range   = intervals:union(L1#lease.range, L2#lease.range),
                   aux     = {valid, merge, get_range(L1), get_range(L2)},
                   timeout = new_timeout()},
    ContentCheck = is_valid_merge_step2(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 4, {l_on_cseq, merge_reply_step2, L1, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, merge_reply_step2, _L1, {qwrite_deny, _ReqId, _Round, _L2, _Reason}}, State) ->
    % @todo if success update lease in State
    % retry?
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step3)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step2,
    L1 = #lease{id=Id,epoch=OldEpoch,version=OldVersion},
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    % @todo if success update lease in State
    New = L1#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   aux     = {invalid, merge, stopped}},
    ContentCheck = is_valid_merge_step3(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 4, {l_on_cseq, merge_reply_step3, L2, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, merge_reply_step3, _L2, {qwrite_deny, _ReqId, _Round, _L1, _Reason}}, State) ->
    % @todo if success update lease in State
    % retry?
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step4)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step3,
    L2 = #lease{id=Id,epoch=OldEpoch,version=OldVersion},
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    % @todo if success update lease in State
    New = L2#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   aux     = empty,
                   timeout = new_timeout()},
    ContentCheck = is_valid_merge_step4(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 4, {l_on_cseq, merge_reply_step4, L1, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, merge_reply_step4, _L1, {qwrite_deny, _ReqId, _Round, _L2, _Reason}}, State) ->
    % @todo if success update lease in State
    % retry?
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step1)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split, Lease, R1, R2, ReplyTo}, State) ->
    Id = id(R2),
    ct:pal("split first step: creating second lease ~p~n", [Id]),
    New = #lease{id      = id(R2),
                 epoch   = 1,
                 owner   = comm:this(),
                 range   = R2,
                 aux     = {invalid, split, R1, R2},
                 version = 0,
                 timeout = new_timeout()},
    ContentCheck = is_valid_split_step1(),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 7, {l_on_cseq, split_reply_step1, Lease, R1, R2, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, split_reply_step1, _Lease, _R1, _R2, ReplyTo,
    {qwrite_deny, _ReqId, _Round, Lease, {content_check_failed, Reason}}}, State) ->
    ct:pal("split first step failed: ~p~n", [Reason]),
    case Reason of
        lease_already_exists ->
            comm:send(ReplyTo, {split, fail, Lease}),
            remove_lease_from_dht_node_state(Lease, State)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step2)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split_reply_step1, Lease=#lease{id=Id,epoch=OldEpoch}, R1, R2, ReplyTo,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    ct:pal("split second step: updating L1~n", []),
    New = Lease#lease{
            epoch   = OldEpoch + 1,
            range   = R1,
            aux     = {valid, split, R1, R2},
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(Lease),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 7, {l_on_cseq, split_reply_step2, L2, R1, R2, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    update_lease_in_dht_node_state(L2, State);

on({l_on_cseq, split_reply_step2, L2, R1, R2, ReplyTo,
    {qwrite_deny, _ReqId, _Round, Lease, {content_check_failed, Reason}}}, State) ->
    ct:pal("split second step failed: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send(ReplyTo, {split, fail, Lease}), State; %@todo
        unexpected_owner     -> comm:send(ReplyTo, {split, fail, Lease}),
                                remove_lease_from_dht_node_state(Lease, State); %@todo
        unexpected_range     -> comm:send(ReplyTo, {split, fail, Lease}), State; %@todo
        unexpected_aux       -> comm:send(ReplyTo, {split, fail, Lease}), State; %@todo
        unexpected_timeout ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step1, Lease, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step1, Lease, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        unexpected_epoch ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step1, Lease, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        unexpected_version ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step1, Lease, R1, R2,
                                          {qwrite_done, fake_reqid, fake_round, L2}})
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step3)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split_reply_step2,
    L2 = #lease{id=Id,epoch=OldEpoch}, R1, R2, ReplyTo,
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    ct:pal("split third step: renew L2 ~p~n", [Id]),
    New = L2#lease{
            epoch   = OldEpoch + 1,
            aux     = empty,
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(L2),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 7, {l_on_cseq, split_reply_step3, L1, R1, R2, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    update_lease_in_dht_node_state(L1, State);

on({l_on_cseq, split_reply_step3, L1, R1, R2, ReplyTo,
    {qwrite_deny, _ReqId, _Round, L2, {content_check_failed, Reason}}}, State) ->
    % @todo
    ct:pal("split third step failed: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_owner     -> comm:send(ReplyTo, {split, fail, L2}),
                                remove_lease_from_dht_node_state(L2, State); %@todo
        unexpected_range     -> comm:send(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_aux       -> comm:send(ReplyTo, {split, fail, L2}), State; %@todo
        unexpected_timeout ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step2, L2, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step2, L2, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        unexpected_epoch ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step2, L2, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L1}});
        unexpected_version ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step2, L2, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L1}})
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step4)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split_reply_step3,
    L1 = #lease{id=Id,epoch=OldEpoch}, R1, R2, ReplyTo,
    {qwrite_done, _ReqId, _Round, L2}}, State) ->
    ct:pal("split fourth step: renew L1~n", []),
    New = L1#lease{
            epoch   = OldEpoch + 1,
            aux     = empty,
            version = 0,
            timeout = new_timeout()},
    ContentCheck = generic_content_check(L1),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 7, {l_on_cseq, split_reply_step4, L2, R1, R2, ReplyTo, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    update_lease_in_dht_node_state(L2, State);

on({l_on_cseq, split_reply_step4, _L2, _R1, _R2, ReplyTo,
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    ct:pal("successful split~n", []),
    ct:pal("successful split ~p~n", [ReplyTo]),
    comm:send(ReplyTo, {split, success, L1}),
    update_lease_in_dht_node_state(L1, State);

on({l_on_cseq, split_reply_step4, L2, R1, R2, ReplyTo,
    {qwrite_deny, _ReqId, _Round, L1, {content_check_failed, Reason}}}, State) ->
    % @todo
    ct:pal("split fourth step: ~p~n", [Reason]),
    case Reason of
        lease_does_not_exist -> comm:send(ReplyTo, {split, fail, L1}), State;
        unexpected_owner     -> comm:send(ReplyTo, {split, fail, L1}),
                                remove_lease_from_dht_node_state(L1, State);
        unexpected_range     -> comm:send(ReplyTo, {split, fail, L1}), State;
        unexpected_aux       -> comm:send(ReplyTo, {split, fail, L1}), State;
        unexpected_timeout ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step3, L1, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        timeout_is_not_newer_than_current_lease ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step3, L1, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        unexpected_epoch ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step3, L1, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L2}});
        unexpected_version ->
            % retry
            gen_component:post_op(State, {l_on_cseq, split_reply_step3, L1, R1, R2, ReplyTo,
                                          {qwrite_done, fake_reqid, fake_round, L2}})
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% renew all local leases
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, renew_leases}, State) ->
    LeaseList = dht_node_state:get(State, lease_list),
    io:format("renewing all local leases: ~p~n", [length(LeaseList)]),
    _ = [lease_renew(L) || L <- LeaseList],
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
            ct:pal("is_valid_update: expected ~p, got ~p", [CurrentEpoch, E0]),
            {false, epoch_or_version_mismatch};
        (#lease{version = V0}, _, _)                   when V0 =/= CurrentVersion->
            ct:pal("is_valid_update: expected ~p, got ~p", [CurrentVersion, V0]),
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
    fun (Current, _WriteFilter, Next) ->
            Res = standard_check(Current, Next, Epoch, Version)
            %% checks for debugging
                andalso (Current#lease.epoch+1 == Next#lease.epoch)
                andalso (Next#lease.owner == comm:make_global(pid_groups:get_my(dht_node)))
                andalso (Current#lease.owner =/= Next#lease.owner)
                andalso (Current#lease.range == Next#lease.range)
                andalso (Current#lease.aux == Next#lease.aux)
                andalso (Current#lease.timeout < Next#lease.timeout)
                andalso (os:timestamp() <  Next#lease.timeout),
            {Res, null}
    end.

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

-spec read(lease_id(), comm:erl_local_pid_plain()) -> ok.
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
    dht_node_state:set_lease_list(State, [Lease]).

-spec create_lease(?RT:key()) -> lease_t().
create_lease(Id) ->
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

-spec update_lease_in_dht_node_state(lease_t(), dht_node_state:state()) ->
    dht_node_state:state().
update_lease_in_dht_node_state(Lease, State) ->
    Id = Lease#lease.id,
    LeaseList = dht_node_state:get(State, lease_list),
    NewList = case lists:keyfind(Id, 2, LeaseList) of
                  false ->
                      [Lease|LeaseList];
                  _OldLease ->
                      %io:format("replacing ~p with ~p ~n", [OldLease, Lease]),
                      lists:keyreplace(Id, 2, LeaseList, Lease)
              end,
    dht_node_state:set_lease_list(State, NewList).

-spec remove_lease_from_dht_node_state(lease_t(), dht_node_state:state()) ->
    dht_node_state:state().
remove_lease_from_dht_node_state(Lease, State) ->
    Id = Lease#lease.id,
    LeaseList = dht_node_state:get(State, lease_list),
    NewList = lists:keydelete(Id, 2, LeaseList),
    dht_node_state:set_lease_list(State, NewList).

- spec id(intervals:interval()) -> non_neg_integer().
id([]) -> 0;
id([[]]) -> 0;
id(X) ->
    case lists:member(all, X) of
        true -> 0;
        _ ->
            {_, Id, _, _} = intervals:get_bounds(X),
            Id
    end.

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
