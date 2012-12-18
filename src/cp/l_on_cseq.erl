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
%% @doc lease store based on rbrcseq.
%% @end
%% @version $Id:$
-module(l_on_cseq).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("record_helpers.hrl").
-include("client_types.hrl").

-export([read/1]).
%%-export([write/2]).

-export([on/2]).

-export([lease_renew/1]).
-export([lease_handover/2]).
-export([lease_takeover/1]).
-export([lease_split/3]).
-export([lease_merge/2]).

-export([add_first_lease_to_db/2]).

-ifdef(with_export_type_support).
-export_type([lease_list/0]).
-endif.

%% filters and checks for rbr_cseq operations
%% consistency

-type lease_id() :: ?RT:key().
-type lease_aux() ::
        empty
      | {invalid, split, R1, R2}
      | {valid,   split, R1, R2}
      | {invalid, merge, L1, L2}
      | {invalid, merge, stopped}
      | {valid,   merge, L1, L2}.

-record(lease, {
          id      = ?required(lease, id     ) :: lease_id(),
          epoch   = ?required(lease, epoch  ) :: non_neg_integer(),
          owner   = ?required(lease, owner  ) :: comm:mypid() | nil,
          range   = ?required(lease, range  ) :: intervals:interval(),
          aux     = ?required(lease, aux    ) :: lease_aux(),
          version = ?required(lease, version) :: non_neg_integer(),
          timeout = ?required(lease, timeout) :: erlang_timestamp()}).
-type lease_entry() :: #lease{}.
-type lease_list() :: list(lease_entry()).

-spec delta() -> pos_integer().
delta() -> 5.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Public API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec lease_renew(lease_entry()) -> ok.
lease_renew(Lease) ->
    io:format("trigger renew~n", []),
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, renew, Lease}),
    ok.

-spec lease_handover(lease_entry(), comm:mypid()) -> ok.
lease_handover(Lease, NewOwner) ->
    % @todo precondition: i am owner of Lease
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, handover, Lease, NewOwner}),
    ok.

-spec lease_takeover(lease_entry()) -> ok.
lease_takeover(Lease) ->
    % @todo precondition: Lease has timeouted
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, takeover, Lease}),
    ok.

-spec lease_split(lease_entry(), intervals:interval(), intervals:interval()) -> ok.
lease_split(Lease, R1, R2) ->
    % @todo precondition: i am owner of Lease
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, split, Lease, R1, R2}),
    ok.

-spec lease_merge(lease_entry(), lease_entry()) -> ok.
lease_merge(Lease1, Lease2) ->
    % @todo precondition: i am owner of Lease1 and Lease2
    comm:send_local(pid_groups:get_my(dht_node),
                    {l_on_cseq, merge, Lease1, Lease2}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_component
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease renewal
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec on(any(), dht_node_state:state()) -> dht_node_state:state() | kill.
on({l_on_cseq, renew, Old = #lease{id=Id, epoch=OldEpoch,version=OldVersion}},
   State) ->
    New = Old#lease{version=OldVersion+1, timeout=new_timeout()},
    ContentCheck = is_valid_renewal(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    %% @todo New passed for debugging only:
    Self = comm:reply_as(self(), 3, {l_on_cseq, renew_reply, '_', New}),
    rbrcseq:qwrite(DB, Self, Id, ContentCheck, New),
    State;

on({l_on_cseq, renew_reply, {qwrite_done, _ReqId, _Round, Value}, New}, State) ->
    % @todo if success update lease in State
    update_lease_in_dht_node_state(Value, State);

on({l_on_cseq, renew_reply, {qwrite_deny, _ReqId, _Round, Value, Reason}, New}, State) ->
    % @todo if success update lease in State
    io:format("trying again~n", []),
    lease_renew(Value),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease handover
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, handover, Old = #lease{id=Id, epoch=OldEpoch,version=OldVersion},
    NewOwner}, State) ->
    New = Old#lease{epoch   = OldEpoch + 1,
                    owner   = NewOwner,
                    version = 0,
                    timeout = new_timeout()},
    ContentCheck = is_valid_handover(OldEpoch, OldVersion),
    DB = get_db_for_id(Id),
    Self = comm:reply_as(self(), 3, {l_on_cseq, handover_reply, '_'}),
    rbrcseq:qwrite(DB, Self, Id,
                   ContentCheck,
                   New),
    State;

on({l_on_cseq, handover_reply, {qwrite_done, _ReqId, _Round, _Value}}, State) ->
    % @todo if success update lease in State
    State;

on({l_on_cseq, handover_reply, {qwrite_deny, _ReqId, _Round, _Value, _Reason}}, State) ->
    % @todo if success update lease in State
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease takeover
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step1)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge, L1 = #lease{id=Id, epoch=OldEpoch,version=OldVersion},
    L2}, State) ->
    New = L1#lease{epoch    = OldEpoch + 1,
                    version = 0,
                    aux     = {invalid, merge, L1, L2},
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step2)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, merge_reply_step1,
    L2 = #lease{id=Id,epoch=OldEpoch,version=OldVersion},
    {qwrite_done, _ReqId, _Round, L1}}, State) ->
    % @todo if success update lease in State
    New = L2#lease{epoch   = OldEpoch + 1,
                   version = 0,
                   range   = intervals:union(L1#lease.range, L2#lease.range),
                   aux     = {valid, merge, L1, L2},
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step3)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease merge (step4)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% lease split (step1)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({l_on_cseq, split, _Lease, _R1, _R2}, State) ->
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% content checks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec is_valid_renewal(non_neg_integer(), non_neg_integer()) ->
    fun ((any(), any(), any()) -> {boolean, null}). %% content check
is_valid_renewal(Epoch, Version) ->
    fun (Current, _WriteFilter, Next) ->
            Res = standard_check(Current, Next, Epoch, Version)
            %% checks for debugging
                andalso (Current#lease.epoch == Next#lease.epoch)
                andalso (Current#lease.owner == comm:make_global(pid_groups:get_my(dht_node)))
                andalso (Current#lease.owner == Next#lease.owner)
                andalso (Current#lease.range == Next#lease.range)
                andalso (Current#lease.aux == Next#lease.aux)
                andalso (Current#lease.timeout < Next#lease.timeout)
                andalso (os:timestamp() <  Next#lease.timeout),
            {Res, null}
    end.

-spec is_valid_handover(non_neg_integer(), non_neg_integer()) ->
    fun ((any(), any(), any()) -> {boolean(), null}). %% content check
is_valid_handover(Epoch, Version) ->
    fun (Current, _WriteFilter, Next) ->
            Res = standard_check(Current, Next, Epoch, Version)
            %% checks for debugging
                andalso (Current#lease.epoch+1 == Next#lease.epoch)
                andalso (Current#lease.owner == comm:make_global(pid_groups:get_my(dht_node)))
                andalso (Current#lease.owner =/= Next#lease.owner)
                andalso (Current#lease.range == Next#lease.range)
                andalso (Current#lease.aux == Next#lease.aux)
                andalso (Current#lease.timeout < Next#lease.timeout)
                andalso (os:timestamp() <  Next#lease.timeout),
            {Res, null}
    end.

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

-spec standard_check(prbr_bottom | lease_entry(), lease_entry(),
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% util
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
%%         %%        ?SCALARIS_RECV({qwrite_deny, _ReqId, _Round, _Value, Reason}, {fail, timeout} )
%%         end.

%% -spec write(lease_id(), lease_entry()) -> api_tx:write_result().
%% write(Key, Value) ->
%%     write(Key, Value, fun l_on_cseq:is_valid_state_change/3).

-spec add_first_lease_to_db(?RT:key(), dht_node_state:state()) ->
                                  dht_node_state:state().
add_first_lease_to_db(Id, State) ->
    DB = get_db_for_id(Id),
    Lease = #lease{id      = Id,
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

-spec get_db_for_id(?RT:key()) -> atom().
get_db_for_id(Id) ->
    erlang:list_to_existing_atom(
      lists:flatten(
        io_lib:format("lease_db~p", [?RT:get_key_segment(Id)]))).

-spec new_timeout() -> erlang_timestamp().
new_timeout() ->
    util:time_plus_s(os:timestamp(), delta()).

-spec update_lease_in_dht_node_state(lease_entry(), dht_node_state:state()) ->
    dht_node_state:state().
update_lease_in_dht_node_state(Lease, State) ->
    Id = Lease#lease.id,
    LeaseList = dht_node_state:get(State, lease_list),
    NewList = case lists:keyfind(Id, 2, LeaseList) of
                  false ->
                      [Lease|LeaseList];
                  OldLease ->
                      io:format("replacing ~p with ~p ~n", [OldLease, Lease]),
                      lists:keyreplace(Id, 2, LeaseList, Lease)
              end,
    dht_node_state:set_lease_list(State, NewList).
