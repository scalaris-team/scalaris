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
-export([write/2]).

-export([lease_renew/1]).
-export([lease_handover/2]).

%% filters and checks for rbr_cseq operations
%% consistency
-export([is_valid_state_change/3]).
-export([is_valid_renewal/3]).
-export([is_valid_handover/3]).

-type lease_id() :: ?RT:key().
-type lease_aux() ::
        any().
        %% | {aux, empty}
        %% %% split
        %% | {aux, jhsgjghs} %% 1. L1: 
        %%    | 
        %%    %% merge
%%.

-record(lease, {
          id      = ?required(lease, id     ) :: lease_id(),
          epoch   = ?required(lease, epoch  ) :: non_neg_integer(),
          owner   = ?required(lease, owner  ) :: comm:mypid() | nil,
          range   = ?required(lease, range  ) :: intervals:interval(),
          aux     = ?required(lease, aux    ) :: lease_aux(),
          version = ?required(lease, version) :: non_neg_integer(),
          timeout = ?required(lease, timeout) :: erlang_timestamp()}).
-type lease_entry() :: #lease{}.

delta() -> 5.

%% -type ldb_entry() :: lease_entry().

-spec lease_renew(lease_entry()) -> {ok}.
lease_renew(Old = #lease{id=Id, version=OldVersion}) ->
    Timeout = util:time_plus_s(os:timestamp(), delta()),
    New = Old#lease{version=OldVersion+1, timeout=Timeout},
    write(Id, New, fun l_on_cseq:is_valid_renewal/3).

-spec lease_handover(lease_entry(), comm:this()) -> {ok}.
%% dht_node must remove its old version from its cache immediately
lease_handover(Old = #lease{id=Id, epoch=OldEpoch}, NewOwner) ->
    Timeout = util:time_plus_s(os:timestamp(), delta()),
    New = Old#lease{epoch = OldEpoch + 1,
                    owner = NewOwner,
                    version = 0,
                    timeout = Timeout},
    write(Id, New, fun l_on_cseq:is_valid_handover/3).



-spec read(lease_id()) -> api_tx:read_result().
read(Key) ->
    %% decide which lease db is responsible, ie. if the key is from
    %% the first quarter of the ring, use lease_db1, if from 2nd
    %% quarter -> use lease_db2, ...
    DB = erlang:list_to_existing_atom(
           lists:flatten(
             io_lib:format("lease_db~p", [?RT:get_key_segment(Key, 4)]))),

    %% perform qread
    rbrcseq:qread(DB, self(), Key),
    receive
        ?SCALARIS_RECV({qread_done, _ReqId, _Round, Value},
                       case Value of
                           no_value_yet -> {fail, not_found};
                           _ -> {ok, Value}
                       end
                      )
        end.

write(Key, Value, ContentCheck) ->
    %% decide which lease db is responsible, ie. if the key is from
    %% the first quarter of the ring, use lease_db1, if from 2nd
    %% quarter -> use lease_db2, ...
    DB = erlang:list_to_existing_atom(
           lists:flatten(
             io_lib:format("lease_db~p", ?RT:get_key_segment(Key, 4)))),

    rbrcseq:qwrite(DB, self(), Key,
                   ContentCheck,
                   Value),
    receive
        ?SCALARIS_RECV({qwrite_done, _ReqId, _Round, _Value}, {ok} ) %%;
        %%        ?SCALARIS_RECV({qwrite_deny, _ReqId, _Round, _Value}, {fail, timeout} )
        end.

-spec write(lease_id(), lease_entry()) -> api_tx:write_result().
write(Key, Value) ->
    write(Key, Value, fun l_on_cseq:is_valid_state_change/3).

%% content checks
-spec is_valid_state_change(any(), prbr:write_filter(), any()) ->
                                   {boolean(), null}.
is_valid_state_change(rbr_bottom, _WriteFilter, NewLease) ->
    is_valid_creation(NewLease);
is_valid_state_change(OldQRLease, WriteFilter, NewLease) ->
    Result =
        case OldQRLease#lease.aux of
            {aux, empty} ->
                is_valid_renewal(OldQRLease, WriteFilter, NewLease)
                    orelse true; %% list further checks...
            %% {aux, {create, Parent}} ->
            %%     is_valid_create_step2(
            %%        andalso true,
            _ -> true
        end,
    {Result, null}.

is_valid_creation(New) ->
    %% check with info in aux field (epoch from parent lease)
    %%    (Old#lease.epoch == New#lease.epoch) andalso
    (nil == New#lease.owner)
        andalso (1 == New#lease.version)
    %%    (Old#lease.range == New#lease.range) andalso
    %%    ({aux, {create, ParentId}} == New#lease.aux) andalso
        andalso ({0,0,0} == New#lease.timeout).

-spec is_valid_renewal(lease_entry(), prbr:write_filter(), any()) ->
                              {boolean(), null}.
is_valid_renewal(Current, _WriteFilter, Next) ->
    standard_check(Current, Next)
    %% checks for debugging
        andalso (Current#lease.epoch == Next#lease.epoch)
        andalso (Current#lease.owner == (_Self = pid_groups:get_my(dht_node)))
        andalso (Current#lease.owner == Next#lease.owner)
        andalso (Current#lease.range == Next#lease.range)
        andalso (Current#lease.aux == Next#lease.aux)
        andalso (Current#lease.timeout < Next#lease.timeout)
        andalso (os:timestamp() <  Next#lease.timeout).

-spec is_valid_handover(lease_entry(), prbr:write_filter(), any()) ->
                              {boolean(), null}.
is_valid_handover(Current, _WriteFilter, Next) ->
    standard_check(Current, Next)
    %% checks for debugging
        andalso (Current#lease.epoch+1 == Next#lease.epoch)
        andalso (Current#lease.owner == (_Self = pid_groups:get_my(dht_node)))
        andalso (Current#lease.owner =/= Next#lease.owner)
        andalso (Current#lease.range == Next#lease.range)
        andalso (Current#lease.aux == Next#lease.aux)
        andalso (Current#lease.timeout < Next#lease.timeout)
        andalso (os:timestamp() <  Next#lease.timeout).

standard_check(Current, Next) ->
    %% this serializes all operations on leases
    %% additional checks only for debugging the protocol and ensuring
    %% valid maintenance of the lease data structure and state machine

    %% normal operation incs version
    ( ( (Current#lease.version+1 == Next#lease.version)
     andalso (Current#lease.epoch == Next#lease.epoch))
    %% reset version counter on epoch change
        orelse ((0 == Next#lease.version)
                andalso (Current#lease.epoch+1 == Next#lease.epoch))
    )
    %% debugging test
        andalso (Current#lease.id == Next#lease.id).

