%  @copyright 2012 Zuse Institute Berlin

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
%% @doc    data_node replicated lease storage and management
%% @end
%% @version $Id$
-module(rlease_mgmt).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id ').
-include("scalaris.hrl").
-include("record_helpers.hrl").

%% part of data_node on-handler for lease management
-export([on/2]).
-type msg() :: {rlease_mgmt, {renew_lease}}.

%% operations on rlease_mgmt state
-export([new_state/0,   %% create new state
%         get_rlease/3,  %% get a lease by its lease_id
         store_rlease/3 %% store a lease (replaced, when id already exists)
        ]).

-type nth() :: pos_integer().
%-record(rlease, {
%          key   = ?required(rlease, key) :: {?RT:key(), nth()}, %% key, nth replica
%          lease = ?required(rlease, lease) :: leases:lease()
%         }).
%-type rlease() :: #rlease{}.

-record(rleasedb, {
          rleases    = ?required(rleasedb, rleases) :: leases:leases(),
          persistent = ?required(rleasedb, persistent) :: tid() %% ?DB:db(), this is not possible, because for dialyzer ?DB ist strictly depending to store db_entries...
         }).
-type rleasedb() :: #rleasedb{}.
-type state() :: rleasedb().

-ifdef(with_export_type_support).
-export_type([msg/0, state/0]).
-endif.

-spec new_state() -> state().
new_state() ->
    % ets prefix: leases_ + random name
    RandomName = randoms:getRandomString(),
    DBPName = "leases_p_" ++ RandomName, % persistent
    PersistentTable = ets:new(list_to_atom(DBPName), [ordered_set | ?DB_ETS_ADDITIONAL_OPS]),
    #rleasedb{rleases = [],
              persistent = PersistentTable}.

%% TODO: replace by quorum read on replicated lease stores
-spec get_rlease([state()], leases:lease_id(), nth()) -> leases:lease() | unknown.
get_rlease(L, LeaseId, ReplNo) ->
    RLeases = lists:nth(ReplNo, L),
    VList = RLeases#rleasedb.rleases,
    leases:find_by_id(VList, LeaseId).

%% TODO: replace by paxos on replicated lease stores
-spec store_rlease([state()], leases:lease(), nth()) -> [state()].
store_rlease(RLeases, Lease, ReplNo) ->
    RLeaseDB = lists:nth(ReplNo, RLeases),
    VTable = RLeaseDB#rleasedb.rleases,
    PTable = RLeaseDB#rleasedb.persistent,
    LeaseId = leases:get_id(Lease),
    LeaseEpoch = leases:get_epoch(Lease),
    PLease = leases:get_persistent_info(Lease),
    Res =
        case leases:find_by_id(VTable, LeaseId) of
            unknown -> %% lease is new
                ets:insert(PTable, PLease),
                RLeaseDB#rleasedb{rleases=[Lease | VTable]};
            FoundLease ->
                case leases:get_epoch(FoundLease) of
                    LeaseEpoch ->
                        %% epoch is unchanged -> update in ram
                        NewRLeases = lists:keyreplace(LeaseId, #rleasedb.rleases, VTable, Lease),
                        RLeaseDB#rleasedb{rleases=NewRLeases};
                    _ ->
                        %% epoch is changed -> write to disk
                        ets:insert(PTable, PLease),
                        NewRLeaseDB = lists:keyreplace(LeaseId, #rleasedb.rleases, VTable, Lease),
                        RLeaseDB#rleasedb{rleases=NewRLeaseDB}
                end
        end,
    util:list_set_nth(RLeases, ReplNo, Res).

%% %% scan DB for own lease entries (used on recovery)
%% -spec get_my_leases(state()) -> [rlease()].
%% get_my_leases(#rleasedb{rleases=MyLeases}) -> MyLeases.

-spec on(msg(), data_node:state()) -> data_node:state().
on({rlease_mgmt, {renew_lease}}, State) ->
    State.
