%% @copyright 2009-2012 Zuse Institute Berlin
%%                 onScale solutions GmbH

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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc Part of replicated DHT implementation.
%%      The write operation.
%% @version $Id$
-module(rdht_tx_write).
-author('schintke@onscale.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("scalaris.hrl").

-behaviour(tx_op_beh).
-export([work_phase/3,
         validate_prefilter/1, validate/2,
         commit/3, abort/3]).

-behaviour(gen_component).
-export([init/1, on/2]).
-export([start_link/1]).
-export([check_config/0]).

-ifdef(with_export_type_support).
-export_type([req_id/0]).
-endif.

-type req_id() :: {rdht_tx:req_id(), pid(), any()}.

%% reply messages a client should expect (when calling asynch work_phase/3)
-spec msg_reply(req_id(), tx_tlog:tlog_entry())
               -> comm:message().
msg_reply(Id, TLogEntry) ->
    {rdht_tx_write_reply, Id, TLogEntry}.

-spec work_phase(pid(), rdht_tx:req_id(), api_tx:request()) -> ok.
work_phase(ClientPid, ReqId, Request) ->
    ?TRACE("rdht_tx_write:work_phase asynch~n", []),
    %% Find rdht_tx_write process
    RdhtTxWritePid = pid_groups:find_a(?MODULE),
    % hash key here so that any error during the process is thrown in the client's context
    HashedKey = ?RT:hash_key(element(2, Request)),
    comm:send_local(RdhtTxWritePid, {start_work_phase, ReqId, ClientPid, HashedKey, Request}).

%% May make several ones from a single TransLog item (item replication)
%% validate_prefilter(TransLogEntry) ->
%%   [TransLogEntries] (replicas)
-spec validate_prefilter(tx_tlog:tlog_entry()) -> [tx_tlog:tlog_entry()].
validate_prefilter(TLogEntry) ->
    ?TRACE("rdht_tx_write:validate_prefilter(~p)~n", [TLog]),
    Key = tx_tlog:get_entry_key(TLogEntry),
    RKeys = ?RT:get_replica_keys(?RT:hash_key(Key)),
    [ tx_tlog:set_entry_key(TLogEntry, X) || X <- RKeys ].

%% validate the translog entry and return the proposal
-spec validate(?DB:db(), tx_tlog:tlog_entry()) -> {?DB:db(), ?prepared | ?abort}.
validate(DB, RTLogEntry) ->
    %% contact DB to check entry
    %% set locks on DB
    DBEntry = ?DB:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),

    RTVers = tx_tlog:get_entry_version(RTLogEntry),
    DBVers = db_entry:get_version(DBEntry),

%%%    case RTVers > DBVers of
%%%        true ->
%%%            %% This trick would need the old value in the rtlog.
%%%            %% DB is outdated, in workphase a quorum responded with a
%%%            %% newer version, so a newer version was committed
%%%            %% reset all locks, set version and set writelock
%%%            T1Entry = db_entry:reset_locks(DBEntry),
%%%            T2Entry = db_entry:set_version(T1, RTVers),
%%%            T3Entry = db_entry:set_writelock(T2, true),
%%%            NewDB = ?DB:set_entry(DB, T3Entry),
%%%            {NewDB, ?prepared};
%%%        false ->
    VersionOK = (RTVers =:= DBVers),
    Lockable = not db_entry:is_locked(DBEntry),
    case (VersionOK andalso Lockable) of
        true ->
            %% set locks on entry
            NewEntry = db_entry:set_writelock(DBEntry),
            NewDB = ?DB:set_entry(DB, NewEntry),
            {NewDB, ?prepared};
        false ->
            {DB, ?abort}
    end.

-spec commit(?DB:db(), tx_tlog:tlog_entry(), ?prepared | ?abort) -> ?DB:db().
commit(DB, RTLogEntry, _OwnProposalWas) ->
    ?TRACE("rdht_tx_write:commit)~n", []),
    DBEntry = ?DB:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),
    %% perform op
    RTLogVers = tx_tlog:get_entry_version(RTLogEntry),
    DBVers = db_entry:get_version(DBEntry),
    NewEntry =
        case DBVers > RTLogVers of
            true ->
                DBEntry; %% outdated commit
            false ->
                T2DBEntry = db_entry:set_value(
                              DBEntry, tx_tlog:get_entry_value(RTLogEntry)),
                T3DBEntry = db_entry:set_version(T2DBEntry, RTLogVers + 1),
                db_entry:reset_locks(T3DBEntry)
        end,
    ?DB:set_entry(DB, NewEntry).

-spec abort(?DB:db(), tx_tlog:tlog_entry(), ?prepared | ?abort) -> ?DB:db().
abort(DB, RTLogEntry, OwnProposalWas) ->
    ?TRACE("rdht_tx_write:abort)~n", []),
    %% abort operation
    %% release locks?
    case OwnProposalWas of
        ?prepared ->
            DBEntry = ?DB:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),
            RTLogVers = tx_tlog:get_entry_version(RTLogEntry),
            DBVers = db_entry:get_version(DBEntry),
            case RTLogVers of
                DBVers ->
                    NewEntry = db_entry:unset_writelock(DBEntry),
                    ?DB:set_entry(DB, NewEntry);
                _ -> DB
            end;
        ?abort ->
            DB
    end.

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [],
                             [{pid_groups_join_as, DHTNodeGroup, ?MODULE}]).

%% initialize: return initial state.
-spec init([]) -> null.
init([]) ->
    ?TRACE("rdht_tx_write: Starting rdht_tx_write for DHT node: ~p~n",
           [pid_groups:my_groupname()]),
    DHTNodeGroup = pid_groups:my_groupname(),
    Table = list_to_atom(DHTNodeGroup ++ "_rdht_tx_write"),
    pdb:new(Table, [set, private, named_table]),
    Table.

-spec on(comm:message(), pdb:tableid()) -> pdb:tableid().
on({start_work_phase, ReqId, ClientPid, HashedKey, Request}, TableName) ->
    %% PRE: No entry for key in TLog
    %% build translog entry from quorum read
    pdb:set({ReqId, ClientPid, element(3, Request)}, TableName),
    rdht_tx_read:work_phase_key(self(), ReqId, element(2, Request), HashedKey),
    TableName;

%% reply triggered by rdht_tx_write:work_phase/3
on({rdht_tx_read_reply, Id, TLogEntry}, TableName) ->
    {Id, ClientPid, WriteValue} = pdb:get(Id, TableName),
    Key = tx_tlog:get_entry_key(TLogEntry),
    Request = {write, Key, WriteValue},
    NewTLogEntry = update_tlog_entry(TLogEntry, Request),
    Msg = msg_reply(Id, NewTLogEntry),
    comm:send_local(ClientPid, Msg),
    TableName.

-spec update_tlog_entry(tx_tlog:tlog_entry(),
                        api_tx:write_request()) ->
                               tx_tlog:tlog_entry().
update_tlog_entry(TLogEntry, Request) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    Status = tx_tlog:get_entry_status(TLogEntry),
    Version = tx_tlog:get_entry_version(TLogEntry),
    WriteValue = element(3, Request),
    %% we keep always the read version and expect equivalence during
    %% validation and increment then in case of write.
    case Status of
        ?value ->
            tx_tlog:new_entry(?write, Key, Version, ?value, WriteValue);
        {fail, not_found} ->
            tx_tlog:new_entry(?write, Key, Version, ?value, WriteValue);
        {fail, abort} -> %% only for tester? never called this way?
            TLogEntry
%        {fail, timeout} ->
%            tx_tlog:new_entry(?write, Key, Version, {fail, timeout},
%                               WriteValue)
    end.

%% @doc Checks whether used config parameters exist and are valid.
-spec check_config() -> true.
check_config() -> true.
