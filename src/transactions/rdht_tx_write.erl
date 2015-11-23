%% @copyright 2009-2015 Zuse Institute Berlin
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
%% -define(TRACE_SNAP(X, Y), ct:pal(X, Y)).
-define(TRACE_SNAP(X, Y), ?TRACE(X, Y)).

-include("scalaris.hrl").
-include("client_types.hrl").

-behaviour(tx_op_beh).
-export([work_phase/3,
         validate_prefilter/1, validate/3,
         commit/5, abort/5,
         extract_from_tlog/4]).

-behaviour(gen_component).
-export([init/1, on/2]).
-export([start_link/1]).
-export([check_config/0]).

-export_type([req_id/0]).

-include("gen_component.hrl").

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
    Key = element(2, Request),
    HashedKey = ?RT:hash_key(Key),
    comm:send_local(RdhtTxWritePid, {start_work_phase, ReqId, ClientPid, HashedKey, Key}).

%% @doc Get a result entry for a write from the given TLog entry.
%%      Update the TLog entry accordingly.
-spec extract_from_tlog
        (tx_tlog:tlog_entry(), client_key(), client_value(), EnDecode::true) ->
                       {tx_tlog:tlog_entry(), api_tx:write_result()};
        (tx_tlog:tlog_entry(), client_key(), rdht_tx:encoded_value(), EnDecode::false) ->
                       {tx_tlog:tlog_entry(), api_tx:write_result()}.
extract_from_tlog(Entry, _Key, Value1, EnDecode) ->
    Value = ?IIF(EnDecode, rdht_tx:encode_value(Value1), Value1),
    E1 = tx_tlog:set_entry_value(Entry, ?value, Value),
    case tx_tlog:get_entry_operation(E1) of
        ?write -> {E1, {ok}};
        ?read  -> {tx_tlog:set_entry_operation(E1, ?write), {ok}}
    end.

%% May make several ones from a single TransLog item (item replication)
%% validate_prefilter(TransLogEntry) ->
%%   [TransLogEntries] (replicas)
-spec validate_prefilter(tx_tlog:tlog_entry()) -> [tx_tlog:tlog_entry()].
validate_prefilter(TLogEntry) ->
    ?TRACE("rdht_tx_write:validate_prefilter(~p)~n", [TLogEntry]),
    Key = tx_tlog:get_entry_key(TLogEntry),
    RKeys = ?RT:get_replica_keys(?RT:hash_key(Key)),
    [ tx_tlog:set_entry_key(TLogEntry, X) || X <- RKeys ].

%% validate the translog entry and return the proposal
-spec validate(db_dht:db(), tx_tlog:snap_number(), tx_tlog:tlog_entry()) ->
    {db_dht:db(), ?prepared | ?abort}.
validate(DB, OwnSnapNumber, RTLogEntry) ->
    %% contact DB to check entry
    %% set locks on DB
    DBEntry = db_dht:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),

    RTVers = tx_tlog:get_entry_version(RTLogEntry),
    DBVers = db_entry:get_version(DBEntry),

    %% Note: RTVers contains the latest version in the system (if not outdated).
    %%       We can not update the DB entry's version with it though since this
    %%       would need the old value in the rtlog in case of rollback and to
    %%       serve read requests properly (version and value have to be changed
    %%       atomically as a pair).
    ReadLocks = db_entry:get_readlock(DBEntry),
    WriteLock = db_entry:get_writelock(DBEntry),
    ?TRACE("rdht_tx_write:validate: local snapnumber is ~p; snapnumber in tlog
           entry is ~p~n",[OwnSnapNumber,tx_tlog:get_entry_snapshot(RTLogEntry)]),
    TLogSnapNo = tx_tlog:get_entry_snapshot(RTLogEntry),
    SnapNumbersOK = (TLogSnapNo >= OwnSnapNumber),
    if ((RTVers =:= DBVers andalso ReadLocks =:= 0) orelse RTVers > DBVers) andalso
           (WriteLock =:= false orelse WriteLock < RTVers) andalso SnapNumbersOK->
           %% set locks on entry (use RTVers for write locks to allow proper
           %% handling of outdated commit and abort messages - only clean up
           %% if the write lock version matches!)
           NewEntry = db_entry:set_writelock(DBEntry, RTVers),
           NewDB = db_dht:set_entry(DB, NewEntry, TLogSnapNo, OwnSnapNumber),
            ?TRACE_SNAP("rdht_tx_write:validate prepare: ~p~n~p  ~p~n~p~n~p~n~p",
                        [comm:this(), tx_tlog:get_entry_snapshot(RTLogEntry),
                         OwnSnapNumber, NewEntry, DB, NewDB]),
           {NewDB, ?prepared};
       true ->
            ?TRACE_SNAP("rdht_tx_write:validate abort: ~p~n~p  ~p",
                        [comm:this(), tx_tlog:get_entry_snapshot(RTLogEntry),
                         OwnSnapNumber]),
           {DB, ?abort}
    end.

-spec commit(db_dht:db(), tx_tlog:tlog_entry(), ?prepared | ?abort,
             tx_tlog:snap_number(), tx_tlog:snap_number()) -> db_dht:db().
commit(DB, RTLogEntry, _OwnProposalWas, _TMSnapNo, OwnSnapNo) ->
    ?TRACE("rdht_tx_write:commit)~n", []),
    DBEntry = db_dht:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),
    %% perform op
    RTLogVers = tx_tlog:get_entry_version(RTLogEntry),
    %% Note: if false =/= WriteLock -> WriteLock is always >= own DBVers!
    DBVers = db_entry:get_version(DBEntry),
    WriteLock = db_entry:get_writelock(DBEntry),
    if DBVers =< RTLogVers ->
            {?value, Value} = tx_tlog:get_entry_value(RTLogEntry),
            T2DBEntry = db_entry:set_value(DBEntry, Value, RTLogVers + 1),
            NewEntry =
                if WriteLock =/= false andalso WriteLock =< RTLogVers ->
                        %% op that created the write lock or outdated WL?
                        db_entry:reset_locks(T2DBEntry);
                   true -> T2DBEntry
                end,
            TLogSnapNo = tx_tlog:get_entry_snapshot(RTLogEntry),
            ?TRACE_SNAP("rdht_tx_write:commit ~p~ncommiting entry~n~p~n~p",
                [comm:this(), NewEntry, DB]),
            db_dht:set_entry(DB, NewEntry, TLogSnapNo, OwnSnapNo);
       true ->
            ?TRACE_SNAP("rdht_tx_write:commit ~p~noutdated commit~n~p",
                [comm:this(), DB]),
            DB %% outdated commit
    end.

-spec abort(db_dht:db(), tx_tlog:tlog_entry(), OwnProposalWas::?prepared | ?abort,
            tx_tlog:snap_number(), tx_tlog:snap_number()) -> db_dht:db().
abort(DB, _RTLogEntry, ?abort, _TMSnapNo, _OwnSnapNo) ->
    %% our own proposal was abort so no locks to clean up
    DB;
abort(DB, RTLogEntry, ?prepared, _TMSnapNo, OwnSnapNo) ->
    ?TRACE("rdht_tx_write:abort)~n", []),
    %% abort operation when we voted prepared
    %% need to release locks
    DBEntry = db_dht:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),
    RTLogVers = tx_tlog:get_entry_version(RTLogEntry),
    % Note: WriteLock is always >= DBVers! - old check:
    %%             DBVers = db_entry:get_version(DBEntry),
    %%             if RTLogVers =:= DBVers ->
    WriteLock = db_entry:get_writelock(DBEntry),
    if WriteLock =/= false andalso WriteLock =< RTLogVers ->
            %% op that created the write lock or outdated WL?
            NewEntry = db_entry:unset_writelock(DBEntry),
            TLogSnapNo = tx_tlog:get_entry_snapshot(RTLogEntry),
            db_dht:set_entry(DB, NewEntry, TLogSnapNo, OwnSnapNo);
        true -> DB
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
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = erlang:list_to_atom(pid_groups:group_to_filename(pid_groups:my_groupname()) ++ "_rdht_tx_write"),
    %%_Table = pdb:new(TableName, [set, protected, named_table]).
    %% use random table name provided by ets to *not* generate an atom
    _Table = pdb:new(?MODULE, [set]).

-spec on(comm:message(), pdb:tableid()) -> pdb:tableid().
on({start_work_phase, ReqId, ClientPid, HashedKey, Key}, TableName) ->
    %% PRE: No entry for key in TLog
    %% build translog entry from quorum read
    rdht_tx_read:work_phase_key(self(), ReqId, Key, HashedKey, ?write),
    pdb:set({ReqId, ClientPid}, TableName),
    TableName;

%% reply triggered by rdht_tx_write:work_phase/3
on({rdht_tx_read_reply, Id, TLogEntry}, TableName) ->
    {Id, ClientPid} = pdb:take(Id, TableName),
    NewTLogEntry = update_tlog_entry(TLogEntry),
    Msg = msg_reply(Id, NewTLogEntry),
    comm:send_local(ClientPid, Msg),
    TableName.

-spec update_tlog_entry(tx_tlog:tlog_entry()) -> tx_tlog:tlog_entry().
update_tlog_entry(TLogEntry) ->
    %% we keep always the read version and expect equivalence during
    %% validation and increment then in case of write.
    T2 = tx_tlog:set_entry_operation(TLogEntry, ?write),
    % should always be true (a partial read for the version never fails)!
    % ?ok =:= tx_tlog:get_entry_status(T2),
    tx_tlog:set_entry_value(T2, ?value, ?value_dropped).

%% @doc Checks whether used config parameters exist and are valid.
-spec check_config() -> true.
check_config() -> true.
