%% @copyright 2009, 2010 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("../include/scalaris.hrl").

-behaviour(tx_op_beh).
-export([work_phase/2,work_phase/3,
         validate_prefilter/1,validate/2,
         commit/2,abort/2]).

-behaviour(rdht_op_beh).
-export([tlogentry_get_status/1, tlogentry_get_value/1,
         tlogentry_get_version/1]).

-behaviour(gen_component).
-export([init/1, on/2]).
-export([start_link/1]).
-export([check_config/0]).

%% reply messages a client should expect (when calling asynch work_phase/3)
msg_reply(Id, TLogEntry, ResultEntry) ->
    {rdht_tx_write_reply, Id, TLogEntry, ResultEntry}.

tlogentry_get_status(TLogEntry) ->
    element(3, TLogEntry).
tlogentry_get_value(TLogEntry) ->
    element(4, TLogEntry).
tlogentry_get_version(TLogEntry) ->
    element(5, TLogEntry).

work_phase(TLogEntry, {Num, Request}) ->
    {NewTLogEntry, Result} = my_make_tlog_result_entry(TLogEntry, Request),
    {NewTLogEntry, {Num, Result}}.

work_phase(ClientPid, ReqId, Request) ->
    ?TRACE("rdht_tx_write:work_phase asynch~n", []),
    %% PRE: No entry for key in TLog
    %% build translog entry from quorum read
    %% Find rdht_tx_write process
    WriteValue = erlang:element(3, Request),
    RdhtTxWritePid = process_dictionary:get_group_member(?MODULE),
    rdht_tx_read:work_phase(RdhtTxWritePid, {ReqId, ClientPid, WriteValue},
                            Request),
    ok.

%% May make several ones from a single TransLog item (item replication)
%% validate_prefilter(TransLogEntry) ->
%%   [TransLogEntries] (replicas)
validate_prefilter(TLogEntry) ->
    ?TRACE("rdht_tx_write:validate_prefilter(~p)~n", [TLog]),
    Key = erlang:element(2, TLogEntry),
    RKeys = ?RT:get_keys_for_replicas(Key),
    [ setelement(2, TLogEntry, X) || X <- RKeys ].

%% validate the translog entry and return the proposal
validate(DB, RTLogEntry) ->
    %% contact DB to check entry
    %% set locks on DB
    DBEntry = ?DB:get_entry(DB, element(2, RTLogEntry)),
    VersionOK =
        (tx_tlog:get_entry_version(RTLogEntry)
         =:= db_entry:get_version(DBEntry)),
    Lockable = (false =:= db_entry:get_writelock(DBEntry))
        andalso (0 =:= db_entry:get_readlock(DBEntry)),
    case (VersionOK andalso Lockable) of
        true ->
            %% set locks on entry
            NewEntry = db_entry:set_writelock(DBEntry),
            NewDB = ?DB:set_entry(DB, NewEntry),
            {NewDB, prepared};
        false ->
%%            io:format("WriteLock: ~p VersionOK: ~p (DB) ~p (Tx) ~n", [Lockable, db_entry:get_version(DBEntry), tx_tlog:get_entry_version(RTLogEntry) ]),
            {DB, abort}
    end.

commit(DB, RTLogEntry) ->
    ?TRACE("rdht_tx_write:commit)~n", []),
    DBEntry = ?DB:get_entry(DB, element(2, RTLogEntry)),
    %% perform op
    TmpDBEntry = db_entry:set_value(DBEntry, tx_tlog:get_entry_value(RTLogEntry)),
    Tmp2Entry = db_entry:inc_version(TmpDBEntry),
    %% release locks
    NewEntry = db_entry:unset_writelock(Tmp2Entry),
    ?DB:set_entry(DB, NewEntry).

abort(DB, RTLogEntry) ->
    ?TRACE("rdht_tx_write:abort)~n", []),
    %% abort operation
    %% release locks
    DB.

%% be startable via supervisor, use gen_component
start_link(InstanceId) ->
    gen_component:start_link(?MODULE,
                             [InstanceId],
                             [{register, InstanceId, ?MODULE}]).

%% initialize: return initial state.
init([_InstanceID]) ->
    ?TRACE("rdht_tx_write: Starting rdht_tx_write for instance: ~p~n", [_InstanceID]),
    %% For easier debugging, use a named table (generates an atom)
    %%TableName =
    %%    list_to_atom(lists:flatten(
    %%                   io_lib:format("~p_rdht_tx_write", [InstanceID]))),
    %%ets:new(TableName, [set, private, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    %%TableName = ets:new(?MODULE, [set, private]),
    Reps = config:read(replication_factor),
    Maj = config:read(quorum_factor),
    _State = {Reps, Maj}.

%% reply triggered by rdht_tx_write:work_phase/3
%% ClientPid and WriteValue could also be stored in local process state via ets
on({rdht_tx_read_reply, {Id, ClientPid, WriteValue}, TLogEntry, ResultEntry},
   State) ->
    Key = element(2, TLogEntry),
    Request = {?MODULE, Key, WriteValue},
    {NewTLogEntry, NewResultEntry} =
        my_make_tlog_result_entry(TLogEntry, Request),
    Msg = msg_reply(Id, NewTLogEntry, NewResultEntry),
    cs_send:send_local(ClientPid, Msg),
%%    erlang:yield(),
    State;

on(_, _State) ->
    unknown_event.

my_make_tlog_result_entry(TLogEntry, Request) ->
    Status = apply(element(1, TLogEntry), tlogentry_get_status, [TLogEntry]),
    Version = apply(element(1, TLogEntry), tlogentry_get_version, [TLogEntry]),
    Key = element(2, TLogEntry),
    WriteValue = element(3, Request),
    %% we keep always the read version and expect equivalence during
    %% validation and increment then in case of write.
    case Status of
        not_found ->
            {{?MODULE, Key, value, WriteValue, Version},
             {?MODULE, Key, {value, WriteValue}}};
        value ->
            {{?MODULE, Key, value, WriteValue, Version},
             {?MODULE, Key, {value, WriteValue}}}
    end.

%% @doc Checks whether used config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(quorum_factor) and
    config:is_greater_than(quorum_factor, 0) and
    config:is_integer(replication_factor) and
    config:is_greater_than(replication_factor, 0).
