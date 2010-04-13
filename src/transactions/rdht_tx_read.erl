%% @copyright 2009, 2010 onScale solutions GmbH

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
%%      The read operation.
%% @version $Id$
-module(rdht_tx_read).
-author('schintke@onscale.de').
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("scalaris.hrl").

-behaviour(tx_op_beh).
-export([work_phase/2, work_phase/3,
         validate_prefilter/1, validate/2,
         commit/2, abort/2]).

-behaviour(rdht_op_beh).
-export([tlogentry_get_status/1, tlogentry_get_value/1,
         tlogentry_get_version/1]).

-behaviour(gen_component).
-export([init/1, on/2]).
-export([start_link/1]).
-export([check_config/0]).

%% reply messages a client should expect (when calling asynch work_phase/3)
msg_reply(Id, TLogEntry, ResultEntry) ->
    {rdht_tx_read_reply, Id, TLogEntry, ResultEntry}.

tlogentry_get_status(TLogEntry) ->
    element(3, TLogEntry).
tlogentry_get_value(TLogEntry) ->
    element(4, TLogEntry).
tlogentry_get_version(TLogEntry) ->
    element(5, TLogEntry).

work_phase(TLogEntry, {Num, Request}) ->
    ?TRACE("rdht_tx_read:work_phase~n", []),
    %% PRE no failed entries in TLog
    Status = apply(element(1, TLogEntry), tlogentry_get_status, [TLogEntry]),
    Value = apply(element(1, TLogEntry), tlogentry_get_value, [TLogEntry]),
    Version = apply(element(1, TLogEntry), tlogentry_get_version, [TLogEntry]),
    NewTLogEntry =
        {?MODULE, element(2, Request), Status, Value, Version},
    Result =
        case Status of
            not_found -> {Num, {?MODULE, element(2, Request), {fail, Status}}};
            value -> {Num, {?MODULE, element(2, Request), {Status, Value}}}
        end,
    {NewTLogEntry, Result}.

work_phase(ClientPid, ReqId, Request) ->
    ?TRACE("rdht_tx_read:work_phase asynch~n", []),
    %% PRE: No entry for key in TLog
    %% find rdht_tx_read process as collector
    CollectorPid = process_dictionary:get_group_member(?MODULE),
    Key = element(2, Request),
    %% do a quorum read
    quorum_read(cs_send:make_global(CollectorPid), ReqId, Request),
    %% inform CollectorPid on whom to inform after quorum reached
    cs_send:send_local(CollectorPid, {client_is, ReqId, ClientPid, Key}),
    ok.

quorum_read(CollectorPid, ReqId, Request) ->
    ?TRACE("rdht_tx_read:quorum_read~n", []),
    Key = element(2, Request),
    RKeys = ?RT:get_keys_for_replicas(Key),
    [ cs_lookup:unreliable_get_key(CollectorPid, ReqId, X) || X <- RKeys ],
    ok.

%% May make several ones from a single TransLog item (item replication)
%% validate_prefilter(TransLogEntry) ->
%%   [TransLogEntries] (replicas)
validate_prefilter(TLogEntry) ->
    ?TRACE("rdht_tx_read:validate_prefilter(~p)~n", [TLog]),
    Key = erlang:element(2, TLogEntry),
    RKeys = ?RT:get_keys_for_replicas(Key),
    [ setelement(2, TLogEntry, X) || X <- RKeys ].

%% validate the translog entry and return the proposal
validate(DB, RTLogEntry) ->
    ?TRACE("rdht_tx_read:validate)~n", []),
    %% contact DB to check entry
    DBEntry = ?DB:get_entry(DB, element(2, RTLogEntry)),
    VersionOK =
        (tx_tlog:get_entry_version(RTLogEntry)
         >= db_entry:get_version(DBEntry)),
    Lockable = (false =:= db_entry:get_writelock(DBEntry)),
    case (VersionOK andalso Lockable) of
        true ->
            %% set locks on entry
            NewEntry = db_entry:inc_readlock(DBEntry),
            NewDB = ?DB:set_entry(DB, NewEntry),
            {NewDB, prepared};
        false ->
          {DB, abort}
    end.

commit(DB, RTLogEntry) ->
    ?TRACE("rdht_tx_read:commit)~n", []),
    DBEntry = ?DB:get_entry(DB, element(2, RTLogEntry)),
    %% perform op: nothing to do
    %% release locks
    NewEntry = db_entry:dec_readlock(DBEntry),
    ?DB:set_entry(DB, NewEntry).

abort(DB, RTLogEntry) ->
    ?TRACE("rdht_tx_read:abort)~n", []),
    %% same as when committing
    commit(DB, RTLogEntry).


%% be startable via supervisor, use gen_component
-spec start_link(instanceid()) -> {ok, pid()}.
start_link(InstanceId) ->
    gen_component:start_link(?MODULE,
                             [InstanceId],
                             [{register, InstanceId, ?MODULE}]).

%% initialize: return initial state.
-spec init([instanceid()]) -> any().
init([InstanceID]) ->
    ?TRACE("rdht_tx_read: Starting rdht_tx_read for instance: ~p~n", [InstanceID]),
    %% For easier debugging, use a named table (generates an atom)
    ActiveTable =
        list_to_atom(lists:flatten(
                       io_lib:format("~p_rdht_tx_read", [InstanceID]))),
    ets:new(ActiveTable, [set, private, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    %% ActiveTable = ets:new(?MODULE, [set, private]),
    Reps = config:read(replication_factor),
    Maj = config:read(quorum_factor),
    EmptyEntry = rdht_tx_read_state:new('$_no_curr_entry'),
    %% use 2nd table to record events for timeout handling
    %% rotate tables periodically
    WaitTable = ets:new(?MODULE, [set, private]),
    cs_send:send_local_after(config:read(transaction_lookup_timeout),
                             self(), {periodic_timeout}),
    _State = {EmptyEntry, Reps, Maj, WaitTable, ActiveTable}.

%% reply triggered by cs_lookup:unreliable_get_key/3
on({get_key_with_id_reply, Id, _Key, {ok, Val, Vers}},
   {_CurrEntry, Reps, Maj, WaitTable, ActiveTable} = State) ->
    ?TRACE("rdht_tx_read:on(get_key_with_id_reply)~n", []),
    Entry = my_get_entry(Id, State),
    %% @todo inform sender when its entry is outdated?
    %% @todo inform former sender on outdated entry when we
    %% get a newer entry?
    %% @todo got replies from all reps? -> delete ets entry
    TmpEntry = rdht_tx_read_state:add_reply(Entry, Val, Vers, Maj),
    NewEntry =
        case {rdht_tx_read_state:is_newly_decided(TmpEntry),
              rdht_tx_read_state:get_client(TmpEntry)} of
            {true, unknown} ->
                %% when we get a client, we inform it
                TmpEntry;
            {true, Client} ->
                my_inform_client(Client, TmpEntry),
                rdht_tx_read_state:set_client_informed(TmpEntry);
            {false, unknown} ->
                TmpEntry;
            {false, _Client} ->
                my_delete_if_all_replied(TmpEntry, Reps, WaitTable, ActiveTable)
        end,
    my_set_entry(NewEntry, State);

%% triggered by ?MODULE:work_phase/3
on({client_is, Id, Pid, Key}, {_CurrEntry, Reps, _Maj, WaitTable, ActiveTable} = State) ->
    ?TRACE("rdht_tx_read:on(client_is)~n", []),
    Entry = my_get_entry(Id, State),
    Tmp1Entry = rdht_tx_read_state:set_client(Entry, Pid),
    TmpEntry = rdht_tx_read_state:set_key(Tmp1Entry, Key),
    NewEntry =
        case rdht_tx_read_state:is_newly_decided(TmpEntry) of
            true ->
                my_inform_client(Pid, TmpEntry),
                Tmp2Entry = rdht_tx_read_state:set_client_informed(TmpEntry),
                my_delete_if_all_replied(Tmp2Entry, Reps, WaitTable, ActiveTable);
            false -> TmpEntry
        end,
    my_set_entry(NewEntry, State);

%% triggered periodically
on({periodic_timeout}, {CurrEntry, Reps, Maj, WaitTable, ActiveTable} = _State) ->
    ?TRACE("rdht_tx_read:on(timeout)~n", []),
    %% CurrEntry in WaitTable? -> clean cache, else -> put into next Waittable
    Id = rdht_tx_read_state:get_id(CurrEntry),
    NewEntry =
        case Id of
            '$_no_curr_entry' -> CurrEntry;
            _ ->
                case ets:member(WaitTable, Id) of
                    true ->
                        %% write back to WaitTable and delete from cache
                        ets:insert(WaitTable, CurrEntry),
                        rdht_tx_read_state:new('$_no_curr_entry');
                    false ->
                        %% store in new wait table
                        ets:insert(ActiveTable, CurrEntry),
                        CurrEntry
                end
        end,
    %% inform client on timeout if Id exists and client is not informed
    my_timeout_inform(WaitTable, ActiveTable, ets:first(WaitTable)),
    ets:delete_all_objects(WaitTable),
    cs_send:send_local_after(config:read(transaction_lookup_timeout),
                             self(), {periodic_timeout}),
    %% swap the two tables.
    {NewEntry, Reps, Maj, ActiveTable, WaitTable};

on(_, _State) ->
    unknown_event.

%% inform client on timeout if Id exists and client is not informed yet
my_timeout_inform(_Table, _ActiveTable, '$end_of_table') ->
    ok;
my_timeout_inform(Table, ActiveTable, IterKey) ->
    [Entry] = ets:lookup(Table, IterKey),
    ets:delete(ActiveTable, rdht_tx_read_state:get_id(Entry)),
    case {rdht_tx_read_state:is_client_informed(Entry),
          rdht_tx_read_state:get_client(Entry)} of
        {_, unknown} -> ok;
        {false, Client} ->
            TmpEntry = rdht_tx_read_state:set_decided(Entry, timeout),
            my_inform_client(Client, TmpEntry);
        _ -> ok
    end,
    my_timeout_inform(Table, ActiveTable, ets:next(Table,IterKey)).

my_inform_client(Client, Entry) ->
    Id = rdht_tx_read_state:get_id(Entry),
    Msg = msg_reply(Id, my_make_tlog_entry(Entry),
                    my_make_result_entry(Entry)),
    cs_send:send_local(Client, Msg).

my_make_tlog_entry(Entry) ->
    {Val, Vers} = rdht_tx_read_state:get_result(Entry),
    Key = rdht_tx_read_state:get_key(Entry),
    Status = rdht_tx_read_state:get_decided(Entry),
    {?MODULE, Key, Status, Val, Vers}.

my_make_result_entry(Entry) ->
    Key = rdht_tx_read_state:get_key(Entry),
    {Val, _Vers} = rdht_tx_read_state:get_result(Entry),
    case rdht_tx_read_state:get_decided(Entry) of
        timeout -> {?MODULE, Key, {fail, timeout}};
        not_found -> {?MODULE, Key, {fail, not_found}};
        value -> {?MODULE, Key, {value, Val}}
    end.

my_get_ets_entry(ActiveTable, WaitTable, Id) ->
    case ets:lookup(ActiveTable, Id) of
        [] ->
            case ets:lookup(WaitTable, Id) of
                [] -> rdht_tx_read_state:new(Id);
                [OldEntry] ->
                    ets:delete(WaitTable, Id),
                    OldEntry
            end;
        [Entry] -> Entry
    end.

my_get_entry(Id, {CurrEntry, _Reps, _Maj, WaitTable, ActiveTable} = _State) ->
    %% implement a cache for the current entry
    %% only write back to ets table, if concurrent requests are handled
    CachedId = rdht_tx_read_state:get_id(CurrEntry),
    case CachedId of
        Id -> CurrEntry;
        '$_no_curr_entry' -> my_get_ets_entry(ActiveTable, WaitTable, Id);
        _ ->
            %% write back the cached entry.
            %% It would have been already deleted if not needed anymore.
            ets:insert(ActiveTable, CurrEntry),
            ets:delete(WaitTable, CurrEntry),
            my_get_ets_entry(ActiveTable, WaitTable, Id)
    end.

my_set_entry(NewEntry, {_CurrEntry, Reps, Maj, WaitTable, ActiveTable} = _State) ->
    {NewEntry, Reps, Maj, WaitTable, ActiveTable}.

my_delete_if_all_replied(Entry, Reps, WaitTable, ActiveTable) ->
    Id = rdht_tx_read_state:get_id(Entry),
    case (Reps =:= rdht_tx_read_state:get_numreplied(Entry))
        andalso (rdht_tx_read_state:is_client_informed(Entry)) of
        true ->
            ets:delete(ActiveTable, Id),
            ets:delete(WaitTable, Id),
            rdht_tx_read_state:new('$_no_curr_entry');
        false -> Entry
    end.

%% @doc Checks whether config parameters for rdht_tx_read exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(quorum_factor) and
    config:is_greater_than(quorum_factor, 0) and
    config:is_integer(replication_factor) and
    config:is_greater_than(replication_factor, 0) and

    config:is_integer(transaction_lookup_timeout) and
    config:is_greater_than(transaction_lookup_timeout, 0).
