%% @copyright 2009-2012 Zuse Institute Berlin
%%            2009 onScale solutions GmbH

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
-vsn('$Id$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("scalaris.hrl").
-include("client_types.hrl").

-behaviour(tx_op_beh).
-export([work_phase/3, work_phase_key/4,
         validate_prefilter/1, validate/2,
         commit/3, abort/3]).

-behaviour(gen_component).
-export([init/1, on/2]).

-export([start_link/1]).
-export([check_config/0]).

%% reply messages a client should expect (when calling asynch work_phase/3)
-spec msg_reply(rdht_tx:req_id() | rdht_tx_write:req_id(),
                tx_tlog:tlog_entry()) -> comm:message().
msg_reply(Id, TLogEntry) ->
    {rdht_tx_read_reply, Id, TLogEntry}.

-spec work_phase(pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                 api_tx:request()) -> ok.
work_phase(ClientPid, ReqId, Request) ->
    Key = element(2, Request),
    HashedKey = ?RT:hash_key(Key),
    work_phase_key(ClientPid, ReqId, Key, HashedKey).

-spec work_phase_key(pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                     client_key(), ?RT:key()) -> ok.
work_phase_key(ClientPid, ReqId, Key, HashedKey) ->
    ?TRACE("rdht_tx_read:work_phase asynch~n", []),
    %% PRE: No entry for key in TLog
    %% find rdht_tx_read process as collector
    CollectorPid = pid_groups:find_a(?MODULE),
    %% trigger quorum read
    quorum_read(comm:make_global(CollectorPid), ReqId, HashedKey),
    %% inform CollectorPid on whom to inform after quorum reached
    comm:send_local(CollectorPid, {client_is, ReqId, ClientPid, Key}),
    ok.

-spec quorum_read(CollectorPid::comm:mypid(), ReqId::rdht_tx:req_id() | rdht_tx_write:req_id(),
                  HashedKey::?RT:key()) -> ok.
quorum_read(CollectorPid, ReqId, HashedKey) ->
    ?TRACE("rdht_tx_read:quorum_read ~p Collector: ~p~n", [self(), CollectorPid]),
    RKeys = ?RT:get_replica_keys(HashedKey),
    _ = [ api_dht_raw:unreliable_get_key(CollectorPid, ReqId, X) || X <- RKeys ],
    ok.

%% May make several ones from a single TransLog item (item replication)
%% validate_prefilter(TransLogEntry) ->
%%   [TransLogEntries] (replicas)
-spec validate_prefilter(tx_tlog:tlog_entry()) -> [tx_tlog:tlog_entry()].
validate_prefilter(TLogEntry) ->
    ?TRACE("rdht_tx_read:validate_prefilter(~p)~n", [TLogEntry]),
    Key = tx_tlog:get_entry_key(TLogEntry),
    RKeys = ?RT:get_replica_keys(?RT:hash_key(Key)),
    [ tx_tlog:set_entry_key(TLogEntry, X) || X <- RKeys ].

%% validate the translog entry and return the proposal
-spec validate(?DB:db(), tx_tlog:tlog_entry()) -> {?DB:db(), ?prepared | ?abort}.
validate(DB, RTLogEntry) ->
    ?TRACE("rdht_tx_read:validate)~n", []),
    %% contact DB to check entry
    DBEntry = ?DB:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),
    VersionOK =
        (tx_tlog:get_entry_version(RTLogEntry)
         >= db_entry:get_version(DBEntry)),
    Lockable = (false =:= db_entry:get_writelock(DBEntry)),
    if VersionOK andalso Lockable ->
           %% set locks on entry
           NewEntry = db_entry:inc_readlock(DBEntry),
           NewDB = ?DB:set_entry(DB, NewEntry),
           {NewDB, ?prepared};
       true ->
           {DB, ?abort}
    end.

-spec commit(?DB:db(), tx_tlog:tlog_entry(), ?prepared | ?abort) -> ?DB:db().
commit(DB, RTLogEntry, OwnProposalWas) ->
    ?TRACE("rdht_tx_read:commit)~n", []),
    %% perform op: nothing to do for 'read'
    %% release locks
    case OwnProposalWas of
        ?prepared ->
            DBEntry = ?DB:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),
            RTLogVers = tx_tlog:get_entry_version(RTLogEntry),
            DBVers = db_entry:get_version(DBEntry),
            if RTLogVers =:= DBVers ->
                   NewEntry = db_entry:dec_readlock(DBEntry),
                   ?DB:set_entry(DB, NewEntry);
               true -> DB %% a write has already deleted this lock
            end;
        ?abort ->
            %% we could compare DB with RTLogEntry and update if outdated
            %% as this commit confirms the status of a majority of the
            %% replicas. Could also be possible already in the validate req?
            DB
    end.

-spec abort(?DB:db(), tx_tlog:tlog_entry(), ?prepared | ?abort) -> ?DB:db().
abort(DB, RTLogEntry, OwnProposalWas) ->
    ?TRACE("rdht_tx_read:abort)~n", []),
    %% same as when committing
    commit(DB, RTLogEntry, OwnProposalWas).


%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [],
                             [{pid_groups_join_as, DHTNodeGroup, ?MODULE}]).

-type state() :: {integer(), integer(), integer(), atom()}.

%% initialize: return initial state.
-spec init([]) -> state().
init([]) ->
    DHTNodeGroup = pid_groups:my_groupname(),
    ?TRACE("rdht_tx_read: Starting rdht_tx_read for DHT node: ~p~n", [DHTNodeGroup]),
    %% For easier debugging, use a named table (generates an atom)
    Table = pdb:new(DHTNodeGroup ++ "_rdht_tx_read", [set, private, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    %% Table = pdb:new(?MODULE, [set, private]),
    Reps = config:read(replication_factor),
    MajOk = quorum:majority_for_accept(Reps),
    MajDeny = quorum:majority_for_deny(Reps),

    _State = {Reps, MajOk, MajDeny, Table}.

-spec on(comm:message(), state()) -> state().
%% reply triggered by api_dht_raw:unreliable_get_key/3
on({?get_key_with_id_reply, Id, _Key, {ok, Val, Vers}},
   {Reps, MajOk, MajDeny, Table} = State) ->
    ?TRACE("~p rdht_tx_read:on(get_key_with_id_reply) ID ~p~n", [self(), Id]),
    Entry = get_entry(Id, Table),
    %% @todo inform sender when its entry is outdated?
    %% @todo inform former sender on outdated entry when we
    %% get a newer entry?
    TmpEntry = rdht_tx_read_state:add_reply(Entry, Val, Vers, MajOk, MajDeny),
    _ = case rdht_tx_read_state:get_client(TmpEntry) of
            unknown ->
                %% when we get a client, we will inform it
                pdb:set(TmpEntry, Table);
            Client ->
                NewEntry =
                    case rdht_tx_read_state:is_newly_decided(TmpEntry) of
                        true  -> inform_client(Client, TmpEntry);
                        false -> TmpEntry
                    end,
                pdb:set(NewEntry, Table),
                delete_if_all_replied(NewEntry, Reps, Table)
        end,
    State;

%% triggered by ?MODULE:work_phase/3
on({client_is, Id, Pid, Key}, {Reps, _MajOk, _MajDeny, Table} = State) ->
    ?TRACE("~p rdht_tx_read:on(client_is)~n", [self()]),
    Entry = get_entry(Id, Table),
    Tmp1Entry = rdht_tx_read_state:set_client(Entry, Pid),
    TmpEntry = rdht_tx_read_state:set_key(Tmp1Entry, Key),
    _ = case rdht_tx_read_state:is_newly_decided(TmpEntry) of
            true ->
                Tmp2Entry = inform_client(Pid, TmpEntry),
                pdb:set(Tmp2Entry, Table),
                delete_if_all_replied(Tmp2Entry, Reps, Table);
            false -> pdb:set(TmpEntry, Table)
        end,
%    State;
%
%%% triggered periodically
%on({timeout_id, Id}, {_Reps, _MajOk, _MajDeny, Table} = State) ->
%    ?TRACE("~p rdht_tx_read:on(timeout) Id ~p~n", [self(), Id]),
%    case pdb:get(Id, Table) of
%        undefined -> ok;
%        Entry ->
%            %% inform client on timeout if Id exists and client is not informed
%            timeout_inform(Entry),
%            pdb:delete(Id, Table)
%    end,
    State.

-spec get_entry(rdht_tx:req_id(), atom()) -> rdht_tx_read_state:read_state().
get_entry(Id, Table) ->
    case pdb:get(Id, Table) of
        undefined ->
%%            msg_delay:send_local(config:read(transaction_lookup_timeout) div 1000,
%%                                 self(), {timeout_id, Id}),
            rdht_tx_read_state:new(Id);
        Any -> Any
    end.

% -spec timeout_inform(rdht_tx_read_state:read_state()) -> ok.
% %% inform client on timeout if Id exists and client is not informed yet
% timeout_inform(Entry) ->
%     case {rdht_tx_read_state:is_client_informed(Entry),
%           rdht_tx_read_state:get_client(Entry)} of
%         {_, unknown} -> ok;
%         {false, Client} ->
%             TmpEntry = rdht_tx_read_state:set_decided(Entry, {fail, timeout}),
%             inform_client(Client, TmpEntry);
%         _ -> ok
%     end.

-spec inform_client(pid(), rdht_tx_read_state:read_state()) ->
                           rdht_tx_read_state:read_state().
inform_client(Client, Entry) ->
    Id = rdht_tx_read_state:get_id(Entry),
    Msg = msg_reply(Id, make_tlog_entry(Entry)),
    comm:send_local(Client, Msg),
    rdht_tx_read_state:set_client_informed(Entry).

%% -spec make_tlog_entry_feeder(
%%         {rdht_tx_read_state:read_state(), ?RT:key()})
%%                             -> {rdht_tx_read_state:read_state()}.
%% make_tlog_entry_feeder({State, Key}) ->
%%     %% when make_tlog_entry is called, the key of the state is never
%%     %% 'unknown'
%%     {rdht_tx_read_state:set_key(State, Key)}.

-spec make_tlog_entry(rdht_tx_read_state:read_state()) ->
                                tx_tlog:tlog_entry().
make_tlog_entry(Entry) ->
    {Val, Vers} = rdht_tx_read_state:get_result(Entry),
    Key = rdht_tx_read_state:get_key(Entry),
    Status = rdht_tx_read_state:get_decided(Entry),
    tx_tlog:new_entry(?read, Key, Vers, Status, Val).

-spec delete_if_all_replied(rdht_tx_read_state:read_state(),
                            pos_integer(), atom())
                           -> rdht_tx_read_state:read_state() | ok.
delete_if_all_replied(Entry, Reps, Table) ->
    ?TRACE("rdht_tx_read:delete_if_all_replied Reps: ~p =?= ~p, ClientInformed: ~p Client: ~p~n",
              [Reps, rdht_tx_read_state:get_numreplied(Entry), rdht_tx_read_state:is_client_informed(Entry), rdht_tx_read_state:get_client(Entry)]),
    Id = rdht_tx_read_state:get_id(Entry),
    case (Reps =:= rdht_tx_read_state:get_numreplied(Entry))
        andalso (rdht_tx_read_state:is_client_informed(Entry)) of
        true  -> pdb:delete(Id, Table);
        false -> Entry
    end.

%% @doc Checks whether config parameters for rdht_tx_read exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(quorum_factor) and
    config:cfg_is_greater_than(quorum_factor, 0) and
    config:cfg_is_integer(replication_factor) and
    config:cfg_is_greater_than(replication_factor, 0) and

    config:cfg_is_integer(transaction_lookup_timeout) and
    config:cfg_is_greater_than_equal(transaction_lookup_timeout, 1000).
