% @copyright 2012-2016 Zuse Institute Berlin,

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
%% @doc    Generic paxos round based register (prbr) implementation.
%%         The read/write store alias acceptor.
%% @end
%% @version $Id$
-module(prbr).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").

-define(PDB, db_prbr).

%% Optimization for (log-structured) disk backends when writing
%% large values. Each time and entry is read (e.g. due to a
%% normal read operation), its read round is incremented and
%% the entry is written to the backend. The write-through information
%% contain the written value. Storing the WTI separate prevents
%% that the complete value is written repeatedly.
-define(SEP_WTI, config:read(separate_write_through_info)).

%%% the prbr has to be embedded into a gen_component using it.
%%% The state it operates on has to be passed to the on handler
%%% correctly. All messages it handles start with the token
%%% prbr to support a generic, embeddable on-handler trigger.

%%% functions for module where embedded into
-export([on/2, init/1, close/1, close_and_delete/1]).
-export([check_config/0]).
-export([noop_read_filter/1]).  %% See rbrcseq for explanation.
-export([noop_write_filter/3]). %% See rbrcseq for explanation.
-export([new/2]).
-export([set_entry/2]).
-export([get_entry/2]).
-export([delete_entry/2]).
-export([entry_key/1]).
-export([entry_val/1]).
-export([entry_set_val/2]).

-export([tester_create_write_filter/1]).

%% let fetch the number of DB entries
-export([get_load/1]).

%% only for unittests
-export([tab2list_raw_unittest/1]).

%% only during recover
-export([tab2list/1]).

-export_type([message/0]).
-export_type([state/0]).
-export_type([read_filter/0]).
-export_type([write_filter/0]).
-export_type([entry/0]).

%% read_filter(custom_data() | no_value_yet) -> read_info()
-type read_filter() :: fun((term()) -> term()).

%% write_filter(OldLocalDBentry :: custom_data(),
%%              InfosToUpdateOutdatedEntry :: info_passed_from_read_to_write(),
%%              ValueForWriteOperation:: Value())
%% -> {custom_data(), value_returned_to_caller()}
-type write_filter() :: fun((term(), term(), term()) -> {term(), term()}).

-type state() :: ?PDB:db().

-type message() ::
        {prbr, read, DB :: dht_node_state:db_selector(),
         WasConsistentLookup :: boolean(),
         Proposer :: comm:mypid(), ?RT:key(), DataType :: module(),
         InRound :: pr:pr(),
         read_filter()}
      | {prbr, write, DB :: dht_node_state:db_selector(),
         WasConsistentLookup :: boolean(),
         Proposer :: comm:mypid(), ?RT:key(), DataType :: module(),
         InRound :: pr:pr(), Value :: term(), PassedToUpdate :: term(),
         write_filter()}
      | {prbr, delete_key, DB :: dht_node_state:db_selector(),
         Client :: comm:mypid(), Key :: ?RT:key()}
      | {prbr, tab2list_raw, DB :: dht_node_state:db_selector(),
         Client :: comm:mypid()}.


%% improvements to usual paxos:

%% for reads the client has just to send a unique identifier and the
%% acceptor provides a valid round number. The actual round number of
%% the request is then the tuple {round_number, unique_id}.

%% A proposer then may receive answers with different round numbers
%% and selects that one with the highest round, where he also takes
%% the value from.

%% on read the acceptor can assign the next round number. They remain
%% unique as we get the node_id in the read request and that is part of
%% the round number.

%% so there are no longer any read denies, all reads succeed.
-type entry() :: { any(), %% key
                   pr:pr(), %% r_read
                   pr:pr(), %% r_write
                   atom(), %% signals if last round request was part of a read or write
                   any(), %% value
                   [comm:mypid()] %% learner
                 }.

%% Messages to expect from this module
-spec msg_round_request_reply(comm:mypid(), boolean(), pr:pr(), pr:pr(), any()) -> ok.
msg_round_request_reply(Client, Cons, ReadRound, WriteRound, Value) ->
    comm:send(Client, {round_request_reply, Cons,  ReadRound, WriteRound, Value}).

-spec msg_read_reply(comm:mypid(), Consistency::boolean(),
                     pr:pr(), any(), pr:pr())
             -> ok.
msg_read_reply(Client, Cons, YourRound, Val, LastWriteRound) ->
    comm:send(Client, {read_reply, Cons, YourRound, Val, LastWriteRound}).

-spec msg_read_deny(comm:mypid(), Consistency::boolean(), pr:pr(), pr:pr()) -> ok.
msg_read_deny(Client, Cons, YourRound, LargerRound) ->
    comm:send(Client, {read_deny, Cons, YourRound, LargerRound}).

-spec msg_write_reply(comm:mypid(), Consistency::boolean(),
                      any(), pr:pr(), pr:pr(), any()) -> ok.
msg_write_reply(Client, Cons, Key, UsedWriteRound, YourNextRoundForWrite, WriteRet) ->
    comm:send(Client, {write_reply, Cons, Key, UsedWriteRound, YourNextRoundForWrite, WriteRet}).

-spec msg_write_deny(comm:mypid(), Consistency::boolean(), any(), pr:pr())
                    -> ok.
msg_write_deny(Client, Cons, Key, NewerRound) ->
    comm:send(Client, {write_deny, Cons, Key, NewerRound}).

-spec noop_read_filter(term()) -> term().
noop_read_filter(X) -> X.

-spec noop_write_filter(Old :: term(), WF :: term(), Val :: term()) -> {term(), none}.
noop_write_filter(_, _, X) -> {X, none}.

%% initialize: return initial state.
-spec init(atom() | tuple()) -> state().
init(DBName) -> ?PDB:new(DBName).

%% @doc Closes the given DB (it may be recoverable using open/1 depending on
%%      the DB back-end).
-spec close(state()) -> true.
close(State) -> ?PDB:close(State).

%% @doc Closes the given DB and deletes all contents (this DB can thus not be
%%      re-opened using open/1).
-spec close_and_delete(state()) -> true.
close_and_delete(State) -> ?PDB:close_and_delete(State).

-spec on(message(), state()) -> state().
on({prbr, round_request, _DB, Cons, Proposer, Key, DataType, ProposerUID, ReadFilter, OpType}, TableName) ->
    ?TRACE("prbr:round_request: ~p in round ~p~n", [Key, ProposerUID]),
    KeyEntry = get_entry(Key, TableName),

    {NewKeyEntryVal, ReadVal} =
        case erlang:function_exported(DataType, prbr_read_handler, 3) of
            true -> DataType:prbr_read_handler(KeyEntry, TableName, ReadFilter);
            _    -> {entry_val(KeyEntry), ReadFilter(entry_val(KeyEntry))}
        end,

    %% Assign a valid next read round number
    %% If the last round request was part of a read and this one is a
    %% also part of a read, it is not necessary to increment the read round
    %% number. This prevents sequencing of concurrent reads.
    AssignedReadRound =
        case OpType =:= read andalso entry_last_op_type(KeyEntry) =:= read of
            true ->
                OldRound = entry_r_read(KeyEntry),
                pr:new(pr:get_r(OldRound), ProposerUID);
            _ ->
                next_read_round(KeyEntry, ProposerUID)
        end,
    trace_mpath:log_info(self(), {'prbr:on(round_request)',
                                  %% key, Key,
                                  round, AssignedReadRound,
                                  val, NewKeyEntryVal,
                                  read_filter, ReadFilter}),

    %% for now do not send write through information in rr_replies
    %% since they are not used. saves a bit bandwith. 
    EntryWriteRound = pr:set_wf(entry_r_write(KeyEntry), none),
    msg_round_request_reply(Proposer, Cons, AssignedReadRound,
                            EntryWriteRound, ReadVal),

    NewKeyEntry = entry_set_r_read(KeyEntry, AssignedReadRound),
    NewKeyEntry2 = entry_set_val(NewKeyEntry, NewKeyEntryVal), % prbr read handler might change value (e.g. due to GCing)
    NewKeyEntry3 = entry_set_last_op_type(NewKeyEntry2, OpType),
    _ = set_entry(NewKeyEntry3, TableName),
    TableName;

on({prbr, read, _DB, Cons, Proposer, Key, DataType, _ProposerUID, ReadFilter, ReadRound}, TableName) ->
    ?TRACE("prbr:read: ~p in round ~p~n", [Key, ReadRound]),
    KeyEntry = get_entry(Key, TableName),

    _ = case pr:get_r(ReadRound) > pr:get_r(entry_r_read(KeyEntry)) of
         true ->
            {NewKeyEntryVal, ReadVal} =
                case erlang:function_exported(DataType, prbr_read_handler, 3) of
                    true -> DataType:prbr_read_handler(KeyEntry, TableName, ReadFilter);
                    _    -> {entry_val(KeyEntry), ReadFilter(entry_val(KeyEntry))}
                end,
            trace_mpath:log_info(self(), {'prbr:on(read)',
                                          %% key, Key,
                                          round, ReadRound,
                                          val, NewKeyEntryVal,
                                          read_filter, ReadFilter}),
            EntryWriteRound =
                %% see macro definition for explanation
                case ?SEP_WTI of
                    true ->
                        %% retrieve WTI from separate location
                        case ?PDB:get(TableName, get_wti_key(Key)) of
                            {} ->
                                pr:set_wf(entry_r_write(KeyEntry), none);
                            {_Key, WTI} ->
                                pr:set_wf(entry_r_write(KeyEntry), WTI)
                        end;
                    _ -> entry_r_write(KeyEntry)
                end,

            msg_read_reply(Proposer, Cons, ReadRound,
                           ReadVal, EntryWriteRound),

            NewKeyEntry = entry_set_r_read(KeyEntry, ReadRound),
            NewKeyEntry2 = entry_set_val(NewKeyEntry, NewKeyEntryVal), % prbr read handler might change value (e.g. due to GCing)
            _ = set_entry(NewKeyEntry2, TableName);
         _ ->
            msg_read_deny(Proposer, Cons, ReadRound, entry_r_read(KeyEntry))
    end,
    TableName;

on({prbr, write, _DB, Cons, Proposer, Key, DataType, InRound, OldWriteRound, Value,
    PassedToUpdate, WriteFilter, IsWriteThrough}, TableName) ->
    ?TRACE("prbr:write for key: ~p in round ~p~n", [Key, InRound]),
    trace_mpath:log_info(self(), {prbr_on_write}),
    OrigKeyEntry = get_entry(Key, TableName),
    %% update learners:
    %% when IsWriteThrough add current proposer as learner
    %% otherwise clean registered learners by setting current proposer
    %% as learner as we start a new consensus.
    KeyEntry = case IsWriteThrough of
                   true ->
                       %% ct:pal("WriteThrough~n"),
                       entry_add_learner(OrigKeyEntry, Proposer);
                   _ -> entry_set_learner(OrigKeyEntry, Proposer)
               end,
    %% we store the writefilter to be able to reproduce the request in
    %% write_throughs. We modify the InRound here to avoid duplicate
    %% transfer of the Value etc.
    RoundForWrite =
        case fun prbr:noop_write_filter/3 =:= WriteFilter of
            true ->
                pr:set_wf(InRound, none);
            _ ->
                pr:set_wf(InRound, {WriteFilter, PassedToUpdate, Value})
        end,
    _ = case writable(KeyEntry, RoundForWrite, OldWriteRound) of
            {ok, NewKeyEntry, NextWriteRound} ->
                {NewVal, Ret} =
                    case erlang:function_exported(DataType, prbr_write_handler, 5) of
                        true -> DataType:prbr_write_handler(NewKeyEntry,
                                     PassedToUpdate, Value, TableName, WriteFilter);
                        _    -> WriteFilter(entry_val(NewKeyEntry),
                                     PassedToUpdate, Value)
                    end,
                trace_mpath:log_info(self(), {'prbr:on(write)',
                                  %% key, Key,
                                  round, RoundForWrite,
                                  passed_to_update, PassedToUpdate,
                                  val, Value,
                                  write_filter, WriteFilter,
                                  newval, NewVal}),
                [ msg_write_reply(P, Cons, Key, InRound,
                                  NextWriteRound, Ret)
                  || P <- entry_get_learner(NewKeyEntry) ],
                NewKeyEntry2 =
                    %% see macro definition for explanation
                    case ?SEP_WTI of
                        true ->
                            %% store WTI at separate location
                            WriteRound = entry_r_write(NewKeyEntry),
                            WriteRoundWTI = pr:get_wf(WriteRound),
                            WTIKey= get_wti_key(entry_key(NewKeyEntry)),
                            _ = ?PDB:set(TableName, {WTIKey, WriteRoundWTI}),

                            entry_set_r_write(NewKeyEntry, pr:set_wf(WriteRound, none));
                        _ ->
                            NewKeyEntry
                    end,
                set_entry(entry_set_val(NewKeyEntry2, NewVal), TableName);
            {dropped, NewerRound} ->
                trace_mpath:log_info(self(), {'prbr:on(write) denied',
                                  %% key, Key,
                                  round, RoundForWrite,
                                  newer_round, NewerRound}),
                %% log:pal("Denied ~p ~p ~p~n", [Key, InRound, NewerRound]),
                [ msg_write_deny(P, Cons, Key, NewerRound)
                  || P <- entry_get_learner(KeyEntry) ],
                %% we added proposers to the KeyEntry, so we have to store it


                %% for a deny update the read round to the max(InRound
                %% and entryroiundr)

                set_entry(KeyEntry, TableName)
        end,
    TableName;

on({prbr, delete_key, _DB, Client, Key}, TableName) ->
    %% for normal delete we will have to have a special write operation taking
    %% the Paxos round numbers into account...
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    ct:pal("R~p deleted~n", [?RT:get_key_segment(Key)]),
    Entry = get_entry(Key, TableName),
    _ = delete_entry(TableName, Entry),
    comm:send_local(Client, {delete_key_reply, Key}),
    TableName;

on({prbr, get_entry, _DB, Client, Key}, TableName) ->
    comm:send_local(Client, {entry, get_entry(Key, TableName)}),
    TableName;

on({prbr, tab2list_raw, DB, Client}, TableName) ->
    comm:send_local(Client, {DB, tab2list_raw(TableName)}),
    TableName.

-spec get_entry(any(), state()) -> entry().
get_entry(Id, TableName) ->
    case ?PDB:get(TableName, Id) of
        {}    -> new(Id);
        Entry -> Entry
    end.

-spec set_entry(entry(), state()) -> state().
set_entry(NewEntry, TableName) ->
    _ = ?PDB:set(TableName, NewEntry),
    TableName.

-spec delete_entry(state(), entry()) -> db_prbr:db().
delete_entry(TableName, Entry) ->
    ?PDB:delete_entry(TableName, Entry).

-spec get_load(state()) -> non_neg_integer().
get_load(State) -> ?PDB:get_load(State).

-spec tab2list(state()) -> [{any(),any()}].
tab2list(State) ->
    %% without prbr own data
    Entries = tab2list_raw(State),
    [ {entry_key(X), entry_val(X)} || X <- Entries].

-spec tab2list_raw_unittest(state()) -> [entry()].
tab2list_raw_unittest(State) ->
    ?ASSERT(util:is_unittest()), % may only be used in unit-tests
    tab2list_raw(State).

-spec tab2list_raw(state()) -> [entry()].
tab2list_raw(State) ->
    %% with prbr own data
    ?PDB:tab2list(State).

%% operations for abstract data type entry()

-spec new(any()) -> entry().
new(Key) ->
    new(Key, prbr_bottom).

-spec new(any(), any()) -> entry().
new(Key, Val) ->
    {Key,
     %% Note: atoms < pids, so this is a good default.
     _R_Read = pr:new(0, '_'),
     %% Note: atoms < pids, so this is a good default.
     _R_Write = pr:new(0, '_'),
     _LastOP = created,
     _Value = Val,
     _Learner = []}.

-spec entry_key(entry()) -> any().
entry_key(Entry) -> element(1, Entry).
%% -spec entry_set_key(entry(), any()) -> entry().
%% entry_set_key(Entry, Key) -> setelement(2, Entry, Key).
-spec entry_r_read(entry()) -> pr:pr().
entry_r_read(Entry) -> element(2, Entry).
-spec entry_set_r_read(entry(), pr:pr()) -> entry().
entry_set_r_read(Entry, Round) -> setelement(2, Entry, Round).
-spec entry_r_write(entry()) -> pr:pr().
entry_r_write(Entry) -> element(3, Entry).
-spec entry_set_r_write(entry(), pr:pr()) -> entry().
entry_set_r_write(Entry, Round) -> setelement(3, Entry, Round).
-spec entry_set_last_op_type(entry(), atom()) -> entry().
entry_set_last_op_type(Entry, OpType) -> setelement(4, Entry, OpType).
-spec entry_last_op_type(entry()) -> atom().
entry_last_op_type(Entry) -> element(4, Entry).
-spec entry_val(entry()) -> any().
entry_val(Entry) -> element(5, Entry).
-spec entry_set_val(entry(), any()) -> entry().
entry_set_val(Entry, Value) -> setelement(5, Entry, Value).
-spec entry_add_learner(entry(), comm:mypid()) -> entry().
entry_add_learner(Entry, Learner) ->
    OldLearner = entry_get_learner(Entry),
    case lists:member(Learner, OldLearner) of
        true -> Entry;
        false -> setelement(6, Entry, [Learner | OldLearner])
    end.
-spec entry_set_learner(entry(), comm:mypid()) -> entry().
entry_set_learner(Entry, Learner) -> setelement(6, Entry, [Learner]).
-spec entry_get_learner(entry()) -> [comm:mypid()].
entry_get_learner(Entry) -> element(6, Entry).

-spec next_read_round(entry(), any()) -> pr:pr().
next_read_round(Entry, ProposerUID) ->
    LatestSeenRead = pr:get_r(entry_r_read(Entry)),
    LatestSeenWrite = pr:get_r(entry_r_write(Entry)),
    pr:new(util:max(LatestSeenRead, LatestSeenWrite) + 1, ProposerUID).



-spec writable(entry(), pr:pr(), pr:pr()) -> {ok, entry(),
                                     NextWriteRound :: pr:pr()} |
                                    {dropped,
                                     NewerSeenRound :: pr:pr()}.
writable(Entry, InRound, OldWriteRound) ->
    LatestSeenRead = entry_r_read(Entry),
    LatestSeenWrite = entry_r_write(Entry),
    InRoundR = pr:get_r(InRound),
    InRoundId = pr:get_id(InRound),
    LatestSeenReadR = pr:get_r(LatestSeenRead),
    LatestSeenReadId = pr:get_id(LatestSeenRead),
    LatestSeenWriteR = pr:get_r(LatestSeenWrite),
    LatestSeenWriteId = pr:get_id(LatestSeenWrite),
    OldWriteR = pr:get_r(OldWriteRound),
    OldWriteId = pr:get_id(OldWriteRound),
    if ((InRoundR =:= LatestSeenReadR andalso InRoundId=:= LatestSeenReadId)
        orelse (InRoundR > LatestSeenReadR))
       andalso (InRoundR > LatestSeenWriteR)
       %% make sure that no write filter operations are missing in the
       %% sequence of writes
       andalso (OldWriteR =:= LatestSeenWriteR
               andalso OldWriteId =:= LatestSeenWriteId) ->

           T1Entry = entry_set_r_write(Entry, InRound),
           %% prepare fast_paxos for this client:
           NextWriteRound = next_read_round(T1Entry,
                                            pr:get_id(InRound)),
           %% assume this token was seen in a read already, so no one else
           %% can interfere without paxos noticing it
           T2Entry = entry_set_r_read(T1Entry, NextWriteRound),
           {ok, T2Entry, NextWriteRound};
       true ->
            %% proposer may not have latest value for a clean content
            %% check, and another proposer is concurrently active, so
            %% we do not prepare a fast_paxos for this client, but let
            %% the other proposer the chance to pass read and write
            %% phase.  The denied proposer has to perform a read and write
            %% phase on its own (including a new content check).
            {dropped, util:max(LatestSeenRead, LatestSeenWrite)}
    end.

-spec get_wti_key(any())-> any().
get_wti_key(Key) -> {Key, wti}.

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_bool(separate_write_through_info).

-spec tester_create_write_filter(0) -> write_filter().
tester_create_write_filter(0) -> fun prbr:noop_write_filter/3.

