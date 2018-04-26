% @copyright 2012-2018 Zuse Institute Berlin,

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
%% @doc    Allow a sequence of consensus using a prbr.
%% @end
%% @version $Id$
-module(rbrcseq).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%%-define(PDB, pdb_ets).
-define(PDB, pdb).
-define(REDUNDANCY, (config:read(redundancy_module))).
-define(READ_RETRY_COUNT, (config:read(read_attempts_without_progress))).

%%-define(TRACE(X,Y), log:pal("~p" X,[self()|Y])).
%%-define(TRACE(X,Y),
%%        Name = pid_groups:my_pidname(),
%%        case Name of
%%            kv_db -> log:pal("~p ~p " X, [self(), Name | Y]);
%%            _ -> ok
%%        end).
-define(TRACE(X,Y), ok).
-define(TRACE_PERIOD(X,Y), ok).
-include("scalaris.hrl").
-include("client_types.hrl").

-behaviour(gen_component).

%% api:
-export([qread/4, qread/5]).
-export([qwrite/6, qwrite/8]).
-export([qwrite_fast/8, qwrite_fast/10]).
-export([get_db_for_id/2]).

-export([check_config/0]).
-export([start_link/3]).
-export([init/1, on/2]).

-type state() :: { ?PDB:tableid(),
                   dht_node_state:db_selector(),
                   non_neg_integer() %% period this process is in
                 }.

%% Aggregation of the distributed write state received in round_request
%% or read replies.
-record(write_state, {% The highest write round received in replies
                      highest_write_round :: pr:pr(),
                      % The number of replies seen with this round
                      highest_write_count :: non_neg_integer(),
                      % The write_filter used in this round
                      highest_write_filter :: prbr:write_filter() | none,
                      %% Value aggregation based on the seen replies. The content depends
                      %% on the chosen redundancy strategy. In the case of replication,
                      %% it is simply equal to the value stored at any acceptor with the
                      %% highest seen write round. Aggregation behaviour is defined in
                      %% the respective ?REDUNDANCY module.
                      value :: any()
                     }).

%% Aggregator for round_request replies.
%% rr_replies are received after executing a read without explicit round number
%% to receive the currently highest rounds from a quorum of replicas.
-record(rr_replies, {reply_count :: non_neg_integer(),        %% total number of replies recieved
                     highest_read_count :: non_neg_integer(), %% number of replies seen with highest read round
                     highest_read_round :: pr:pr(),           %% highest read round received in replies
                     write_state :: #write_state{}
                    }).

%% Aggregator for read replies.
%% If qread phase 1 has not found a consistent quorum, a read with an explicit
%% round number is startet. The replies are aggregated in this record.
-record(r_replies, {ack_count :: non_neg_integer(),  %% number of acks reveiced
                    deny_count :: non_neg_integer(), %% number of denies received
                    write_state :: #write_state{}
                   }).

%% Aggregator for write replies.
-record(w_replies, {ack_count :: non_neg_integer(),  %% number of ack reveiced
                    deny_count :: non_neg_integer(), %% number of denies received
                    highest_write_round :: pr:pr()   %% highest write round reveiced
                   }).

-type replies() :: #rr_replies{} | #r_replies{} | #w_replies{}.

-type entry() :: {any(), %% ReqId
                  any(), %% key of entry keeping track of open requests for this client
                  any(), %% debug field
                  non_neg_integer(), %% period of last retriggering / starting
                  non_neg_integer(), %% period of next retriggering
                  ?RT:key(), %% key
                  module(), %% data type
                  comm:erl_local_pid(), %% client
                  any(), %% filter (read) or tuple of filters (write)
                  is_read | any(), %% value to write if entry belongs to write
                  read | write | denied_write, %% operation type
                  pr:pr(), %% my round
                  any(), %% read retry information
                  replies() %% maintains replies to check for consistent quorums
%%% Attention: There is a case that checks the size of this tuple below!!
                 }.

-type check_next_step() :: fun((term(), term()) -> term()).

-export_type([check_next_step/0]).

-include("gen_component.hrl").

%% quorum read protocol for consensus sequence
%%
%% user definable functions and types for qread and abbreviations:
%% RF = ReadFilter(dbdata() | no_value_yet) -> read_info().
%% r = replication degree.
%%
%% qread(Client, Key, RF) ->
%%   % read phase
%%     r * lookup -> r * dbaccess ->
%%     r * read_filter(DBEntry)
%%   collect quorum, select newest (prbr knows that itself) ->
%%     send newest to client.

%% This variant works on whole dbentries without filtering.
-spec qread(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), module()) -> ok.
qread(CSeqPidName, Client, Key, DataType) ->
    RF = fun prbr:noop_read_filter/1,
    qread(CSeqPidName, Client, Key, DataType, RF).

-spec qread(pid_groups:pidname(), comm:erl_local_pid(), any(), module(), prbr:read_filter()) -> ok.
qread(CSeqPidName, Client, Key, DataType, ReadFilter) ->
    Pid = pid_groups:find_a(CSeqPidName),

    ReqMsg  = {qround_request, Client, '_', Key, DataType,
                ReadFilter, read, _RetriggerAfter = 1},
    start_request(Pid, ReqMsg),
    %% the process will reply to the client directly
    ok.

%% quorum write protocol for consensus sequence
%%
%% user definable functions and types for qwrite and abbreviations:
%% RF = ReadFilter(dbdata() | no_value_yet) -> read_info().
%% CC = ContentCheck(read_info(), WF, value()) ->
%%         {true, UI}
%%       | {false, Reason}.
%% WF = WriteFilter(old_dbdata(), UI, value()) -> dbdata().
%% RI = ReadInfo produced by RF
%% UI = UpdateInfo (data that could be used to update/detect outdated replicas)
%% r = replication degree
%%
%% qwrite(Client, RF, CC, WF, Val) ->
%%   % read phase
%%     r * lookup -> r * dbaccess ->
%%     r * RF(DBEntry) -> read_info();
%%   collect quorum, select newest read_info() = RI
%%   % allowed next value? (version outdated for example?)
%%   CC(RI, WF, Val) -> {IsValid = boolean(), UI}
%%   if false =:= IsValid => return abort to the client
%%   if true =:= IsValid =>
%%   % write phase
%%     r * lookup -> r * dbaccess ->
%%     r * WF(OldDBEntry, UI, Val) -> NewDBEntry
%%   collect quorum of 'written' acks
%%   inform client on done.

%% if the paxos register is changed concurrently or a majority of
%% answers cannot be collected, rbrcseq automatically restarts with
%% the read phase. Either the CC fails then (which informs the client
%% and ends the protocol) or the operation passes through (or another
%% retry will happen).

%% This variant works on whole dbentries without filtering.
-spec qwrite(pid_groups:pidname(),
             comm:erl_local_pid(),
             ?RT:key(),
             module(),
             fun ((any(), any(), any()) -> {boolean(), any()}), %% CC (Content Check)
             client_value()) -> ok.
qwrite(CSeqPidName, Client, Key, DataType, CC, Value) ->
    RF = fun prbr:noop_read_filter/1,
    WF = fun prbr:noop_write_filter/3,
    qwrite(CSeqPidName, Client, Key, DataType, RF, CC, WF, Value).

-spec qwrite_fast(pid_groups:pidname(),
                  comm:erl_local_pid(),
                  ?RT:key(),
                  module(),
                  fun ((any(), any(), any()) -> {boolean(), any()}), %% CC (Content Check)
                  client_value(), pr:pr(),
                  client_value() | prbr_bottom) -> ok.
qwrite_fast(CSeqPidName, Client, Key, DataType, CC, Value, Round, OldVal) ->
    RF = fun prbr:noop_read_filter/1,
    WF = fun prbr:noop_write_filter/3,
    qwrite_fast(CSeqPidName, Client, Key, DataType, RF, CC, WF, Value, Round, OldVal).

-spec qwrite(pid_groups:pidname(),
             comm:erl_local_pid(),
             ?RT:key(),
             module(),
             fun ((any()) -> any()), %% read filter
             fun ((any(), any(), any()) -> {boolean(), any()}), %% content check
             fun ((any(), any(), any()) -> {any(), any()}), %% write filter
%%              %% select what you need to read for the operation
%%              fun ((CustomData) -> ReadInfo),
%%              %% is it an allowed follow up operation? and what info is
%%              %% needed to update outdated replicas (could be rather old)?
%%              fun ((CustomData, ReadInfo,
%%                    {fun ((ReadInfo, WriteValue) -> CustomData),
%%                     WriteValue}) -> {boolean(), PassedInfo}),
%%              %% update the db entry with the given infos, must
%%              %% generate a valid custom datatype. ReturnValue is included
%%              %% in qwrite_done message for the caller.
%%              fun ((PassedInfo, WriteValue) -> {CustomData, ReturnValue}),
%%              %%module(),
             client_value()) -> ok.
qwrite(CSeqPidName, Client, Key, DataType, ReadFilter, ContentCheck,
       WriteFilter, Value) ->
    Pid = pid_groups:find_a(CSeqPidName),

    ReqMsg = {qwrite, Client, '_', Key,
                   DataType, {ReadFilter, ContentCheck, WriteFilter},
                   Value, _RetriggerAfter = 20},
    start_request(Pid, ReqMsg),
    %% the process will reply to the client directly
    ok.

-spec qwrite_fast(pid_groups:pidname(),
                  comm:erl_local_pid(),
                  ?RT:key(),
                  module(),
                  fun ((any()) -> any()), %% read filter
                      fun ((any(), any(), any()) -> {boolean(), any()}), %% content check
                          fun ((any(), any(), any()) -> any()), %% write filter
                              client_value(), pr:pr(), client_value() | prbr_bottom)
-> ok.
qwrite_fast(CSeqPidName, Client, Key, DataType, ReadFilter, ContentCheck,
            WriteFilter, Value, Round, OldValue) ->
    Pid = pid_groups:find_a(CSeqPidName),

    %% Writes need the old write round to ensure every replica has the same
    %% sequence of applied write filter. A qwrite_fast is only sucessfull
    %% if no other process has touched the the replica's in the mean time.
    %% In this case, the round received (parameter Round) from the last
    %% write is the used write round inrecemented by one.
    OldWriteRound = pr:new(pr:get_r(Round)-1, pr:get_id(Round)),

    ReqMsg =  {qwrite_fast, Client, '_', Key,
                          DataType, {ReadFilter, ContentCheck, WriteFilter},
                          Value, _RetriggerAfter = 20, Round, OldWriteRound,
                          OldValue},
    start_request(Pid, ReqMsg),
    %% the process will reply to the client directly
    ok.

%% start the request with message to on({request_init... before starting
%% the first step of the protocol
-spec start_request(pid_groups:pidname(), any()) -> ok.
start_request(CseqPid, Msg) ->
    comm:send_local(CseqPid, {request_init, _ClientPosInMsg=2,
                              _OpenReqEntryPlaceHolder=3, Msg}).

%% @doc spawns a rbrcseq, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), pid_groups:pidname(), dht_node_state:db_selector())
                -> {ok, pid()}.
start_link(DHTNodeGroup, Name, DBSelector) ->
    gen_component:start_link(
      ?MODULE, fun ?MODULE:on/2, DBSelector,
      [{pid_groups_join_as, DHTNodeGroup, Name}]).

-spec init(dht_node_state:db_selector()) -> state().
init(DBSelector) ->
    _ = code:ensure_loaded(?REDUNDANCY),
    case erlang:function_exported(?REDUNDANCY, init, 0) of
        true ->
            ?REDUNDANCY:init();
        _ -> ok
    end,
    msg_delay:send_trigger(1, {next_period, 1}),
    {?PDB:new(?MODULE, [set]), DBSelector, 0}.

-spec on(comm:message(), state()) -> state().
%% ; ({qread, any(), client_key(), fun ((any()) -> any())},
%% state()) -> state().


on({request_init, ClientPosInMsg, OpenReqEntryPos, InitMsg}, State) ->
    %% will be used to track open entries belonging to this request
    EntryReg = create_open_entry_register(tablename(State)),
    %% fill placeholder in msg with EntryReg
    NewMsg = setelement(OpenReqEntryPos, InitMsg, EntryReg),
    %% once the request is done, the result is sent to Client. However, cleanup
    %% is requires to close eventual open entries (happens if proposer receives
    %% a quorum of success replies from a write through caused by its incomplete
    %% write). Replaces the client in the message, so that it is sent to the
    %% proposer's request_cleanup on-handler which in turn will forward the message
    %% to the client after it is done with its cleanup.
    Client = element(ClientPosInMsg, NewMsg),
    InjectedCleanupClient = comm:reply_as(self(), 4,
                                          {request_cleanup, Client, EntryReg, '_'}),
    NewMsg2 = setelement(ClientPosInMsg, NewMsg, InjectedCleanupClient),
    comm:send_local(self(), NewMsg2),
    State;

on({request_cleanup, Client, EntryReg, Result}, State) ->
    case delete_all_entries(EntryReg, tablename(State),_DeleteOpenEntryReqToo=true) of
        ok ->
            comm:send_local(Client, Result);
        error ->
            %% Open request register does not exist anymore. This means
            %% cleanup was already called and the client has received a reply.
            ok
    end,
    State;

%% qread step 1: request the current read/write rounds of + values from replicas.
on({qround_request, Client, EntryReg, Key, DataType, ReadFilter, OpType=read, RetriggerAfter}, State) ->
    ReadRetryInfo =  {?READ_RETRY_COUNT, _MaxSeenReadRoundNum = 0, _MaxSeenWriteRoundNum = 0},
    gen_component:post_op({qround_request, Client, EntryReg, Key, DataType, ReadFilter,
                           OpType, ReadRetryInfo, RetriggerAfter}, State);

on({qround_request, Client, EntryReg, Key, DataType, ReadFilter, OpType, RetriggerAfter}, State) ->
    gen_component:post_op({qround_request, Client, EntryReg, Key, DataType, ReadFilter,
                           OpType, _ReadRetryInfo=none, RetriggerAfter}, State);

on({qround_request, Client, EntryReg, Key, DataType, ReadFilter, OpType, ReadRetryInfo,
        RetriggerAfter}, State) ->
    ?TRACE("rbrcseq:read step 1: round_request, Client ~p~n", [Client]),
    %% assign new reqest-id; (also assign new ReqId when retriggering)

    %% if the caller process may handle more than one request at a
    %% time for the same key, the pids id has to be unique for each
    %% request to use the prbr correctly.
    ReqId = uid:get_pids_uid(),

    ?TRACE("rbrcseq:read step 1: round_request ReqId ~p~n", [ReqId]),
    %% initiate lookups for replicas(Key) and perform
    %% rbr reads there apply the content filter to only retrieve the required information
    This = comm:reply_as(comm:this(), 2, {qround_request_collect, '_'}),

    %% add the ReqId in case we concurrently perform several requests
    %% for the same key from the same process, which may happen.
    %% later: retrieve the request id from the assigned round number
    %% to get the entry from the pdb
    MyId = {my_id(), ReqId},
    Dest = pid_groups:find_a(routing_table),
    DB = db_selector(State),

    %% create local state for the request
    Entry = entry_new_round_request(qround_request, ReqId, EntryReg, Key, DataType, Client,
                                    period(State), ReadFilter, RetriggerAfter, OpType,
                                    ReadRetryInfo),
    _ = case save_entry(Entry, tablename(State)) of
            ok ->
                [ begin
                    %% let fill in whether lookup was consistent
                    LookupEnvelope = dht_node_lookup:envelope(4,
                            {prbr, round_request, DB, '_', This, X,
                             DataType, MyId, ReadFilter, OpType}),
                    comm:send_local(Dest,{?lookup_aux, X, 0, LookupEnvelope})
                  end
                || X <- ?REDUNDANCY:get_keys(Key) ];
            error ->
                %% client has already received an reply.
                ok
        end,
    %% retriggering of the request is done via the periodic dictionary scan
    %% {next_period, ...}

    State;

%% qread step 2: collect round_request replies (step 1) from replicas.
%% If a majority replied and it is a consistent quorum (i.e. all received rounds are the same)
%% we can deliver the read. If not, a qread with explicit round number is started.
on({qround_request_collect,
    {round_request_reply, Cons, ReceivedReadRound, ReceivedWriteRound, ReadValue, LastWF}}, State) ->
    ?TRACE("rbrcseq:on round_request_collect reply with r_round: ~p~n", [ReceivedReadRound]),

    {_Round, ReqId} = pr:get_id(ReceivedReadRound),
    case get_entry(ReqId, tablename(State)) of
        undefined ->
            %% drop replies for unknown requests, as they must be outdated as all replies
            %% run through the same process.
            State;
        Entry ->
            Replies = entry_replies(Entry),
            {Result, NewReplies, NewRound} =
                add_rr_reply(Replies, db_selector(State), ReceivedReadRound,
                             ReceivedWriteRound, ReadValue, entry_optype(Entry), LastWF,
                             entry_datatype(Entry), entry_filters(Entry), Cons),
            TEntry = entry_set_my_round(Entry, NewRound),
            NewEntry = entry_set_replies(TEntry, NewReplies),
            case Result of
                false ->
                    %% no majority replied yet
                    update_entry(NewEntry, tablename(State)),
                    State;
                consistent ->
                    WriteState = NewReplies#rr_replies.write_state,
                    %% majority replied and we have a consistent quorum ->
                    %% we can skip read with explicit round number and deliver directly
                    trace_mpath:log_info(self(),
                                          {qread_done, readval,
                                           WriteState#write_state.highest_write_round,
                                           WriteState#write_state.value}),
                    inform_client(qread_done, NewEntry,
                                  WriteState#write_state.highest_write_round,
                                  WriteState#write_state.value),
                    delete_entry(NewEntry, tablename(State)),
                    State;
                inconsistent ->
                    %% majority replied, but we do not have a consistent quorum
                    delete_entry(NewEntry, tablename(State)),

                    {RetryWithoutIncrement, ReadRetryInfo} =
                        case entry_optype(NewEntry) =:= read of
                            true ->
                                %% check if read can be retried because we made progress since
                                %% last read or our attempt number is not depleted
                                WriteState = NewReplies#rr_replies.write_state,
                                SeenMaxRR = pr:get_r(NewReplies#rr_replies.highest_read_round),
                                SeenMaxWR = pr:get_r(WriteState#write_state.highest_write_round),
                                {RetriesRemaining, PrevMaxRR, PrevMaxWR} =
                                    entry_get_read_retry_info(NewEntry),
                                NewReadRetryInfo =
                                    case SeenMaxRR =< PrevMaxRR andalso
                                         SeenMaxWR =< PrevMaxWR of
                                                     true -> %% no progress since last retry
                                                        {RetriesRemaining - 1, PrevMaxRR,
                                                         PrevMaxWR};
                                                     false ->
                                                        {?READ_RETRY_COUNT,
                                                         max(PrevMaxRR, SeenMaxRR),
                                                         max(PrevMaxRR, SeenMaxWR)}
                                             end,
                                {RetriesRemaining > 0, NewReadRetryInfo};
                            false ->
                                %% writes always modify acceptor states during
                                %% first phase
                                {false, none}
                        end,

                    case RetryWithoutIncrement of
                        true ->
                            %% retry the read without causing a state change in acceptors
                            gen_component:post_op({qround_request,
                                            entry_client(NewEntry),
                                            entry_openreqentry(NewEntry),
                                            entry_key(NewEntry),
                                            entry_datatype(NewEntry),
                                            entry_filters(NewEntry),
                                            entry_optype(NewEntry),
                                            ReadRetryInfo,
                                            entry_retrigger(NewEntry)}, State);
                        false ->
                            %% do a qread with highest received read round + 1
                            gen_component:post_op({qread,
                                           entry_client(NewEntry),
                                           entry_openreqentry(NewEntry),
                                           entry_key(NewEntry),
                                           entry_datatype(NewEntry),
                                           entry_filters(NewEntry),
                                           entry_retrigger(NewEntry),
                                           1+pr:get_r(entry_my_round(NewEntry)),
                                           entry_optype(NewEntry)}, State)
                    end
            end
    end;

%% qread step 3 with explicit read round number
on({qread, Client, OpenReqEntry, Key, DataType, ReadFilter, RetriggerAfter, ReadRound, OpType}, State) ->
    ?TRACE("rbrcseq:on qread ReqId ~p~n", [ReqId]),
    %% if the caller process may handle more than one request at a
    %% time for the same key, the pids id has to be unique for each
    %% request to use the prbr correctly.
    ReqId = uid:get_pids_uid(),

    %% initiate lookups for replicas(Key) and perform
    %% rbr reads in a certain round (work as paxos proposer)
    %% there apply the content filter to only retrieve the required information
    This = comm:reply_as(comm:this(), 2, {qread_collect, '_'}),

    %% add the ReqId in case we concurrently perform several requests
    %% for the same key from the same process, which may happen.
    %% later: retrieve the request id from the assigned round number
    %% to get the entry from the pdb
    MyId = {my_id(), ReqId},
    Dest = pid_groups:find_a(routing_table),
    DB = db_selector(State),
    %% create local state for the request
    Entry = entry_new_read(qread, ReqId, OpenReqEntry, Key, DataType, Client, period(State),
                           ReadFilter, RetriggerAfter, OpType),
    _ = case save_entry(Entry, tablename(State)) of
        ok ->
            [ begin
                %% let fill in whether lookup was consistent
                LookupEnvelope = dht_node_lookup:envelope(4,
                    {prbr, read, DB, '_', This, X, DataType, MyId, ReadFilter,
                     pr:new(ReadRound, MyId)}),
                comm:send_local(Dest, {?lookup_aux, X, 0, LookupEnvelope})
              end
            || X <- ?REDUNDANCY:get_keys(Key) ];
        error ->
            %% client has already received a reply
            ok
    end,

    %% retriggering of the request is done via the periodic dictionary scan
    %% {next_period, ...}

    State;

%% qread step 4: a replica replied to read from step 3
%%               when      majority reached
%%                  -> finish when consens is stable enough or
%%                  -> trigger write_through to stabilize an open consens
%%               otherwise just register the reply.
on({qread_collect,
    {read_reply, Cons, MyRwithId, Val, SeenWriteRound, SeenLastWF}}, State) ->
    ?TRACE("rbrcseq:on qread_collect read_reply MyRwithId: ~p~n", [MyRwithId]),
    %% collect a majority of answers and select that one with the highest
    %% round number.
    {_Round, ReqId} = pr:get_id(MyRwithId),
    case get_entry(ReqId, tablename(State)) of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        Entry ->
            Replies = entry_replies(Entry),
            {Result, NewReplies, NewRound} =
                add_read_reply(Replies, db_selector(State), MyRwithId, Val, SeenWriteRound,
                               entry_my_round(Entry), entry_optype(Entry), SeenLastWF,
                               entry_datatype(Entry), entry_filters(Entry), Cons),
            TE = entry_set_my_round(Entry, NewRound),
            NewEntry = entry_set_replies(TE, NewReplies),
            WriteState = NewReplies#r_replies.write_state,
            case Result of
                false ->
                    update_entry(NewEntry, tablename(State)),
                    State;
                true ->
                    trace_mpath:log_info(self(),
                                         {qread_done,
                                          readval, WriteState#write_state.highest_write_round,
                                          WriteState#write_state.value}),
                    inform_client(qread_done, NewEntry, WriteState#write_state.highest_write_round,
                                   WriteState#write_state.value),
                    delete_entry(NewEntry, tablename(State)),
                    State;
                write_through ->
                    %% in case a consensus was started, but not yet finished,
                    %% we first have to finish it
                    trace_mpath:log_info(self(), {qread_write_through_necessary}),
                    case randoms:rand_uniform(1,5) of
                        1 ->
                            %% delete entry, so outdated answers from minority
                            %% are not considered
                            delete_entry(NewEntry, tablename(State)),
                            gen_component:post_op({qread_initiate_write_through,
                                                   NewEntry}, State);
                        3 ->
                            %% delay a bit
                            _ = comm:send_local_after(
                                  15 + randoms:rand_uniform(1,10), self(),
                                  {qread_initiate_write_through, NewEntry}),
                            delete_entry(NewEntry, tablename(State)),
                            State;
                        2 ->
                            delete_entry(NewEntry, tablename(State)),
                            comm:send_local(self(), {qread_initiate_write_through,
                                                   NewEntry}),
                            State;
                        4 ->
                            delete_entry(NewEntry, tablename(State)),
                            %% retry read
                            gen_component:post_op({qread,
                                                   entry_client(NewEntry),
                                                   entry_openreqentry(NewEntry),
                                                   entry_key(NewEntry),
                                                   entry_datatype(NewEntry),
                                                   entry_filters(NewEntry),
                                                   entry_retrigger(NewEntry),
                                                   1+pr:get_r(entry_my_round(NewEntry)),
                                                   entry_optype(NewEntry)}, State);
                        5 ->
                            delete_entry(NewEntry, tablename(State)),
                            %% retry read
                            comm:send_local_after(15 + randoms:rand_uniform(1,10), self(),
                                                  {qread,
                                                   entry_client(NewEntry),
                                                   entry_openreqentry(NewEntry),
                                                   entry_key(NewEntry),
                                                   entry_datatype(NewEntry),
                                                   entry_filters(NewEntry),
                                                   entry_retrigger(NewEntry),
                                                   1+pr:get_r(entry_my_round(NewEntry)),
                                                   entry_optype(NewEntry)}),
                            State
                        end
            end
        end;

on({qread_collect, {read_deny, Cons, MyRwithId, LargerRound}}, State) ->
    {_Round, ReqId} = pr:get_id(MyRwithId),
    case get_entry(ReqId, tablename(State)) of
        undefined ->
            State;
        Entry ->
            Replies = entry_replies(Entry),
            {Result, NewReplies, NewRound} =
                add_read_deny(Replies, db_selector(State), entry_my_round(Entry),
                              LargerRound, Cons),
            TEntry = entry_set_my_round(Entry, NewRound),
            NewEntry = entry_set_replies(TEntry, NewReplies),
            case Result of
                false ->
                    update_entry(NewEntry, tablename(State)),
                    State;
                retry ->
                    % we can no longer achieve a quorum accept because of a
                    % concurrent request
                    NextMsg = {qread,
                               entry_client(NewEntry),
                               entry_openreqentry(NewEntry),
                               entry_key(NewEntry),
                               entry_datatype(NewEntry),
                               entry_filters(NewEntry),
                               entry_retrigger(NewEntry),
                               1 + pr:get_r(entry_my_round(NewEntry)),
                               entry_optype(NewEntry)},
                    delete_entry(NewEntry, tablename(State)),

                    case randoms:rand_uniform(1, 2) of
                        1 ->
                            %% retry read immediately
                            gen_component:post_op(NextMsg, State);
                        2 ->
                            %% delay before retry
                            Delay = 15 + randoms:rand_uniform(1, 10),
                            comm:send_local_after(Delay, self(), NextMsg),
                            State
                    end
            end
    end;

on({qread_initiate_write_through, ReadEntry}, State) ->
    ?TRACE("rbrcseq:on qread_initiate_write_through ~p~n", [ReadEntry]),
    %% if a read_filter was active, we cannot take over the value for
    %% a write_through.  We then have to retrigger the read without a
    %% read-filter, but in the end have to reply with a filtered
    %% value!
    case entry_filters(ReadEntry) =:= fun prbr:noop_read_filter/1 of
        true ->
            %% we are only allowed to try the write once in exactly
            %% this Round otherwise we may overwrite newer values with
            %% older ones?! If it fails, we have to restart with the
            %% read as we observed concurrency happening.

            %% we need a new id to collect the answers of this write
            %% the client in this new id will be ourselves, so we can
            %% proceed, when we got enough replies.
            This = comm:reply_as(
                     self(), 4,
                     {qread_write_through_done, ReadEntry, no_filtering, '_'}),

            ReqId = uid:get_pids_uid(),
            MyId = {my_id(), ReqId},

            %% we only try to re-write a consensus in exactly this
            %% round without retrying, so having no content check is
            %% fine here
            ReadReplies = entry_replies(ReadEntry),
            WriteState = ReadReplies#r_replies.write_state,
            ReadVal = WriteState#write_state.value,
            PreviousWTI = pr:get_wti(WriteState#write_state.highest_write_round),
            {WTWF, WTUI, WTVal} = %% WT.. means WriteThrough here
                case PreviousWTI of
                    none ->
                        {fun prbr:noop_write_filter/3, none, ReadVal};
                    {WriteRet, _} ->
                        WTI = {fun prbr:noop_write_filter/3, WriteRet, ReadVal},
                        ?TRACE("Setting write through write filter ~p",
                               [WTI]),
                        WTI
                 end,
            WriteRound = pr:set_wti(entry_my_round(ReadEntry), PreviousWTI),

            Filters = {fun prbr:noop_read_filter/1,
                       fun(_,_,_) -> {true, WTUI} end,
                       WTWF},

            Entry = entry_new_write(write_through, ReqId,
                                    entry_openreqentry(ReadEntry),
                                    entry_key(ReadEntry),
                                    entry_datatype(ReadEntry),
                                    This,
                                    period(State),
                                    Filters, WTVal,
                                    entry_retrigger(ReadEntry)
                                    - entry_period(ReadEntry)),

            Collector = comm:reply_as(
                          comm:this(), 3,
                          {qread_write_through_collect, ReqId, '_'}),

            Dest = pid_groups:find_a(routing_table),
            DB = db_selector(State),
            Keys = ?REDUNDANCY:get_keys(entry_key(Entry)),
            WTVals = ?REDUNDANCY:write_values_for_keys(Keys,  WTVal),
            _ = case save_entry(Entry, tablename(State)) of
                    ok ->
                        [ begin
                            %% let fill in whether lookup was consistent
                            LookupEnvelope =
                                dht_node_lookup:envelope(
                                    4,
                                    {prbr, write, DB, '_', Collector, K,
                                     entry_datatype(ReadEntry), MyId, WriteRound,
                                     pr:new(0,0), %% has no effect because write_through
                                     V, WTUI, WTWF, _IsWriteThrough = true}),
                            comm:send_local(Dest, {?lookup_aux, K, 0, LookupEnvelope})
                          end
                        || {K, V} <- lists:zip(Keys, WTVals) ];
                    error ->
                        %% client already received a reply
                        ok
                end,
            State;
        false ->
            %% apply the read-filter after the write_through: just
            %% initiate a read without filtering, which then - if the
            %% consens is still open - can trigger a repair and will
            %% reply to us with a full entry, that we can filter
            %% ourselves before sending it to the original client
            This = comm:reply_as(
                     self(), 4,
                     {qread_write_through_done, ReadEntry, apply_filter, '_'}),

            gen_component:post_op({qround_request, This, entry_openreqentry(ReadEntry),
               entry_key(ReadEntry), entry_datatype(ReadEntry), fun prbr:noop_read_filter/1, write,
               entry_retrigger(ReadEntry) - entry_period(ReadEntry)},
              State)
    end;

on({qread_write_through_collect, ReqId,
    {write_reply, Cons, _Key, Round, NextRound, WriteRet}}, State) ->
    ?TRACE("rbrcseq:on qread_write_through_collect reply ~p~n", [ReqId]),
    Entry = get_entry(ReqId, tablename(State)),
    _ = case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            ?TRACE("rbrcseq:on qread_write_through_collect Client: ~p~n", [entry_client(Entry)]),

            Replies = entry_replies(Entry),
            {Done, NewReplies, _HigherRound} = add_write_reply(Replies, Round, Cons),
            NewEntry = entry_set_replies(Entry, NewReplies),
            case Done of
                false -> update_entry(NewEntry, tablename(State));
                true ->
                    ?TRACE("rbrcseq:on qread_write_through_collect infcl: ~p~n", [entry_client(Entry)]),
                    ReplyEntry = entry_set_my_round(NewEntry, NextRound),
                    inform_client(qwrite_done, ReplyEntry, WriteRet),
                    delete_entry(Entry, tablename(State))
            end
    end,
    State;

on({qread_write_through_collect, ReqId,
    {write_deny, Cons, Key, RoundTried}}, State) ->
    ?TRACE("rbrcseq:on qread_write_through_collect deny ~p~n", [ReqId]),
    TableName = tablename(State),
    Entry = get_entry(ReqId, TableName),
    _ = case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            ?TRACE("rbrcseq:on qread_write_through_collect deny Client: ~p~n", [entry_client(Entry)]),
            Replies = entry_replies(Entry),
            {Done, NewReplies} = add_write_deny(Replies, RoundTried, Cons),
            NewEntry = entry_set_replies(Entry, NewReplies),

            case Done of
                false ->
                    update_entry(NewEntry, tablename(State)),
                    State;
                true ->
                    %% retry original read
                    delete_entry(Entry, TableName),

                    %% we want to retry with the read, the original
                    %% request is packed in the client field of the
                    %% entry as we created a reply_as with
                    %% qread_write_through_done The 2nd field of the
                    %% reply_as was filled with the original state
                    %% entry (including the original client and the
                    %% original read filter.
                    {_Pid, Msg1} = comm:unpack_cookie(entry_client(Entry), {whatever}),
                    %% reply_as from qread write through without filtering
                    qread_write_through_done = comm:get_msg_tag(Msg1),
                    UnpackedEntry = element(2, Msg1),
                    UnpackedClient = entry_client(UnpackedEntry),

                    %% In case of filters enabled, we packed once more
                    %% to write through without filters and applying
                    %% the filters afterwards. Let's check this by
                    %% unpacking and seeing whether the reply msg tag
                    %% is still a qread_write_through_done. Then we
                    %% have to use the 2nd unpacking to get the
                    %% original client entry.
                    {_Pid2, Msg2} = comm:unpack_cookie(
                                      UnpackedClient, {whatever2}),

                    {Client, Filter} =
                        case comm:get_msg_tag(Msg2) of
                            qread_write_through_done ->
                                %% we also have to delete this request
                                %% as no one will answer it.
                                UnpackedEntry2 = element(2, Msg2),
                                delete_entry(UnpackedEntry2, TableName),
                                {entry_client(UnpackedEntry2),
                                 entry_filters(UnpackedEntry2)};
                            _ ->
                                {UnpackedClient,
                                 entry_filters(UnpackedEntry)}
                        end,
                    gen_component:post_op({qround_request, Client, entry_openreqentry(Entry),
                      Key, entry_datatype(Entry), Filter, write, entry_retrigger(Entry) - entry_period(Entry)},
                      State)
            end
    end;

on({qread_write_through_done, ReadEntry, _Filtering,
    {qwrite_done, _ReqId, _Round, _Val, _WriteRet}}, State) ->
    ?TRACE("rbrcseq:on qread_write_through_done qwrite_done ~p ~p~n", [_ReqId, ReadEntry]),
    %% as we applied a write filter, the actual distributed consensus
    %% result may be different from the highest paxos version, that we
    %% collected in the beginning. So we have to initiate the qread
    %% again to get the latest value.

%%    Old:
%%    ClientVal =
%%        case Filtering of
%%            apply_filter -> F = entry_filters(ReadEntry), F(Val);
%%            _ -> Val
%%        end,
%%    TReplyEntry = entry_set_val(ReadEntry, ClientVal),
%%    ReplyEntry = entry_set_my_round(TReplyEntry, Round),
%%    %% log:pal("Write through of write request done informing ~p~n", [ReplyEntry]),
%%    inform_client(qread_done, ReplyEntry),

    Client = entry_client(ReadEntry),
    OpenReqEntry = entry_openreqentry(ReadEntry),
    Key = entry_key(ReadEntry),
    DataType = entry_datatype(ReadEntry),
    ReadFilter = entry_filters(ReadEntry),
    RetriggerAfter = entry_retrigger(ReadEntry) - entry_period(ReadEntry),

    gen_component:post_op(
      {qround_request, Client, OpenReqEntry, Key, DataType, ReadFilter, write, RetriggerAfter},
      State);

on({qread_write_through_done, ReadEntry, Filtering,
    {qread_done, _ReqId, Round, OldWriteRound, Val}}, State) ->
    ?TRACE("rbrcseq:on qread_write_through_done qread_done ~p ~p~n", [_ReqId, ReadEntry]),
    ClientVal =
        case Filtering of
            apply_filter -> F = entry_filters(ReadEntry), F(Val);
            _ -> Val
        end,
    Replies = entry_replies(ReadEntry),
    WriteState = Replies#r_replies.write_state,
    NewWriteState = WriteState#write_state{value=ClientVal},
    NewReplies = Replies#r_replies{write_state=NewWriteState},
    TReplyEntry = entry_set_replies(ReadEntry, NewReplies),
    ReplyEntry = entry_set_my_round(TReplyEntry, Round),

    inform_client(qread_done, ReplyEntry, OldWriteRound,
                  NewWriteState#write_state.value),

    State;

%% normal qwrite step 1: preparation and starting read-phase
on({qwrite, Client, OpenReqEntry, Key, DataType, Filters, WriteValue, RetriggerAfter}, State) ->
    ?TRACE("rbrcseq:on qwrite~n", []),
    %% assign new reqest-id
    ReqId = uid:get_pids_uid(),
    ?TRACE("rbrcseq:on qwrite c ~p uid ~p ~n", [Client, ReqId]),

    %% create local state for the request id, including used filters
    Entry = entry_new_write(qwrite, ReqId, OpenReqEntry, Key, DataType, Client, period(State),
                            Filters, WriteValue, RetriggerAfter),

    This = comm:reply_as(self(), 3, {qwrite_read_done, ReqId, '_'}),
    case save_entry(Entry, tablename(State)) of
        ok ->
            gen_component:post_op({qround_request, This, OpenReqEntry, Key, DataType,
                                 element(1, Filters), write, _RetriggerAfter = 1}, State);
        error ->
            %% client has already received reply
            State
    end;

%% qwrite step 2: qread is done, we trigger a quorum write in the given Round
on({qwrite_read_done, ReqId,
    {qread_done, _ReadId, Round, OldWriteRound, ReadValue}},
   State) ->
    ?TRACE("rbrcseq:on qwrite_read_done qread_done~n", []),
    gen_component:post_op({do_qwrite_fast, ReqId, Round, OldWriteRound, ReadValue}, State);

on({qwrite_fast, Client, OpenReqEntry, Key, DataType, Filters = {_RF, _CC, _WF},
    WriteValue, RetriggerAfter, Round, OldWriteRound, ReadFilterResultValue}, State) ->

    %% create state and ReqId, store it and trigger 'do_qwrite_fast'
    %% which is also the write phase of a slow write.
        %% assign new reqest-id
    ReqId = uid:get_pids_uid(),
    ?TRACE("rbrcseq:on qwrite c ~p uid ~p ~n", [Client, ReqId]),

    %% create local state for the request id, including used filters
    Entry = entry_new_write(qwrite, ReqId, OpenReqEntry, Key, DataType, Client, period(State),
                            Filters, WriteValue, RetriggerAfter),

    case save_entry(Entry, tablename(State)) of
        ok ->
            gen_component:post_op({do_qwrite_fast, ReqId, Round, OldWriteRound,
                                  ReadFilterResultValue}, State);
        error ->
            %% client hjas already received reply
            State
    end;

on({do_qwrite_fast, ReqId, Round, OldWriteRound, OldRFResultValue}, State) ->
    %% What if ReqId does no longer exist? Can that happen? How?
    %% a) Lets analyse the paths to do_qwrite_fast:
    %% b) Lets analyse when entries are removed from the database:
    Entry = get_entry(ReqId, tablename(State)),
    MyId = {my_id(), ReqId},
    _ = case Entry of
        undefined ->
          %% drop actions for unknown requests, as they must be
          %% outdated. The retrigger mechanism may delete entries at
          %% any time, so we have to be prepared for that.
          State;
        _ ->
          NewEntry = entry_set_debug(Entry, do_qwrite_fast),
          ContentCheck = element(2, entry_filters(NewEntry)),
          WriteFilter = element(3, entry_filters(NewEntry)),
          WriteValue = entry_write_val(NewEntry),
          DataType = entry_datatype(NewEntry),

          _ = case ContentCheck(OldRFResultValue,
                                WriteFilter,
                                WriteValue) of
              {true, PassedToUpdate} ->
                %% own proposal possible as next instance in the
                %% consens sequence
                This = comm:reply_as(comm:this(), 3, {qwrite_collect, ReqId, '_'}),
                DB = db_selector(State),
                Keys = ?REDUNDANCY:get_keys(entry_key(NewEntry)),
                WrVals = ?REDUNDANCY:write_values_for_keys(Keys,  WriteValue),

                [ begin
                    %% let fill in whether lookup was consistent
                    LookupEnvelope =
                      dht_node_lookup:envelope(
                        4,
                        {prbr, write, DB, '_', This, K, DataType, MyId, Round, OldWriteRound,
                        V, PassedToUpdate, WriteFilter, _IsWriteThrough = false}),
                    api_dht_raw:unreliable_lookup(K, LookupEnvelope)
                  end
                  || {K, V} <- lists:zip(Keys, WrVals)];
                {false, Reason} = _Err ->
                  %% own proposal not possible as of content check
                  comm:send_local(entry_client(NewEntry),
                            {qwrite_deny, ReqId, Round, OldRFResultValue,
                            {content_check_failed, Reason}}),
                  delete_entry(NewEntry, tablename(State))
                end,
            State
        end;

%% qwrite step 3: a replica replied to write from step 2
%%                when      majority reached, -> finish.
%%                otherwise just register the reply.
on({qwrite_collect, ReqId,
    {write_reply, Cons, _Key, Round, NextRound, WriteRet}}, State) ->
    ?TRACE("rbrcseq:on qwrite_collect write_reply~n", []),
    Entry = get_entry(ReqId, tablename(State)),
    _ = case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            Replies = entry_replies(Entry),
            {Done, NewReplies, IsHigherRound} = add_write_reply(Replies, Round, Cons),
            NewEntry = case IsHigherRound andalso entry_optype(Entry) =:= denied_write of
                            true ->
                                %% If we entered this branch that means we received
                                %% the first success reply from a writethrough
                                %% that tries to establish our previous failed
                                %% write attempt. It is possible that this proposer
                                %% already retried its write in a new request
                                %% (howerver, it cannot have started the write phase yet)
                                %% To prevent that the write is applied twice, delete
                                %% the newer request. In case that this WT fails
                                %% this proposer will be notified of it
                                %% and will then start a new try.
                                TEntry = entry_set_optype(Entry, write),
                                delete_newer_entries(TEntry, tablename(State)),
                                entry_set_replies(TEntry, NewReplies);
                            false ->
                                entry_set_replies(Entry, NewReplies)
                       end,
            case Done of
                false -> update_entry(NewEntry, tablename(State));
                true ->
                    ReplyEntry = entry_set_my_round(NewEntry, NextRound),
                    trace_mpath:log_info(self(),
                                         {qwrite_done,
                                          value, entry_write_val(ReplyEntry)}),
                    inform_client(qwrite_done, ReplyEntry, WriteRet),

                    %% a successfull write is always the end of the protocol.
                    %% immediately clear all open requests associated
                    %% with the client request, and do not wait unitl this
                    %% process received the cleanup msg. This must be done
                    %% to prevent a second write beeing submitted by this proposer
                    delete_all_entries(entry_openreqentry(NewEntry),
                                       tablename(State), _DeleteReqEntryToo=false)
            end
    end,
    State;

%% qwrite step 3: a replica replied to write from step 2
%%                when      majority reached, -> finish.
%%                otherwise just register the reply.
on({qwrite_collect, ReqId,
    {write_deny, Cons, _Key, RoundTried}}, State) ->
    ?TRACE("rbrcseq:on qwrite_collect write_deny~n", []),
    TableName = tablename(State),
    Entry = get_entry(ReqId, TableName),
    case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            Replies = entry_replies(Entry),
            {Done, NewReplies} = add_write_deny(Replies, RoundTried, Cons),
            NewEntry = entry_set_replies(Entry, NewReplies),
            %% If this request was already denied, do not deny again
            %% (see true case why this might happen)
            case Done andalso entry_optype(NewEntry) =/= denied_write of
                false ->
                    update_entry(NewEntry, TableName),
                    State;
                true ->
                    %% retry
                    %% log:pal("Concurrency detected, retrying~n"),

                    %% we have to reshuffle retries a bit, so no two
                    %% proposers using the same rbrcseq process steal
                    %% each other the token forever.
                    %% On a random basis, we either reenqueue the
                    %% request to ourselves or we retry the request
                    %% directly via a post_op.

                    %% As this happens only when concurrency is detected (not the critical
                    %% failure- and concurrency-free path), we have the time to choose a
                    %% random number. This is still faster than using msg_delay or
                    %% comm:local_send_after() with a random delay.
                    %% TODO: random is not allowed for proto_sched reproducability...
                    %% log:log("Concurrency retry"),
                    UpperLimit = case proto_sched:infected() of
                                     true -> 2;
                                     false -> 3
                                 end,

                    %% The request is retriggered. However, the current entry
                    %% representing *this* request should NOT be deleted. This
                    %% write attempt may have written some (albeit the minority of)
                    %% replicas. A concurrent proposer might see an inconsistent write state,
                    %% thus triggering a write through. If acceptors vote for this
                    %% WT-proposal, the original proposer (i.e. this process) is notified.
                    %% These messages are aggregated using the current req-id. Thus
                    %% we need to keep it around for now until some write suceeded.
                    %%
                    %% To prevent a deny msg that lags behind from triggering
                    %% this branch again, mark this request as denied
                    %% Also we need to prevent a retrigger of this request
                    %% since it already spawns a successor request.
                    %% Do NOT pass these updates to req_for_retrigger below!
                    NewEntry2 = entry_set_optype(NewEntry, denied_write),
                    NewEntry3 = entry_disable_retrigger(NewEntry2),
                    update_entry(NewEntry3, TableName),
                    case randoms:rand_uniform(1, UpperLimit) of
                        1 ->
                            NewReq = req_for_retrigger(NewEntry, noincdelay),
                            gen_component:post_op(NewReq, State);
                        2 ->
                            NewReq = req_for_retrigger(NewEntry, noincdelay),
                            %% TODO: maybe record number of retries
                            %% and make timespan chosen from
                            %% dynamically wider
                            _ = comm:send_local_after(
                                  10 + randoms:rand_uniform(1,90), self(),
                                  NewReq),
                            State
                    end
            end
    end
    %% decide somehow whether a fast paxos or a normal paxos is necessary
    %% if full paxos: perform qread(self(), Key, ContentReadFilter)

    %% if can propose andalso ContentValidNextStep(Result,
    %% ContentWriteFilter, Value)?
    %%   initiate lookups to replica keys and perform rbr:write on each

    %% collect a majority of ok answers or maj -1 of deny answers.
    %% inform the client
    %% delete the local state of the request

    %% reissue the write if not enough replies collected (with higher
    %% round number)

    %% drop replies for unknown requests, as they must be outdated
    %% as all initiations run through the same process.
    ;

%% periodically scan the local states for long lasting entries and
%% retrigger them
on({next_period, NewPeriod}, State) ->
    ?TRACE_PERIOD("~p ~p rbrcseq:on next_period~n", [self(), pid_groups:my_pidname()]),
    %% reissue (with higher round number) the read if not enough
    %% replies collected somehow take care of the event and retrigger,
    %% if it takes to long. Either use msg_delay or record a timestamp
    %% and periodically revisit all open requests and check whether
    %% they remain unanswered to long in the system, (could be
    %% combined with a field of allowed duration which is increased
    %% per retriggering to catch slow or overloaded systems)

    %% could also be done in another (spawned?) process to avoid
    %% longer service interruptions in this process?

    %% scan for open requests older than NewPeriod and initiate
    %% retriggering for them
    Table = tablename(State),
    _ = [ retrigger(X, Table, incdelay)
          || X <- ?PDB:tab2list(Table), is_tuple(X), 14 =:= erlang:tuple_size(X),
             0 =/= entry_retrigger(X), NewPeriod > entry_retrigger(X)],

    %% re-trigger next next_period
    msg_delay:send_trigger(1, {next_period, NewPeriod + 1}),
    set_period(State, NewPeriod).

-spec req_for_retrigger(entry(), incdelay|noincdelay) ->
                               {qround_request,
                                Client :: comm:erl_local_pid(),
                                OpenReqEntry :: any(),
                                Key :: ?RT:key(),
                                DataType :: module(),
                                Filters :: any(),
                                OpType :: atom(),
                                Delay :: non_neg_integer()}
                               | {qwrite,
                                Client :: comm:erl_local_pid(),
                                OPenReqEntry :: any(),
                                Key :: ?RT:key(),
                                DataType :: module(),
                                Filters :: any(),
                                Val :: any(),
                                Delay :: non_neg_integer()}.
req_for_retrigger(Entry, IncDelay) ->
    RetriggerDelay = case IncDelay of
                         incdelay -> erlang:max(1, (entry_retrigger(Entry) - entry_period(Entry)) + 1);
                         noincdelay -> entry_retrigger(Entry)
                     end,
    ?ASSERT(erlang:tuple_size(Entry) =:= 14),
    Filters = entry_filters(Entry),
    if is_tuple(Filters) -> %% write request
           {qwrite, entry_client(Entry), entry_openreqentry(Entry),
            entry_key(Entry), entry_datatype(Entry),
            entry_filters(Entry), entry_write_val(Entry),
            RetriggerDelay};
       true -> %% read request
           {qround_request, entry_client(Entry), entry_openreqentry(Entry),
            entry_key(Entry), entry_datatype(Entry), entry_filters(Entry),
            entry_optype(Entry), RetriggerDelay}
    end.

-spec retrigger(entry(), ?PDB:tableid(), incdelay|noincdelay) -> ok.
retrigger(Entry, TableName, IncDelay) ->
    Request = req_for_retrigger(Entry, IncDelay),
    ?TRACE("Retrigger caused by timeout or concurrency for ~.0p~n", [Request]),
    comm:send_local(self(), Request),
    delete_entry(Entry, TableName),
    ok.

-spec get_entry(any(), ?PDB:tableid()) -> entry() | {any(), [any()]} | undefined.
get_entry(ReqId, TableName) ->
    ?PDB:get(ReqId, TableName).

%% Refreshs the content of an existing entry. ATTENTION: No check if this entry
%% already exists will be performed.
-spec update_entry(entry(), ?PDB:tableid()) -> ok.
update_entry(NewEntry, TableName) ->
    ?PDB:set(NewEntry, TableName).

%% Stores a new entry in ?PDB as long as the open-request-entry
%% still exists. Does nothing otherwise. If such entry does not exist,
%% this means that the client has already reveiced a reply and no further
%% work should be done for this request (Otherwise a write might be
%% applied twice.)
-spec save_entry(entry(), ?PDB:tableid()) -> ok | error.
save_entry(NewEntry, TableName) ->
    ReqId = entry_reqid(NewEntry),
    OpenReqEntry = entry_openreqentry(NewEntry),
    case get_entry(OpenReqEntry, TableName) of
        {OpenReqEntry, OpenReqList} ->
            ?PDB:set({OpenReqEntry, [ReqId | OpenReqList]}, TableName),
            ?PDB:set(NewEntry, TableName),
            ok;
        _ -> error
    end.

%% Deletes an request entry, as well as its entry in the open-request-entry.
-spec delete_entry(entry(), ?PDB:tableid()) -> ok.
delete_entry(Entry, TableName) ->
    ReqId = entry_reqid(Entry),
    OpenReqEntry = entry_openreqentry(Entry),
    {OpenReqEntry, OpenReqList} = get_entry(OpenReqEntry, TableName),
    ?PDB:set({OpenReqEntry, lists:delete(ReqId, OpenReqList)}, TableName),
    ?PDB:delete(ReqId, TableName).

%% Deletes all entries that where added to the open request list after
%% the given entry.
-spec delete_newer_entries(entry(), ?PDB:tableid()) -> ok.
delete_newer_entries(Entry, TableName) ->
    {OpenReqEntryId, OpenReqList} = get_entry(entry_openreqentry(Entry), TableName),
    EntryReqId = entry_reqid(Entry),
    {ToDelete, ToKeep} = lists:splitwith(fun(E) -> E =/= EntryReqId end, OpenReqList),
    lists:foreach(fun(E) -> ?PDB:delete(E, TableName) end, ToDelete),
    ?PDB:set({OpenReqEntryId, ToKeep}, TableName).

%% Deletes all entries included in the open-request-entry. No more can be done.
%% Return an error if entry is malformed or does not exist
-spec delete_all_entries(any(), ?PDB:tableid(), boolean()) -> ok | error.
delete_all_entries(OpenReqEntryId, TableName, DeleteOpenReqEntryToo) ->
    case get_entry(OpenReqEntryId, TableName) of
        {OpenReqEntryId, EntryList} ->
            lists:foreach(fun(E) -> ?PDB:delete(E, TableName) end, EntryList),
            case DeleteOpenReqEntryToo of
                true ->
                    ?PDB:delete(OpenReqEntryId, TableName);
                false ->
                    ?PDB:set({OpenReqEntryId, []}, TableName),
                    ok
            end;
        _ -> error
    end.

%% Creates entry which will be used to keep track of open requests and returns
%% its keys by which it can be retrieved.
-spec create_open_entry_register(?PDB:tableid()) -> any().
create_open_entry_register(TableName) ->
    RegId = uid:get_pids_uid(),
    InitVal = [],
    ?PDB:set({RegId, InitVal}, TableName),
    RegId.

%% abstract data type to collect quorum read/write replies
-spec entry_new_round_request(any(), any(), any(), ?RT:key(), module(),
                     comm:erl_local_pid(), non_neg_integer(), any(),
                     non_neg_integer(), read | write, any())
                    -> entry().
entry_new_round_request(Debug, ReqId, EntryReg, Key, DataType, Client, Period, Filter, RetriggerAfter,
                        OpType, ReadRetryInfo) ->
    {ReqId, EntryReg, Debug, Period, Period + RetriggerAfter + 20, Key, DataType, Client,
     Filter, _ValueToWrite = is_read, OpType, _MyRound = pr:new(0,0), ReadRetryInfo, new_rr_replies()}.

-spec entry_new_read(any(), any(), any(), ?RT:key(), module(),
                     comm:erl_local_pid(), non_neg_integer(), any(),
                     non_neg_integer(), read | write)
                    -> entry().
entry_new_read(Debug, ReqId, EntryReg,  Key, DataType, Client, Period, Filter, RetriggerAfter, OpType) ->
    {ReqId, EntryReg, Debug, Period, Period + RetriggerAfter + 20, Key, DataType, Client,
     Filter, _ValueToWrite = is_read, OpType, _MyRound = pr:new(0,0), 0, new_read_replies()}.

-spec entry_new_write(any(), any(), any(), ?RT:key(), module(), comm:erl_local_pid(),
                      non_neg_integer(), tuple(), any(), non_neg_integer())
                     -> entry().
entry_new_write(Debug, ReqId, EntryReg, Key, DataType, Client, Period, Filters, Value, RetriggerAfter) ->
    {ReqId, EntryReg, Debug, Period, Period + RetriggerAfter, Key, DataType, Client,
     Filters, _ValueToWrite = Value, write, _MyRound = pr:new(0,0), 0, new_write_replies()}.

-spec new_rr_replies() -> #rr_replies{}.
new_rr_replies() ->
    #rr_replies{reply_count = 0, highest_read_count = 0,
                highest_read_round = pr:new(0,0), write_state = new_write_state()}.

-spec new_read_replies() -> #r_replies{}.
new_read_replies() ->
    #r_replies{ack_count = 0, deny_count = 0, write_state = new_write_state()}.

-spec new_write_state() -> #write_state{}.
new_write_state() ->
    #write_state{highest_write_count = 0, highest_write_round = pr:new(0,0),
                  highest_write_filter = none, value = new_empty_value}.

-spec new_write_replies() -> #w_replies{}.
new_write_replies() ->
    #w_replies{ack_count = 0, deny_count = 0,
               highest_write_round = pr:new(0,0)}.

-spec entry_reqid(entry())        -> any().
entry_reqid(Entry)                -> element(1, Entry).
-spec entry_openreqentry(entry()) -> any().
entry_openreqentry(Entry)         -> element(2, Entry).
-spec entry_set_debug(entry(), any()) -> entry().
entry_set_debug(Entry,Debug)      -> setelement(3, Entry, Debug).
-spec entry_period(entry())       -> non_neg_integer().
entry_period(Entry)               -> element(4, Entry).
-spec entry_retrigger(entry())    -> non_neg_integer().
entry_retrigger(Entry)            -> element(5, Entry).
-spec entry_disable_retrigger(entry()) -> entry().
entry_disable_retrigger(Entry) -> setelement(5, Entry, 0).
-spec entry_key(entry())          -> any().
entry_key(Entry)                  -> element(6, Entry).
-spec entry_datatype(entry())     -> module().
entry_datatype(Entry)             -> element(7, Entry).
-spec entry_client(entry())       -> comm:erl_local_pid().
entry_client(Entry)               -> element(8, Entry).
-spec entry_filters(entry())      -> any().
entry_filters(Entry)              -> element(9, Entry).
-spec entry_write_val(entry())    -> is_read | any().
entry_write_val(Entry)            -> element(10, Entry).
-spec entry_optype(entry())       -> read | write | denied_write.
entry_optype(Entry)               -> element(11, Entry).
-spec entry_set_optype(entry(), read | write | denied_write) -> entry().
entry_set_optype(Entry, OpType)   -> setelement(11, Entry, OpType).
-spec entry_my_round(entry())     -> pr:pr().
entry_my_round(Entry)             -> element(12, Entry).
-spec entry_set_my_round(entry(), pr:pr()) -> entry().
entry_set_my_round(Entry, Round)  -> setelement(12, Entry, Round).
-spec entry_get_read_retry_info(entry()) -> any().
entry_get_read_retry_info(Entry) -> element(13, Entry).
-spec entry_replies(entry())      -> replies().
entry_replies(Entry)              -> element(14, Entry).
-spec entry_set_replies(entry(), replies()) -> entry().
entry_set_replies(Entry, Replies) -> setelement(14, Entry, Replies).

-spec add_rr_reply(#rr_replies{}, dht_node_state:db_selector(),
                   pr:pr(), pr:pr(), client_value(), atom(), prbr:write_filter(),
                   module(), any(), boolean())
                   -> {false | consistent | inconsistent | write_through,
                       #rr_replies{}, pr:pr()}.
add_rr_reply(Replies, _DBSelector, SeenReadRound, SeenWriteRound, Value,
             OpType, SeenLastWF, Datatype, Filters, _Cons) ->
    %% increment number of replies received
    ReplyCount = Replies#rr_replies.reply_count + 1,
    R1 = Replies#rr_replies{reply_count=ReplyCount},

    %% update number of newest read rounds received
    PrevMaxReadR = Replies#rr_replies.highest_read_round,
    R2 =
        if PrevMaxReadR =:= SeenReadRound ->
                MaxRCount = R1#rr_replies.highest_read_count + 1,
                R1#rr_replies{highest_read_count=MaxRCount};
           PrevMaxReadR < SeenReadRound ->
                R1#rr_replies{highest_read_count=1,
                              highest_read_round=SeenReadRound};
           true ->
                R1
        end,

    %% update write rounds and value
    NewWriteState = update_write_state(Replies#rr_replies.write_state,
                                       SeenWriteRound, SeenLastWF, Value, Datatype),
    R3 = R2#rr_replies{write_state=NewWriteState},

    %% If enough replicas have replied, decide on the next action
    %% to take.
    {Result, R4} =
        case ?REDUNDANCY:quorum_accepted(ReplyCount) of
            true ->
                NewHighestWF = NewWriteState#write_state.highest_write_filter,
                ReadFilter =
                    case Filters of
                        {RF, _, _}   -> RF;
                        RF           -> RF
                    end,

                ConsReadRounds = ReplyCount =:= R3#rr_replies.highest_read_count,
                ConsWriteRounds = ReplyCount =:= NewWriteState#write_state.highest_write_count,
                ConsQuorum = ConsReadRounds andalso ConsWriteRounds,

                IsRead = OpType =:= read,
                IsCommutingRead = IsRead andalso is_read_commuting(ReadFilter, NewHighestWF, Datatype),

                if  %% A consistent quorum is the base case for successful delivery
                    ConsQuorum orelse
                    %% Reads never interfere with writes. Here, read rounds are inconsistent
                    %% which means there is a write in progress. But we can be certain that
                    %% the write is not done because the write rounds are consistent. Thus,
                    %% this read is concurrent to the write and the 'old' state can be safely
                    %% delivered.
                    (IsRead andalso ConsWriteRounds) orelse
                    %% this is a read operation that commutes with in-progress write
                    IsCommutingRead ->
                        %% construct value from received replies (has no effect when
                        %% data is simply replicated).
                        CollectedVal = NewWriteState#write_state.value,
                        ReadValue = ?REDUNDANCY:get_read_value(CollectedVal, ReadFilter),
                        T1 = R3#rr_replies{write_state=NewWriteState#write_state{value=ReadValue}},
                        {consistent, T1};

                    %% For everything else, the default is starting a qread which
                    %% might receive a consistent state
                    true ->
                        {inconsistent, R3}
                end;
            false ->
                %% no majority yet
                {false, R3}
        end,
    {Result, R4, R4#rr_replies.highest_read_round}.

-spec add_read_reply(#r_replies{}, dht_node_state:db_selector(),
                     pr:pr(),  client_value(),  pr:pr(), pr:pr(), atom(),
                     prbr:write_filter(), module(), any(), Consistency::boolean())
                    -> {Done::boolean() | write_through, #r_replies{}, pr:pr()}.
add_read_reply(Replies, _DBSelector, AssignedRound, Val, SeenWriteRound,
               CurrentRound, OpType, SeenLastWF, Datatype, Filters, _Cons) ->
    %% either decide on a majority of consistent replies, than we can
    %% just take the newest consistent value and do not need a
    %% write_through?
    %% Otherwise we decide on a consistent quorum (a majority agrees
    %% on the same version). We ensure this by write_through on odd
    %% cases.
    NewAckCount = Replies#r_replies.ack_count + 1,
    R1 = Replies#r_replies{ack_count=NewAckCount},

    NewWriteState = update_write_state(Replies#r_replies.write_state,
                                       SeenWriteRound, SeenLastWF, Val, Datatype),
    R2 = R1#r_replies{write_state=NewWriteState},


    {Result, R4} =
        case ?REDUNDANCY:quorum_accepted(NewAckCount) of
            true ->
                %% we have the majority of acks and do not have to wait for
                %% more replies

                ReadFilter =
                    case Filters of
                        {RF, _, _} -> RF;
                        RF         -> RF
                    end,

                %% construct read value from replies and update reply aggregator
                Collected = NewWriteState#write_state.value,
                Constructed = ?REDUNDANCY:get_read_value(Collected, ReadFilter),
                R3 = R2#r_replies{write_state=NewWriteState#write_state{value=Constructed}},

                SawConsWriteState = NewWriteState#write_state.highest_write_count =:= NewAckCount,
                IsCommutingRead = OpType =:= read andalso
                                      is_read_commuting(ReadFilter,
                                                        NewWriteState#write_state.highest_write_filter,
                                                        Datatype),

                if SawConsWriteState orelse IsCommutingRead ->
                        %% yay, we can deliver the result!
                        {true, R3};
                   true ->
                        %% Due to the inconsistent write state, we must help
                        %% to establish the current value in a write-through
                        {write_through, R3}
                end;
            _ ->
                {false, R2}
        end,

    NewRound = erlang:max(CurrentRound, AssignedRound),
    {Result, R4, NewRound}.

-spec update_write_state(#write_state{}, pr:pr(), prbr:write_filter(), any(), module()) -> #write_state{}.
update_write_state(WriteState, SeenRound, SeenLastWF, SeenValue, Datatype) ->
    %% extract write through info for round comparisons since
    %% they can be key-dependent if something different than
    %% replication is used for redundancy
    CurrentRoundNoWTI = pr:set_wti(WriteState#write_state.highest_write_round, none),
    SeenRoundNoWTI = pr:set_wti(SeenRound, none),

    CurrentValue = WriteState#write_state.value,
    if CurrentRoundNoWTI =:= SeenRoundNoWTI ->
            WriteState#write_state{
              highest_write_count=WriteState#write_state.highest_write_count+1,
              value=?REDUNDANCY:collect_read_value(CurrentValue, SeenValue,Datatype)
             };
       CurrentRoundNoWTI < SeenRoundNoWTI ->
            WriteState#write_state{
              highest_write_round=SeenRound,
              highest_write_count=1,
              highest_write_filter=SeenLastWF,
              value=?REDUNDANCY:collect_newer_read_value(CurrentValue,SeenValue, Datatype)
             };
       true ->
            WriteState#write_state{
              value = ?REDUNDANCY:collect_older_read_value(CurrentValue, SeenValue, Datatype)
             }
    end.

-spec add_read_deny(#r_replies{}, dht_node_state:db_selector(), pr:pr(), pr:pr(), boolean())
                    -> {retry | false, #r_replies{}, pr:pr()}.
add_read_deny(Replies, _DBSelector, CurrentRound, ReceivedRound, _Cons) ->
    %% increment deny count
    NewDenies = Replies#r_replies.deny_count + 1,
    R1 = Replies#r_replies{deny_count = NewDenies},

    %% the entries new round will be the maximum received round
    NewRound = erlang:max(CurrentRound, ReceivedRound),

    %% retry the read if enough acceptors have denied
    Result = case ?REDUNDANCY:quorum_denied(NewDenies) of
                     true -> retry;
                     false -> false
             end,
    {Result, R1, NewRound}.

-spec add_write_reply(#w_replies{}, pr:pr(), Consistency::boolean())
                     -> {Done::boolean(), #w_replies{}, IsHigherRound::boolean()}.
add_write_reply(Replies, Round, _Cons) ->
    RepliesMaxWriteR = Replies#w_replies.highest_write_round,
    RepliesRoundCmp = {pr:get_r(RepliesMaxWriteR), pr:get_id(RepliesMaxWriteR)},
    RoundCmp = {pr:get_r(Round), pr:get_id(Round)},
    {R1, IsHigherRound} =
        case RoundCmp > RepliesRoundCmp of
            false -> {Replies, false};
            true ->
                %% Running into this case can mean two things:
                %% 1. This is the first reply received
                %% 2. This is a reply of a write through which tries to repair the
                %% partially written value of this request
                %% If this is case 2, all previous received replies are obsolete.
                {Replies#w_replies{highest_write_round=Round,
                                   ack_count=0, deny_count=0}, true}
        end,
    R2 =
        case RoundCmp >= RepliesRoundCmp of
            %% We must ignore all replies based on the original write (older round), if
            %% we already received write through replies.
            false -> R1;
            true ->
                NewAckCount = R1#w_replies.ack_count + 1,
                R1#w_replies{ack_count=NewAckCount}
        end,
    Done = ?REDUNDANCY:quorum_accepted(R2#w_replies.ack_count),
    {Done, R2, IsHigherRound}.

-spec add_write_deny(#w_replies{}, pr:pr(), Consistency::boolean())
                    -> {Done::boolean(), #w_replies{}}.
add_write_deny(Replies, RoundTried, _Cons) ->
    RepliesMaxWriteR = Replies#w_replies.highest_write_round,
    RepliesRoundCmp = {pr:get_r(RepliesMaxWriteR), pr:get_id(RepliesMaxWriteR)},
    RoundCmp = {pr:get_r(RoundTried), pr:get_id(RoundTried)},
    R1 =
        case RoundCmp > RepliesRoundCmp of
            false -> Replies;
            true ->
                %% Running into this case can mean two things:
                %% 1. This is the first reply received
                %% 2. This is a reply of a write through which tries to repair the
                %% partially written value of this request
                %% If this is case 2, all previous received replies are obsolete.
                Replies#w_replies{highest_write_round=RoundTried,
                                  ack_count=0,
                                  deny_count=0}
        end,
    R2 =
        case RoundCmp >= RepliesRoundCmp of
            %% We must ignore all replies based on the original write (older round), if
            %% we already received write through replies.
            false -> R1;
            true ->
                NewDenyCount = R1#w_replies.deny_count + 1,
                R1#w_replies{deny_count=NewDenyCount}
        end,
    Done = ?REDUNDANCY:quorum_denied(R2#w_replies.deny_count),
    {Done, R2}.

-spec is_read_commuting(prbr:read_filter(), prbr:write_filter(), module()) -> boolean().
is_read_commuting(ReadFilter, HighestWriteFilterSeen, Datatype) ->
    %% A WF is considered commuting to a RF iff RF(v) =:= RF(WF(v)) for any v
    %% To decide if a read can be can be deliverd when seeing inconsistent
    %% write rounds, it is enough to check if the latest WF of the highest received
    %% reply does commute with the RF of the current read.
    %% Proof sketch:
    %% (v_x, wf_x -> value/write_filter of reply with write round x)
    %% Assume a set of replies with arbitrary write rounds. h* -> highest round recieved
    %% Assume wf_h* commutes with current RF (therefore it is not a write through)
    %% Assume knowing the previous write round in replica of h* -> let this round be c
    %% - There once was a consistent write quorum in round c
    %% - All replies with rounds smaller c can be ignored (a newer val was delivered)
    %% - For every round h greater c but smaller h* it can be shown:
    %%      - h was and will never be consistent quorum
    %%      - the previous write round in its replica was also c
    %%      - therefore wf_h(v_c) =:= v_h
    %%      - no read with RF rf delivered v_h if rf(v_c) =/= rf(v_h)
    case erlang:function_exported(Datatype, get_commuting_wf_for_rf, 1) of
        true ->
            CommutingWF = Datatype:get_commuting_wf_for_rf(ReadFilter),
            lists:member(HighestWriteFilterSeen, CommutingWF);
        false ->
            false
    end.

-spec inform_client(qread_done, entry(), pr:pr(), any()) -> ok.
inform_client(qread_done, Entry, WriteRound, ReadValue) ->
    comm:send_local(
      entry_client(Entry),
      {qread_done,
       entry_reqid(Entry),
       entry_my_round(Entry), %% here: round for client's next fast qwrite
       WriteRound, %% round which client must provide to ensure that full sequence of consensus is guaranteed
       ReadValue
      }).
-spec inform_client(qwrite_done, entry(), any()) -> ok.
inform_client(qwrite_done, Entry, WriteRet) ->
    comm:send_local(
      entry_client(Entry),
      {qwrite_done,
       entry_reqid(Entry),
       entry_my_round(Entry), %% here: round for client's next fast qwrite
       entry_write_val(Entry),
       WriteRet
      }).

%% @doc needs to be unique for this process in the whole system
-spec my_id() -> any().
my_id() ->
    %% TODO: use the id of the dht_node and later the current lease_id
    %% and epoch number which should be shorter on the wire than
    %% comm:this(). Changes in the node id or current lease and epoch
    %% have to be pushed to this process then.
    comm:this().

-spec tablename(state()) -> ?PDB:tableid().
tablename(State) -> element(1, State).
-spec db_selector(state()) -> dht_node_state:db_selector().
db_selector(State) -> element(2, State).
-spec period(state()) -> non_neg_integer().
period(State) -> element(3, State).
-spec set_period(state(), non_neg_integer()) -> state().
set_period(State, Val) -> setelement(3, State, Val).

-spec get_db_for_id(atom(), ?RT:key()) -> {atom(), pos_integer()}.
get_db_for_id(DBName, Key) ->
    {DBName, ?RT:get_key_segment(Key)}.


%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(read_attempts_without_progress).

