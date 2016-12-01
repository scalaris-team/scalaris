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
%% @doc    Allow a sequence of consensus using a prbr.
%% @end
%% @version $Id$
-module(rbrcseq).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%%-define(PDB, pdb_ets).
-define(PDB, pdb).
-define(REDUNDANCY, (config:read(redundancy_module))).

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

-export([start_link/3]).
-export([init/1, on/2]).

-type state() :: { ?PDB:tableid(),
                   dht_node_state:db_selector(),
                   non_neg_integer() %% period this process is in
                 }.

%% TODO: add support for *consistent* quorum by counting the number of
%% same-round-number replies

-type entry() :: {any(), %% ReqId
                  any(), %% debug field
                  non_neg_integer(), %% period of last retriggering / starting
                  non_neg_integer(), %% period of next retriggering
                  ?RT:key(), %% key
                  module(), %% data type
                  comm:erl_local_pid(), %% client
                  pr:pr(), %% my round
                  non_neg_integer(), %% number of acks
                  non_neg_integer(), %% number of denies
                  pr:pr(), %% highest accepted write round in replies
                  any(), %% value of highest seen round in replies
                  any(), %% filter (read) or tuple of filters (write)
                  non_neg_integer() %% number of newest replies
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
    comm:send_local(Pid, {qread, Client, Key, DataType, ReadFilter, _RetriggerAfter = 1})
    %% the process will reply to the client directly
    .

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
    comm:send_local(Pid, {qwrite, Client, Key,
                          DataType, {ReadFilter, ContentCheck, WriteFilter},
                          Value, _RetriggerAfter = 20}),
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
    comm:send_local(Pid, {qwrite_fast, Client, Key,
                          DataType, {ReadFilter, ContentCheck, WriteFilter},
                          Value, _RetriggerAfter = 20, Round, OldValue}),
    %% the process will reply to the client directly
    ok.

%% @doc spawns a rbrcseq, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), pid_groups:pidname(), dht_node_state:db_selector())
                -> {ok, pid()}.
start_link(DHTNodeGroup, Name, DBSelector) ->
    gen_component:start_link(
      ?MODULE, fun ?MODULE:on/2, DBSelector,
      [{pid_groups_join_as, DHTNodeGroup, Name}]).

-spec init(dht_node_state:db_selector()) -> state().
init(DBSelector) ->
    _ = case code:is_loaded(?REDUNDANCY) of
        false -> code:load_file(?REDUNDANCY);
        _ -> ok
    end,
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


%% normal qread step 1: preparation and starting read-phase
on({qread, Client, Key, DataType, ReadFilter, RetriggerAfter}, State) ->
    ?TRACE("rbrcseq:on qread, Client ~p~n", [Client]),
    %% assign new reqest-id; (also assign new ReqId when retriggering)

    %% if the caller process may handle more than one request at a
    %% time for the same key, the pids id has to be unique for each
    %% request to use the prbr correctly.
    ReqId = uid:get_pids_uid(),

    ?TRACE("rbrcseq:on qread ReqId ~p~n", [ReqId]),
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
    _ = [ begin
              %% let fill in whether lookup was consistent
              LookupEnvelope =
                  dht_node_lookup:envelope(
                    4,
                    {prbr, read, DB, '_', This, X, DataType, MyId, ReadFilter}),
              comm:send_local(Dest,
                              {?lookup_aux, X, 0,
                               LookupEnvelope})
          end
          || X <- ?REDUNDANCY:get_keys(Key) ],

    %% retriggering of the request is done via the periodic dictionary scan
    %% {next_period, ...}

    %% create local state for the request id
    Entry = entry_new_read(qread, ReqId, Key, DataType, Client, period(State),
                           ReadFilter, RetriggerAfter),
%%    log_entry(qread, Entry),
    %% store local state of the request
    set_entry(Entry, tablename(State)),
    State;

%% qread step 1 with explicit read round number
on({qread, Client, Key, DataType, ReadFilter, RetriggerAfter, ReadRound}, State) ->
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
    _ = [ begin
              %% let fill in whether lookup was consistent
              LookupEnvelope =
                  dht_node_lookup:envelope(
                    4,
                    {prbr, read, DB, '_', This, X, DataType, MyId, ReadFilter, pr:new(ReadRound, MyId)}),
              comm:send_local(Dest,
                              {?lookup_aux, X, 0,
                               LookupEnvelope})
          end
          || X <- ?REDUNDANCY:get_keys(Key) ],

    %% retriggering of the request is done via the periodic dictionary scan
    %% {next_period, ...}

    %% create local state for the request id
    Entry = entry_new_read(qread, ReqId, Key, DataType, Client, period(State),
                           ReadFilter, RetriggerAfter),
%%    log_entry(qread, Entry),
    %% store local state of the request
    set_entry(Entry, tablename(State)),
    State;

%% qread step 2: a replica replied to read from step 1
%%               when      majority reached
%%                  -> finish when consens is stable enough or
%%                  -> trigger write_through to stabilize an open consens
%%               otherwise just register the reply.
on({qread_collect,
    {read_reply, Cons, MyRwithId, Val, SeenWriteRound}}, State) ->
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
            case add_read_reply(Entry, db_selector(State), MyRwithId,
                                Val, SeenWriteRound, Cons) of
                %% {decided?, Entry}
                {false, NewEntry} ->
%%                    log_entry(qread_collect_false, NewEntry),
                    set_entry(NewEntry, tablename(State)),
                    State;
                {true, NewEntry} ->
%%                    log_entry(qread_collect_true, NewEntry),
                    trace_mpath:log_info(self(),
                                         {qread_done,
                                          readval, entry_val(NewEntry)}),
                    inform_client(qread_done, NewEntry),
                    ?PDB:delete(ReqId, tablename(State)),
                    State;
                {write_through, NewEntry} ->
%%                    log_entry(qread_collect_WT, NewEntry),
                    %% in case a consensus was started, but not yet finished,
                    %% we first have to finish it

                    trace_mpath:log_info(self(), {qread_write_through_necessary}),
%%                    log:log("Write through necessary, newest: ~p", [entry_val(NewEntry)]),
%%                    log_entry(qread_collect_write_through, NewEntry),
                    case randoms:rand_uniform(1,3) of
                        1 ->
                            %% delete entry, so outdated answers from minority
                            %% are not considered
                            ?PDB:delete(ReqId, tablename(State)),
                            gen_component:post_op({qread_initiate_write_through,
                                                   NewEntry}, State);
                        3 ->
                            %% delay a bit
                            _ = comm:send_local_after(
                                  15 + randoms:rand_uniform(1,10), self(),
                                  {qread_initiate_write_through, NewEntry}),
                            ?PDB:delete(ReqId, tablename(State)),
                            State;
                        2 ->
                            ?PDB:delete(ReqId, tablename(State)),
                            comm:send_local(self(), {qread_initiate_write_through,
                                                   NewEntry}),
                            State;
                        4 ->
                            ?PDB:delete(ReqId, tablename(State)),
                            %% retry read
                            gen_component:post_op({qread,
                                                   entry_client(NewEntry),
                                                   entry_key(NewEntry),
                                                   entry_datatype(NewEntry),
                                                   entry_filters(NewEntry),
                                                   entry_retrigger(NewEntry),
                                                   1+pr:get_r(entry_my_round(NewEntry))}, State);
                        5 ->
                            ?PDB:delete(ReqId, tablename(State)),
                            %% retry read
                            comm:send_local_after(15 + randoms:rand_uniform(1,10), self(),
                                                  {qread,
                                                   entry_client(NewEntry),
                                                   entry_key(NewEntry),
                                                   entry_datatype(NewEntry),
                                                   entry_filters(NewEntry),
                                                   entry_retrigger(NewEntry),
                                                   1+pr:get_r(entry_my_round(NewEntry))}),
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
            case add_read_deny(Entry, db_selector(State), MyRwithId,
                               LargerRound, Cons) of
                {false, NewEntry} ->
                    set_entry(NewEntry, tablename(State)),
                    State;
                {retry, NewEntry} ->
                    %%ct:pal("Retry with round (Was ID ~p)", [ReqId]),
                    ?PDB:delete(ReqId, tablename(State)),
                    %% retry read
                    gen_component:post_op({qread,
                                           entry_client(NewEntry),
                                           entry_key(NewEntry),
                                           entry_datatype(NewEntry),
                                           entry_filters(NewEntry),
                                           entry_retrigger(NewEntry),
                                           1+pr:get_r(entry_my_round(NewEntry))}, State)
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
            %% log:pal("Write through without filtering ~p.~n",
            %%        [entry_key(ReadEntry)]),
            This = comm:reply_as(
                     self(), 4,
                     {qread_write_through_done, ReadEntry, no_filtering, '_'}),

            ReqId = uid:get_pids_uid(),

            %% we only try to re-write a consensus in exactly this
            %% round without retrying, so having no content check is
            %% fine here
            {WTWF, WTUI, WTVal} = %% WT.. means WriteThrough here
                case pr:get_wf(entry_max_write_r(ReadEntry)) of
                    none ->
                        {fun prbr:noop_write_filter/3, none,
                         entry_val(ReadEntry)};
                    WTInfos ->
                        DataType = entry_datatype(ReadEntry),
                        %% Depending on the datatype the write through value might
                        %% not equal the read value e.g due to additionally
                        %% generated information or custom read/write handler
                        WTI = case erlang:function_exported(DataType,
                                                            get_write_through_value, 1) of
                                  true ->
                                        setelement(3, WTInfos,
                                                   DataType:get_write_through_value(
                                                     entry_val(ReadEntry)));
                                  _    -> WTInfos
                              end,
                        % WTInfo = write through infos
                        ?TRACE("Setting write through write filter ~p",
                               [WTI]),
                        WTI
                 end,
            Filters = {fun prbr:noop_read_filter/1,
                       fun(_,_,_) -> {true, none} end,
                       WTWF},

            Entry = entry_new_write(write_through, ReqId, entry_key(ReadEntry),
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
            _ = [ begin
                      %% let fill in whether lookup was consistent
                      LookupEnvelope =
                          dht_node_lookup:envelope(
                            4,
                            {prbr, write, DB, '_', Collector, K,
                             entry_datatype(ReadEntry),
                             entry_my_round(ReadEntry),
                             V,
                             WTUI,
                             WTWF, _IsWriteThrough = true}),
                      comm:send_local(Dest,
                                      {?lookup_aux, K, 0,
                                       LookupEnvelope})
                  end
                  || {K, V} <- lists:zip(Keys, WTVals) ],
            set_entry(Entry, tablename(State)),
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

            gen_component:post_op({qread, This, entry_key(ReadEntry), entry_datatype(ReadEntry),
               fun prbr:noop_read_filter/1,
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
            %% log:pal("Collect reply ~p ~p~n", [ReqId, Round]),
            case add_write_reply(Entry, Round, Cons) of
                {false, NewEntry} -> set_entry(NewEntry, tablename(State));
                {true, NewEntry} ->
                    ?TRACE("rbrcseq:on qread_write_through_collect infcl: ~p~n", [entry_client(Entry)]),
                    ReplyEntry = entry_set_my_round(NewEntry, NextRound),
                    inform_client(qwrite_done, ReplyEntry, WriteRet),
                    ?PDB:delete(ReqId, tablename(State))
            end
    end,
    State;

on({qread_write_through_collect, ReqId,
    {write_deny, Cons, Key, NewerRound}}, State) ->
    ?TRACE("rbrcseq:on qread_write_through_collect deny ~p~n", [ReqId]),
    TableName = tablename(State),
    Entry = get_entry(ReqId, TableName),
    _ = case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            %% log:pal("Collect deny ~p ~p~n", [ReqId, NewerRound]),
            ?TRACE("rbrcseq:on qread_write_through_collect deny Client: ~p~n", [entry_client(Entry)]),
            {Done, NewEntry} = add_write_deny(Entry, NewerRound, Cons),
            %% log:pal("#Denies = ~p, ~p~n", [entry_num_denies(NewEntry), Done]),
            case Done of
                false ->
                    set_entry(NewEntry, tablename(State)),
                    State;
                true ->
                    %% retry original read
                    ?PDB:delete(ReqId, TableName),

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
                                ?PDB:delete(entry_reqid(UnpackedEntry2),
                                            TableName),
                                {entry_client(UnpackedEntry2),
                                 entry_filters(UnpackedEntry2)};
                            _ ->
                                {UnpackedClient,
                                 entry_filters(UnpackedEntry)}
                        end,
                    gen_component:post_op({qread, Client, Key, entry_datatype(Entry), Filter,
                       entry_retrigger(Entry) - entry_period(Entry)},
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
    Key = entry_key(ReadEntry),
    DataType = entry_datatype(ReadEntry),
    ReadFilter = entry_filters(ReadEntry),
    RetriggerAfter = entry_retrigger(ReadEntry) - entry_period(ReadEntry),

    gen_component:post_op(
      {qread, Client, Key, DataType, ReadFilter, RetriggerAfter},
      State);

on({qread_write_through_done, ReadEntry, Filtering,
    {qread_done, _ReqId, Round, Val}}, State) ->
    ?TRACE("rbrcseq:on qread_write_through_done qread_done ~p ~p~n", [_ReqId, ReadEntry]),
    ClientVal =
        case Filtering of
            apply_filter -> F = entry_filters(ReadEntry), F(Val);
            _ -> Val
        end,
    TReplyEntry = entry_set_val(ReadEntry, ClientVal),
    ReplyEntry = entry_set_my_round(TReplyEntry, Round),
    %% log:pal("Write through of read done informing ~p~n", [ReplyEntry]),
    inform_client(qread_done, ReplyEntry),

    State;

%% normal qwrite step 1: preparation and starting read-phase
on({qwrite, Client, Key, DataType, Filters, WriteValue, RetriggerAfter}, State) ->
    ?TRACE("rbrcseq:on qwrite~n", []),
    %% assign new reqest-id
    ReqId = uid:get_pids_uid(),
    ?TRACE("rbrcseq:on qwrite c ~p uid ~p ~n", [Client, ReqId]),

    %% create local state for the request id, including used filters
    Entry = entry_new_write(qwrite, ReqId, Key, DataType, Client, period(State),
                            Filters, WriteValue, RetriggerAfter),

    This = comm:reply_as(self(), 3, {qwrite_read_done, ReqId, '_'}),
    set_entry(Entry, tablename(State)),
    gen_component:post_op({qread, This, Key, DataType, element(1, Filters), 1}, State);

%% qwrite step 2: qread is done, we trigger a quorum write in the given Round
on({qwrite_read_done, ReqId,
    {qread_done, _ReadId, Round, ReadValue}},
   State) ->
    ?TRACE("rbrcseq:on qwrite_read_done qread_done~n", []),
    gen_component:post_op({do_qwrite_fast, ReqId, Round, ReadValue}, State);

on({qwrite_fast, Client, Key, DataType, Filters = {_RF, _CC, _WF},
    WriteValue, RetriggerAfter, Round, ReadFilterResultValue}, State) ->

    %% create state and ReqId, store it and trigger 'do_qwrite_fast'
    %% which is also the write phase of a slow write.
        %% assign new reqest-id
    ReqId = uid:get_pids_uid(),
    ?TRACE("rbrcseq:on qwrite c ~p uid ~p ~n", [Client, ReqId]),

    %% create local state for the request id, including used filters
    Entry = entry_new_write(qwrite, ReqId, Key, DataType, Client, period(State),
                            Filters, WriteValue, RetriggerAfter),

    set_entry(Entry, tablename(State)),
    gen_component:post_op({do_qwrite_fast, ReqId, Round,
                                  ReadFilterResultValue}, State);

on({do_qwrite_fast, ReqId, Round, OldRFResultValue}, State) ->
    %% What if ReqId does no longer exist? Can that happen? How?
    %% a) Lets analyse the paths to do_qwrite_fast:
    %% b) Lets analyse when entries are removed from the database:
    Entry = get_entry(ReqId, tablename(State)),
    _ = case Entry of
        undefined ->
          %% drop actions for unknown requests, as they must be
          %% outdated. The retrigger mechanism may delete entries at
          %% any time, so we have to be prepared for that.
          State;
        _ ->
          NewEntry = setelement(2, Entry, do_qwrite_fast),
          ContentCheck = element(2, entry_filters(NewEntry)),
          WriteFilter = element(3, entry_filters(NewEntry)),
          WriteValue = entry_val(NewEntry),
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
                        {prbr, write, DB, '_', This, K, DataType, Round,
                        V, PassedToUpdate, WriteFilter, _IsWriteThrough = false}),
                    api_dht_raw:unreliable_lookup(K, LookupEnvelope)
                  end
                  || {K, V} <- lists:zip(Keys, WrVals)];
                {false, Reason} = _Err ->
                  %% ct:pal("Content Check failed: ~p~n", [Reason]),
                  %% own proposal not possible as of content check
                  comm:send_local(entry_client(NewEntry),
                            {qwrite_deny, ReqId, Round, OldRFResultValue,
                            {content_check_failed, Reason}}),
                  ?PDB:delete(ReqId, tablename(State))
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
            case add_write_reply(Entry, Round, Cons) of
                {false, NewEntry} -> set_entry(NewEntry, tablename(State));
                {true, NewEntry} ->
                    ReplyEntry = entry_set_my_round(NewEntry, NextRound),
                    trace_mpath:log_info(self(),
                                         {qwrite_done,
                                          value, entry_val(ReplyEntry)}),
                    inform_client(qwrite_done, ReplyEntry, WriteRet),
                    ?PDB:delete(ReqId, tablename(State))
            end
    end,
    State;

%% qwrite step 3: a replica replied to write from step 2
%%                when      majority reached, -> finish.
%%                otherwise just register the reply.
on({qwrite_collect, ReqId,
    {write_deny, Cons, _Key, NewerRound}}, State) ->
    ?TRACE("rbrcseq:on qwrite_collect write_deny~n", []),
    TableName = tablename(State),
    Entry = get_entry(ReqId, TableName),
    case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            case add_write_deny(Entry, NewerRound, Cons) of
                {false, NewEntry} -> set_entry(NewEntry, TableName),
                                     State;
                {true, NewEntry} ->
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
                                     true -> 3;
                                     false -> 4
                                 end,
                    case randoms:rand_uniform(1, UpperLimit) of
                        1 ->
                            retrigger(NewEntry, TableName, noincdelay),
                            %% delete of entry is done in retrigger!
                            State;
                        2 ->
                            NewReq = req_for_retrigger(NewEntry, noincdelay),
                            ?PDB:delete(element(1, NewEntry), TableName),
                            gen_component:post_op(NewReq, State);
                        3 ->
                            NewReq = req_for_retrigger(NewEntry, noincdelay),
                            %% TODO: maybe record number of retries
                            %% and make timespan chosen from
                            %% dynamically wider
                            _ = comm:send_local_after(
                                  10 + randoms:rand_uniform(1,90), self(),
                                  NewReq),
                            ?PDB:delete(element(1, NewEntry), TableName),
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
          || X <- ?PDB:tab2list(Table), is_tuple(X),
             14 =:= erlang:tuple_size(X), NewPeriod > element(4, X) ],

    %% re-trigger next next_period
    msg_delay:send_trigger(1, {next_period, NewPeriod + 1}),
    set_period(State, NewPeriod).

-spec req_for_retrigger(entry(), incdelay|noincdelay) ->
                               {qread,
                                Client :: comm:erl_local_pid(),
                                Key :: ?RT:key(),
                                DataType :: module(),
                                Filters :: any(),
                                Delay :: non_neg_integer()}
                               | {qwrite,
                                Client :: comm:erl_local_pid(),
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
    if is_tuple(element(13, Entry)) -> %% write request
           {qwrite, entry_client(Entry),
            entry_key(Entry), entry_datatype(Entry),
            entry_filters(Entry), entry_val(Entry),
            RetriggerDelay};
       true -> %% read request
           {qread, entry_client(Entry), entry_key(Entry),
            entry_datatype(Entry), entry_filters(Entry),
            RetriggerDelay}
    end.

-spec retrigger(entry(), ?PDB:tableid(), incdelay|noincdelay) -> ok.
retrigger(Entry, TableName, IncDelay) ->
    Request = req_for_retrigger(Entry, IncDelay),
    ?TRACE("Retrigger caused by timeout or concurrency for ~.0p~n", [Request]),
    comm:send_local(self(), Request),
    ?PDB:delete(element(1, Entry), TableName).

-spec get_entry(any(), ?PDB:tableid()) -> entry() | undefined.
get_entry(ReqId, TableName) ->
    ?PDB:get(ReqId, TableName).

-spec set_entry(entry(), ?PDB:tableid()) -> ok.
set_entry(NewEntry, TableName) ->
    ?PDB:set(NewEntry, TableName).

%% abstract data type to collect quorum read/write replies
-spec entry_new_read(any(), any(), ?RT:key(), module(),
                     comm:erl_local_pid(), non_neg_integer(), any(),
                     non_neg_integer())
                    -> entry().
entry_new_read(Debug, ReqId, Key, DataType, Client, Period, Filter, RetriggerAfter) ->
    {ReqId, Debug, Period, Period + RetriggerAfter + 20, Key, DataType, Client,
     _MyRound = pr:new(0, 0), _NumAcked = 0,
     _NumDenied = 0, _SeenWriteRound = pr:new(0, 0), _Val = empty_new_read_entry, Filter, 0}.

-spec entry_new_write(any(), any(), ?RT:key(), module(), comm:erl_local_pid(),
                      non_neg_integer(), tuple(), any(), non_neg_integer())
                     -> entry().
entry_new_write(Debug, ReqId, Key, DataType, Client, Period, Filters, Value, RetriggerAfter) ->
    {ReqId, Debug, Period, Period + RetriggerAfter, Key, DataType, Client,
     _MyRound = pr:new(0, 0), _NumAcked = 0, _NumDenied = 0, _SeenWriteRound = pr:new(0, 0), Value, Filters, 0}.

-spec entry_reqid(entry())        -> any().
entry_reqid(Entry)                -> element(1, Entry).
-spec entry_period(entry())       -> non_neg_integer().
entry_period(Entry)               -> element(3, Entry).
-spec entry_retrigger(entry())    -> non_neg_integer().
entry_retrigger(Entry)            -> element(4, Entry).
-spec entry_key(entry())          -> any().
entry_key(Entry)                  -> element(5, Entry).
-spec entry_datatype(entry())     -> module().
entry_datatype(Entry)             -> element(6, Entry).
-spec entry_client(entry())       -> comm:erl_local_pid().
entry_client(Entry)               -> element(7, Entry).
-spec entry_my_round(entry())     -> pr:pr().
entry_my_round(Entry)             -> element(8, Entry).
-spec entry_set_my_round(entry(), pr:pr()) -> entry().
entry_set_my_round(Entry, Round)  -> setelement(8, Entry, Round).
-spec entry_num_acks(entry())     -> non_neg_integer().
entry_num_acks(Entry)             -> element(9, Entry).
-spec entry_inc_num_acks(entry()) -> entry().
entry_inc_num_acks(Entry) -> setelement(9, Entry, element(9, Entry) + 1).
-spec entry_set_num_acks(entry(), non_neg_integer()) -> entry().
entry_set_num_acks(Entry, Num)    -> setelement(9, Entry, Num).
-spec entry_num_denies(entry())   -> non_neg_integer().
entry_num_denies(Entry)           -> element(10, Entry).
-spec entry_inc_num_denies(entry()) -> entry().
entry_inc_num_denies(Entry) -> setelement(10, Entry, element(10, Entry) + 1).
-spec entry_set_num_denies(entry(), non_neg_integer()) -> entry().
entry_set_num_denies(Entry, Val) -> setelement(10, Entry, Val).
-spec entry_max_write_r(entry())  -> pr:pr().
entry_max_write_r(Entry)          -> element(11, Entry).
-spec entry_set_max_write_r(entry(), pr:pr()) -> entry().
entry_set_max_write_r(Entry, Round) -> setelement(11, Entry, Round).
-spec entry_val(entry())           -> any().
entry_val(Entry)                   -> element(12, Entry).
-spec entry_set_val(entry(), any()) -> entry().
entry_set_val(Entry, Val)          -> setelement(12, Entry, Val).
-spec entry_filters(entry())       -> any().
entry_filters(Entry)               -> element(13, Entry).
-spec entry_set_num_newest(entry(), non_neg_integer())  -> entry().
entry_set_num_newest(Entry, Val)        -> setelement(14, Entry, Val).
-spec entry_inc_num_newest(entry()) -> entry().
entry_inc_num_newest(Entry)        -> setelement(14, Entry, 1 + element(14, Entry)).
-spec entry_num_newest(entry())    -> non_neg_integer().
entry_num_newest(Entry)            -> element(14, Entry).

-spec add_read_reply(entry(), dht_node_state:db_selector(),
                     pr:pr(), client_value(),
                     pr:pr(), Consistency::boolean())
                    -> {Done::boolean() | write_through, entry()}.
add_read_reply(Entry, _DBSelector, AssignedRound, Val, SeenWriteRound, _Cons) ->
    %% either decide on a majority of consistent replies, than we can
    %% just take the newest consistent value and do not need a
    %% write_through?

    %% Otherwise we decide on a consistent quorum (a majority agrees
    %% on the same version). We ensure this by write_through on odd
    %% cases.
    MaxWriteR = entry_max_write_r(Entry),
    %% extract write through info for round comparisons since
    %% they can be key-dependent if something different than
    %% replication is used for redundancy
    MaxWriteRNoWTInfo = pr:set_wf(MaxWriteR, none),
    SeenWriteRoundNoWTInfo = pr:set_wf(SeenWriteRound, none),
    E1 =
        if SeenWriteRoundNoWTInfo > MaxWriteRNoWTInfo ->
                T1 = entry_set_max_write_r(Entry, SeenWriteRound),
                T2 = entry_set_num_newest(T1, 1),
                NewVal = ?REDUNDANCY:collect_newer_read_value(entry_val(T2),
                                             Val, entry_datatype(Entry)),
                entry_set_val(T2, NewVal);
           SeenWriteRoundNoWTInfo =:= MaxWriteRNoWTInfo ->
                %% ?DBG_ASSERT2(Val =:= entry_val(Entry),
                %%    {collected_different_values_with_same_round,
                %%     Val, entry_val(Entry), proto_sched:get_infos()}),

                %% ?DBG_ASSERT2(Val =:= entry_val(Entry),
                %%    {collected_different_values_with_same_round,
                %%     Val, entry_val(Entry), proto_sched:get_infos()}),
                NewVal = ?REDUNDANCY:collect_read_value(entry_val(Entry),
                                             Val, entry_datatype(Entry)),
                T1 = entry_set_val(Entry, NewVal),
                entry_inc_num_newest(T1);
           true ->
               NewVal = ?REDUNDANCY:collect_older_read_value(entry_val(Entry),
                                            Val, entry_datatype(Entry)),
               entry_set_val(Entry, NewVal)
    end,
    MyRound = erlang:max(entry_my_round(E1), AssignedRound),
    E2 = case entry_num_acks(E1) of
             0 -> entry_set_my_round(E1, MyRound);
             1 -> entry_set_my_round(E1, MyRound);
             _ -> E1
         end,
    E3 = entry_inc_num_acks(E2),
    E3NumAcks = entry_num_acks(E3),
    E3RF = case entry_filters(E3) of
               {RF, _, _} -> RF;
               RF         -> RF
           end,
    case ?REDUNDANCY:quorum_accepted(entry_key(E3), E3NumAcks) of
        true ->
            %% we have majority of acks

            %% construct read value from replies
            Collected = entry_val(E3),
            Constructed = ?REDUNDANCY:get_read_value(Collected, E3RF),
            E4 = entry_set_val(E3, Constructed),

            Done = case entry_num_newest(E4) =:= E3NumAcks orelse
                            ?REDUNDANCY:skip_write_through(Constructed) of
                     true -> true; %% done
                     _ -> write_through
                   end,
%%% FS add read_retry as possibility

            {Done, E4};
        _ ->
            {false, E3}
    end.

add_read_deny(Entry, _DBSelector, MyRound, LargerRound, _Cons) ->
    {retry, entry_set_my_round(Entry, LargerRound)}.

-spec add_write_reply(entry(), pr:pr(), Consistency::boolean())
                     -> {Done::boolean(), entry()}.
add_write_reply(Entry, Round, _Cons) ->
    EntryMaxWriteR = entry_max_write_r(Entry),
    EntryRoundCmp = {pr:get_r(EntryMaxWriteR), pr:get_id(EntryMaxWriteR)},
    RoundCmp = {pr:get_r(Round), pr:get_id(Round)},
    E1 =
        case RoundCmp > EntryRoundCmp of
            false -> Entry;
            true ->
                %% this is the first reply with this round number.
                %% Older replies are (already) counted as denies, so
                %% we expect no accounted acks here.
%%                ?DBG_ASSERT2(entry_num_acks(Entry) =< 0,
%%                             {found_unexpected_acks, entry_num_acks(Entry)}),
                %% set rack and store newer round
                OldAcks = entry_num_acks(Entry),
                T1Entry = entry_set_max_write_r(Entry, Round),
                T2Entry = entry_set_num_acks(T1Entry, 0),
                _T3Entry = entry_set_num_denies(
                             T2Entry, OldAcks + entry_num_denies(T2Entry))
        end,
    E2 = entry_inc_num_acks(E1),
    Done = ?REDUNDANCY:quorum_accepted(entry_key(E2), entry_num_acks(E2)),
    {Done, E2}.

-spec add_write_deny(entry(), pr:pr(), Consistency::boolean())
                    -> {Done::boolean(), entry()}.
add_write_deny(Entry, Round, _Cons) ->
    EntryMaxWriteR = entry_max_write_r(Entry),
    EntryRoundCmp = {pr:get_r(EntryMaxWriteR), pr:get_id(EntryMaxWriteR)},
    RoundCmp = {pr:get_r(Round), pr:get_id(Round)},
    E1 =
        case RoundCmp > EntryRoundCmp of
            false -> Entry;
            true ->
                %% reset rack and store newer round
                OldAcks = entry_num_acks(Entry),
                T1Entry = entry_set_max_write_r(Entry, Round),
                T2Entry = entry_set_num_acks(T1Entry, 0),
                _T3Entry = entry_set_num_denies(
                             T2Entry, OldAcks + entry_num_denies(T2Entry))
        end,
    E2 = entry_inc_num_denies(E1),
    Done = ?REDUNDANCY:quorum_denied(entry_key(E2), entry_num_denies(E2)),
    {Done, E2}.

-spec inform_client(qread_done, entry()) -> ok.
inform_client(qread_done, Entry) ->
    comm:send_local(
      entry_client(Entry),
      {qread_done,
       entry_reqid(Entry),
       entry_my_round(Entry), %% here: round for client's next fast qwrite
       entry_val(Entry)
      }).

-spec inform_client(qwrite_done, entry(), any()) -> ok.
inform_client(qwrite_done, Entry, WriteRet) ->
    comm:send_local(
      entry_client(Entry),
      {qwrite_done,
       entry_reqid(Entry),
       entry_my_round(Entry), %% here: round for client's next fast qwrite
       entry_val(Entry),
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



log_entry(Prefix, Entry) ->
    ct:pal("################# ~.0p:~n"
           "ReqId           : ~p~n"
%%           "Debug           : ~p~n"
           "LastRetrigger   : ~p~n"
           "NextRetrigger   : ~p~n"
           "Key             : ~p~n"
           "Module          : ~p~n"
           "Client          : ~p~n"
           "MyRound         : ~p~n"
           "NumAcks         : ~p~n"
           "NumDenies       : ~p~n"
           "MaxWriteRound   : ~p~n"
           "MaxWRoundVal    : ~p~n"
           "Filter          : ~p~n"
           "NumNewestReplies: ~p~n"
          , [Prefix,
             entry_reqid(Entry),
             entry_period(Entry),
             entry_retrigger(Entry),
             entry_key(Entry),
             entry_datatype(Entry),
             entry_client(Entry),
             entry_my_round(Entry),
             entry_num_acks(Entry),
             entry_num_denies(Entry),
             entry_max_write_r(Entry),
             entry_val(Entry),
             entry_filters(Entry),
             entry_num_newest(Entry)
            ]).


