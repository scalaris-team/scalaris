% @copyright 2012-2013 Zuse Institute Berlin,

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

%%-define(TRACE(X,Y), log:pal("~p" X,[self()|Y])).
%%-define(TRACE(X,Y),
%%        Name = pid_groups:my_pidname(),
%%        case Name of
%%            kv_rbrcseq -> log:pal("~p ~p " X, [self(), Name | Y]);
%%            _ -> ok
%%        end).
-define(TRACE(X,Y), ok).
-define(TRACE_PERIOD(X,Y), ok).
-include("scalaris.hrl").
-include("client_types.hrl").

%% api:
-export([qread/3, qread/4]).
-export([qwrite/5, qwrite/7]).
-export([qwrite_fast/9]).

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
                  comm:erl_local_pid(), %% client
                  prbr:r_with_id(), %% my round
                  non_neg_integer(), %% number of acks
                  non_neg_integer(), %% number of denies
                  prbr:r_with_id(), %% highest seen round in replies
                  any(), %% value of highest seen round in replies
                  any(), %% filter (read) or tuple of filters (write)
                  non_neg_integer() %% number of newest replies
%%% Attention: There is a case that checks the size of this tuple below!!
                 }.

-type check_next_step() :: fun((term(), term()) -> term()).

-ifdef(with_export_type_support).
-export_type([check_next_step/0]).
-endif.

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
-spec qread(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key()) -> ok.
qread(CSeqPidName, Client, Key) ->
    RF = fun prbr:noop_read_filter/1,
    qread(CSeqPidName, Client, Key, RF).

-spec qread(pid_groups:pidname(), comm:erl_local_pid(), any(), prbr:read_filter()) -> ok.
qread(CSeqPidName, Client, Key, ReadFilter) ->
    Pid = pid_groups:find_a(CSeqPidName),
    comm:send_local(Pid, {qread, Client, Key, ReadFilter, _RetriggerAfter = 1})
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
             fun ((any(), any(), any()) -> any()), %% CC (Content Check)
             client_value()) -> ok.
qwrite(CSeqPidName, Client, Key, CC, Value) ->
    RF = fun prbr:noop_read_filter/1,
    WF = fun prbr:noop_write_filter/3,
    qwrite(CSeqPidName, Client, Key, RF, CC, WF, Value).

-spec qwrite(pid_groups:pidname(),
             comm:erl_local_pid(),
             ?RT:key(),
             fun ((any()) -> any()), %% read filter
             fun ((any(), any(), any()) -> any()), %% content check
             fun ((any(), any(), any()) -> any()), %% write filter
%%              %% select what you need to read for the operation
%%              fun ((CustomData) -> ReadInfo),
%%              %% is it an allowed follow up operation? and what info is
%%              %% needed to update outdated replicas (could be rather old)?
%%              fun ((CustomData, ReadInfo,
%%                    {fun ((ReadInfo, WriteValue) -> CustomData),
%%                     WriteValue}) -> {boolean(), PassedInfo}),
%%              %% update the db entry with the given infos, must
%%              %% generate a valid custom datatype
%%              fun ((PassedInfo, WriteValue) -> CustomData),
%%              %%module(),
             client_value()) -> ok.
qwrite(CSeqPidName, Client, Key, ReadFilter, ContentCheck,
       WriteFilter, Value) ->
    Pid = pid_groups:find_a(CSeqPidName),
    comm:send_local(Pid, {qwrite, Client,
                          Key, {ReadFilter, ContentCheck, WriteFilter},
                          Value, _RetriggerAfter = 2}),
    %% the process will reply to the client directly
    ok.

-spec qwrite_fast(pid_groups:pidname(),
             comm:erl_local_pid(),
             ?RT:key(),
             fun ((any()) -> any()), %% read filter
             fun ((any(), any(), any()) -> any()), %% content check
             fun ((any(), any(), any()) -> any()), %% write filter
             client_value(), prbr:r_with_id(), client_value() | prbr_bottom)
            -> ok.
qwrite_fast(CSeqPidName, Client, Key, ReadFilter, ContentCheck,
            WriteFilter, Value, Round, OldValue) ->
    Pid = pid_groups:find_a(CSeqPidName),
    comm:send_local(Pid, {qwrite_fast, Client,
                          Key, {ReadFilter, ContentCheck, WriteFilter},
                          Value, _RetriggerAfter = 2, Round, OldValue}),
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
    msg_delay:send_local(1, self(), {next_period, 1}),
    {?PDB:new(?MODULE, [set, protected]), DBSelector, 0}.

-spec on(comm:message(), state()) -> state().
%% ; ({qread, any(), client_key(), fun ((any()) -> any())},
%% state()) -> state().


%% normal qread step 1: preparation and starting read-phase
on({qread, Client, Key, ReadFilter, RetriggerAfter}, State) ->
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
    %% for the same key from the same process, which may happen
    %% later: retrieve the request id from the assigned round number
    %% to get the entry from the pdb
    MyId = {my_id(), ReqId},
    Dest = pid_groups:find_a(dht_node),
    DB = db_selector(State),
    _ = [ comm:send_local(Dest,
                          {?lookup_aux, X, 0,
                           {prbr, read, DB, This, X, MyId, ReadFilter}})
          || X <- ?RT:get_replica_keys(Key) ],

    %% retriggering of the request is done via the periodic dictionary scan
    %% {next_period, ...}

    %% create local state for the request id
    Entry = entry_new_read(qread, ReqId, Key, Client, period(State),
                           ReadFilter, RetriggerAfter),
    %% store local state of the request
    set_entry(Entry, tablename(State)),
    State;

%% qread step 2: a replica replied to read from step 1
%%               when      majority reached
%%                  -> finish when consens is stable enough or
%%                  -> trigger write_through to stabilize an open consens
%%               otherwise just register the reply.
on({qread_collect,
    {read_reply, MyRwithId, Val, AckRound}}, State) ->
    ?TRACE("rbrcseq:on qread_collect read_reply MyRwithId: ~p~n", [MyRwithId]),
    %% collect a majority of answers and select that one with the highest
    %% round number.
    {_Round, ReqId} = prbr:r_with_id_get_id(MyRwithId),
    Entry = get_entry(ReqId, tablename(State)),
    _ = case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            {Done, NewEntry} = add_read_reply(Entry, MyRwithId, Val, AckRound),
            case Done of
                false ->
                    set_entry(NewEntry, tablename(State)),
                    State;
                true ->
                    inform_client(qread_done, NewEntry),
                    ?PDB:delete(ReqId, tablename(State)),
                    State;
                write_through ->
                    %% in case a consensus was started, but not yet finished,
                    %% we first have to finish it

                    %% delete entry, so outdated answers from minority
                    %% are not considered
                    ?PDB:delete(ReqId, tablename(State)),

                    gen_component:post_op(State,
                      {qread_initiate_write_through, NewEntry})
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
            Filters = {fun prbr:noop_read_filter/1,
                       fun(_,_,_) -> {true, null} end,
                       fun prbr:noop_write_filter/3},

            Entry = entry_new_write(write_through, ReqId, entry_key(ReadEntry),
                                    This,
                                    period(State),
                                    Filters, entry_val(ReadEntry),
                                    entry_retrigger(ReadEntry)
                                    - entry_period(ReadEntry)),

            Collector = comm:reply_as(
                          comm:this(), 3,
                          {qread_write_through_collect, ReqId, '_'}),

            Dest = pid_groups:find_a(dht_node),
            DB = db_selector(State),
            _ = [ comm:send_local(Dest,
                                  {?lookup_aux, X, 0,
                                   {prbr, write, DB, Collector, X,
                                    entry_my_round(ReadEntry),
                                    entry_val(ReadEntry),
                                    null,
                                    fun prbr:noop_write_filter/3}})
                  || X <- ?RT:get_replica_keys(entry_key(Entry)) ],
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

            gen_component:post_op(State,
              {qread, This, entry_key(ReadEntry),
               fun prbr:noop_read_filter/1,
               entry_retrigger(ReadEntry) - entry_period(ReadEntry)})
    end;

on({qread_write_through_collect, ReqId,
    {write_reply, _Key, Round, _NextRound}}, State) ->
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
            {Done, NewEntry} = add_write_reply(Entry, Round),
            case Done of
                false -> set_entry(NewEntry, tablename(State));
                true ->
                    ?TRACE("rbrcseq:on qread_write_through_collect infcl: ~p~n", [entry_client(Entry)]),
                    inform_client(qwrite_done, NewEntry),
                    ?PDB:delete(ReqId, tablename(State))
            end
    end,
    State;

on({qread_write_through_collect, ReqId,
    {write_deny, Key, NewerRound}}, State) ->
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
            {Done, NewEntry} = add_write_deny(Entry, NewerRound),
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
                    gen_component:post_op(State,
                      {qread, Client, Key, Filter,
                       entry_retrigger(Entry) - entry_period(Entry)})
            end
    end;

on({qread_write_through_done, ReadEntry, Filtering,
    {qwrite_done, _ReqId, Round, Val}}, State) ->
    ?TRACE("rbrcseq:on qread_write_through_done qwrite_done ~p ~p~n", [_ReqId, ReadEntry]),
    ClientVal =
        case Filtering of
            apply_filter -> F = entry_filters(ReadEntry), F(Val);
            _ -> Val
        end,
    TReplyEntry = entry_set_val(ReadEntry, ClientVal),
    ReplyEntry = entry_set_my_round(TReplyEntry, Round),
    %% log:pal("Write through of write request done informing ~p~n", [ReplyEntry]),
    inform_client(qread_done, ReplyEntry),
    State;

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
on({qwrite, Client, Key, Filters, Value, RetriggerAfter}, State) ->
    ?TRACE("rbrcseq:on qwrite~n", []),
    %% assign new reqest-id
    ReqId = uid:get_pids_uid(),
    ?TRACE("rbrcseq:on qwrite c ~p uid ~p ~n", [Client, ReqId]),

    %% create local state for the request id, including used filters
    Entry = entry_new_write(qwrite, ReqId, Key, Client, period(State),
                            Filters, Value, RetriggerAfter),

    This = comm:reply_as(self(), 3, {qwrite_read_done, ReqId, '_'}),
    set_entry(Entry, tablename(State)),
    gen_component:post_op(State, {qread, This, Key, element(1, Filters), 1});

%% qwrite step 2: qread is done, we trigger a quorum write in the given Round
on({qwrite_read_done, ReqId,
    {qread_done, _ReadId, Round, ReadValue}},
   State) ->
    ?TRACE("rbrcseq:on qwrite_read_done qread_done~n", []),
    gen_component:post_op(State, {do_qwrite_fast, ReqId, Round, ReadValue});

on({qwrite_fast, Client, Key, Filters = {_RF, _CC, _WF},
    WriteValue, RetriggerAfter, Round, OldValue}, State) ->

    %% create state and ReqId, store it and trigger 'do_qwrite_fast'
    %% which is also the write phase of a slow write.
        %% assign new reqest-id
    ReqId = uid:get_pids_uid(),
    ?TRACE("rbrcseq:on qwrite c ~p uid ~p ~n", [Client, ReqId]),

    %% create local state for the request id, including used filters
    Entry = entry_new_write(qwrite, ReqId, Key, Client, period(State),
                            Filters, WriteValue, RetriggerAfter),

    set_entry(Entry, tablename(State)),
    gen_component:post_op(State, {do_qwrite_fast, ReqId, Round, OldValue});

on({do_qwrite_fast, ReqId, Round, OldValue}, State) ->
    Entry = setelement(2, get_entry(ReqId, tablename(State)), do_qwrite_fast),
    ContentCheck = element(2, entry_filters(Entry)),
    WriteFilter = element(3, entry_filters(Entry)),
    WriteValue = entry_val(Entry),

    _ = case ContentCheck(OldValue, WriteFilter, WriteValue) of
        {true, PassedToUpdate} ->
            %% own proposal possible as next instance in the consens sequence
            This = comm:reply_as(comm:this(), 3, {qwrite_collect, ReqId, '_'}),
            Dest = pid_groups:find_a(dht_node),
            DB = db_selector(State),
            [ comm:send_local(Dest,
                              {?lookup_aux, X, 0,
                               {prbr, write, DB, This, X, Round,
                                WriteValue,
                                PassedToUpdate,
                                WriteFilter}})
              || X <- ?RT:get_replica_keys(entry_key(Entry)) ];
        {false, Reason} = _Err ->
            %% own proposal not possible as of content check
            comm:send_local(entry_client(Entry),
                            {qwrite_deny, ReqId, Round, OldValue,
                             {content_check_failed, Reason}}),
            ?PDB:delete(ReqId, tablename(State))
    end,
    State;

%% qwrite step 3: a replica replied to write from step 2
%%                when      majority reached, -> finish.
%%                otherwise just register the reply.
on({qwrite_collect, ReqId,
    {write_reply, _Key, Round, _NextRound}}, State) ->
    ?TRACE("rbrcseq:on qwrite_collect write_reply~n", []),
    Entry = get_entry(ReqId, tablename(State)),
    _ = case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            {Done, NewEntry} = add_write_reply(Entry, Round),
            case Done of
                false -> set_entry(NewEntry, tablename(State));
                true ->
                    inform_client(qwrite_done, NewEntry),
                    ?PDB:delete(ReqId, tablename(State))
            end
    end,
    State;

%% qwrite step 3: a replica replied to write from step 2
%%                when      majority reached, -> finish.
%%                otherwise just register the reply.
on({qwrite_collect, ReqId,
    {write_deny, _Key, NewerRound}}, State) ->
    ?TRACE("rbrcseq:on qwrite_collect write_deny~n", []),
    TableName = tablename(State),
    Entry = get_entry(ReqId, TableName),
    case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            {Done, NewEntry} = add_write_deny(Entry, NewerRound),
            case Done of
                false -> set_entry(NewEntry, TableName),
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
                    case randoms:rand_uniform(1,4) of
                        1 ->
                            retrigger(NewEntry, TableName, noincdelay),
                            %% delete of entry is done in retrigger!
                            State;
                        2 ->
                            NewReq = req_for_retrigger(NewEntry, noincdelay),
                            %% TODO: maybe record number of retries
                            %% and make timespan chosen from
                            %% dynamically wider
                            comm:send_local_after(
                              10 + randoms:rand_uniform(1,90), self(),
                              NewReq),
                            ?PDB:delete(element(1, NewEntry), TableName),
                            State;
                        3 ->
                            NewReq = req_for_retrigger(NewEntry, noincdelay),
                            ?PDB:delete(element(1, NewEntry), TableName),
                            gen_component:post_op(State, NewReq)
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
    _ = [ begin
              log:pal("Retrigger in period ~p caused by timeout for ~.0p~n",
                     [NewPeriod, X]),
              retrigger(X, Table, incdelay)
          end
          || X <- ?PDB:tab2list(Table), is_tuple(X),
             13 =:= erlang:size(X), NewPeriod > element(4, X) ],

    %% re-trigger next next_period
    msg_delay:send_local(1, self(), {next_period, NewPeriod + 1}),
    set_period(State, NewPeriod).

-spec req_for_retrigger(entry(), incdelay|noincdelay) ->
                               {qread,
                                Client :: comm:erl_local_pid(),
                                Key :: ?RT:key(),
                                Filters :: any(),
                                Delay :: non_neg_integer()}
                               | {qwrite,
                                Client :: comm:erl_local_pid(),
                                Key :: ?RT:key(),
                                Filters :: any(),
                                Val :: any(),
                                Delay :: non_neg_integer()}.
req_for_retrigger(Entry, IncDelay) ->
    RetriggerDelay = case IncDelay of
                         incdelay -> erlang:max(1, (entry_retrigger(Entry) - entry_period(Entry)) + 1);
                         noincdelay -> entry_retrigger(Entry)
                     end,
    case erlang:size(Entry) of
        13 when is_tuple(element(12, Entry)) -> %% write request
            {qwrite, entry_client(Entry),
             entry_key(Entry), entry_filters(Entry),
             entry_val(Entry),
             RetriggerDelay};
        13 -> %% read request
            {qread, entry_client(Entry),
             entry_key(Entry), entry_filters(Entry),
             RetriggerDelay}
    end.

-spec retrigger(entry(), ?PDB:tableid(), incdelay|noincdelay) -> ok.
retrigger(Entry, TableName, IncDelay) ->
    Request = req_for_retrigger(Entry, IncDelay),
    comm:send_local(self(), Request),
    ?PDB:delete(element(1, Entry), TableName).

-spec get_entry(any(), ?PDB:tableid()) -> entry() | undefined.
get_entry(ReqId, TableName) ->
    ?PDB:get(ReqId, TableName).

-spec set_entry(entry(), ?PDB:tableid()) -> ok.
set_entry(NewEntry, TableName) ->
    ?PDB:set(NewEntry, TableName).

%% abstract data type to collect quorum read/write replies
-spec entry_new_read(any(), any(), ?RT:key(),
                     comm:erl_local_pid(), non_neg_integer(), any(),
                     non_neg_integer())
                    -> entry().
entry_new_read(Debug, ReqId, Key, Client, Period, Filter, RetriggerAfter) ->
    {ReqId, Debug, Period, Period + RetriggerAfter, Key, Client,
     _MyRound = {0, 0}, _NumAcked = 0,
     _NumDenied = 0, _AckRound = {0, 0}, _AckVal = 0, Filter, 0}.

-spec entry_new_write(any(), any(), ?RT:key(), comm:erl_local_pid(),
                      non_neg_integer(), tuple(), any(), non_neg_integer())
                     -> entry().
entry_new_write(Debug, ReqId, Key, Client, Period, Filters, Value, RetriggerAfter) ->
    {ReqId, Debug, Period, Period + RetriggerAfter, Key, Client,
     _MyRound = {0, 0}, _NumAcked = 0, _NumDenied = 0, _AckRound = {0, 0}, Value, Filters, 0}.

-spec entry_reqid(entry())        -> any().
entry_reqid(Entry)                -> element(1, Entry).
-spec entry_period(entry())       -> non_neg_integer().
entry_period(Entry)               -> element(3, Entry).
-spec entry_retrigger(entry())    -> non_neg_integer().
entry_retrigger(Entry)            -> element(4, Entry).
-spec entry_key(entry())          -> any().
entry_key(Entry)                  -> element(5, Entry).
-spec entry_client(entry())       -> comm:erl_local_pid().
entry_client(Entry)               -> element(6, Entry).
-spec entry_my_round(entry())     -> prbr:r_with_id().
entry_my_round(Entry)             -> element(7, Entry).
-spec entry_set_my_round(entry(), prbr:r_with_id()) -> entry().
entry_set_my_round(Entry, Round)  -> setelement(7, Entry, Round).
-spec entry_num_acks(entry())     -> non_neg_integer().
entry_num_acks(Entry)             -> element(8, Entry).
-spec entry_inc_num_acks(entry()) -> entry().
entry_inc_num_acks(Entry) -> setelement(8, Entry, element(8, Entry) + 1).
-spec entry_set_num_acks(entry(), non_neg_integer()) -> entry().
entry_set_num_acks(Entry, Num)    -> setelement(8, Entry, Num).
-spec entry_num_denies(entry())   -> non_neg_integer().
entry_num_denies(Entry)           -> element(9, Entry).
-spec entry_inc_num_denies(entry()) -> entry().
entry_inc_num_denies(Entry) -> setelement(9, Entry, element(9, Entry) + 1).
-spec entry_set_num_denies(entry(), non_neg_integer()) -> entry().
entry_set_num_denies(Entry, Val) -> setelement(9, Entry, Val).
-spec entry_latest_seen(entry())  -> prbr:r_with_id().
entry_latest_seen(Entry)          -> element(10, Entry).
-spec entry_set_latest_seen(entry(), prbr:r_with_id()) -> entry().
entry_set_latest_seen(Entry, Round) -> setelement(10, Entry, Round).
-spec entry_val(entry())           -> any().
entry_val(Entry)                   -> element(11, Entry).
-spec entry_set_val(entry(), any()) -> entry().
entry_set_val(Entry, Val)          -> setelement(11, Entry, Val).
-spec entry_filters(entry())       -> any().
entry_filters(Entry)               -> element(12, Entry).
-spec entry_set_num_newest(entry(), non_neg_integer())  -> entry().
entry_set_num_newest(Entry, Val)        -> setelement(13, Entry, Val).
-spec entry_inc_num_newest(entry()) -> entry().
entry_inc_num_newest(Entry)        -> setelement(13, Entry, 1 + element(13, Entry)).
-spec entry_num_newest(entry())    -> non_neg_integer().
entry_num_newest(Entry)            -> element(13, Entry).

-spec add_read_reply(entry(), prbr:r_with_id(), client_value(), prbr:r_with_id()) -> {boolean() | write_through, entry()}.
add_read_reply(Entry, AssignedRound, Val, AckRound) ->
    LatestSeen = entry_latest_seen(Entry),
    TmpEntry =
        if AckRound > LatestSeen ->
                T1 = entry_set_latest_seen(Entry, AckRound),
                T2 = entry_set_num_newest(T1, 1),
                entry_set_val(T2, Val);
           AckRound =:= LatestSeen ->
                entry_inc_num_newest(Entry);
           true -> Entry
    end,
    MyRound = erlang:max(entry_my_round(TmpEntry), AssignedRound),
    E2 = entry_set_my_round(TmpEntry, MyRound),
    {case 3 =< 1+entry_num_acks(E2) of
         true ->
             case entry_num_newest(E2) of
                 1 -> write_through;
                 _ -> true
             end;
         false -> false
     end,
     entry_inc_num_acks(E2)}.

-spec add_write_reply(entry(), prbr:r_with_id()) -> {boolean(), entry()}.
add_write_reply(Entry, Round) ->
    E1 =
        case Round > entry_latest_seen(Entry) of
            false -> Entry;
            true ->
                %% have to count the dropped acks as denies.
                case entry_num_acks(Entry) > 0 of
                    true ->
                        log:pal("This should not happen a~n");
                    false -> ok
                end,
                %% reset rack and store newer round
                T1Entry = entry_set_latest_seen(Entry, Round),
                _T2Entry = entry_set_num_acks(T1Entry, 0)
    end,
    {3 =< (1+entry_num_acks(E1)), entry_inc_num_acks(E1)}.

-spec add_write_deny(entry(), prbr:r_with_id()) -> {boolean(), entry()}.
add_write_deny(Entry, Round) ->
    E1 =
        case Round > entry_latest_seen(Entry) of
            false -> Entry;
            true ->
                %% reset rack and store newer round
                OldAcks = entry_num_acks(Entry),
                T1Entry = entry_set_latest_seen(Entry, Round),
                T2Entry = entry_set_num_acks(T1Entry, 0),
                _T3Entry = entry_set_num_denies(
                             T2Entry, OldAcks + entry_num_denies(T2Entry))
    end,
    {2 =< (1+entry_num_denies(E1)), entry_inc_num_denies(E1)}.


-spec inform_client(qread_done | qwrite_done, entry()) -> ok.
inform_client(Tag, Entry) ->
    comm:send_local(entry_client(Entry),
                    {Tag,
                     entry_reqid(Entry),
                     entry_my_round(Entry),
                     entry_val(Entry)
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
