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

%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    Paxos register for CRDT's. Implements the role of proposer
%% @end
-module(crdt_proposer).
-author('skrzypczak.de').

-define(PDB, pdb).
%-define(TRACE(X,Y), ct:pal(X,Y)).
-define(TRACE(X,Y), ok).

-define(ROUTING_DISABLED, false).
-define(READ_BATCHING_INTERVAL, (config:read(read_batching_interval))).
-define(READ_BATCHING_INTERVAL_DIVERGENCE, 2).
-define(WRITE_BATCHING_INTERVAL, (config:read(write_batching_interval))).
-define(WRITE_BATCHING_INTERVAL_DIVERGENCE, 2).

-include("scalaris.hrl").

-behaviour(gen_component).

-export([write/5, write_eventual/5]).
-export([read/5, read_eventual/5]).

-export([check_config/0]).
-export([start_link/3]).
-export([init/1, on/2]).

-type state() :: { ?PDB:tableid(),
                   dht_node_state:db_selector(),
                   non_neg_integer(), %% period this process is in
                   boolean(),
                   boolean()
                 }.

-record(r_replies,  {
                        reply_count = 0 :: non_neg_integer(),
                        highest_replies = 0 :: non_neg_integer(),
                        highest_seen_round :: pr:pr(),
                        cons_value = true :: boolean(),
                        value :: crdt:crdt()
                    }).

-record(w_replies, {reply_count = 0 :: non_neg_integer()}).

-type entry() :: {any(),        %% request id
                  comm:erl_local_pid(), %% client
                  ?RT:key(),    %% key
                  crdt:crdt_module(),     %% data type
                  crdt:query_fun() | crdt:update_fun(), %% fun used to read/write crdt state
                  replies(),     %% aggregates replies
                  non_neg_integer() %% round trips executed
                 }.

-type batch_entry() :: {read_batch_entry | write_batch_entry,
                        non_neg_integer(),
                        dict:dict(?RT:key(), [{comm:erl_local_pid(), crdt:crdt_module(),
                                               crdt:update_fun() | crdt:query_fun()}])
                       }.

-type replies() :: #w_replies{} | #r_replies{}.

-include("gen_component.hrl").


-spec start_link(pid_groups:groupname(), pid_groups:pidname(), dht_node_state:db_selector())
                -> {ok, pid()}.
start_link(DHTNodeGroup, Name, DBSelector) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, DBSelector,
                             [{pid_groups_join_as, DHTNodeGroup, Name}]).

-spec init(dht_node_state:db_selector()) -> state().
init(DBSelector) ->
    {?PDB:new(?MODULE, [set]), DBSelector, 0, false, false}.


%%%% API

-spec read(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:query_fun()) -> ok.
read(CSeqPidName, Client, Key, DataType, QueryFun) ->
    case read_batching_enabled() of
        true ->
            start_request(CSeqPidName, {add_to_read_batch, Client, Key, DataType, QueryFun});
        false ->
            start_request(CSeqPidName, {req_start, {read, strong, Client, Key, DataType, QueryFun, none, 0}})
    end.

-spec read_eventual(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:query_fun()) -> ok.
read_eventual(CSeqPidName, Client, Key, DataType, QueryFun) ->
    start_request(CSeqPidName, {req_start, {read, eventual, Client, Key, DataType, QueryFun}}).

-spec write(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:update_fun()) -> ok.
write(CSeqPidName, Client, Key, DataType, UpdateFun) ->
    case write_batching_enabled() of
        true ->
            start_request(CSeqPidName, {add_to_write_batch, Client, Key, DataType, UpdateFun});
        false ->
            start_request(CSeqPidName, {req_start, {write, strong, Client, Key, DataType, UpdateFun}})
    end.

-spec write_eventual(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:update_fun()) -> ok.
write_eventual(CSeqPidName, Client, Key, DataType, UpdateFun) ->
    start_request(CSeqPidName, {req_start, {write, eventual, Client, Key, DataType, UpdateFun}}).

-spec start_request(pid_groups:pidname(), comm:message()) -> ok.
start_request(CSeqPidName, Msg) ->
    Pid = pid_groups:find_a(CSeqPidName),
    trace_mpath:log_info(self(), {start_request, request, Msg}),
    comm:send_local(Pid, Msg).


-spec on(comm:message(), state()) -> state().

%%%%% batching loops
on({add_to_read_batch, Client, Key, DataType, QueryFun}, State) ->
    {Id, Count, Reqs} =
        case get_entry(read_batch_entry, tablename(State)) of
            undefined ->
                {read_batch_entry, 0, dict:new()};
            Batch -> Batch
        end,
    ?TRACE("Add to batch ~p",  [{Key, DataType, QueryFun, Count}]),
    NewReqs = dict:append(Key, {Client, DataType, QueryFun}, Reqs),
    _ = save_entry({Id, Count+1, NewReqs}, tablename(State)),

    set_read_batch_in_progress(State);

on({read_batch_trigger}, State) ->
    {Id, Count, Reqs} =
        case get_entry(read_batch_entry, tablename(State)) of
            undefined ->
                {read_batch_entry, 0, dict:new()};
            Batch -> Batch
        end,

    case Count of
        0 -> ok; % no reqs queued
        Count ->
            ?TRACE("Processing batch ... ~p", [{Id, Count, Reqs}]),
            _ = dict:map(
                fun(K, ReqsForThisKey=[{_, DataType, _}|_]) ->
                    This = comm:reply_as(comm:this(), 3, {read_batch_reply, ReqsForThisKey, '_'}),
                    comm:send_local(self(),
                        {req_start, {read, strong, This, K, DataType, fun crdt:query_noop/1, none, 0}})
                end, Reqs),
            _ = save_entry({Id, 0, dict:new()}, tablename(State))
    end,

    set_read_batch_done(State);

on({read_batch_reply, Reqs, {read_done, CRDTState}}, State) ->
    ?TRACE("Batch reply ... ~p", [{Reqs, CRDTState}]),
    _ = [begin
            ReturnVal = DataType:apply_query(QueryFun, CRDTState),
            comm:send_local(Client, {read_done, ReturnVal})
         end || {Client, DataType, QueryFun} <- Reqs],
    State;

on({add_to_write_batch, Client, Key, DataType, UpdateFun}, State) ->
    {Id, Count, Reqs} =
        case get_entry(write_batch_entry, tablename(State)) of
            undefined ->
                {write_batch_entry, 0, dict:new()};
            Batch -> Batch
        end,
    ?TRACE("Add to write batch ~p",  [{Key, DataType, UpdateFun, Count}]),
    NewReqs = dict:append(Key, {Client, DataType, UpdateFun}, Reqs),
    _ = save_entry({Id, Count+1, NewReqs}, tablename(State)),

    set_write_batch_in_progress(State);

on({write_batch_trigger}, State) ->
    {Id, Count, Reqs} =
        case get_entry(write_batch_entry, tablename(State)) of
            undefined ->
                {write_batch_entry, 0, dict:new()};
            Batch -> Batch
        end,

    case Count of
        0 -> ok; % no reqs queued
        Count ->
            ?TRACE("Processing batch ... ~p", [{Id, Count, Reqs}]),
            _ = dict:map(
                fun(K, ReqsForThisKey=[{_, DataType, _}|_]) ->
                    This = comm:reply_as(comm:this(), 3, {write_batch_reply, ReqsForThisKey, '_'}),
                    UpdateFuns = [U || {_Client, _DataType, U} <- ReqsForThisKey],
                    CompositeFun = fun(RepId, Crdt) ->
                                        lists:foldl(fun(UF, TCrdt) ->
                                                        DataType:apply_update(UF, RepId, TCrdt)
                                                    end, Crdt, UpdateFuns)
                                   end,
                    comm:send_local(self(),
                        {req_start, {write, strong, This, K, DataType, CompositeFun}})
                end, Reqs),
            _ = save_entry({Id, 0, dict:new()}, tablename(State))
    end,

    set_write_batch_done(State);

on({write_batch_reply, Reqs, {write_done}}, State) ->
    ?TRACE("Batch reply ... ~p", [{Reqs}]),
    _ = [begin
            comm:send_local(Client, {write_done})
         end || {Client, _DataType, _UpdateFun} <- Reqs],
    State;


%%%%% strong consistent read

on({req_start, {read, strong, Client, Key, DataType, QueryFun, PreviousRound, PreviousRoundTrips}}, State) ->
    ReqId = uid:get_pids_uid(),
    Entry = entry_new_read(ReqId, Client, Key, DataType, QueryFun, DataType:new()),

    Round = case PreviousRound of
                none ->
                    {inc, ReqId};
                _ ->
                    round_inc(PreviousRound, ReqId)
            end,

    This = comm:reply_as(comm:this(), 3, {read, strong, '_'}),
    Msg = {crdt_acceptor, prepare, '_', This, ReqId, key, DataType, Round, DataType:new()},

    NewEntry = entry_set_round_trips(Entry, PreviousRoundTrips + 1),
    save_entry(NewEntry, tablename(State)),
    send_to_all_replicas(Key, Msg),

    State;

on({read, strong, {prepare_reply, ReqId, UsedReadRound, WriteRound, CVal}}, State) ->
    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                %% ignore replies for unknown requests (i.e. because we already
                %% have processed them)
                State;
            Entry ->
                Replies = entry_replies(Entry),
                {Done, NewReplies} = add_read_reply(Replies, UsedReadRound, WriteRound,
                                                    CVal, entry_datatype(Entry)),
                NewEntry = entry_set_replies(Entry, NewReplies),

                case Done of
                    false ->
                        save_entry(NewEntry, tablename(State)),
                        State;
                    cons_read ->
                        %% value is already established in a quorum, skip 2. phase
                        QueryFun = entry_fun(NewEntry),
                        Type = entry_datatype(NewEntry),
                        ReturnVal = Type:apply_query(QueryFun, NewReplies#r_replies.value),
                        trace_mpath:log_info(self(), {read_strong_cons_done,
                                                      crdt_value, NewReplies#r_replies.value,
                                                      return_value, ReturnVal}),
                        inform_client(read_done, Entry, ReturnVal),
                        delete_entry(Entry, tablename(State)),
                        State;
                    retry_read ->
                        %% we received inconsistent read rounds... thus we have
                        %% concurrency and must retry to get consistent rounds accepts
                        %% entry will be deleted in post_op call to on handler
                        gen_component:post_op({read, strong,
                                               {read_deny, ReqId, fixed, UsedReadRound,
                                                round_inc(NewReplies#r_replies.highest_seen_round)}},
                                             State);
                    true ->
                        delete_entry(NewEntry, tablename(State)),

                        %% new entry for next step of protocol
                        NewReqId = uid:get_pids_uid(),
                        Type = entry_datatype(NewEntry),
                        TEntry = entry_new_read(NewReqId,
                                                entry_client(NewEntry),
                                                entry_key(NewEntry),
                                                Type,
                                                entry_fun(NewEntry),
                                                Type:new()),
                        T2Entry = entry_set_round_trips(TEntry, entry_round_trips(NewEntry) + 1),
                        %% keep the merged value we have collected in this phase, which will
                        %% be the value returned if phase 2 succeeds
                        NextStepEntry = entry_set_replies(T2Entry, NewReplies#r_replies{reply_count=0}),
                        save_entry(NextStepEntry, tablename(State)),

                        trace_mpath:log_info(self(), {read_strong_phase2_start,
                                                      round, UsedReadRound,
                                                      value, NewReplies#r_replies.value}),
                        This = comm:reply_as(comm:this(), 3, {read, strong, '_'}),
                        Msg = {crdt_acceptor, vote, '_', This, NewReqId, key,
                               entry_datatype(NewEntry), UsedReadRound, NewReplies#r_replies.value},
                        send_to_all_replicas(entry_key(NewEntry), Msg),
                        State
                end
        end;

on({read, strong, {vote_reply, ReqId, done}}, State) ->
    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                ok;
            Entry ->
                Replies = entry_replies(Entry),
                {Done, NewReplies} = add_vote_reply(Replies),
                NewEntry = entry_set_replies(Entry, NewReplies),

                case Done of
                    false -> save_entry(NewEntry, tablename(State));
                    true ->
                        QueryFun = entry_fun(NewEntry),
                        DataType = entry_datatype(NewEntry),
                        ReturnVal = DataType:apply_query(QueryFun, NewReplies#r_replies.value),
                        trace_mpath:log_info(self(), {read_strong_done,
                                                      crdt_value, NewReplies#r_replies.value,
                                                      return_value, ReturnVal}),
                        inform_client(read_done, Entry, ReturnVal),
                        delete_entry(Entry, tablename(State))
                end
        end,
    State;

on({read, strong, {read_deny, ReqId, RetryMode, TriedRound, RequiredRound}}, State) ->
    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                %% ignore replies for unknown requests
                ok;
            Entry ->
                %set_last_used_round(NextRound, tablename(State)),
                delete_entry(Entry, tablename(State)),

                NextRound = case RetryMode of
                                inc -> none;
                                fixed -> round_inc(RequiredRound)
                            end,

                trace_mpath:log_info(self(), {read_strong_deny,
                                              retry_mode, RetryMode,
                                              round_tried, TriedRound,
                                              round_requed, RequiredRound
                                             }),
                %% retry the read in a higher round...
                %% TODO more intelligent retry mechanism?
                RoundTrips = entry_round_trips(Entry),
                Delay = randoms:rand_uniform(0, 10),
                comm:send_local_after(Delay, self(),
                                        {req_start, {read, strong,
                                        entry_client(Entry),
                                        entry_key(Entry),
                                        entry_datatype(Entry),
                                        entry_fun(Entry),
                                        NextRound,
                                        RoundTrips}})
        end,
    State;


%%%%% eventual consistent read

on({req_start, {read, eventual, Client, Key, DataType, QueryFun}}, State) ->
    ReqId = uid:get_pids_uid(),
    Entry = entry_new_read(ReqId, Client, Key, DataType, QueryFun, DataType:new()),
    save_entry(Entry, tablename(State)),

    This = comm:reply_as(comm:this(), 3, {read, eventual, '_'}),
    Msg = {crdt_acceptor, query_req, '_', This, ReqId, key, DataType, QueryFun},
    send_to_local_replica(Key, Msg),

    State;

on({read, eventual, {query_reply, ReqId, QueryResult}}, State) ->
    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                %% ignore replies for unknown requests (i.e. because we already
                %% have processed them)
                ok;
            Entry ->
                trace_mpath:log_info(self(), {read_eventual_done}),
                % eventual consistent writes just write on a single replica
                % and expect the update to eventual spread through gossiping
                % or similiar behaviour
                inform_client(read_done, Entry, QueryResult),
                delete_entry(Entry, tablename(State))
        end,
    State;

%%%%% strong consistent write

on({req_start, {write, strong, Client, Key, DataType, UpdateFun}}, State) ->
    ReqId = uid:get_pids_uid(),
    Entry = entry_new_write(ReqId, Client, Key, DataType, UpdateFun),
    save_entry(Entry, tablename(State)),

    This = comm:reply_as(comm:this(), 3, {write, strong, '_'}),
    Msg = {crdt_acceptor, update, '_', This, ReqId, key, DataType, UpdateFun},
    send_to_local_replica(Key, Msg),

    State;

on({write, strong, {update_reply, ReqId, CVal}}, State) ->
    This = comm:reply_as(comm:this(), 3, {write, strong, '_'}),

    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                %% ignore replies for unknown requests (i.e. because we already
                %% have processed them)
                ok;
            Entry ->
                Msg = {crdt_acceptor, merge, '_', This, ReqId, key,
                       entry_datatype(Entry), CVal},
                trace_mpath:log_info(self(), {write_strong_start,
                                             value, CVal}),
                NewEntry = entry_inc_round_trips(Entry),
                save_entry(NewEntry, tablename(State)),
                send_to_all_replicas(entry_key(NewEntry), Msg)
        end,
    State;

on({write, strong, {merge_reply, ReqId, done}}, State) ->
    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                %% ignore replies for unknown requests
                ok;
            Entry ->
                Replies = entry_replies(Entry),
                {Done, NewReplies} = add_write_reply(Replies),
                NewEntry = entry_set_replies(Entry, NewReplies),
                case Done of
                    false -> save_entry(NewEntry, tablename(State));
                    true ->
                        trace_mpath:log_info(self(), {write_strong_done}),
                        inform_client(write_done, Entry),
                        delete_entry(Entry, tablename(State))
                end
        end,
    State;

%%%%% eventual consistent write

on({req_start, {write, eventual, Client, Key, DataType, UpdateFun}}, State) ->
    ReqId = uid:get_pids_uid(),
    Entry = entry_new_write(ReqId, Client, Key, DataType, UpdateFun),
    save_entry(Entry, tablename(State)),

    This = comm:reply_as(comm:this(), 3, {write, eventual, '_'}),
    Msg = {crdt_acceptor, update, '_', This, ReqId, key, DataType, UpdateFun},
    send_to_local_replica(Key, Msg),

    State;

on({write, eventual, {update_reply, ReqId, _CVal}}, State) ->
    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                %% ignore replies for unknown requests (i.e. because we already
                %% have processed them)
                ok;
            Entry ->
                trace_mpath:log_info(self(), {write_eventual_done}),
                % eventual consistent writes just write on a single replica
                % and expect the update to eventual spread through gossiping
                % or similiar behaviour
                inform_client(write_done, Entry),
                delete_entry(Entry, tablename(State))
        end,
    State;

on({local_range_req, Key, Message, {get_state_response, LocalRange}}, State) ->
    Keys = replication:get_keys(Key),

    LocalKeys = lists:filter(fun(K) -> intervals:in(K, LocalRange) end, Keys),

    K = case LocalKeys of
        [] ->
            ?TRACE("cannot send locally ~p ~p ", [Keys, LocalRange]),
            %% the local dht node is not responsible for any replca... route to
            %% random replica
            Idx = randoms:rand_uniform(1, length(Keys)+1),
            lists:nth(Idx, Keys);
        [H|_] ->
            %% use replica managed by local dht node
            H
        end,

    Dest = pid_groups:find_a(routing_table),
    LookupEnvelope = dht_node_lookup:envelope(3, setelement(6, Message, K)),
    comm:send_local(Dest, {?lookup_aux, K, 0, LookupEnvelope}),
    State.

%%%%%% internal helper

-spec send_to_local_replica(?RT:key(), tuple()) -> ok.
send_to_local_replica(Key, Message) ->
    %% assert element(3, message) =:= '_'
    %% assert element(6, message) =:= key
    send_to_local_replica(Key, Message, not ?ROUTING_DISABLED).

-spec send_to_local_replica(?RT:key(), tuple(), boolean()) -> ok.
send_to_local_replica(Key, Message, _Routing=true) ->
    LocalDhtNode = pid_groups:get_my(dht_node),
    This = comm:reply_as(comm:this(), 4, {local_range_req, Key, Message, '_'}),
    comm:send_local(LocalDhtNode, {get_state, This, my_range}).

-spec send_to_all_replicas(?RT:key(), tuple()) -> ok.
send_to_all_replicas(Key, Message) ->
    %% assert element(3, message) =:= '_'
    %% assert element(6, message) =:= key
    send_to_all_replicas(Key, Message, not ?ROUTING_DISABLED).

-spec send_to_all_replicas(?RT:key(), tuple(), boolean()) -> ok.
send_to_all_replicas(Key, Message, _Routing=true) ->
    Dest = pid_groups:find_a(routing_table),

    _ = [begin
            LookupEnvelope = dht_node_lookup:envelope(3, setelement(6, Message, K)),
            comm:send_local(Dest, {?lookup_aux, K, 0, LookupEnvelope})
         end
        || K <- replication:get_keys(Key)],

    ok.

-spec inform_client(write_done, entry()) -> ok.
inform_client(write_done, Entry) ->
    Client = entry_client(Entry),
    case is_tuple(Client) of
        true ->
            % must unpack envelope
            comm:send(entry_client(Entry), {write_done});
        false ->
            comm:send_local(entry_client(Entry), {write_done})
    end.

-spec inform_client(read_done, entry(), any()) -> ok.
inform_client(read_done, Entry, QueryResult) ->
    Client = entry_client(Entry),
    case is_tuple(Client) of
        true ->
            % must unpack envelope
            comm:send(entry_client(Entry), {read_done, QueryResult});
        false ->
            comm:send_local(entry_client(Entry), {read_done, QueryResult})
    end.

-spec add_write_reply(#w_replies{}) -> {boolean(), #w_replies{}}.
add_write_reply(Replies) ->
    ReplyCount = Replies#w_replies.reply_count + 1,
    NewReplies = Replies#w_replies{reply_count=ReplyCount},
    Done = replication:quorum_accepted(ReplyCount),
    {Done, NewReplies}.

-spec add_read_reply(#r_replies{}, pr:pr(), pr:pr(), crdt:crdt(), crdt:crdt_module()) ->
    {boolean() | cons_read | retry_read, #r_replies{}}.
add_read_reply(Replies, UsedReadRound, _WriteRound, Value, DataType) ->
    NewReplyCount = Replies#r_replies.reply_count + 1,
    NewMaxRound = max(Replies#r_replies.highest_seen_round, UsedReadRound),
    HighestReplies =
        case NewMaxRound =:= Replies#r_replies.highest_seen_round of
            true -> Replies#r_replies.highest_replies + 1;
            false -> 1
        end,
    {NewValue, IsValueCons} =
        case DataType:eq(Value, Replies#r_replies.value) of
            true ->
                {Value, true andalso Replies#r_replies.cons_value};
            false ->
                {DataType:merge(Replies#r_replies.value, Value),
                NewReplyCount =:= 1} % not inconsistent if this is our first reply
        end,
    NewReplies = Replies#r_replies{
                   reply_count=NewReplyCount,
                   highest_seen_round=NewMaxRound,
                   highest_replies = HighestReplies,
                   cons_value = IsValueCons,
                   value=NewValue
                  },

    %% There are multiple ways to terminate the current protocol step. All
    %% of them requiring a quorum of replies first:
    %% (cons_read) We received consistent values in replies
    %%  ->  This means the current value is already establish and thus no subsequent
    %%      read request will be able to read a smaller value. Therefore, we can skip
    %%      the 2. phase (vote) and directly deliver the result to the client
    %% (true) We received inconsistent values and consistent read rounds:
    %% ->   Default Paxos-like behaviour. Proceed to 2. phase to establish value
    %%      in a quorum
    %% (retry_read) We received inconsistent value and inconsistent reads rounds:
    %% ->   Might happen for concurrent phase 1 read executions since we are doing
    %%      an incremental round negotiaton mechanism. We have to retry the first phase
    %%      with an explicit round, otherwise reads might return conflicting values
    Done = case replication:quorum_accepted(NewReplyCount) of
               false -> false;
               true ->
                   case {IsValueCons, HighestReplies =:= NewReplyCount} of
                       {true, _} -> cons_read;
                       {false, true} -> true;
                       {false, false} -> retry_read
                   end
           end,

    {Done, NewReplies}.

-spec add_vote_reply(#r_replies{}) -> {boolean(), #r_replies{}}.
add_vote_reply(Replies) ->
    ReplyCount = Replies#r_replies.reply_count + 1,
    NewReplies = Replies#r_replies{reply_count=ReplyCount},
    Done = replication:quorum_accepted(ReplyCount),
    {Done, NewReplies}.

-spec entry_new_read(any(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:query_fun(),
                    crdt:crdt()) -> entry().
entry_new_read(ReqId, Client, Key, DataType, QueryFun, EmptyVal) ->
    {ReqId, Client, Key, DataType, QueryFun,
     #r_replies{reply_count=0, highest_seen_round=pr:new(0,0), highest_replies=0,
                value=EmptyVal, cons_value=true}, _RoundTrips=0}.

-spec entry_new_write(any(), comm:erl_local_pid(), ?RT:key(), crdt:crdt_module(), crdt:update_fun()) -> entry().
entry_new_write(ReqId, Client, Key, DataType, UpdateFun) ->
    {ReqId, Client, Key, DataType, UpdateFun, #w_replies{reply_count=0}, _RoundTrips=0}.

-spec entry_reqid(entry())        -> any().
entry_reqid(Entry)                -> element(1, Entry).
-spec entry_client(entry())       -> comm:erl_local_pid().
entry_client(Entry)               -> element(2, Entry).
-spec entry_key(entry())          -> any().
entry_key(Entry)                  -> element(3, Entry).
-spec entry_datatype(entry())     -> crdt:crdt_module().
entry_datatype(Entry)             -> element(4, Entry).
-spec entry_fun(entry())          -> crdt:update_fun() | crdt:query_fun().
entry_fun(Entry)                  -> element(5, Entry).
-spec entry_replies(entry())      -> replies().
entry_replies(Entry)              -> element(6, Entry).
-spec entry_set_replies(entry(), replies()) -> entry().
entry_set_replies(Entry, Replies) -> setelement(6, Entry, Replies).
-spec entry_round_trips(entry())  -> non_neg_integer().
entry_round_trips(Entry)          -> element(7, Entry).
-spec entry_inc_round_trips(entry()) -> entry().
entry_inc_round_trips(Entry) -> setelement(7, Entry, element(7, Entry) + 1).
-spec entry_set_round_trips(entry(), non_neg_integer()) -> entry().
entry_set_round_trips(Entry, RoundTrips) -> setelement(7, Entry, RoundTrips).

-spec get_entry(any(), ?PDB:tableid()) -> entry() | batch_entry() | undefined.
get_entry(ReqId, TableName) ->
    ?PDB:get(ReqId, TableName).

-spec save_entry(entry() | batch_entry(), ?PDB:tableid()) -> ok.
save_entry(NewEntry, TableName) ->
    ?PDB:set(NewEntry, TableName).

-spec delete_entry(entry() | batch_entry(), ?PDB:tableid()) -> ok.
delete_entry(Entry, TableName) ->
    ReqId = entry_reqid(Entry),
    ?PDB:delete(ReqId, TableName).

-spec tablename(state()) -> ?PDB:tableid().
tablename(State) -> element(1, State).

-spec round_inc(pr:pr()) -> pr:pr().
round_inc(Round) ->
    pr:new(pr:get_r(Round)+1, pr:get_id(Round)).

-spec round_inc(pr:pr(), any()) -> pr:pr().
round_inc(Round, ID) ->
    pr:new(pr:get_r(Round)+1, ID).

%%%% batching helpers
-spec set_read_batch_in_progress(state()) -> state().
set_read_batch_in_progress(State) ->
    case element(4, State) =:= false andalso read_batching_enabled() of
        true ->
            comm:send_local_after(next_read_batching_interval(), self(), {read_batch_trigger}),
            setelement(4, State, true);
        false -> State
    end.

-spec set_read_batch_done(state()) -> state().
set_read_batch_done(State) ->
    setelement(4, State, false).

-spec set_write_batch_in_progress(state()) -> state().
set_write_batch_in_progress(State) ->
    case element(5, State) =:= false andalso write_batching_enabled() of
        true ->
            comm:send_local_after(next_write_batching_interval(), self(), {write_batch_trigger}),
            setelement(5, State, true);
        false -> State
    end.

-spec set_write_batch_done(state()) -> state().
set_write_batch_done(State) ->
    setelement(5, State, false).

-spec read_batching_enabled() -> boolean().
read_batching_enabled() -> ?READ_BATCHING_INTERVAL > 0.

-spec write_batching_enabled() -> boolean().
write_batching_enabled() -> ?WRITE_BATCHING_INTERVAL > 0.

-spec next_read_batching_interval() -> non_neg_integer().
next_read_batching_interval() ->
    Div = randoms:rand_uniform(0, ?READ_BATCHING_INTERVAL_DIVERGENCE*2 + 1),
    max(0, ?READ_BATCHING_INTERVAL - ?READ_BATCHING_INTERVAL_DIVERGENCE + Div).

-spec next_write_batching_interval() -> non_neg_integer().
next_write_batching_interval() ->
    Div = randoms:rand_uniform(0, ?WRITE_BATCHING_INTERVAL_DIVERGENCE*2 + 1),
    max(0, ?WRITE_BATCHING_INTERVAL - ?WRITE_BATCHING_INTERVAL_DIVERGENCE + Div).

%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(read_batching_interval) andalso
    config:cfg_is_integer(write_batching_interval).

