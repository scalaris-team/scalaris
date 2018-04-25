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
%%         @TODO:
%%              gossiping?
%% @end
-module(crdt_proposer).
-author('skrzypczak.de').

-define(PDB, pdb).

-include("scalaris.hrl").

-behaviour(gen_component).

-export([write/5, write_eventual/5]).
-export([read/5, read_eventual/5]).

-export([start_link/3]).
-export([init/1, on/2]).

-type state() :: { ?PDB:tableid(),
                   dht_node_state:db_selector(),
                   non_neg_integer() %% period this process is in
                 }.

-record(r_replies,  {
                        reply_count :: non_neg_integer(),
                        highest_replies :: non_neg_integer(),
                        highest_seen_round :: pr:pr(),
                        cons_value :: boolean(),
                        value :: crdt:crdt()
                    }).

-record(w_replies, {reply_count :: non_neg_integer()}).

-type entry() :: {any(),        %% request id
                  comm:erl_local_pid(), %% client
                  ?RT:key(),    %% key
                  module(),     %% data type
                  crdt:query_fun() | crdt:update_fun(), %% fun used to read/write crdt state
                  replies()     %% aggregates replies
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
    {?PDB:new(?MODULE, [set]), DBSelector, 0}.


%%%% API

-spec read(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), module(), crdt:query_fun()) -> ok.
read(CSeqPidName, Client, Key, DataType, QueryFun) ->
    start_request(CSeqPidName, {req_start, {read, strong, Client, Key, DataType, QueryFun, none}}).

-spec read_eventual(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), module(), crdt:query_fun()) -> ok.
read_eventual(CSeqPidName, Client, Key, DataType, QueryFun) ->
    start_request(CSeqPidName, {req_start, {read, eventual, Client, Key, DataType, QueryFun}}).

-spec write(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), module(), crdt:update_fun()) -> ok.
write(CSeqPidName, Client, Key, DataType, UpdateFun) ->
    start_request(CSeqPidName, {req_start, {write, strong, Client, Key, DataType, UpdateFun}}).

-spec write_eventual(pid_groups:pidname(), comm:erl_local_pid(), ?RT:key(), module(), crdt:update_fun()) -> ok.
write_eventual(CSeqPidName, Client, Key, DataType, UpdateFun) ->
    start_request(CSeqPidName, {req_start, {write, eventual, Client, Key, DataType, UpdateFun}}).

-spec start_request(pid_groups:pidname(), comm:message()) -> ok.
start_request(CSeqPidName, Msg) ->
    Pid = pid_groups:find_a(CSeqPidName),
    comm:send_local(Pid, Msg).


-spec on(comm:message(), state()) -> state().

%%%%% strong consistent read

on({req_start, {read, strong, Client, Key, DataType, QueryFun, PreviousRound}}, State) ->
    ReqId = uid:get_pids_uid(),
    Entry = entry_new_read(ReqId, Client, Key, DataType, QueryFun, DataType:new()),
    save_entry(Entry, tablename(State)),

    Round = case PreviousRound of
                none ->
                    {inc, ReqId};
                _ ->
                    round_inc(PreviousRound, ReqId)
            end,

    This = comm:reply_as(comm:this(), 3, {read, strong, '_'}),
    Msg = {crdt_acceptor, prepare, '_', This, ReqId, key, DataType, Round, DataType:new()},
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
                        %% keep the merged value we have collected in this phase, which will
                        %% be the value returned if phase 2 succeeds
                        NextStepEntry = entry_set_replies(TEntry, NewReplies#r_replies{reply_count=0}),
                        save_entry(NextStepEntry, tablename(State)),

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
                        inform_client(read_done, Entry, ReturnVal),
                        delete_entry(Entry, tablename(State))
                end
        end,
    State;

on({read, strong, {read_deny, ReqId, RetryMode, _TriedRound, RequiredRound}}, State) ->
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

                %% retry the read in a higher round...
                %% TODO more intelligent retry mechanism?
                Delay = randoms:rand_uniform(0, 30),
                comm:send_local_after(Delay, self(),
                                        {req_start, {read, strong,
                                        entry_client(Entry),
                                        entry_key(Entry),
                                        entry_datatype(Entry),
                                        entry_fun(Entry),
                                        NextRound}})
        end,
    State;


%%%%% eventual consistent read

on({req_start, {read, eventual, Client, Key, DataType, QueryFun}}, State) ->
    ReqId = uid:get_pids_uid(),
    Entry = entry_new_read(ReqId, Client, Key, DataType, QueryFun, DataType:new()),
    save_entry(Entry, tablename(State)),

    This = comm:reply_as(comm:this(), 3, {read, eventual, '_'}),
    Msg = {crdt_acceptor, query_req, '_', This, ReqId, key, DataType, QueryFun},
    send_to_local_replica(Key, Client, Msg),

    State;

on({read, eventual, {query_reply, ReqId, QueryResult}}, State) ->
    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                %% ignore replies for unknown requests (i.e. because we already
                %% have processed them)
                ok;
            Entry ->
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
    send_to_local_replica(Key, Client, Msg),

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
                send_to_all_replicas(entry_key(Entry), Msg)
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
    send_to_local_replica(Key, Client, Msg),

    State;

on({write, eventual, {update_reply, ReqId, _CVal}}, State) ->
    _ = case get_entry(ReqId, tablename(State)) of
            undefined ->
                %% ignore replies for unknown requests (i.e. because we already
                %% have processed them)
                ok;
            Entry ->
                % eventual consistent writes just write on a single replica
                % and expect the update to eventual spread through gossiping
                % or similiar behaviour
                inform_client(write_done, Entry),
                delete_entry(Entry, tablename(State))
        end,
    State.

%%%%%% internal helper

-spec send_to_local_replica(?RT:key(), comm:erl_local_pid(), tuple()) -> ok.
send_to_local_replica(Key, Client, Message) ->
    %% TODO really ugly hack, only ensures that the same client gets the same
    %% replica key every time, but does not care about locality
    %% assert element(3, message) =:= '_'
    %% assert element(6, message) =:= key
    Dest = pid_groups:find_a(routing_table),

    Hash = ?RT:hash_key(term_to_binary(Client)),
    Keys = replication:get_keys(Key),
    Idx = (Hash rem length(Keys)) + 1,
    K = lists:nth(Idx, Keys),

    LookupEnvelope = dht_node_lookup:envelope(3, setelement(6, Message, K)),
    comm:send_local(Dest, {?lookup_aux, K, 0, LookupEnvelope}),
    ok.

-spec send_to_all_replicas(?RT:key(), tuple()) -> ok.
send_to_all_replicas(Key, Message) ->
    %% assert element(3, message) =:= '_'
    Dest = pid_groups:find_a(routing_table),

    _ = [begin
            LookupEnvelope = dht_node_lookup:envelope(3, setelement(6, Message, K)),
            comm:send_local(Dest, {?lookup_aux, K, 0, LookupEnvelope})
         end
        || K <- replication:get_keys(Key)],

    ok.

-spec inform_client(write_done, entry()) -> ok.
inform_client(write_done, Entry) ->
    comm:send_local(entry_client(Entry), {write_done}).

-spec inform_client(read_done, entry(), any()) -> ok.
inform_client(read_done, Entry, QueryResult) ->
    comm:send_local(entry_client(Entry), {read_done, QueryResult}).

-spec add_write_reply(#w_replies{}) -> {boolean(), #w_replies{}}.
add_write_reply(Replies) ->
    ReplyCount = Replies#w_replies.reply_count + 1,
    NewReplies = Replies#w_replies{reply_count=ReplyCount},
    Done = replication:quorum_accepted(ReplyCount),
    {Done, NewReplies}.

-spec add_read_reply(#r_replies{}, pr:pr(), pr:pr(), crdt:crdt(), module()) ->
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

-spec entry_new_read(any(), comm:erl_local_pid(), ?RT:key(), module(), crdt:query_fun(),
                    crdt:crdt()) -> entry().
entry_new_read(ReqId, Client, Key, DataType, QueryFun, EmptyVal) ->
    {ReqId, Client, Key, DataType, QueryFun,
     #r_replies{reply_count=0, highest_seen_round=pr:new(0,0), highest_replies=0,
                value=EmptyVal, cons_value=true}}.

-spec entry_new_write(any(), comm:erl_local_pid(), ?RT:key(), module(), crdt:update_fun()) -> entry().
entry_new_write(ReqId, Client, Key, DataType, UpdateFun) ->
    {ReqId, Client, Key, DataType, UpdateFun, #w_replies{reply_count=0}}.

-spec entry_reqid(entry())        -> any().
entry_reqid(Entry)                -> element(1, Entry).
-spec entry_client(entry())       -> comm:erl_local_pid().
entry_client(Entry)               -> element(2, Entry).
-spec entry_key(entry())          -> any().
entry_key(Entry)                  -> element(3, Entry).
-spec entry_datatype(entry())     -> module().
entry_datatype(Entry)             -> element(4, Entry).
-spec entry_fun(entry())          -> crdt:update_fun() | crdt:query_fun().
entry_fun(Entry)                  -> element(5, Entry).
-spec entry_replies(entry())      -> replies().
entry_replies(Entry)              -> element(6, Entry).
-spec entry_set_replies(entry(), replies()) -> entry().
entry_set_replies(Entry, Replies) -> setelement(6, Entry, Replies).

-spec get_entry(any(), ?PDB:tableid()) -> entry() | undefined.
get_entry(ReqId, TableName) ->
    ?PDB:get(ReqId, TableName).

-spec save_entry(entry(), ?PDB:tableid()) -> ok.
save_entry(NewEntry, TableName) ->
    ?PDB:set(NewEntry, TableName).

-spec delete_entry(entry(), ?PDB:tableid()) -> ok.
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
