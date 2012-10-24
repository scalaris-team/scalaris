% @copyright 2012 Zuse Institute Berlin,

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
%% @doc Allow a sequence of consensus using a prbr.
%% @end
%% @version $Id:$
-module(rbrcseq).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%%-define(PDB, pdb_ets).
-define(PDB, pdb).

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("client_types.hrl").

%% api:
-export([qread/2, qread/3]).
-export([qwrite/6]).

-export([start_link/2]).
-export([init/1, on/2]).

-type state() :: { ?PDB:tableid(),
                   dht_node_state:db_selector(),
                   non_neg_integer() %% period this process is in
                 }.

%% TODO: add support for *consistent* quorum by counting the number of
%% same round number replies

-type entry() :: {any(), %% ReqId
                  non_neg_integer(), %% period of last retriggering / starting
                  non_neg_integer(), %% period of next retriggering
                  client_key(), %% key
                  comm:erl_local_pid(), %% client
                  prbr:r_with_id(), %% my round
                  non_neg_integer(), %% number of acks
                  non_neg_integer(), %% number of denies
                  prbr:r_with_id(), %% highest seen round in replies
                  any(), %% value of highest seen round in replies
                  any() %% filter or tuple of filters
%%% Attention: There is a case that checks the size of this tuple below!!
                 }.

-type check_next_step() :: fun((term(), term()) -> term()).

-ifdef(with_export_type_support).
-export_type([check_next_step/0]).
-endif.

-spec qread(comm:erl_local_pid(), client_key()) -> ok.
qread(Client, Key) ->
    qread(Client, Key, fun prbr:noop_read_filter/1).

%% user definable functions for read:
%% ContentReadFilter(custom_data() | no_value_yet) -> any().

-spec qread(comm:erl_local_pid(), any(), prbr:read_filter()) -> ok.
qread(Client, Key, ReadFilter) ->
    Pid = pid_groups:find_a(?MODULE),
    comm:send_local(Pid, {qread, Client, Key, ReadFilter, _RetriggerAfter = 1})
    %% the process will reply to the client directly
    .

%% user definable functions for write:
%% ContentReadFilter(custom_data() | no_value_yet) -> read_info().
%% ContentValidNextStep(read_info(), {ContentWriteFilter, Value}) ->
%%    {boolean(), info_passed_from_read_to_write()}
%% info_passed_from_read_to_write() must be used to update older
%% versions, so in all replicas the same data is written
%% ContentWriteFilter(custom_data(), info_passed_from_read_to_write(), Value()) -> custom_data()
-spec qwrite(comm:erl_local_pid(),
             client_key(),
             fun ((any()) -> any()),
             fun ((any(), any()) -> any()),
             fun ((any(), any(), any()) -> any()),
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
qwrite(Client, Key, ReadFilter, ContentCheck,
       WriteFilter, Value) ->
    Pid = pid_groups:find_a(?MODULE),
    comm:send_local(Pid, {qwrite, Client,
                          Key, {ReadFilter, ContentCheck, WriteFilter},
                          Value, _RetriggerAfter = 1}),
    %% the process will reply to the client directly
    ok.

%% @doc spawns a rbrcseq, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), dht_node_state:db_selector())
                -> {ok, pid()}.
start_link(DHTNodeGroup, DBSelector) ->
    gen_component:start_link(
      ?MODULE, fun ?MODULE:on/2, DBSelector,
      [{pid_groups_join_as, DHTNodeGroup, rbrcseq}]).

-spec init(dht_node_state:db_selector()) -> state().
init(DBSelector) ->
    msg_delay:send_local(1, self(), {next_period, 1}),
    {?PDB:new(?MODULE, [set, protected]), DBSelector, 0}.

-spec on(comm:message(), state()) -> state().
%% ; ({qread, any(), client_key(), fun ((any()) -> any())},
%% state()) -> state().
on({qread, Client, Key, ReadFilter, RetriggerAfter}, State) ->
    ?TRACE("rbrcseq:on qread ~n", []),
    %% assign new reqest-id; (also assign new ReqId when retriggering)

    %% if the caller process may handle more than one request at a
    %% time for the same key, the pids id has to be unique for each
    %% request to use the prbr correctly.
    ReqId = uid:get_pids_uid(),

    %% initiate lookups for replicas(Key) and perform
    %% rbr reads in a certain round (work as paxos proposer)
    %% there apply the content filter to only retrieve the required information

    This = comm:reply_as(comm:this(), 2, {qread_collect, '_'}),
    %%This = comm:this(),

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
      || X <- api_rdht:get_replica_keys(Key) ],

    %% retriggering of the request is done via the periodic dictionary scan

    %% create local state for the request id
    Entry = entry_new_read(ReqId, Key, Client, period(State),
                           ReadFilter, RetriggerAfter),
    %% store local state of the request
    set_entry(Entry, tablename(State)),
    State;

on({qread_collect, {read_reply, MyRwithId, Val, AckRound}}, State) ->
%%on({read_reply, MyRwithId, Val, AckRound}, State) ->
    ?TRACE("rbrcseq:on qread_collect read_reply ~n", []),
    %% collect a majority of answers and select that one with the highest
    %% round number.
    {_, ReqId} = prbr:r_with_id_get_id(MyRwithId),
    Entry = get_entry(ReqId, tablename(State)),
    _ = case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            {Done, NewEntry} = add_read_reply(Entry, MyRwithId, Val, AckRound),
            case Done of
                false -> set_entry(NewEntry, tablename(State));
                true ->
                    inform_client(qread_done, NewEntry),
                    ?PDB:delete(ReqId, tablename(State))
            end
    end,
    State;

on({qwrite, Client, Key, Filters, Value, RetriggerAfter}, State) ->
    %% assign new reqest-id
    ReqId = uid:get_pids_uid(),

    This = comm:reply_as(self(), 3, {qwrite_read_done, ReqId, '_'}),
    comm:send_local(self(), {qread, This, Key, element(1, Filters), 1}),

    %% create local state for the request id, including used filters
    Entry = entry_new_write(ReqId, Key, Client, period(State),
                            Filters, Value, RetriggerAfter),
    set_entry(Entry, tablename(State)),
    State;


on({qwrite_read_done, ReqId, {qread_done, _ReadId, Round, ReadValue}},
   State) ->
    Entry = get_entry(ReqId, tablename(State)),
    ContentCheck = element(2, entry_filters(Entry)),
    WriteFilter = element(3, entry_filters(Entry)),
    WriteValue = entry_val(Entry),

    _ = case ContentCheck(ReadValue, {WriteFilter, WriteValue}) of
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
              || X <- api_rdht:get_replica_keys(entry_key(Entry)) ];
        {false, _Reason} = Err ->
            %% own proposal not possible as of content check
            %% should rewrite old consensus??
            comm:send_local(entry_client(Entry), Err)
    end,
    State;

on({qwrite_collect, ReqId, {write_reply, _Key, Round}}, State) ->
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
                    ?PDB:delete(entry_reqid(Entry), tablename(State))
            end
    end,
    State;

on({qwrite_collect, ReqId, {write_deny, _Key, NewerRound}}, State) ->
    TableName = tablename(State),
    Entry = get_entry(ReqId, TableName),
    _ = case Entry of
        undefined ->
            %% drop replies for unknown requests, as they must be
            %% outdated as all replies run through the same process.
            State;
        _ ->
            {Done, NewEntry} = add_write_deny(Entry, NewerRound),
            case Done of
                false -> set_entry(NewEntry, tablename(State));
                true ->
                    %% retry
                    retrigger(NewEntry, TableName)%%,
%%                    ?PDB:delete(entry_reqid(Entry), tablename(State))
            end
    end,
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
    State;

on({next_period, NewPeriod}, State) ->
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
    _ = [ retrigger(X, Table) || X <- ?PDB:tab2list(Table),
                                 NewPeriod > element(3, X) ],

    %% re-trigger next next_period
    msg_delay:send_local(1, self(), {next_period, NewPeriod + 1}),

    set_period(State, NewPeriod).

-spec retrigger(entry(), ?PDB:tableid()) -> ok.
retrigger(Entry, TableName) ->
    RetriggerDelay = (entry_retrigger(Entry) - entry_period(Entry)) + 1,
    case erlang:size(Entry) of
        11 when is_tuple(element(11, Entry)) -> %% write request
            comm:send_local(self(),
                            {qwrite, entry_client(Entry),
                             entry_key(Entry), entry_filters(Entry),
                             entry_val(Entry),
                             RetriggerDelay});
        11 -> %% read request
            comm:send_local(self(),
                            {qread, entry_client(Entry),
                              entry_key(Entry), entry_filters(Entry),
                             RetriggerDelay})
        end,
    ?PDB:delete(element(1, Entry), TableName).

-spec get_entry(any(), ?PDB:tableid()) -> entry() | undefined.
get_entry(ReqId, TableName) ->
    ?PDB:get(ReqId, TableName).

-spec set_entry(entry(), ?PDB:tableid()) -> ok.
set_entry(NewEntry, TableName) ->
    ?PDB:set(NewEntry, TableName).

%% abstract data type to collect quorum read/write replies
-spec entry_new_read(any(), client_key(),
                     comm:erl_local_pid(), non_neg_integer(), any(),
                     non_neg_integer())
                    -> entry().
entry_new_read(ReqId, Key, Client, Period, Filter, RetriggerAfter) ->
    {ReqId, Period, Period + RetriggerAfter, Key, Client,
     _MyRound = {0, 0}, _NumAcked = 0,
     _NumDenied = 0, _AckRound = {0, 0}, _AckVal = 0, Filter}.

-spec entry_new_write(any(), client_key(), comm:erl_local_pid(),
                      non_neg_integer(), tuple(), any(), non_neg_integer())
                     -> entry().
entry_new_write(ReqId, Key, Client, Period, Filters, Value, RetriggerAfter) ->
    {ReqId, Period, Period + RetriggerAfter, Key, Client, _MyRound = {0, 0},
     _NumAcked = 0, _NumDenied = 0, _AckRound = {0, 0}, Value, Filters}.

-spec entry_reqid(entry())        -> any().
entry_reqid(Entry)                -> element(1, Entry).
-spec entry_period(entry())       -> non_neg_integer().
entry_period(Entry)               -> element(2, Entry).
-spec entry_retrigger(entry())    -> non_neg_integer().
entry_retrigger(Entry)            -> element(3, Entry).
-spec entry_key(entry())          -> any().
entry_key(Entry)                  -> element(4, Entry).
-spec entry_client(entry())       -> comm:erl_local_pid().
entry_client(Entry)               -> element(5, Entry).
-spec entry_my_round(entry())     -> prbr:r_with_id().
entry_my_round(Entry)             -> element(6, Entry).
-spec entry_set_my_round(entry(), prbr:r_with_id()) -> entry().
entry_set_my_round(Entry, Round)  -> setelement(6, Entry, Round).
-spec entry_num_acks(entry())     -> non_neg_integer().
entry_num_acks(Entry)             -> element(7, Entry).
-spec entry_inc_num_acks(entry()) -> entry().
entry_inc_num_acks(Entry) -> setelement(7, Entry, element(7, Entry) + 1).
-spec entry_set_num_acks(entry(), non_neg_integer()) -> entry().
entry_set_num_acks(Entry, Num)    -> setelement(7, Entry, Num).
-spec entry_num_denies(entry())   -> non_neg_integer().
entry_num_denies(Entry)           -> element(8, Entry).
-spec entry_inc_num_denies(entry()) -> entry().
entry_inc_num_denies(Entry) -> setelement(8, Entry, element(8, Entry) + 1).
-spec entry_latest_seen(entry())  -> prbr:r_with_id().
entry_latest_seen(Entry)          -> element(9, Entry).
-spec entry_set_latest_seen(entry(), prbr:r_with_id()) -> entry().
entry_set_latest_seen(Entry, Round) -> setelement(9, Entry, Round).
-spec entry_val(entry())           -> any().
entry_val(Entry)                   -> element(10, Entry).
-spec entry_set_val(entry(), any()) -> entry().
entry_set_val(Entry, Val)          -> setelement(10, Entry, Val).
-spec entry_filters(entry())       -> any().
entry_filters(Entry)               -> element(11, Entry).

-spec add_read_reply(entry(), prbr:r_with_id(), client_value(), prbr:r_with_id()) -> {boolean(), entry()}.
add_read_reply(Entry, AssignedRound, Val, AckRound) ->
    TmpEntry =
        case AckRound > entry_latest_seen(Entry) of
            true ->
                E1 = entry_set_latest_seen(Entry, AckRound),
                entry_set_val(E1, Val);
            false -> Entry
    end,
    E2 = entry_set_my_round(TmpEntry, AssignedRound),
    {3 =< 1+entry_num_acks(E2), entry_inc_num_acks(E2)}.

-spec add_write_reply(entry(), prbr:r_with_id()) -> {boolean(), entry()}.
add_write_reply(Entry, Round) ->
    E1 =
        case Round > entry_latest_seen(Entry) of
            false -> Entry;
            true ->
                %% reset rack and store newer round
                T1Entry = entry_set_latest_seen(Entry, Round),
                _T2Entry = entry_set_num_acks(T1Entry, 0)
    end,
    {3 =< 1+entry_num_acks(E1), entry_inc_num_acks(E1)}.

-spec add_write_deny(entry(), prbr:r_with_id()) -> {boolean(), entry()}.
add_write_deny(Entry, Round) ->
    E1 =
        case Round > entry_latest_seen(Entry) of
            false -> Entry;
            true ->
                %% reset rack and store newer round
                T1Entry = entry_set_latest_seen(Entry, Round),
                _T2Entry = entry_set_num_acks(T1Entry, 0)
    end,
    {2 =< 1+entry_num_denies(E1), entry_inc_num_denies(E1)}.


-spec inform_client(qread_done | qwrite_done | qwrite_deny, entry()) -> ok.
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
