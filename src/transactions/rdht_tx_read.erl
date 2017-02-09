%% @copyright 2009-2015 Zuse Institute Berlin

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
%% @doc Part of replicated DHT implementation.
%%      The read operation.
%% @version $Id$
-module(rdht_tx_read).
-author('schintke@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
%% -define(TRACE_SNAP(X, Y), ct:pal(X, Y)).
-define(TRACE_SNAP(X, Y), ?TRACE(X, Y)).

-include("scalaris.hrl").
-include("client_types.hrl").

-behaviour(tx_op_beh).
-export([work_phase/3, work_phase_key/5,
         validate_prefilter/1, validate/3,
         commit/5, abort/5,
         extract_from_value/3, extract_from_tlog/4]).

-behaviour(gen_component).
-export([init/1, on/2]).

-export([start_link/1]).
-export([check_config/0]).

% feeder for tester
-export([extract_from_tlog_feeder/4]).

-include("gen_component.hrl").
-include("rdht_tx_read_state.hrl").

%% reply messages a client should expect (when calling asynch work_phase/3)
-spec msg_reply(rdht_tx:req_id() | rdht_tx_write:req_id(),
                tx_tlog:tlog_entry()) -> comm:message().
msg_reply(Id, TLogEntry) ->
    {rdht_tx_read_reply, Id, TLogEntry}.

-spec work_phase(pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                 api_tx:request()) -> ok.
work_phase(ClientPid, ReqId, Request) ->
    Key = element(2, Request),
    Op = if element(1, Request) =/= read -> ?read; % e.g. {add_del_on_list, Key, ToAdd, ToRemove}
            tuple_size(Request) =:= 3 ->
                case element(3, Request) of
                    random_from_list -> ?random_from_list;
                     % let client crash if input data is not correct:
                    {sublist, Start, Len} when is_integer(Start)
                      andalso Start =/= 0 andalso is_integer(Len)
                      -> {?sublist, Start, Len}
                end;
            true -> ?read
         end,
    HashedKey = ?RT:hash_key(Key),
    work_phase_key(ClientPid, ReqId, Key, HashedKey, Op).

-spec work_phase_key(comm:erl_local_pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                     client_key(), ?RT:key(),
                     Op::?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()})
        -> ok.
-ifdef(TXNEW).
work_phase_key(ClientPid, ReqId, Key, HashedKey, Op) ->
    ?TRACE("rdht_tx_read:work_phase asynch~n", []),
    %% PRE: No entry for key in TLog
    %% find rdht_tx_read process as collector
    CollectorPid = pid_groups:find_a(?MODULE),
    %% it is ok to pack the client_is info into the collector pid as
    %% it is only send around in the local VM but not across the LAN
    %% or WAN: first to the rbrcseq process and then to the
    %% rdht_tx_read process. So the reply for the client can be build
    %% in a single on handler and we do not need to store a state for
    %% each request.
    MyCollectorPid =
        comm:reply_as(CollectorPid, 5,
                      {work_phase_done, ClientPid, Key, Op, '_'}),
    kv_on_cseq:work_phase_async(MyCollectorPid, ReqId, HashedKey, Op),

    ok.
-else.
work_phase_key(ClientPid, ReqId, Key, HashedKey, Op) ->
    ?TRACE("rdht_tx_read:work_phase asynch~n", []),
    %% PRE: No entry for key in TLog
    %% find rdht_tx_read process as collector
    CollectorPid = pid_groups:find_a(?MODULE),
    %% trigger quorum read
    quorum_read(comm:make_global(CollectorPid), ReqId, HashedKey, Op),
    %% inform CollectorPid on whom to inform after quorum reached
    comm:send_local(CollectorPid, {client_is, ReqId, ClientPid, Key, Op}),
    ok.
-endif.

%% not needed for newtx, but required by type_check_SUITE (excluded private fun)
-spec quorum_read(CollectorPid::comm:mypid(), ReqId::rdht_tx:req_id() | rdht_tx_write:req_id(),
                  HashedKey::?RT:key(),
                  Op::?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()})
        -> ok.
quorum_read(CollectorPid, ReqId, HashedKey, Op) ->
    ?TRACE("rdht_tx_read:quorum_read ~p Collector: ~p~n", [self(), CollectorPid]),
    RKeys = ?RT:get_replica_keys(HashedKey),
    _ = [ api_dht_raw:unreliable_lookup(
            RKey, {?read_op, CollectorPid, ReqId, RKey, Op})
        || RKey <- RKeys ],
    ok.

%% @doc Performs the requested operation in the dht_node context.
-spec extract_from_value
        (rdht_tx:encoded_value(), client_version(), Op::?read) -> Result::{?ok, rdht_tx:encoded_value(), client_version()};
        (rdht_tx:encoded_value(), client_version(), Op::?random_from_list)
            -> Result::{?ok, rdht_tx:encoded_value(), client_version()} | {?fail, ?empty_list | ?not_a_list, client_version()};
        (rdht_tx:encoded_value(), client_version(), Op::{?sublist, Start::pos_integer() | neg_integer(), Len::integer()})
            -> Result::{?ok, rdht_tx:encoded_value(), client_version()} | {?fail, ?not_a_list, client_version()};
        (rdht_tx:encoded_value(), client_version(), Op::?write) -> Result::{?ok, ?value_dropped, client_version()};
        (empty_val, -1, Op::?read | ?write) -> Result::{?ok, ?value_dropped, -1};
        (empty_val, -1, Op::?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()})
            -> Result::{?fail, ?not_found, -1}.
extract_from_value(empty_val, Version = -1, ?read) ->
    {?ok, ?value_dropped, Version};
extract_from_value(Value, Version, ?read) ->
    {?ok, Value, Version};
extract_from_value(empty_val, Version = -1, ?write) ->
    {?ok, ?value_dropped, Version};
extract_from_value(_Value, Version, ?write) ->
    {?ok, ?value_dropped, Version};
extract_from_value(empty_val, Version = -1, ?random_from_list) ->
    {?fail, ?not_found, Version};
extract_from_value(ValueEnc, Version, ?random_from_list) ->
    Value = rdht_tx:decode_value(ValueEnc),
    case Value of
        [_|_]     -> RandVal_ListLen = util:randomelem_and_length(Value),
                     {?ok, rdht_tx:encode_value(RandVal_ListLen), Version};
        []        -> {?fail, ?empty_list, Version};
        _         -> {?fail, ?not_a_list, Version}
    end;
extract_from_value(empty_val, Version = -1, {?sublist, _Start, _Len}) ->
    {?fail, ?not_found, Version};
extract_from_value(ValueEnc, Version, {?sublist, Start, Len}) ->
    Value = rdht_tx:decode_value(ValueEnc),
    if is_list(Value) ->
           SubList_ListLen = util:sublist(Value, Start, Len),
           {?ok, rdht_tx:encode_value(SubList_ListLen), Version};
       true ->
           {?fail, ?not_a_list, Version}
    end.

-spec extract_from_tlog_feeder(
        tx_tlog:tlog_entry(), client_key(),
        Op::read | random_from_list | {sublist, Start::pos_integer() | neg_integer(), Len::integer()},
        EnDecode::boolean())
        -> {tx_tlog:tlog_entry(), client_key(),
            Op::read | random_from_list | {sublist, Start::pos_integer() | neg_integer(), Len::integer()},
            EnDecode::boolean()}.
extract_from_tlog_feeder(Entry, Key, read = Op, EnDecode) ->
    {ValType, EncVal} = tx_tlog:get_entry_value(Entry),
    NewEntry =
        % transform unknow value types to a valid ?value type (this is allowed for any entry operation):
        case not lists:member(ValType, [?value, ?not_found]) of
            true -> tx_tlog:set_entry_value(Entry, ?value, EncVal);
            _    -> Entry
        end,
    {NewEntry, Key, Op, EnDecode};
extract_from_tlog_feeder(Entry, Key, random_from_list = Op, EnDecode) ->
    {ValType, EncVal} = tx_tlog:get_entry_value(Entry),
    NewEntry =
        % transform unknow value types to a valid ?value type (this is allowed for any entry operation):
        case not lists:member(ValType, [?value, ?not_found, {?fail, ?empty_list}, {?fail, ?not_a_list}]) of
            true -> tx_tlog:set_entry_value(Entry, ?value, EncVal);
            _    -> Entry
        end,
    {NewEntry, Key, Op, EnDecode};
extract_from_tlog_feeder(Entry, Key, {sublist, _Start, _Len} = Op, EnDecode) ->
    {ValType, EncVal} = tx_tlog:get_entry_value(Entry),
    NewEntry =
        % transform unknow value types to a valid ?value type (this is allowed for any entry operation):
        case not lists:member(ValType, [?value, ?not_found, {?fail, ?not_a_list}]) of
            true -> tx_tlog:set_entry_value(Entry, ?value, EncVal);
            _    -> Entry
        end,
    {NewEntry, Key, Op, EnDecode}.

%% @doc Get a result entry for a read from the given TLog entry.
-spec extract_from_tlog
        (tx_tlog:tlog_entry(), client_key(), Op::read, EnDecode::boolean())
            -> {tx_tlog:tlog_entry(), api_tx:read_result()};
        (tx_tlog:tlog_entry(), client_key(), Op::random_from_list, EnDecode::boolean())
            -> {tx_tlog:tlog_entry(), api_tx:read_random_from_list_result()};
        (tx_tlog:tlog_entry(), client_key(), Op::{sublist, Start::pos_integer() | neg_integer(), Len::integer()}, EnDecode::boolean())
            -> {tx_tlog:tlog_entry(), api_tx:read_sublist_result()}.
extract_from_tlog(Entry, _Key, read, EnDecode) ->
    {ValType, EncVal} = tx_tlog:get_entry_value(Entry),
    Res = case ValType of
              ?value -> {ok, ?IIF(EnDecode, rdht_tx:decode_value(EncVal), EncVal)};
              ?not_found -> {fail, not_found}
          end,
    {Entry, Res};
extract_from_tlog(Entry, _Key, Op, EnDecode) ->
    {ValType, EncVal} = tx_tlog:get_entry_value(Entry),
    % note: Value and ValType can either contain the partial read or a failure
    %       from the requested op - nothing else is possible since otherwise a
    %       full read would have been executed!
    case ValType of
        ?partial_value ->
            ClientVal =
                case Op of
                    random_from_list ->
                        ?IIF(EnDecode, rdht_tx:decode_value(EncVal), EncVal);
                    {sublist, _Start, _Len} ->
                        ?IIF(EnDecode, rdht_tx:decode_value(EncVal), EncVal)
                end,
            {Entry, {ok, ClientVal}};
        ?value ->
            case Op of
                random_from_list ->
                    Value = rdht_tx:decode_value(EncVal),
                    case Value of
                        [_|_] ->
                            DecodedVal = util:randomelem_and_length(Value),
                            % note: if not EnDecode is given, an encoded value is
                            % expected like the original value!
                            ResVal = ?IIF(not EnDecode, rdht_tx:encode_value(DecodedVal),
                                          DecodedVal),
                            {Entry, {ok, ResVal}};
                        _ ->
                            Res = case Value of
                                      []        -> {fail, empty_list};
                                      _         -> {fail, not_a_list}
                                  end,
                            {tx_tlog:set_entry_status(Entry, ?fail), Res}
                    end;
                {sublist, Start, Len} ->
                    Value = rdht_tx:decode_value(EncVal),
                    if is_list(Value) ->
                           DecodedVal = util:sublist(Value, Start, Len),
                           % note: if not EnDecode is given, an encoded value is
                           % expected like the original value!
                            ResVal = ?IIF(not EnDecode, rdht_tx:encode_value(DecodedVal),
                                          DecodedVal),
                           {Entry, {ok, ResVal}};
                       true ->
                           {tx_tlog:set_entry_status(Entry, ?fail),
                            {fail, not_a_list}}
                    end
            end;
        ?not_found ->
            Res = {fail, not_found},
            case Op of
                random_from_list ->
                    {tx_tlog:set_entry_status(Entry, ?fail), Res};
                {sublist, _Start, _Len} ->
                    {tx_tlog:set_entry_status(Entry, ?fail), Res};
                _ -> {Entry, Res}
            end;
        {?fail, ?empty_list} when Op =:= random_from_list -> {Entry, {fail, empty_list}};
        {?fail, ?not_a_list} -> {Entry, {fail, not_a_list}}
    end.

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
-spec validate(db_dht:db(), tx_tlog:snap_number(), tx_tlog:tlog_entry()) ->
    {db_dht:db(), ?prepared | ?abort}.
validate(DB, OwnSnapNumber, RTLogEntry) ->
    ?TRACE("rdht_tx_read:validate)~n", []),
    %% contact DB to check entry
    DBEntry = db_dht:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),
    VersionOK =
        (tx_tlog:get_entry_version(RTLogEntry)
         >= db_entry:get_version(DBEntry)),
    Lockable = (false =:= db_entry:get_writelock(DBEntry)),
    TLogSnapNo = tx_tlog:get_entry_snapshot(RTLogEntry),
    SnapNumbersOK = (TLogSnapNo >= OwnSnapNumber),
    if VersionOK andalso Lockable andalso SnapNumbersOK ->
           %% if a snapshot instance is running, copy old value to snapshot db before setting lock
           %% set locks on entry
           NewEntry = db_entry:inc_readlock(DBEntry),
           NewDB = db_dht:set_entry(DB, NewEntry, TLogSnapNo, OwnSnapNumber),
           {NewDB, ?prepared};
       true ->
           {DB, ?abort}
    end.

-spec commit(db_dht:db(), tx_tlog:tlog_entry(), OwnProposalWas::?prepared | ?abort,
             tx_tlog:snap_number(), tx_tlog:snap_number()) -> db_dht:db().
commit(DB, _RTLogEntry, ?abort, _TMSnapNo, _OwnSnapNo) ->
    %% own proposal was abort, so no lock to clean up
    %% we could compare DB with RTLogEntry and update if outdated
    %% as this commit confirms the status of a majority of the
    %% replicas. Could also be possible already in the validate req?
    DB;
commit(DB, RTLogEntry, ?prepared, _TMSnapNo, OwnSnapNo) ->
    ?TRACE("rdht_tx_read:commit)~n", []),
    %% perform op: nothing to do for 'read'
    %% release locks
    DBEntry = db_dht:get_entry(DB, tx_tlog:get_entry_key(RTLogEntry)),
    RTLogVers = tx_tlog:get_entry_version(RTLogEntry),
    DBVers = db_entry:get_version(DBEntry),
    if RTLogVers =:= DBVers ->
            NewEntry = db_entry:dec_readlock(DBEntry),
            TLogSnapNo = tx_tlog:get_entry_snapshot(RTLogEntry),
            db_dht:set_entry(DB, NewEntry, TLogSnapNo, OwnSnapNo);
        true -> DB %% a write has already deleted this lock
    end.

-spec abort(db_dht:db(), tx_tlog:tlog_entry(), ?prepared | ?abort,
            tx_tlog:snap_number(), tx_tlog:snap_number()) -> db_dht:db().
abort(DB, RTLogEntry, OwnProposalWas, TMSnapNo, OwnSnapNo) ->
    ?TRACE("rdht_tx_read:abort)~n", []),
    %% same as when committing
    commit(DB, RTLogEntry, OwnProposalWas, TMSnapNo, OwnSnapNo).


%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [],
                             [{pid_groups_join_as, DHTNodeGroup, ?MODULE}]).

-type state() :: {pos_integer(), pos_integer(), pos_integer(), atom()}.

%% initialize: return initial state.
-spec init([]) -> state().
init([]) ->
    ?TRACE("rdht_tx_read: Starting rdht_tx_read for DHT node: ~p~n",
           [pid_groups:my_groupname()]),
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = erlang:list_to_atom(pid_groups:group_to_filename(pid_groups:my_groupname()) ++ "_rdht_tx_read"),
    %%Table = pdb:new(TableName, [set, protected, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    Table = pdb:new(?MODULE, [set]),
    Reps = config:read(replication_factor),
    MajOk = quorum:majority_for_accept(Reps),
    MajDeny = quorum:majority_for_deny(Reps),

    _State = {Reps, MajOk, MajDeny, Table}.

-spec on(comm:message(), state()) -> state().
%% reply triggered by api_dht_raw:unreliable_get_key/3
on({?read_op_with_id_reply, Id, SnapNumber, Ok_Fail, Val_Reason, Vers},
   {Reps, MajOk, MajDeny, Table} = State) ->
    ?TRACE("~p rdht_tx_read:on(get_key_with_id_reply) ID ~p~n", [self(), Id]),
    Entry = get_entry(Id, Table),
    %% @todo inform sender when its entry is outdated?
    %% @todo inform former sender on outdated entry when we
    %% get a newer entry?
    TmpEntry = state_add_reply(Entry, {Ok_Fail, Val_Reason, Vers}, SnapNumber),
    _ = case state_get_client(TmpEntry) of
            unknown ->
                %% when we get a client, we will inform it
                pdb:set(TmpEntry, Table);
            Client ->
                decide_set_and_inform_client_if_ready(Client, TmpEntry, Reps, MajOk, MajDeny, Table)
        end,
    State;

%% triggered by ?MODULE:work_phase/3
on({client_is, Id, Pid, Key, Op}, {Reps, MajOk, MajDeny, Table} = State) ->
    ?TRACE("~p rdht_tx_read:on(client_is)~n", [self()]),
    Entry = get_entry(Id, Table),
    Tmp0Entry = state_set_op(Entry, Op),
    Tmp1Entry = state_set_client(Tmp0Entry, Pid),
    TmpEntry = state_set_key(Tmp1Entry, Key),
    decide_set_and_inform_client_if_ready(Pid, TmpEntry, Reps, MajOk, MajDeny, Table),
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
    State;

%% used by new tx protocol
on({work_phase_done, ClientPid, Key, Op,
    {work_phase_async_done, Id, {qread_done, _, _, _, {Val, Vers}}}},
   {_Reps, _MajOk, _MajDeny, Table} = State) ->
    Entry = get_entry(Id, Table),
    T1 = state_set_op(Entry, Op),
    T2 = state_set_client(T1, ClientPid),
    T3 = state_set_key(T2, Key),

    T4 = state_set_result(T3, {?ok, Val, Vers}),
%%    log:log("Seeing: ~p ~p~n", [Val, Vers]),
    ValType = case Vers of
                  -1 -> ?not_found;
                  _ -> ?value
              end,
    T5 = case ValType of
             ?not_found ->
                 case Op of
                     {?sublist, _, _} ->
                         state_set_decided(T4, ?fail);
                     ?random_from_list ->
                         state_set_decided(T4, ?fail);
                     _ ->
                         state_set_decided(T4, ?ok)
                 end;
             ?value -> state_set_decided(T4, ?ok)
         end,
    set_and_inform_client(ClientPid, T5, 1, Table, ValType),
    State.

-spec get_entry(rdht_tx:req_id(), atom()) -> read_state().
get_entry(Id, Table) ->
    case pdb:get(Id, Table) of
        undefined ->
%%            msg_delay:send_local(config:read(transaction_lookup_timeout) div 1000,
%%                                 self(), {timeout_id, Id}),
            state_new(Id);
        Any -> Any
    end.

% -spec timeout_inform(read_state()) -> ok.
% %% inform client on timeout if Id exists and client is not informed yet
% timeout_inform(Entry) ->
%     case {state_is_client_informed(Entry),
%           state_get_client(Entry)} of
%         {_, unknown} -> ok;
%         {false, Client} ->
%             TmpEntry = state_set_decided(Entry, {fail, timeout}),
%             inform_client(Client, TmpEntry);
%         _ -> ok
%     end.

%% @doc Takes a decision (if possible) and informs the client once a decision
%%      has been taken and the client is not
%%      informed yet. Also sets the state into the pdb.
-spec decide_set_and_inform_client_if_ready(pid(), read_state(), Reps::pos_integer(),
                                     MajOk::pos_integer(), MajDeny::pos_integer(),
                                     Table::atom()) -> ok.
decide_set_and_inform_client_if_ready(Client, Entry, Reps, MajOk, MajDeny, Table) ->
    case state_is_client_informed(Entry) of
        false ->
            % false = state_get_decided(Entry), % should always be true here!
            NumReplied = state_get_numreplied(Entry),

            % if majority replied, we can given an answer!
            % (but: need all Reps replicas for not_found)
            if NumReplied >= MajOk ->
                   {Ok_Fail, Val_Reason, Vers} = state_get_result(Entry),
                   % ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}
                   Op = state_get_op(Entry),
                   if Vers >= 0 ->
                          NumAbort = state_get_numfailed(Entry),
                          % note: MajDeny =:= 2, so 2 times not_found results
                          % in {fail, not_found} although the 3rd reply may have
                          % contained an actual value. This is to make sure that
                          % if 'a' is written, then 'b' and then 'c', no matter
                          % what happens during the write of 'c', 'a' is never
                          % read again!
                          % Example: b is written       => (a, b, b, b), then
                          %          c is being written => (a, b+wl, b+wl, c)
                          %          (crash during write), then
                          %          churn happens,     => (a, not_found, not_found, c)
                          %          => could read (a, not_found, not_found)
                          %             but should not return a!
                          % note: 2 failures are normally not supported but we
                          % want to be on the safe side here
                          Entry2 =
                              if NumAbort >= MajDeny andalso Op =:= ?write ->
                                     % note: there is no error for write ops so
                                     % far (the user does not get a value) but
                                     % we can send our known version for write
                                     % ops to be committed
                                     ValType = ?value_dropped,
                                     state_set_decided(Entry, Ok_Fail);
                                 NumAbort >= MajDeny ->
                                     % note: not_found is reported to the user,
                                     % but we know there is a newer version
                                     % -> decide for fail
                                     % also make sure to report the according
                                     % result, especially the version!
                                     Entry1 = state_set_decided(Entry, ?fail),
                                     ValType = ?not_found,
                                     state_set_result(Entry1, {Ok_Fail, empty_val, -1});
                                 Ok_Fail =:= ?ok ->
                                      NumOk = state_get_numok(Entry),
                                      ValType = if Op =:= ?read andalso NumOk >= MajDeny ->
                                                        %% a value visible in every majority r=4: at least in 2 visible
                                                        ?value;
                                                   Op =:= ?read ->
                                 %% maybe a value not visible in every majority

                                 %% (a) if we return not_found a read after a
                                 %% write could return not_found, which is wrong
                                 %% (majority has value, but not yet the
                                 %% complete remaining minority).

                                 %% (b) Returning the partly written value
                                 %% (persistent in a minority of the replicas)
                                 %% is also problematic, a subsequent read may
                                 %% return not_found when it accesses other
                                 %% replicas.

                                 %% We have no correct solution here.
                                 %% The solution is done in the new
                                 %% rbrcseq.

                                 %% We return the value as
                                 %% api_tx_SUITE:random_write_read needs it.

                                                        %% ?not_found;
                                                        ?value;
                                                   true ->
                                                        ?partial_value
                                               end,
                                     state_set_decided(Entry, Ok_Fail);
                                 Ok_Fail =:= ?fail ->
                                     ValType = {?fail, Val_Reason},
                                     state_set_decided(Entry, Ok_Fail)
                              end,
                          % a decision was taken in any of the cases
                          % -> inform the client
                          set_and_inform_client(Client, Entry2, Reps, Table, ValType);
                      true ->
                          if NumReplied =:= Reps ->
                                 % all replied with -1
                                 ValType = if Op =:= ?write -> ?value_dropped;
                                              true          -> ?not_found
                                           end,
                                 Entry2 = state_set_decided(Entry, Ok_Fail),
                                 set_and_inform_client(Client, Entry2, Reps, Table, ValType);
                             true -> pdb:set(Entry, Table)
                          end
                   end;
               true -> pdb:set(Entry, Table)
            end;
        true -> set_or_delete_if_all_replied(Entry, Reps, Table)
    end.

%% @doc Informs the client, updates the read state accordingly and sets it in
%%      the pdb.
-spec set_and_inform_client(pid(), read_state_decided(), Reps::pos_integer(),
                            Table::atom(), ValType::?value | ?partial_value | ?not_found | ?value_dropped)
        -> ok.
set_and_inform_client(Client, Entry, Reps, Table, ValType) ->
    Id = state_get_id(Entry),
    TLogEntry = make_tlog_entry(Entry, ValType),
    Msg = msg_reply(Id, TLogEntry),
    comm:send_local(Client, Msg),
    Entry2 = state_set_client_informed(Entry),
    set_or_delete_if_all_replied(Entry2, Reps, Table).

-compile({nowarn_unused_function, {make_tlog_entry_feeder, 2}}).
-spec make_tlog_entry_feeder(
        read_state_decided(), {ValType::?value | ?partial_value | ?not_found | ?value_dropped, ?RT:key()})
        -> {read_state_decided(), ?value | ?partial_value | ?not_found | ?value_dropped}.
make_tlog_entry_feeder(Entry, {ValType, Key}) ->
    %% when make_tlog_entry is called, the key of the state is never
    %% 'unknown'
    {state_set_key(Entry, Key), ValType}.

%% @doc Creates a tlog entry from a read state.
%% Pre: a decision must be present in the read state
-spec make_tlog_entry(read_state_decided(), ValType::?value | ?partial_value | ?not_found | ?value_dropped)
        -> tx_tlog:tlog_entry().
make_tlog_entry(Entry, ValType) ->
    {_, Val0, Vers} = state_get_result(Entry),
    Key = state_get_key(Entry),
    Status = state_get_decided(Entry),
    Val = case ValType of
              ?value_dropped -> ?value_dropped;
              ?not_found     -> ?value_dropped;
              {?fail, _}     -> ?value_dropped;
              _              -> Val0
          end,
    SnapNumber = state_get_snapshot_number(Entry),
    tx_tlog:new_entry(?read, Key, Vers, Status, SnapNumber, ValType, Val).

-spec set_or_delete_if_all_replied(read_state_decided(),
                                   pos_integer(), atom()) -> ok.
set_or_delete_if_all_replied(Entry, Reps, Table) ->
    ?TRACE("rdht_tx_read:delete_if_all_replied Reps: ~p =?= ~p, ClientInformed: ~p Client: ~p~n",
              [Reps, state_get_numreplied(Entry), state_is_client_informed(Entry), state_get_client(Entry)]),
    case (Reps =:= state_get_numreplied(Entry))
        andalso (state_is_client_informed(Entry)) of
        true  -> pdb:delete(state_get_id(Entry), Table);
        false -> pdb:set(Entry, Table)
    end.

%% @doc Checks whether config parameters for rdht_tx_read exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(replication_factor) and
    config:cfg_is_greater_than(replication_factor, 0).
