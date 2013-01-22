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
-export([work_phase/3, work_phase_key/5,
         validate_prefilter/1, validate/2,
         commit/3, abort/3,
         extract_from_value/3, extract_from_tlog/4]).

-behaviour(gen_component).
-export([init/1, on/2]).

-export([start_link/1]).
-export([check_config/0]).

% feeder for tester
-export([extract_from_tlog_feeder/4]).

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
            size(Request) =:= 3 ->
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

-spec work_phase_key(pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                     client_key(), ?RT:key(),
                     Op::?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()})
        -> ok.
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
        (rdht_tx:encoded_value(), ?DB:version(), Op::?read) -> Result::{?ok, rdht_tx:encoded_value(), ?DB:version()};
        (rdht_tx:encoded_value(), ?DB:version(), Op::?random_from_list)
            -> Result::{?ok, rdht_tx:encoded_value(), ?DB:version()} | {fail, empty_list | not_a_list, ?DB:version()};
        (rdht_tx:encoded_value(), ?DB:version(), Op::{?sublist, Start::pos_integer() | neg_integer(), Len::integer()})
            -> Result::{?ok, rdht_tx:encoded_value(), ?DB:version()} | {fail, not_a_list, ?DB:version()};
        (rdht_tx:encoded_value(), ?DB:version(), Op::?write) -> Result::{?ok, ?value_dropped, ?DB:version()};
        (empty_val, -1, Op::?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()})
            -> Result::{?ok, ?value_dropped, -1}.
extract_from_value(empty_val, Version = -1, _Op) ->
    {?ok, ?value_dropped, Version}; % will be handled later
extract_from_value(Value, Version, ?read) ->
    {?ok, Value, Version};
extract_from_value(_Value, Version, ?write) ->
    {?ok, ?value_dropped, Version};
extract_from_value(ValueEnc, Version, ?random_from_list) ->
    Value = rdht_tx:decode_value(ValueEnc),
    case Value of
        [_|_]     -> RandVal_ListLen = util:randomelem_and_length(Value),
                     {?ok, rdht_tx:encode_value(RandVal_ListLen), Version};
        []        -> {fail, empty_list, Version};
        _         -> {fail, not_a_list, Version}
    end;
extract_from_value(ValueEnc, Version, {?sublist, Start, Len}) ->
    Value = rdht_tx:decode_value(ValueEnc),
    if is_list(Value) ->
           SubList_ListLen = util:sublist(Value, Start, Len),
           {?ok, rdht_tx:encode_value(SubList_ListLen), Version};
       true ->
           {fail, not_a_list, Version}
    end.

-spec extract_from_tlog_feeder(
        tx_tlog:tlog_entry(), client_key(),
        Op::read | random_from_list | {sublist, Start::pos_integer() | neg_integer(), Len::integer()},
        {EnDecode::boolean(), ListLength::pos_integer()})
        -> {tx_tlog:tlog_entry(), client_key(),
            Op::read | random_from_list | {sublist, Start::pos_integer() | neg_integer(), Len::integer()},
            EnDecode::boolean()}.
extract_from_tlog_feeder(Entry, Key, read = Op, {EnDecode, _ListLength}) ->
    NewEntry =
        case tx_tlog:get_entry_status(Entry) of
            ?partial_value -> % only ?value allowed here
                tx_tlog:set_entry_status(Entry, ?value);
            _ -> Entry
        end,
    {NewEntry, Key, Op, EnDecode};
extract_from_tlog_feeder(Entry, Key, random_from_list = Op, {EnDecode, ListLength}) ->
    NewEntry =
        case tx_tlog:get_entry_status(Entry) of
            ?partial_value -> % need partial value according to random_from_list op!
                PartialValue = rdht_tx:encode_value({tx_tlog:get_entry_value(Entry), ListLength}),
                tx_tlog:set_entry_value(Entry, PartialValue);
            _ -> Entry
        end,
    {NewEntry, Key, Op, EnDecode};
extract_from_tlog_feeder(Entry, Key, {sublist, _Start, _Len} = Op, {EnDecode, ListLength}) ->
    NewEntry =
        case tx_tlog:get_entry_status(Entry) of
            ?partial_value -> % need partial value according to sublist op!
                PartialValue = rdht_tx:encode_value({[tx_tlog:get_entry_value(Entry)], ListLength}),
                tx_tlog:set_entry_value(Entry, PartialValue);
            _ -> Entry
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
    Res = case tx_tlog:get_entry_status(Entry) of
              ?value -> {ok, tx_tlog:get_entry_value(Entry)};
              {fail, not_found} = R -> R; %% not_found
              %% try reading from a failed entry (type mismatch was the reason?)
              {fail, Reason} when is_atom(Reason) -> {ok, tx_tlog:get_entry_value(Entry)}
          end,
    {Entry, ?IIF(EnDecode, rdht_tx:decode_result(Res), Res)};
extract_from_tlog(Entry, Key, Op, EnDecode) ->
    case tx_tlog:get_entry_status(Entry) of
        ?partial_value ->
            % this MUST BE the partial value from this op!
            % (otherwise a full read would have been executed)
            ClientVal =
                case Op of
                    random_from_list ->
                        EncodedVal = tx_tlog:get_entry_value(Entry),
                        ?IIF(EnDecode, rdht_tx:decode_value(EncodedVal), EncodedVal);
                    {sublist, _Start, _Len} ->
                        EncodedVal = tx_tlog:get_entry_value(Entry),
                        ?IIF(EnDecode, rdht_tx:decode_value(EncodedVal), EncodedVal)
                end,
            {Entry, {ok, ClientVal}};
        ?value ->
            extract_partial_from_full(Entry, Key, Op, EnDecode);
        {fail, not_found} = R -> {Entry, R}; %% not_found
        % note: the following two error states depend on the actual op, if a
        %       state is not defined for an op, the generic handler below must
        %       be used!
        {fail, empty_list} = R when Op =:= random_from_list -> {Entry, R};
        {fail, not_a_list} = R -> {Entry, R};
        %% try reading from a failed entry (type mismatch was the reason?)
        % any other failure should work like ?value or ?partial_value since it
        % is from another operation and thus independent from this one
        % -> there is no other potential failure at the moment though
        % TODO: don't know whether it is a partial or full value!!
        % -> at the moment, though, any other failure can only contain a full
        %    value since reads only fail with not_found and other ops write a
        %    full value
        % NOTE: this is not true any more! e.g. add_on_nr may fail with {fail, abort}
        % -> this will lead to wrong results for partial read ops, e.g. the following
        %    will result in {fail,not_a_list} which is not correct:
        %    api_tx:write("a",  [42])
        %    {T1, _} = api_tx:req_list([{add_on_nr,"a",[{[[]],[{1.977014147676681}],[]}]}]).
        %    {T2, _} = api_tx:req_list(T1, [{read,"a",random_from_list}]).
        {fail, Reason} when is_atom(Reason) ->
            extract_partial_from_full(Entry, Key, Op, EnDecode)
    end.

%% @doc Helper for extract_from_tlog/4, applying the partial read op on a tlog
%%      entry with a full value.
-spec extract_partial_from_full
        (tx_tlog:tlog_entry(), client_key(), Op::random_from_list, EnDecode::boolean())
            -> {tx_tlog:tlog_entry(), api_tx:read_random_from_list_result()};
        (tx_tlog:tlog_entry(), client_key(), Op::{sublist, Start::pos_integer() | neg_integer(), Len::integer()}, EnDecode::boolean())
            -> {tx_tlog:tlog_entry(), api_tx:read_sublist_result()}.
extract_partial_from_full(Entry, _Key, random_from_list, EnDecode) ->
    Value = rdht_tx:decode_value(tx_tlog:get_entry_value(Entry)),
    case Value of
        [_|_]     ->
            DecodedVal = util:randomelem_and_length(Value),
            % note: if not EnDecode is given, an encoded value is
            % expected like the original value!
            {Entry,
             {ok, ?IIF(not EnDecode, rdht_tx:encode_value(DecodedVal), DecodedVal)}};
        _ ->
            Res = case Value of
                      []        -> {fail, empty_list};
                      _         -> {fail, not_a_list}
                  end,
            {tx_tlog:set_entry_status(Entry, {fail, abort}), Res}
    end;
extract_partial_from_full(Entry, _Key, {sublist, Start, Len}, EnDecode) ->
    Value = rdht_tx:decode_value(tx_tlog:get_entry_value(Entry)),
    if is_list(Value) ->
           DecodedVal = util:sublist(Value, Start, Len),
           % note: if not EnDecode is given, an encoded value is
           % expected like the original value!
           {Entry,
            {ok, ?IIF(not EnDecode, rdht_tx:encode_value(DecodedVal), DecodedVal)}};
       true ->
           {tx_tlog:set_entry_status(Entry, {fail, abort}),
            {fail, not_a_list}}
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

-type state() :: {pos_integer(), pos_integer(), pos_integer(), atom()}.

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
on({?read_op_with_id_reply, Id, Ok_Fail, Val_Reason, Vers},
   {Reps, MajOk, MajDeny, Table} = State) ->
    ?TRACE("~p rdht_tx_read:on(get_key_with_id_reply) ID ~p~n", [self(), Id]),
    Entry = get_entry(Id, Table),
    %% @todo inform sender when its entry is outdated?
    %% @todo inform former sender on outdated entry when we
    %% get a newer entry?
    TmpEntry = state_add_reply(Entry, {Ok_Fail, Val_Reason, Vers}),
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
                   {Ok_Fail, _Val, Vers} = state_get_result(Entry),
                   % ?read | ?write | ?random_from_list | {?sublist, Start::pos_integer() | neg_integer(), Len::integer()}
                   Op = state_get_op(Entry),
                   if Vers =/= -1 ->
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
                                     % note: report not_found but keep the last
                                     % reported version so that write operations
                                     % can be executed (the not_found is not
                                     % reported to the user so we can do this!)
                                     state_set_decided(Entry, {fail, not_found});
                                 NumAbort >= MajDeny ->
                                     % note: not_found is reported to the user,
                                     % so we also need to report the according
                                     % result, especially the version!
                                     Entry1 = state_set_decided(Entry, {fail, not_found}),
                                     state_set_result(Entry1, {?ok, empty_val, -1});
                                 Ok_Fail =:= ?ok andalso Op =:= ?read ->
                                     state_set_decided(Entry, ?value);
                                 Ok_Fail =:= ?ok ->
                                     % all other read ops are partial reads!
                                     state_set_decided(Entry, ?partial_value);
                                 true ->
                                     state_set_decided(Entry, {fail, abort})
                              end,
                          % a decision was taken in any of the cases
                          % -> inform the client
                          set_and_inform_client(Client, Entry2, Reps, Table);
                      true ->
                          if NumReplied =:= Reps ->
                                 % all replied with -1
                                 Entry2 = state_set_decided(Entry, {fail, not_found}),
                                 set_and_inform_client(Client, Entry2, Reps, Table);
                             true -> pdb:set(Entry, Table)
                          end
                   end;
               true -> pdb:set(Entry, Table)
            end;
        true -> pdb:set(Entry, Table)
    end.

%% @doc Informs the client, updates the read state accordingly and sets it in
%%      the pdb.
-spec set_and_inform_client(pid(), read_state(), Reps::pos_integer(),
                                     Table::atom()) -> ok.
set_and_inform_client(Client, Entry, Reps, Table) ->
    Id = state_get_id(Entry),
    Msg = msg_reply(Id, make_tlog_entry(Entry)),
    comm:send_local(Client, Msg),
    Entry2 = state_set_client_informed(Entry),
    set_or_delete_if_all_replied(Entry2, Reps, Table).

%% -spec make_tlog_entry_feeder(
%%         {read_state(), ?RT:key()})
%%                             -> {read_state()}.
%% make_tlog_entry_feeder({State, Key}) ->
%%     %% when make_tlog_entry is called, the key of the state is never
%%     %% 'unknown'
%%     {state_set_key(State, Key)}.

-spec make_tlog_entry(read_state()) ->
                                tx_tlog:tlog_entry().
make_tlog_entry(Entry) ->
    {_, Val, Vers} = state_get_result(Entry),
    Key = state_get_key(Entry),
    Status = state_get_decided(Entry),
    tx_tlog:new_entry(?read, Key, Vers, Status, Val).

-spec set_or_delete_if_all_replied(read_state(),
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
    config:cfg_is_integer(quorum_factor) and
    config:cfg_is_greater_than(quorum_factor, 0) and
    config:cfg_is_integer(replication_factor) and
    config:cfg_is_greater_than(replication_factor, 0).
