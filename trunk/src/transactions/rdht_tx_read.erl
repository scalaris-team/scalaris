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
-export([extract_from_value_feeder/3, extract_from_tlog_feeder/4]).

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
                    random_from_list -> ?random_from_list
                end;
            true -> ?read
         end,
    HashedKey = ?RT:hash_key(Key),
    work_phase_key(ClientPid, ReqId, Key, HashedKey, Op).

-spec work_phase_key(pid(), rdht_tx:req_id() | rdht_tx_write:req_id(),
                     client_key(), ?RT:key(), Op::?read | ?write | ?random_from_list) -> ok.
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
                  HashedKey::?RT:key(), Op::?read | ?write | ?random_from_list) -> ok.
quorum_read(CollectorPid, ReqId, HashedKey, Op) ->
    ?TRACE("rdht_tx_read:quorum_read ~p Collector: ~p~n", [self(), CollectorPid]),
    RKeys = ?RT:get_replica_keys(HashedKey),
    _ = [ api_dht_raw:unreliable_lookup(
            RKey, {?read_op, CollectorPid, ReqId, RKey, Op})
        || RKey <- RKeys ],
    ok.

-spec extract_from_value_feeder
        (?DB:value(), ?DB:version(), Op::?read | ?write | ?random_from_list) -> {?DB:value(), ?DB:version(), Op::?read | ?write | ?random_from_list};
        (empty_val, -1, Op::?read | ?write | ?random_from_list) -> {empty_val, -1, Op::?read | ?write | ?random_from_list}.
extract_from_value_feeder(empty_val = Value, Version, Op) ->
    {Value, Version, Op};
extract_from_value_feeder(Value, Version, ?random_from_list = Op) ->
    {rdht_tx:encode_value(Value), Version, Op}; % need a valid encoded value!
extract_from_value_feeder(Value, Version, Op) ->
    {Value, Version, Op}.

%% @doc Performs the requested operation in the dht_node context.
-spec extract_from_value
        (?DB:value(), ?DB:version(), Op::?read | ?random_from_list) -> Result::{ok, ?DB:value(), ?DB:version()} | {fail, empty_list | not_a_list, ?DB:version()};
        (?DB:value(), ?DB:version(), Op::?write) -> Result::{ok, ?value_dropped, ?DB:version()};
        (empty_val, -1, Op::?read | ?random_from_list) -> Result::{ok, empty_val, -1};
        (empty_val, -1, Op::?write) -> Result::{ok, ?value_dropped, -1}.
extract_from_value(Value, Version, ?read) ->
    {ok, Value, Version};
extract_from_value(_Value, Version, ?write) ->
    {ok, ?value_dropped, Version};
extract_from_value(empty_val, Version, ?random_from_list) ->
    {ok, empty_val, Version}; % will be handled later
extract_from_value(ValueEnc, Version, ?random_from_list) ->
    Value = rdht_tx:decode_value(ValueEnc),
    case Value of
        [_|_]     -> {RandVal, Len} = util:randomelem_and_length(Value),
                     {ok, {rdht_tx:encode_value(RandVal), Len}, Version};
        []        -> {fail, empty_list, Version};
        _         -> {fail, not_a_list, Version}
    end.

-spec extract_from_tlog_feeder(tx_tlog:tlog_entry(), client_key(), Op::read | random_from_list, {EnDecode::boolean(), ListLenght::pos_integer()})
        -> {tx_tlog:tlog_entry(), client_key(), Op::read | random_from_list, EnDecode::boolean()}.
extract_from_tlog_feeder(Entry, Key, read = Op, {EnDecode, _ListLenght}) ->
    NewEntry =
        case tx_tlog:get_entry_status(Entry) of
            ?partial_value -> % only ?value allowed here
                tx_tlog:set_entry_status(Entry, ?value);
            _ -> Entry
        end,
    {NewEntry, Key, Op, EnDecode};
extract_from_tlog_feeder(Entry, Key, random_from_list = Op, {EnDecode, ListLenght}) ->
    NewEntry =
        case tx_tlog:get_entry_status(Entry) of
            ?partial_value -> % need value partial value!
                PartialValue = {tx_tlog:get_entry_value(Entry), ListLenght},
                tx_tlog:set_entry_value(Entry, PartialValue);
            _ -> Entry
        end,
    {NewEntry, Key, Op, EnDecode}.

%% @doc Get a result entry for a read from the given TLog entry.
-spec extract_from_tlog
        (tx_tlog:tlog_entry(), client_key(), Op::read, EnDecode::boolean())
            -> {tx_tlog:tlog_entry(), api_tx:read_result()};
        (tx_tlog:tlog_entry(), client_key(), Op::random_from_list, EnDecode::boolean())
            -> {tx_tlog:tlog_entry(), api_tx:read_random_from_list_result()}.
extract_from_tlog(Entry, _Key, read, EnDecode) ->
    Res = case tx_tlog:get_entry_status(Entry) of
              ?value -> {ok, tx_tlog:get_entry_value(Entry)};
              {fail, not_found} = R -> R; %% not_found
              %% try reading from a failed entry (type mismatch was the reason?)
              {fail, Reason} when is_atom(Reason) -> {ok, tx_tlog:get_entry_value(Entry)}
          end,
    {Entry, ?IIF(EnDecode, rdht_tx:decode_result(Res), Res)};
extract_from_tlog(Entry, _Key, random_from_list, EnDecode) ->
    case tx_tlog:get_entry_status(Entry) of
        ?partial_value ->
            % this MUST BE the partial value from this op!
            % (otherwise a full read would have been executed)
            PValue = tx_tlog:get_entry_value(Entry),
            {Entry, {ok, ?IIF(EnDecode, rdht_tx:decode_value(PValue), PValue)}};
        ?value ->
            Value = rdht_tx:decode_value(tx_tlog:get_entry_value(Entry)),
            case Value of
                [_|_]     ->
                    {RandVal, Len} = DecodedVal = util:randomelem_and_length(Value),
                    % note: if not EnDecode is given, an encoded value is
                    % expected like the original value!
                    {Entry,
                     {ok, ?IIF(not EnDecode, {rdht_tx:encode_value(RandVal), Len}, DecodedVal)}};
                _ ->
                    Res = case Value of
                              []        -> {fail, empty_list};
                              _         -> {fail, not_a_list}
                          end,
                    {tx_tlog:set_entry_status(Entry, {fail, abort}), Res}
            end;
        {fail, not_found} = R -> {Entry, R}; %% not_found
        {fail, empty_list} = R -> {Entry, R};
        {fail, not_a_list} = R -> {Entry, R};
        %% try reading from a failed entry (type mismatch was the reason?)
        % any other failure should work like ?value or ?partial_value since it
        % is from another operation and thus independent from this one
        % -> there is no other potential failure at the moment though
        % TODO: don't know whether it is a partial or full value!!
        % -> at the moment, though, any other failure can only contain a full
        %    value since reads only fail with not_found and other ops write a
        %    full value
        {fail, Reason} when is_atom(Reason) -> 
            Value = rdht_tx:decode_value(tx_tlog:get_entry_value(Entry)),
            case Value of
                [_|_]     ->
                    {RandVal, Len} = DecodedVal = util:randomelem_and_length(Value),
                    % note: if not EnDecode is given, an encoded value is
                    % expected like the original value!
                    {Entry,
                     {ok, ?IIF(not EnDecode, {rdht_tx:encode_value(RandVal), Len}, DecodedVal)}};
                _ ->
                    Res = case Value of
                              []        -> {fail, empty_list};
                              _         -> {fail, not_a_list}
                          end,
                    {Entry, Res} % note: entry is already set to abort
            end
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
on({?read_op_with_id_reply, Id, Result},
   {Reps, MajOk, MajDeny, Table} = State) ->
    ?TRACE("~p rdht_tx_read:on(get_key_with_id_reply) ID ~p~n", [self(), Id]),
    Entry = get_entry(Id, Table),
    %% @todo inform sender when its entry is outdated?
    %% @todo inform former sender on outdated entry when we
    %% get a newer entry?
    TmpEntry = state_add_reply(Entry, Result, MajOk, MajDeny),
    _ = case state_get_client(TmpEntry) of
            unknown ->
                %% when we get a client, we will inform it
                pdb:set(TmpEntry, Table);
            Client ->
                NewEntry =
                    case state_is_newly_decided(TmpEntry) of
                        true  -> inform_client(Client, TmpEntry);
                        false -> TmpEntry
                    end,
                set_or_delete_if_all_replied(NewEntry, Reps, Table)
        end,
    State;

%% triggered by ?MODULE:work_phase/3
on({client_is, Id, Pid, Key, Op}, {Reps, _MajOk, _MajDeny, Table} = State) ->
    ?TRACE("~p rdht_tx_read:on(client_is)~n", [self()]),
    Entry = get_entry(Id, Table),
    Tmp0Entry = state_set_op(Entry, Op),
    Tmp1Entry = state_set_client(Tmp0Entry, Pid),
    TmpEntry = state_set_key(Tmp1Entry, Key),
    _ = case state_is_newly_decided(TmpEntry) of
            true ->
                Tmp2Entry = inform_client(Pid, TmpEntry),
                set_or_delete_if_all_replied(Tmp2Entry, Reps, Table);
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

-spec inform_client(pid(), read_state()) ->
                           read_state().
inform_client(Client, Entry) ->
    % if partial read and decided is ?value -> set to ?partial_value!
    % ?read | ?write | ?random_from_list
    Op = state_get_op(Entry),
    Entry2 = if Op =:= ?read -> Entry;
                true ->
                    case state_get_decided(Entry) of
                        ?value -> state_set_decided(Entry, ?partial_value);
                        _ -> Entry
                    end
             end,
    Id = state_get_id(Entry2),
    Msg = msg_reply(Id, make_tlog_entry(Entry2)),
    comm:send_local(Client, Msg),
    state_set_client_informed(Entry2).

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
