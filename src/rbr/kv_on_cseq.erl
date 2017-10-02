% @copyright 2012-2015 Zuse Institute Berlin,

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
%% @doc    key value store based on rbrcseq.
%% @end
%% @version $Id$
-module(kv_on_cseq).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-define(LOG_CC_FAILS, false).
%%-define(LOG_CC_FAILS, true).

-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/1]).
-export([write/2]).

-export([work_phase_async/4]).

-export([set_lock/3]).
-export([commit_read/5, commit_read_feeder/5]).
-export([commit_write/5, commit_write_feeder/5]).
-export([abort_read/5, abort_read_feeder/5]).
-export([abort_write/5, abort_write_feeder/5]).

%% for selection among differing values in quorums with same paxos
%% round (as they can occur in this data type, as we allow partial
%% operations as setting or unlocking write locks (on maybe outdated
%% replicas).
-export([max/2]).

%% filters and checks for rbr_cseq operations
%% consistency
%% -export([is_valid_next_req/3]).
%% read filters
-export([rf_rl_vers/1]).
-export([rf_rl_wl_vers/1]).
-export([rf_val/1]).
-export([rf_val_vers/1]).
-export([rf_wl_vers/1]).

%% content checks
-export([cc_abort_read/3]).
-export([cc_abort_write/3]).
-export([cc_commit_read/3]).
-export([cc_commit_write/3]).
-export([cc_set_rl/3]).
-export([cc_set_wl/3]).
-export([cc_single_write/3]).
-export([cc_return_val/4]).

%% write filters
-export([wf_set_rl/3]).
-export([wf_set_vers_val/3]).
-export([wf_set_wl/3]).
-export([wf_unset_rl/3]).
-export([wf_unset_wl/3]).
-export([wf_val_unset_wl/3]).

-export([get_commuting_wf_for_rf/1]).


-type txid() :: ?RT:key().

-type readlock()  :: [txid()].
-type writelock() :: txid() | no_write_lock.

-type version()   :: non_neg_integer() | -1.
-type value()     :: any().

-type db_entry()  :: { %% plainkey(),
                readlock(),
                writelock(),
                version(),
                value()
               }.

%% @TODO add support for ?random_from_list | {{?sublist, Start::pos_integer() | neg_integer(), Len::integer()}
-spec work_phase_async(comm:erl_local_pid(), any(), ?RT:key(), read | write) -> ok.
work_phase_async(ClientPid, ReqId, HashedKey, _Op) ->
    ReplyTo = comm:reply_as(ClientPid, 3,
                            {work_phase_async_done, ReqId, '_'}),
    rbrcseq:qread(kv_db, ReplyTo, HashedKey, ?MODULE,
                  fun ?MODULE:rf_val_vers/1),
    ok.

-spec rf_val_vers(db_entry() | prbr_bottom) -> {client_value(), version()}.
rf_val_vers(prbr_bottom) -> {?value_dropped, -1};
rf_val_vers(X)          -> {val(X), vers(X)}.



%% %%%%%%%%%%%%%%%%%%%%%%
%% functions for read
%% %%%%%%%%%%%%%%%%%%%%%%
-spec read(client_key()) -> api_tx:read_result().
read(Key) ->
    rbrcseq:qread(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                  fun ?MODULE:rf_val/1),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, _OldWriteRound, Value},
                       case Value of
                           no_value_yet -> {fail, not_found};
                           _ -> {ok, Value}
                           end
                      )
    after 1000 ->
            log:log("read hangs ~p~n", [erlang:process_info(self(), messages)]),
                receive
                    ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, _OldWriteRound, Value},
                                   case Value of
                                       no_value_yet -> {fail, not_found};
                                       _ -> {ok, Value}
                                   end
                                  )
                    end
        end.

-spec rf_val(db_entry() | prbr_bottom) -> client_value().
rf_val(prbr_bottom) -> no_value_yet;
rf_val(X)          -> val(X).





%% %%%%%%%%%%%%%%%%%%%%%%
%% functions for write: this write ensures not to conflict with running tx
%% %%%%%%%%%%%%%%%%%%%%%%
-spec write(client_key(), client_value()) -> api_tx:write_result().
write(Key, Value) ->
    rbrcseq:qwrite(kv_db, self(), ?RT:hash_key(Key), ?MODULE,
                   fun ?MODULE:rf_rl_wl_vers/1,
                   fun ?MODULE:cc_single_write/3,
                   fun ?MODULE:wf_set_vers_val/3, Value),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, _Value, _WriteRet}, {ok}); %%;
        ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                       begin log:log("Write failed on key ~p: ~p~n", [Key, Reason]),
                       {ok} end) %% TODO: extend write_result type {fail, Reason} )
    after 1000 ->
            log:log("~p write hangs at key ~p, ~p~n",
                    [self(), Key, erlang:process_info(self(), messages)]),
            receive
                ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, Value, _WriteRet},
                               begin
                                   log:log("~p write was only slow at key ~p~n",
                                           [self(), Key]),
                                   {ok}
                               end); %%;
                ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                               begin log:log("~p Write failed: ~p~n",
                                             [self(), Reason]),
                                     {ok} end)
                end
    end.

-spec cc_single_write(no_value_yet | {readlock(), writelock(), version()},
                      prbr:write_filter(), Value :: any()) ->
                             {true, UI :: version()} |
                             {false, [readlock_is_set | writelock_is_set]}.
cc_single_write(no_value_yet, _WriteFilter, _Val) ->
    {true, -1};
cc_single_write({RL, WL, Vers}, _WriteFilter, _Val) ->
    Checks = [ { [] =:= RL,    readlock_is_set },
               { no_write_lock =:= WL, writelock_is_set } ],
    cc_return_val(cc_single_write, Checks, _UI_if_ok = Vers, ?LOG_CC_FAILS).

-spec wf_set_vers_val(db_entry() | prbr_bottom, version(),
                      client_value()) -> {db_entry(), none}.
wf_set_vers_val(prbr_bottom, Version, WriteValue) ->
    {{_RL = [], _WL = no_write_lock, Version, WriteValue}, none};
wf_set_vers_val(Entry, Version, WriteValue) ->
    T = set_vers(Entry, Version + 1),
    {set_val(T, WriteValue), none}.




%% %%%%%%%%%%%%%%%%%%%%%%
%% functions for set_lock
%% %%%%%%%%%%%%%%%%%%%%%%

-spec set_lock(tx_tlog:tlog_entry(), txid(), comm:erl_local_pid()) -> ok.
set_lock(TLogEntry, TxId, ReplyTo) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    % TODO: this is potentially unsafe depending on the RT implementation!
    HashedKey = if is_list(Key) -> ?RT:hash_key(Key);
                   true         -> Key
                end,
    case tx_tlog:get_entry_operation(TLogEntry) of
        ?read ->
            ReadFilter = fun ?MODULE:rf_wl_vers/1, %% read lock is irrelevant for reads
            ContentCheck = fun ?MODULE:cc_set_rl/3,
            WriteFilter = fun ?MODULE:wf_set_rl/3,
            rbrcseq:qwrite(kv_db, ReplyTo, HashedKey, ?MODULE,
                           ReadFilter,
                           ContentCheck,
                           WriteFilter,
                           _Value = {TxId,
                                     tx_tlog:get_entry_version(TLogEntry)});
        ?write ->
            ReadFilter = fun ?MODULE:rf_rl_wl_vers/1,
            ContentCheck = fun ?MODULE:cc_set_wl/3,
            WriteFilter = fun ?MODULE:wf_set_wl/3,
            rbrcseq:qwrite(kv_db, ReplyTo, HashedKey, ?MODULE,
                           ReadFilter,
                           ContentCheck,
                           WriteFilter, _Value = {TxId,
                                                  tx_tlog:get_entry_version(TLogEntry)})
    end.

%% set_lock read: read filter, content check and write filter
-spec rf_wl_vers(db_entry() | prbr_bottom) -> {writelock(), version()} | no_value_yet.
rf_wl_vers(prbr_bottom) -> no_value_yet;
rf_wl_vers(X) -> {writelock(X), vers(X)}.

-spec cc_set_rl(no_value_yet | {writelock(), version()}, prbr:write_filter(),
                {txid_on_cseq:txid(), version()}) ->
                       {true, UI :: none} |
                       {false, any()}.
cc_set_rl(no_value_yet, _WF, _Val = {_TxId, _TLogVers}) ->
    {true, none};
cc_set_rl({WL, Vers}, _WF, _Val = {_TxId, TLogVers}) ->
    Checks = [ { TLogVers =:= Vers, {cc_set_rl_version_mismatch, TLogVers, Vers} },
               { no_write_lock =:= WL,      cc_set_rl_writelock_is_set } ],
    cc_return_val(cc_set_rl, Checks, _UI_if_ok = none, ?LOG_CC_FAILS).

-spec wf_set_rl(db_entry(), UI :: {writelock(), version()} | no_value_yet,
                {txid_on_cseq:txid(), version()}) -> {db_entry(), none}.
wf_set_rl(DBEntry, _UI, {TxId, _Vers}) ->
    {set_readlock(DBEntry, TxId), none}.


%% set_lock write: read filter, content check and write filter
-spec rf_rl_wl_vers(db_entry() | prbr_bottom) ->
        {readlock(), writelock(), version()} | no_value_yet. %% drop -1 in the future?
rf_rl_wl_vers(prbr_bottom) -> no_value_yet;
rf_rl_wl_vers(X)          -> {readlock(X),
                              writelock(X),
                              vers(X)}.

-spec cc_set_wl({readlock, writelock(), version()}, prbr:write_filter(),
                {txid_on_cseq:txid(), version()}) ->
                       {true, UI :: none} |
                       {false, any()}.
cc_set_wl(no_value_yet, _WF, _Val = {_TxId, _TLogVers}) ->
    {true, none};
cc_set_wl({RL, WL, Vers}, _WF, _Val = {TxId, TLogVers}) ->
    Checks =
        [ { TLogVers =:= Vers,
            {cc_set_wl_version_mismatch, TLogVers, Vers} },
          { no_write_lock =:= WL
            %% accept already existing WL from write through
            orelse TxId =:= WL %% old comment (do not rewrite TxId)?
          , {cc_set_wl_txid_mismatch, WL, TxId} },
          { [] =:= RL,
            {cc_set_wl_readlock_not_empty, RL} } ],
    cc_return_val(cc_set_wl, Checks, _UI_if_ok = none, ?LOG_CC_FAILS).

-spec wf_set_wl
       (prbr_bottom, UI :: none, {txid_on_cseq:txid(), -1}) -> {db_entry(), none};
       (db_entry(),  UI :: none, {txid_on_cseq:txid(), version()}) -> {db_entry(), none}.
wf_set_wl(prbr_bottom, _UI = none, {TxId, -1}) ->
    {set_writelock(new_entry(), TxId), none};
wf_set_wl(prbr_bottom, _UI = none, {TxId, _}) ->
%%    ct:pal("should only happen in tester unittests~n"),
    {set_writelock(new_entry(), TxId), none};
wf_set_wl(DBEntry, _UI = none, {TxId, _TLogVers}) ->
    {set_writelock(DBEntry, TxId), none}.

%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% functions for commit_read
%% %%%%%%%%%%%%%%%%%%%%%%%%%
-spec commit_read_feeder(tx_tlog:tlog_entry(), txid(), comm:erl_local_pid(),
                         pr:pr(), {txid_on_cseq:txid(), version()}) ->
                                {tx_tlog:tlog_entry(),
                                 txid(),
                                 comm:erl_local_pid(),
                                 pr:pr(),
                                 {txid_on_cseq:txid(), version()}}.
commit_read_feeder(TLogEntry, TxId, Pid, Round, OldVal) ->
    {TLogEntry, TxId, Pid, Round, OldVal}.

%% @doc Releases the read lock of a given entry, if it was set.
-spec commit_read(tx_tlog:tlog_entry(), txid(), comm:erl_local_pid(),
                  pr:pr(), any()) -> ok.
commit_read(TLogEntry, TxId, ReplyTo, NextRound, OldVal) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    % TODO: this is potentially unsafe depending on the RT implementation!
    HashedKey = if is_list(Key) -> ?RT:hash_key(Key);
                   true         -> Key
                end,
    ReadFilter = fun ?MODULE:rf_rl_vers/1,
    ContentCheck = fun ?MODULE:cc_commit_read/3,
    WriteFilter = fun ?MODULE:wf_unset_rl/3,
    rbrcseq:qwrite_fast(kv_db, ReplyTo, HashedKey, ?MODULE,
                        ReadFilter,
                        ContentCheck,
                        WriteFilter,
                        _Value = {TxId, tx_tlog:get_entry_version(TLogEntry)},
                        NextRound, OldVal),
    ok.

-spec rf_rl_vers(db_entry() | prbr_bottom) ->
                           {readlock(), version()} | no_value_yet.
rf_rl_vers(prbr_bottom) -> no_value_yet;
rf_rl_vers(X) ->
    {readlock(X), vers(X)}.

-spec cc_commit_read(no_value_yet | {readlock, version()}, prbr:write_filter(),
                {txid_on_cseq:txid(), version()}) ->
                            {true, UI :: none} |
                            {false, any()}.
cc_commit_read(no_value_yet, _WF, _Val = {_TxId, _TLogVers}) ->
    {true, none};
cc_commit_read({_RL, Vers}, _WF, _Val = {_TxId, TLogVers}) ->
    Checks =
        [ { TLogVers =:= Vers,  {cc_commit_read_version_mismatch, TLogVers, Vers} } ],
    cc_return_val(cc_commit_read, Checks, _UI_if_ok = none, ?LOG_CC_FAILS);
cc_commit_read({_RL, _WL, Vers}, _WF, _Val = {_TxId, TLogVers}) ->
    Checks =
        [ { TLogVers =:= Vers,  {cc_commit_read_version_mismatch, TLogVers, Vers} } ],
    cc_return_val(cc_commit_read, Checks, _UI_if_ok = none, ?LOG_CC_FAILS).

-spec wf_unset_rl
       (prbr_bottom, UI :: none, {txid_on_cseq:txid(), 0}) -> {prbr_bottom, none};
       (db_entry(), UI :: none, {txid_on_cseq:txid(), version()}) -> {db_entry(), none}.
wf_unset_rl(prbr_bottom, _UI = none, {_TxId, _Vers}) -> {prbr_bottom, none};
wf_unset_rl(DBEntry, _UI = none, {TxId, _TLogVers}) ->
    {unset_readlock(DBEntry, TxId), none}.





%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% functions for commit_write
%% %%%%%%%%%%%%%%%%%%%%%%%%%
-spec commit_write_feeder(tx_tlog:tlog_entry_write(), txid(), comm:erl_local_pid(),
                          pr:pr(), {txid_on_cseq:txid(), version()}) ->
                                 {tx_tlog:tlog_entry_write(), txid(),
                                  comm:erl_local_pid(),
                                  pr:pr(),
                                  {txid_on_cseq:txid(), version()}}.
commit_write_feeder(TLogEntry, TxId, Pid, Round, OldVal) ->
    {_, Val} = tx_tlog:get_entry_value(TLogEntry),
    SaneCommitWriteTLogEntry = tx_tlog:set_entry_value(TLogEntry, ?value, Val),
    {SaneCommitWriteTLogEntry, TxId, Pid, Round, OldVal}.

%% @doc Releases the write lock of a given entry, if it was
%%      set, and writes the new value.
%% erlang shell test call: api_tx:write("a", 1).
-spec commit_write(tx_tlog:tlog_entry_write(), txid(), comm:erl_local_pid(),
                   pr:pr(), any()) -> ok.
commit_write(TLogEntry, TxId, ReplyTo, NextRound, OldVal) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    % TODO: this is potentially unsafe depending on the RT implementation!
    HashedKey = if is_list(Key) -> ?RT:hash_key(Key);
                   true         -> Key
                end,
    ReadFilter = fun ?MODULE:rf_wl_vers/1,
    ContentCheck = fun ?MODULE:cc_commit_write/3,
    WriteFilter = fun ?MODULE:wf_val_unset_wl/3,
    {?value, Value} = tx_tlog:get_entry_value(TLogEntry),
    %% {?value, Value} = tx_tlog:get_entry_value(TLogEntry),
    rbrcseq:qwrite_fast(kv_db, ReplyTo, HashedKey, ?MODULE,
                        ReadFilter,
                        ContentCheck,
                        WriteFilter,
                        _Value = {TxId, tx_tlog:get_entry_version(TLogEntry),
                                  Value},
                        NextRound, OldVal),
    ok.

-spec cc_commit_write(no_value_yet
                      | {writelock(), version()}
                      | {readlock(), writelock(), version()},
                      prbr:write_filter(),
                      {txid_on_cseq:txid(), version(), NewVal :: any()}) ->
                             {true, UI :: none} |
                             {false, Reason :: any()}.
cc_commit_write(no_value_yet, _WF, _Val = {_TxId, _TLogVers, _NewVal}) ->
    {true, none};
cc_commit_write({WL, Vers}, _WF, _Val = {TxId, TLogVers, _NewVal}) ->
    %% normal write:

    %% The system may already went on, as the consensus may already be
    %% free for others when the value was written half and another process
    %% performed a write through.
    %% It is ensured, that our write value was a consensus once, because
    %% otherwise the content checkes of concurrent actions would fail:
    %% (1) this tx was committed, so we proved that we were able to acquire
    %%     the write_lock
    %% (2) only one tx can acquire a write lock.

    %% content check is performed on the latest quorum value, but we
    %% have to accept also the newly written value, which may have
    %% been propagated by a write through of concurrent operation, but
    %% then we do not want to write again (let the cc fail).
    Checks =
        [ {  %% we release the lock
             %% (Vers =:= TLogVers andalso
             (WL =:= TxId)
             orelse (Vers =< TLogVers) %% updated outdated replicas
             %% we were interrupted during the release lock and
             %% another process made a write through? But then the
             %% register is free to be used for further ops... and any
             %% value would be ok? So the outcome of the commit_write
             %% is not important at all?
             %%orelse (Vers >= TLogVers), %% Vers =:= (1 + TLogVers)),
  ,          {cc_commit_write_tlog_version_to_small, WL, TxId, TLogVers, Vers} } ],
    cc_return_val(cc_commit_write, Checks, _UI_if_ok = none, ?LOG_CC_FAILS);


cc_commit_write({_RL, WL, Vers}, _WF, _Val = {TxId, TLogVers, _NewVal}) ->
    %% fast_write:
    %% in case of fast write we get the value of the last read as
    %% write value, which here was produced by the read filter of
    %% set_lock.

    %% As we are in the case of fast_write, we can expect the
    %% writelock to be in place. The version usually matches, but
    %% could be outdated, when the write part of the setting of the
    %% write_lock was successful on a quorum which included an
    %% outdated replica (there, we cannot guarantee anything on the
    %% value). But we will write our own value anyway and habe
    %% exclusive write access, so until we get a quorum updated with
    %% the right value, no one else will substatially interfere.
    %% But what is when decided abort? How to rollback if the values
    %% are no longer consistent across the replicas holding the WL?
    Checks =
        [ { Vers =< TLogVers,  {cc_commit_write_tlog_version_to_small,
                                 TLogVers, Vers} },
          { WL =:= TxId, {cc_commit_write_lock_is_not_txid, WL, TxId} } ],
    cc_return_val(cc_commit_write, Checks, _UI_if_ok = none, ?LOG_CC_FAILS).


-spec wf_val_unset_wl
       (prbr_bottom, UI :: none, {txid_on_cseq:txid(), 0, value()}) -> {prbr_bottom, none};
       (db_entry(), UI :: none, {txid_on_cseq:txid(), version(), value()}) -> {db_entry(), none}.
wf_val_unset_wl(prbr_bottom, _UI = none, {_TxId, _Vers, _Val}) -> {prbr_bottom, none};
wf_val_unset_wl(DBEntry, _UI = none, {_TxId, TLogVers, Val}) ->
    case vers(DBEntry) =< TLogVers of
        true ->
            T1 = set_writelock(DBEntry, no_write_lock),
            T2 = set_val(T1, Val),
            T3 = reset_readlock(T2),
            %% increment version counter on write
            {set_vers(T3, 1 + TLogVers), none};
        _ ->
            {DBEntry, none}
    end.





%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% functions for abort_read
%% %%%%%%%%%%%%%%%%%%%%%%%%%
-spec abort_read_feeder(tx_tlog:tlog_entry(), txid(), comm:erl_local_pid(),
                         pr:pr(), {txid_on_cseq:txid(), version()}) ->
                                {tx_tlog:tlog_entry(),
                                 txid(),
                                 comm:erl_local_pid(),
                                 pr:pr(),
                                 {txid_on_cseq:txid(), version()}}.
abort_read_feeder(TLogEntry, TxId, Pid, Round, OldVal) ->
    {TLogEntry, TxId, Pid, Round, OldVal}.

%% @doc Releases the read lock of a given entry, if it was set.
-spec abort_read(tx_tlog:tlog_entry(), txid(), comm:erl_local_pid(),
                  pr:pr(), any()) -> ok.
abort_read(TLogEntry, TxId, ReplyTo, NextRound, OldVal) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    % TODO: this is potentially unsafe depending on the RT implementation!
    HashedKey = if is_list(Key) -> ?RT:hash_key(Key);
                   true         -> Key
                end,
    ReadFilter = fun ?MODULE:rf_rl_vers/1,
    ContentCheck = fun ?MODULE:cc_abort_read/3,
    WriteFilter = fun ?MODULE:wf_unset_rl/3,
    rbrcseq:qwrite_fast(kv_db, ReplyTo, HashedKey, ?MODULE,
                        ReadFilter,
                        ContentCheck,
                        WriteFilter,
                        _Value = {TxId, tx_tlog:get_entry_version(TLogEntry)},
                        NextRound, OldVal),
    ok.

%% @doc The content check for abort_read has only to ensure, that the
%% readlock is eliminated when it is there. The write operation (unset
%% read lock) is performed, when the result is {true, _}. Otherwise,
%% the tx still passes and it is assumed that either the readlock was
%% not acquired, or was already eliminated by a write_through
%% operation of the rbrcseq module due to concurrency.
-spec cc_abort_read(no_value_yet | {readlock, version()}, prbr:write_filter(),
                {txid_on_cseq:txid(), version()}) ->
                       {boolean(), UI :: none}.
cc_abort_read(no_value_yet, _WF, _Val = {_TxId, _TLogVers}) ->
    %% we do not see a readlock, so it was not acquired
    {false, none};
cc_abort_read({_WL = no_write_lock, _Vers}, _WF, _Val = {_TxId, _TLogVers}) ->
    %% we can get the write_filter value...
    %% we do not see a readlock, so it was not acquired
    {false, none};
cc_abort_read({RL, _Vers}, _WF, _Val = {TxId, _TLogVers}) when is_list(RL) ->
    %% ensure readlock is gone...
    case lists:member(TxId, RL) of
        true -> {true, none};
        false -> {false, none}
                 %% RL is already gone or was not acquired, so we do not
                 %% need to update the entry
    end;
cc_abort_read({_WL, _Vers}, _WF, _Val = {_TxId, _TLogVers}) ->
    %% we can get the write_filter value...
    %% we do not see a readlock, so it was not acquired
    {false, none};
cc_abort_read({RL, _WL, _Vers}, _WF, _Val = {TxId, _TLogVers}) ->
    %% we can get the write_filter value...
    %% ensure readlock is gone...
    case lists:member(TxId, RL) of
        true -> {true, none};
        false -> {false, none}
                 %% RL is already gone or was not acquired, so we do
                 %% not need to update the entry
    end.


%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% functions for abort_write
%% %%%%%%%%%%%%%%%%%%%%%%%%%
-spec abort_write_feeder(tx_tlog:tlog_entry_write(), txid(), comm:erl_local_pid(),
                          pr:pr(), {txid_on_cseq:txid(), version()}) ->
                                 {tx_tlog:tlog_entry_write(), txid(),
                                  comm:erl_local_pid(),
                                  pr:pr(),
                                  {txid_on_cseq:txid(), version()}}.
abort_write_feeder(TLogEntry, TxId, Pid, Round, OldVal) ->
    {_, Val} = tx_tlog:get_entry_value(TLogEntry),
    SaneCommitWriteTLogEntry = tx_tlog:set_entry_value(TLogEntry, ?value, Val),
    {SaneCommitWriteTLogEntry, TxId, Pid, Round, OldVal}.

%% @doc Releases the write lock of a given entry, if it was set.
%% erlang shell test call: api_tx:write("a", 1).
-spec abort_write(tx_tlog:tlog_entry_write(), txid(), comm:erl_local_pid(),
                   pr:pr(), any()) -> ok.
abort_write(TLogEntry, TxId, ReplyTo, NextRound, OldVal) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    % TODO: this is potentially unsafe depending on the RT implementation!
    HashedKey = if is_list(Key) -> ?RT:hash_key(Key);
                   true         -> Key
                end,
    ReadFilter = fun ?MODULE:rf_wl_vers/1,
    ContentCheck = fun ?MODULE:cc_abort_write/3,
    WriteFilter = fun ?MODULE:wf_unset_wl/3,
    {?value, Value} = tx_tlog:get_entry_value(TLogEntry),
    %% {?value, Value} = tx_tlog:get_entry_value(TLogEntry),
    rbrcseq:qwrite_fast(kv_db, ReplyTo, HashedKey, ?MODULE,
                        ReadFilter,
                        ContentCheck,
                        WriteFilter,
                        _Value = {TxId, tx_tlog:get_entry_version(TLogEntry),
                                  Value},
                        NextRound, OldVal),
    ok.

-spec cc_abort_write(no_value_yet
                     | {writelock(), version()}
                     | {readlock(), writelock(), version()},
                     prbr:write_filter(),
                     {txid_on_cseq:txid(), version(), NewVal :: any()}) ->
                            {true, UI :: none} |
                            {false, any()}.
cc_abort_write(no_value_yet, _WF, _Val = {_TxId, _TLogVers, _NewVal}) ->
    {false, none};
cc_abort_write({WL, Vers}, _WF, _Val = {TxId, TLogVers, _NewVal}) ->
    %% normal qwrite

    %% if there is our lock, we release it, otherwise we do not touch the value
    Checks =
        [ { Vers =:= TLogVers,  {cc_abort_write_tlog_version_mismatch,
                                 TLogVers, Vers} },
          { WL =:= TxId, {cc_abort_write_lock_is_not_txid, WL, TxId} } ],
    cc_return_val(cc_abort_write, Checks, _UI_if_ok = none, ?LOG_CC_FAILS);
cc_abort_write({_RL, WL, Vers}, _WF, _Val = {TxId, TLogVers, _NewVal}) ->
    %% qwrite_fast
    %% if there is our lock, we release it, otherwise we were not able
    %% to acquire the lock during validation, so we do not touch the
    %% value (let the cc fail).
    Checks =
        [ { Vers =:= TLogVers,  {cc_abort_write_tlog_version_mismatch,
                                 TLogVers, Vers} },
          { WL =:= TxId, {cc_abort_write_lock_is_not_txid, WL, TxId} } ],
    cc_return_val(cc_abort_write, Checks, _UI_if_ok = none, ?LOG_CC_FAILS).

-spec wf_unset_wl
       (prbr_bottom, UI :: none, {txid_on_cseq:txid(), 0, value()}) -> {prbr_bottom, none};
       (db_entry(), UI :: none, {txid_on_cseq:txid(), version(), value()}) -> {db_entry(), none}.
wf_unset_wl(prbr_bottom, _UI = none, {_TxId, _Vers, _Val}) -> {prbr_bottom, none};
wf_unset_wl(DBEntry, _UI = none, {TxId, _TLogVers, _Val}) ->
    case writelock(DBEntry) of
       TxId -> {set_writelock(DBEntry, no_write_lock), none};
        _ -> {DBEntry, none}
    end.




-spec cc_return_val(atom(), [{boolean(), tuple()|atom()}], UI :: any(), boolean()) ->
                           {true, UI :: any()} |
                           {false, any()}.
cc_return_val(WhichCC, Checks, UI, true) ->
    %% copy cuntion from below to reduce overhead when not logging

    %% log is defined as macro ?LOG_CC_FAILS. Unfortunately
    %% dialyzer claims Log to be always false. But for
    %% debugging purposes we can alter this for individual
    %% content checks or enable logging for all of them by
    %% redefining the macro. So it seems we have to live
    %% with dialyzer's warning here.
    lists:foldl(
      fun({true, _Xreason}, {true, _UI_or_Reasons}) ->
              {true, UI};
         ({true, _Xreason}, {false, UI_or_Reasons}) ->
              {false, UI_or_Reasons};
         ({false, Xreason}, {true, _UI_or_Reasons}) ->
              log:log("~p cc failed: ~.0p~n", [WhichCC, Xreason]),
              {false, [Xreason]};
         ({false, Xreason}, {false, UI_or_Reasons}) ->
              log:log("~p cc failed: ~.0p~n", [WhichCC, Xreason]),
              {false, [Xreason | UI_or_Reasons]}
      end, {true, UI}, Checks);
cc_return_val(_WhichCC, Checks, UI, false) ->
    lists:foldl(
      fun({true, _Xreason}, {true, _UI_or_Reasons}) ->
              {true, UI};
         ({true, _Xreason}, {false, UI_or_Reasons}) ->
              {false, UI_or_Reasons};
         ({false, Xreason}, {true, _UI_or_Reasons}) ->
              {false, [Xreason]};
         ({false, Xreason}, {false, UI_or_Reasons}) ->
              {false, [Xreason | UI_or_Reasons]}
      end, {true, UI}, Checks).




%% abstract data type db_entry()

-spec new_entry() -> db_entry().
new_entry() ->
    {[], no_write_lock, -1, ?value_dropped}.

-spec readlock(db_entry()) -> readlock().
readlock(Entry) -> element(1, Entry).
-spec set_readlock(db_entry(), txid()) -> db_entry().
set_readlock(prbr_bottom, TxId) ->
    set_readlock(new_entry(), TxId);
set_readlock(Entry, TxId) ->
    NewRL = [TxId | element(1, Entry)],
    setelement(1, Entry, NewRL).
-spec unset_readlock(db_entry(), txid()) -> db_entry().
unset_readlock(Entry, TxId) ->
    %% delete all occurrences of TxId
    NewRL = [ X || X <- element(1, Entry), X =/= TxId ],
    setelement(1, Entry, NewRL).
-spec reset_readlock(db_entry()) -> db_entry().
reset_readlock(Entry) ->
    setelement(1, Entry, []).


-spec writelock(db_entry()) -> writelock().
writelock(Entry) -> element(2, Entry).
-spec set_writelock(db_entry(), writelock()) -> db_entry().
set_writelock(Entry, TxId) -> setelement(2, Entry, TxId).
-spec vers(db_entry()) -> version().
vers(Entry) -> element(3, Entry).
-spec set_vers(db_entry(), version()) -> db_entry().
set_vers(Entry, Vers) -> setelement(3, Entry, Vers).
-spec val(db_entry()) -> value().
val(Entry) -> element(4, Entry).
-spec set_val(db_entry(), value()) -> db_entry().
set_val(Entry, Val) -> setelement(4, Entry, Val).

-spec max(db_entry(), db_entry()) -> db_entry();
         ({client_value(), version()}, {client_value(), version()}) -> {client_value(), version()}.
max({_ValA, VersA} = A, {_ValB, VersB} = B) ->
    %% partial entries A and B produced by read filter rf_val_vers
    ?TRACE("A (~p) > B (~p)?", [A,B]),
    if VersA > VersB ->
           A;
       true ->
           %% do we have further to distinguish on same version
           %% values whether locks are set or not?
           %% @TODO Think about it...
           B
    end;
max(A, B) ->
    ?TRACE("A (~p) > B (~p)?", [A,B]),
    VersA = vers(A), VersB = vers(B),
    if VersA > VersB ->
           A;
       true ->
           %% do we have further to distinguish on same version
           %% values whether locks are set or not?
           %% @TODO Further think about it...
           B
    end.

-spec get_commuting_wf_for_rf(prbr:read_filter()) ->
        [prbr:write_filter()].
get_commuting_wf_for_rf(RF) ->
    {name, Name} = erlang:fun_info(RF, name),
    {module, Module} = erlang:fun_info(RF, module),
    case {Module, Name} of
        {?MODULE, rf_val} ->
            [fun ?MODULE:wf_set_rl/3,
             fun ?MODULE:wf_unset_rl/3,
             fun ?MODULE:wf_set_wl/3,
             fun ?MODULE:wf_unset_wl/3];
        {?MODULE, rf_val_vers} ->
            [fun ?MODULE:wf_set_rl/3,
             fun ?MODULE:wf_unset_rl/3,
             fun ?MODULE:wf_set_wl/3,
             fun ?MODULE:wf_unset_wl/3];
        {?MODULE, rf_rl_vers} ->
            [fun ?MODULE:wf_set_wl/3,
             fun ?MODULE:wf_unset_wl/3];
        {?MODULE, rf_wl_vers} ->
            [fun ?MODULE:wf_set_rl/3,
             fun ?MODULE:wf_unset_rl/3];
        _ -> []
    end.
