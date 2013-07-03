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
%% @doc    key value store based on rbrcseq.
%% @end
%% @version $Id$
-module(kv_on_cseq).
-author('schintke@zib.de').
-vsn('$Id:$ ').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/1]).
-export([write/2]).

-export([work_phase_async/4]).

-export([set_lock/3, set_lock_feeder/3]).
-export([commit_read/5, commit_read_feeder/5]).
-export([commit_write/5, commit_write_feeder/5]).
-export([abort_read/5, abort_read_feeder/5]).
-export([abort_write/5, abort_write_feeder/5]).

%% filters and checks for rbr_cseq operations
%% consistency
%% -export([is_valid_next_req/3]).
%% read filters
-export([rf_val/1]).
-export([rf_rl_wl_vers/1]).
%% write filters
-export([wf_set_vers_val/3]).

-type txid() :: ?RT:key().

-type readlock()  :: [txid()]. %% non_neg_integer(). %% later: [tx_id_keys].
-type writelock() :: txid() | false. %% later: tx_id_key | write_lock_is_free

-type version()   :: non_neg_integer() | -1.
-type value()     :: any().

-type db_entry()  :: { %% plainkey(),
                readlock(),
                writelock(),
                version(),
                value()
               }.

-spec work_phase_async(pid(), any(), ?RT:key(), read | write) -> ok.
work_phase_async(ClientPid, ReqId, HashedKey, _Op) ->
    ReplyTo = comm:reply_as(ClientPid, 3,
                            {work_phase_async_done, ReqId, '_'}),
    rbrcseq:qread(kv_rbrcseq, ReplyTo, HashedKey,
                  fun rf_val_vers/1),
    ok.

-spec rf_val_vers(db_entry() | prbr_bottom) -> {client_value(), version()}.
rf_val_vers(prbr_bottom) -> {?value_dropped, -1};
rf_val_vers(X)          -> {val(X), vers(X)}.



%% %%%%%%%%%%%%%%%%%%%%%%
%% functions for read
%% %%%%%%%%%%%%%%%%%%%%%%
-spec read(client_key()) -> api_tx:read_result().
read(Key) ->
    rbrcseq:qread(kv_rbrcseq, self(), ?RT:hash_key(Key),
                  fun rf_val/1),
    receive
        ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, Value},
                       case Value of
                           no_value_yet -> {fail, not_found};
                           _ -> {ok, Value}
                           end
                      )
    after 1000 ->
            log:log("read hangs ~p~n", [erlang:process_info(self(), messages)]),
                receive
                    ?SCALARIS_RECV({qread_done, _ReqId, _NextFastWriteRound, Value},
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
    rbrcseq:qwrite(kv_rbrcseq, self(), ?RT:hash_key(Key),
                   fun rf_rl_wl_vers/1,
                   fun cc_single_write/3,
                   fun wf_set_vers_val/3, Value),
    receive
        ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, Value}, {ok}); %%;
        ?SCALARIS_RECV({qwrite_deny, _ReqId, _NextFastWriteRound, _Value, Reason},
                       begin log:log("Write failed on key ~p: ~p~n", [Key, Reason]),
                       {ok} end) %% TODO: extend write_result type {fail, Reason} )
    after 1000 ->
            log:log("~p write hangs at key ~p, ~p~n",
                    [self(), Key, erlang:process_info(self(), messages)]),
            receive
                ?SCALARIS_RECV({qwrite_done, _ReqId, _NextFastWriteRound, Value},
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
                             {false, [readlock_is_set | write_lock_is_set]}.
cc_single_write(no_value_yet, _WriteFilter, _Val) ->
    {true, -1};
cc_single_write({RL, WL, Vers}, _WriteFilter, _Val) ->
    Checks = [ { [] =:= RL,    readlock_is_set },
               { false =:= WL, writelock_is_set } ],
    cc_return_val(cc_single_write, Checks, _UI_if_ok = Vers, _Log = true).

-spec wf_set_vers_val(db_entry() | prbr_bottom, version(),
                      client_value()) -> db_entry().
wf_set_vers_val(prbr_bottom, Version, WriteValue) ->
    {_RL = [], _WL = false, Version, WriteValue};
wf_set_vers_val(Entry, Version, WriteValue) ->
    T = set_vers(Entry, Version + 1),
    set_val(T, WriteValue).




%% %%%%%%%%%%%%%%%%%%%%%%
%% functions for set_lock
%% %%%%%%%%%%%%%%%%%%%%%%

-spec set_lock_feeder(tx_tlog:tlog_entry(), ?RT:key(), ok) ->
                      {tx_tlog:tlog_entry(), ?RT:key(), comm:erl_local_pid()}.
set_lock_feeder(TLogEntry, TxId, ok) ->
    {TLogEntry, TxId, self()}.

-spec set_lock(tx_tlog:tlog_entry(), ?RT:key(), comm:erl_local_pid()) -> ok.
set_lock(TLogEntry, TxId, ReplyTo) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    HashedKey = case is_list(Key) of
                    true -> ?RT:hash_key(Key);
                    false -> Key
                end,
    case tx_tlog:get_entry_operation(TLogEntry) of
        ?read ->
            ReadFilter = fun rf_wl_vers/1, %% read lock is irrelevant for reads
            ContentCheck = fun cc_set_rl/3,
            WriteFilter = fun wf_set_rl/3,
            rbrcseq:qwrite(kv_rbrcseq, ReplyTo, HashedKey,
                           ReadFilter,
                           ContentCheck,
                           WriteFilter,
                           _Value = {TxId,
                                     tx_tlog:get_entry_version(TLogEntry)});
        ?write ->
            ReadFilter = fun rf_rl_wl_vers/1,
            ContentCheck = fun cc_set_wl/3,
            WriteFilter = fun wf_set_wl/3,
            rbrcseq:qwrite(kv_rbrcseq, ReplyTo, HashedKey,
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
                       {boolean(), UI :: none}.
cc_set_rl(no_value_yet, _WF, _Val = {_TxId, _TLogVers}) ->
    {true, none};
cc_set_rl({WL, Vers}, _WF, _Val = {_TxId, TLogVers}) ->
    Checks = [ { TLogVers =:= Vers, {version_mismatch, TLogVers, Vers} },
               { false =:= WL,      writelock_is_set } ],
    cc_return_val(cc_set_rl, Checks, _UI_if_ok = none, _Log = true).

-spec wf_set_rl(db_entry(), UI :: {writelock(), version()} | no_value_yet,
                {txid_on_cseq:txid(), version()}) -> db_entry().
wf_set_rl(DBEntry, _UI, {TxId, _Vers}) ->
    set_readlock(DBEntry, TxId).


%% set_lock write: read filter, content check and write filter
-spec rf_rl_wl_vers(db_entry() | prbr_bottom) ->
        {readlock(), writelock(), version()} | no_value_yet. %% drop -1 in the future?
rf_rl_wl_vers(prbr_bottom) -> no_value_yet;
rf_rl_wl_vers(X)          -> {readlock(X),
                              writelock(X),
                              vers(X)}.

-spec cc_set_wl({readlock, writelock(), version()}, prbr:write_filter(),
                {txid_on_cseq:txid(), version()}) ->
                       {boolean(), UI :: none}.
cc_set_wl(no_value_yet, _WF, _Val = {_TxId, _TLogVers}) ->
    {true, none};
cc_set_wl({RL, WL, Vers}, _WF, _Val = {TxId, TLogVers}) ->
    Checks =
        [ { TLogVers =:= Vers,  {version_mismatch, TLogVers, Vers} },
          { false =:= WL
            orelse TxId =:= WL, {txid_mismatch, WL, TxId} },
          { [] =:= RL,          {readlock_not_empty, RL} } ],
    cc_return_val(cc_set_wl, Checks, _UI_if_ok = none, _Log = true).

-spec wf_set_wl
       (prbr_bottom, UI :: none, {txid_on_cseq:txid(), -1}) -> db_entry();
       (db_entry(),  UI :: none, {txid_on_cseq:txid(), version()}) -> db_entry().
wf_set_wl(prbr_bottom, _UI = none, {TxId, -1}) ->
    set_writelock(new_entry(), TxId);
wf_set_wl(prbr_bottom, _UI = none, {TxId, _}) ->
    ct:pal("should only happen in tester unittests~n"),
    set_writelock(new_entry(), TxId);
wf_set_wl(DBEntry, _UI = none, {TxId, _Vers}) ->
    set_writelock(DBEntry, TxId).





%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% functions for commit_read
%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% commit read releases the read lock of a given entry, if it was set.
-spec commit_read_feeder(tx_tlog:tlog_entry(), ?RT:key(), ok,
                         prbr:r_with_id(), {txid_on_cseq:txid(), version()}) ->
                                {tx_tlog:tlog_entry(),
                                 ?RT:key(),
                                 comm:erl_local_pid(),
                                 prbr:r_with_id(),
                                 {txid_on_cseq:txid(), version()}}.
commit_read_feeder(TLogEntry, TxId, ok, Round, OldVal) ->
    {TLogEntry, TxId, self(), Round, OldVal}.

-spec commit_read(tx_tlog:tlog_entry(), ?RT:key(), comm:erl_local_pid(),
                  prbr:r_with_id(), any()) -> ok.
commit_read(TLogEntry, TxId, ReplyTo, NextRound, OldVal) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    HashedKey = case is_list(Key) of
                    true -> ?RT:hash_key(Key);
                    false -> Key
                end,
    ReadFilter = fun rf_rl_vers/1,
    ContentCheck = fun cc_commit_read/3,
    WriteFilter = fun wf_unset_rl/3,
    rbrcseq:qwrite_fast(kv_rbrcseq, ReplyTo, HashedKey,
                        ReadFilter,
                        ContentCheck,
                        WriteFilter,
                        _Value = {TxId, tx_tlog:get_entry_version(TLogEntry)},
                        NextRound, OldVal),
    ok.

-spec rf_rl_vers(db_entry() | prbr_bottom) ->
                           {readlock(), version()} | no_value_yet.
rf_rl_vers(prbr_bottom) -> no_value_yet;
rf_rl_vers(X) ->           {readlock(X), vers(X)}.

-spec cc_commit_read(no_value_yet | {readlock, version()}, prbr:write_filter(),
                {txid_on_cseq:txid(), version()}) ->
                       {boolean(), UI :: none}.
cc_commit_read(no_value_yet, _WF, _Val = {_TxId, _TLogVers}) ->
    {true, none};
cc_commit_read({_RL, Vers}, _WF, _Val = {_TxId, TLogVers}) ->
    Checks =
        [ { TLogVers =:= Vers,  {version_mismatch, TLogVers, Vers} } ],
    cc_return_val(cc_commit_read, Checks, _UI_if_ok = none, _Log = true).

-spec wf_unset_rl
       (prbr_bottom, UI :: none, {txid_on_cseq:txid(), 0}) -> prbr_bottom;
       (db_entry(), UI :: none, {txid_on_cseq:txid(), version()}) -> db_entry().
wf_unset_rl(prbr_bottom, _UI = none, {_TxId, _Vers}) -> prbr_bottom;
wf_unset_rl(DBEntry, _UI = none, {TxId, _TLogVers}) ->
    unset_readlock(DBEntry, TxId).





%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% functions for commit_write
%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% commit write releases the write lock of a given entry, if it was
%% set, and writes the new value.
 
%% erlang shell test call: api_tx:write("a", 1).
 
-spec commit_write_feeder(tx_tlog:tlog_entry_write(), ?RT:key(), ok,
                          prbr:r_with_id(), {txid_on_cseq:txid(), version()}) ->
                                 {tx_tlog:tlog_entry_write(), ?RT:key(),
                                  comm:erl_local_pid(),
                                  prbr:r_with_id(),
                                  {txid_on_cseq:txid(), version()}}.
commit_write_feeder(TLogEntry, TxId, ok, Round, OldVal) ->
    {_, Val} = tx_tlog:get_entry_value(TLogEntry),
    SaneCommitWriteTLogEntry = tx_tlog:set_entry_value(TLogEntry, ?value, Val),
    {SaneCommitWriteTLogEntry, TxId, self(), Round, OldVal}.

-spec commit_write(tx_tlog:tlog_entry_write(), ?RT:key(), comm:erl_local_pid(),
                   prbr:r_with_id(), any()) -> ok.
commit_write(TLogEntry, TxId, ReplyTo, NextRound, OldVal) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    HashedKey = case is_list(Key) of
                    true -> ?RT:hash_key(Key);
                    false -> Key
                end,
    ReadFilter = fun rf_wl_vers/1,
    ContentCheck = fun cc_commit_write/3,
    WriteFilter = fun wf_val_unset_wl/3,
    {?value, Value} = tx_tlog:get_entry_value(TLogEntry),
    %% {?value, Value} = tx_tlog:get_entry_value(TLogEntry),
    rbrcseq:qwrite_fast(kv_rbrcseq, ReplyTo, HashedKey,
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
cc_commit_write({_WL, Vers}, _WF, _Val = {_TxId, TLogVers, _NewVal}) ->
    Checks =
        [ { Vers =:= TLogVers,  {version_mismatch, TLogVers, Vers} } ],
    cc_return_val(cc_commit_write, Checks, _UI_if_ok = none, _Log = true);
%% in case of fast write we get the value of the last read as write
%% value, which here was produced by the read filter of set_lock
cc_commit_write({_RL, _WL, Vers}, _WF, _Val = {_TxId, TLogVers, _NewVal}) ->
    Checks =
        [ { Vers =:= TLogVers,  {version_mismatch, TLogVers, Vers} } ],
    cc_return_val(cc_commit_write, Checks, _UI_if_ok = none, _Log = true).


-spec wf_val_unset_wl
       (prbr_bottom, UI :: none, {txid_on_cseq:txid(), 0, _}) -> prbr_bottom;
       (db_entry(), UI :: none, {txid_on_cseq:txid(), version(), value()}) -> db_entry().
wf_val_unset_wl(prbr_bottom, _UI = none, {_TxId, _Vers, _Val}) -> prbr_bottom;
wf_val_unset_wl(DBEntry, _UI = none, {_TxId, TLogVers, Val}) ->
    T1 = set_writelock(DBEntry, false),
    T2 = set_val(T1, Val),
    %% increment version counter on write
    set_vers(T2, 1 + TLogVers).





%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% functions for abort_read
%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% abort read releases the read lock of a given entry, if it was set.
-spec abort_read_feeder(tx_tlog:tlog_entry(), ?RT:key(), ok,
                         prbr:r_with_id(), {txid_on_cseq:txid(), version()}) ->
                                {tx_tlog:tlog_entry(),
                                 ?RT:key(),
                                 comm:erl_local_pid(),
                                 prbr:r_with_id(),
                                 {txid_on_cseq:txid(), version()}}.
abort_read_feeder(TLogEntry, TxId, ok, Round, OldVal) ->
    {TLogEntry, TxId, self(), Round, OldVal}.

-spec abort_read(tx_tlog:tlog_entry(), ?RT:key(), comm:erl_local_pid(),
                  prbr:r_with_id(), any()) -> ok.
abort_read(TLogEntry, TxId, ReplyTo, NextRound, OldVal) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    HashedKey = case is_list(Key) of
                    true -> ?RT:hash_key(Key);
                    false -> Key
                end,
    ReadFilter = fun rf_rl_vers/1,
    ContentCheck = fun cc_abort_read/3,
    WriteFilter = fun wf_unset_rl/3,
    rbrcseq:qwrite_fast(kv_rbrcseq, ReplyTo, HashedKey,
                        ReadFilter,
                        ContentCheck,
                        WriteFilter,
                        _Value = {TxId, tx_tlog:get_entry_version(TLogEntry)},
                        NextRound, OldVal),
    ok.

-spec cc_abort_read(no_value_yet | {readlock, version()}, prbr:write_filter(),
                {txid_on_cseq:txid(), version()}) ->
                       {boolean(), UI :: none}.
cc_abort_read(no_value_yet, _WF, _Val = {_TxId, _TLogVers}) ->
    {true, none};
cc_abort_read({_RL, _Vers}, _WF, _Val = {_TxId, _TLogVers}) ->
    {true, none}.
                                                        
%.%% in case of fast write we get the value of the last read as
%.  %% value, which here was produced by the read filter of set_lock
%.  cc_abort_read({_RL, Vers}, _WF, _Val = {_TxId, TLogVers}) ->
%%      {true, none}.






%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% functions for abort_write
%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% abort write releases the write lock of a given entry, if it was
%% set.
 
%% erlang shell test call: api_tx:write("a", 1).
 
-spec abort_write_feeder(tx_tlog:tlog_entry_write(), ?RT:key(), ok,
                          prbr:r_with_id(), {txid_on_cseq:txid(), version()}) ->
                                 {tx_tlog:tlog_entry_write(), ?RT:key(),
                                  comm:erl_local_pid(),
                                  prbr:r_with_id(),
                                  {txid_on_cseq:txid(), version()}}.
abort_write_feeder(TLogEntry, TxId, ok, Round, OldVal) ->
    {_, Val} = tx_tlog:get_entry_value(TLogEntry),
    SaneCommitWriteTLogEntry = tx_tlog:set_entry_value(TLogEntry, ?value, Val),
    {SaneCommitWriteTLogEntry, TxId, self(), Round, OldVal}.

-spec abort_write(tx_tlog:tlog_entry_write(), ?RT:key(), comm:erl_local_pid(),
                   prbr:r_with_id(), any()) -> ok.
abort_write(TLogEntry, TxId, ReplyTo, NextRound, OldVal) ->
    Key = tx_tlog:get_entry_key(TLogEntry),
    HashedKey = case is_list(Key) of
                    true -> ?RT:hash_key(Key);
                    false -> Key
                end,
    ReadFilter = fun rf_wl_vers/1,
    ContentCheck = fun cc_abort_write/3,
    WriteFilter = fun wf_unset_wl/3,
    {?value, Value} = tx_tlog:get_entry_value(TLogEntry),
    %% {?value, Value} = tx_tlog:get_entry_value(TLogEntry),
    rbrcseq:qwrite_fast(kv_rbrcseq, ReplyTo, HashedKey,
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
                            {true, UI :: none}.
cc_abort_write(no_value_yet, _WF, _Val = {_TxId, _TLogVers, _NewVal}) ->
    {true, none};
cc_abort_write({_WL, _Vers}, _WF, _Val = {_TxId, _TLogVers, _NewVal}) ->
    {true, none};
cc_abort_write({_RL, _WL, _Vers}, _WF, _Val = {_TxId, _TLogVers, _NewVal}) ->
    {true, none}.

-spec wf_unset_wl
       (prbr_bottom, UI :: none, {txid_on_cseq:txid(), 0, _}) -> prbr_bottom;
       (db_entry(), UI :: none, {txid_on_cseq:txid(), version(), value()}) -> db_entry().
wf_unset_wl(prbr_bottom, _UI = none, {_TxId, _Vers, _Val}) -> prbr_bottom;
wf_unset_wl(DBEntry, _UI = none, {TxId, _TLogVers, _Val}) ->
    case writelock(DBEntry) of
       TxId -> set_writelock(DBEntry, false);
        _ -> DBEntry
    end.




-spec cc_return_val(atom(), [{boolean(), tuple()|atom()}], UI :: any(), boolean()) ->
                           {true, UI :: any()} |
                           {false, any()}.
cc_return_val(WhichCC, Checks, UI, Log) ->
    lists:foldl(
      fun({Xbool, Xreason}, {Result, UI_or_Reasons}) ->
              case Log andalso not Xbool of
                  true -> log:log("~p cc failed: ~.0p~n", [WhichCC, Xreason]);
                  false -> ok
              end,
              case {Xbool, Result} of
                  {true, true} ->
                      {true, UI};
                  {true, false} ->
                      {Result, UI_or_Reasons};
                  {false, true} ->
                      {false, [Xreason]};
                  {false, false} ->
                      {false, [Xreason | UI_or_Reasons]}
              end
      end, {true, UI}, Checks).




%% abstract data type db_entry()

-spec new_entry() -> db_entry().
new_entry() ->
    {[], false, -1, ?value_dropped}.

-spec readlock(db_entry()) -> readlock().
readlock(Entry) -> element(1, Entry).
-spec set_readlock(db_entry(), ?RT:key()) -> db_entry().
set_readlock(prbr_bottom, TxId) ->
    set_readlock(new_entry(), TxId);
set_readlock(Entry, TxId) ->
    NewRL = lists:append(element(1, Entry), [TxId]),
    setelement(1, Entry, NewRL).
-spec unset_readlock(db_entry(), ?RT:key()) -> db_entry().
unset_readlock(Entry, TxId) ->
    %% delete all occurrences of TxId
    NewRL = [ X || X <- element(1, Entry), X =/= TxId ],
    setelement(1, Entry, NewRL).


-spec writelock(db_entry()) -> writelock().
writelock(Entry) -> element(2, Entry).
-spec set_writelock(db_entry(), ?RT:key() | false) -> db_entry().
set_writelock(Entry, TxId) -> setelement(2, Entry, TxId).
-spec vers(db_entry()) -> version().
vers(Entry) -> element(3, Entry).
-spec set_vers(db_entry(), version()) -> db_entry().
set_vers(Entry, Vers) -> setelement(3, Entry, Vers).
-spec val(db_entry()) -> value().
val(Entry) -> element(4, Entry).
-spec set_val(db_entry(), value()) -> db_entry().
set_val(Entry, Val) -> setelement(4, Entry, Val).
