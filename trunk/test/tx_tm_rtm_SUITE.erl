% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc Unit tests for transactions under churn
%% @end
-module(tx_tm_rtm_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-define(CS_API, cs_api_v2).
%-define(CS_API, cs_api).

-compile(export_all).
-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [abort_prepared_r,
     abort_prepared_w,
     abort_prepared_rc,
     abort_prepared_rmc,
     abort_prepared_wmc].

suite() -> [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    ct:pal("Starting unittest ~p", [ct:get_status()]),
    Pid = unittest_helper:make_ring_with_ids(fun() -> ?RT:get_replica_keys(?RT:hash_key(0)) end),
    ?equals(?CS_API:write(0, "initial0"), ok),
    %% make a 2nd write, so versiondec does not result in -1 in the DB
    ?equals(?CS_API:write(0, "initial"), ok),
    [{wrapper_pid, Pid} | Config].

end_per_suite(Config) ->
    %error_logger:tty(false),
    {value, {wrapper_pid, Pid}} = lists:keysearch(wrapper_pid, 1, Config),
    unittest_helper:stop_ring(Pid),
    ok.

causes() -> [readlock, writelock, versiondec, versioninc, none].

abort_prepared_r(_) ->
    %% check all combinations of abort / prepared and the corresponding
    %% tx decisions
    %% modify DB and then do operations on the changed DB
    %% quorum reads should alway succeed
    [abort_prepared(0, read, [MC1, MC2, MC3, MC4], ok)
     || MC1 <- causes(), MC2 <- causes(),
        MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared_w(_) ->
    %% check all combinations of abort / prepared and the corresponding
    %% tx decisions
    %% modify DB and then do operations on the changed DB
    [abort_prepared(0, write, [MC1, MC2, MC3, MC4],
                    calc_w_outcome([MC1, MC2, MC3, MC4]))
      || MC1 <- causes(), MC2 <- causes(),
         MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared_rc(_) ->
    %% check all combinations of abort / prepared and the corresponding
    %% tx decisions
    %% modify DB and then do operations on the changed DB
    [abort_prepared(0, read_commit, [MC1, MC2, MC3, MC4],
                    calc_rc_outcome([MC1, MC2, MC3, MC4]))
     || MC1 <- causes(), MC2 <- causes(),
        MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared_rmc(_) ->
     %% modify DB after work_phase, before validation
     %% read - modify - commit (rmc)
     [begin
          {TLog, _} =
              ?CS_API:process_request_list(?CS_API:new_tlog(),
                                           [{read, 0}]),
          abort_prepared(0, {commit_tlog, TLog}, [MC1, MC2, MC3, MC4],
                         calc_rmc_outcome([MC1, MC2, MC3, MC4]))
      end
      || MC1 <- causes(), MC2 <- causes(),
         MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared_wmc(_) ->
     %% modify DB after work_phase, before validation
     %% write - modify - commit (rmc)
     [begin
          {TLog, _} =
              ?CS_API:process_request_list(?CS_API:new_tlog(),
                                           [{write, 0, "wmc"}]),
          Pattern = [MC1, MC2, MC3, MC4],
          abort_prepared(0, {commit_tlog, TLog}, Pattern,
                         calc_wmc_outcome(Pattern))
      end
      || MC1 <- causes(), MC2 <- causes(),
         MC3 <- causes(), MC4 <- causes()],
    ok.

abort_prepared(_Key, Op, PreOps, ExpectedOutcome) ->
    Keys = ?RT:get_replica_keys(?RT:hash_key(0)),
    DBEntries = get_db_entries(Keys),
    NewDBEntries = [ begin
          NewDBEntry =
              case PreOp of
                  readlock -> db_entry:inc_readlock(DBEntry);
                  writelock -> db_entry:set_writelock(DBEntry);
                  versiondec -> db_entry:set_version(DBEntry, db_entry:get_version(DBEntry) -1);
                  versioninc -> db_entry:inc_version(DBEntry);
                  none -> DBEntry
              end,
          lookup:unreliable_lookup(db_entry:get_key(DBEntry),
                                   {set_key_entry, comm:this(), NewDBEntry}),
          receive {set_key_entry_reply, NewDBEntry} -> ok end,
          NewDBEntry
      end
      || {DBEntry, PreOp} <- lists:zip(DBEntries, PreOps) ],

    Outcome =
        case Op of
            write -> ?CS_API:write(0,
                                     io_lib:format("~p with ~p", [Op, PreOps]));
            read ->
                case ?CS_API:read(0) of
                    {fail, Reason} -> {fail, Reason};
                    _ -> ok
                end;
            read_commit ->
                case ?CS_API:process_request_list(?CS_API:new_tlog(),
                                                  [{read, 0}, {commit}]) of
                    %% cs_api
                    {_TLog, {results, [_, {commit, fail, {fail, abort}}]}} -> {fail, abort};
                    {_TLog, {results, [_, {commit, ok, _}]}} -> ok;
                    %% cs_api_v2
                    {_TLog, {results, [_, abort]}} -> {fail, abort};
                    {_TLog, {results, [_, commit]}} -> ok
                end;
            {commit_tlog, TLog} ->
                case ?CS_API:process_request_list(TLog, [{commit}]) of
                    %% cs_api
                    {_TLog, {results, [{commit, fail, {fail, abort}}]}} -> {fail, abort};
                    {_TLog, {results, [{commit, ok, _}]}} -> ok;
                    %% cs_api_v2
                    {_TLog, {results, [abort]}} -> {fail, abort};
                    {_TLog, {results, [commit]}} -> ok
                end
        end,
    case ExpectedOutcome of
        ok_or_abort ->
            %% ct:pal("~w with ~w results in ~w~n", [Op, PreOps, Outcome]),
            ok;
        _ ->
            ?equals_w_note(Outcome, ExpectedOutcome,
                           io_lib:format("~p with ~p and~nDB entries initial: ~p~nafter preops ~p~n and after operation ~p~n",
                                         [Op, PreOps, DBEntries, NewDBEntries, get_db_entries(Keys)]))
    end,

    %% restore original entries
    [ begin
          lookup:unreliable_lookup(db_entry:get_key(X),
                                   {set_key_entry, comm:this(), X}),
          receive {set_key_entry_reply, X} -> ok end
      end
      || X <- DBEntries ],
   ok.

calc_w_outcome(PreOps) ->
    NumReadlock =   length([ X || X <- PreOps, X =:= readlock ]),
    NumWritelock =  length([ X || X <- PreOps, X =:= writelock ]),
    NumVersionDec = length([ X || X <- PreOps, X =:= versiondec ]),
    NumVersionInc = length([ X || X <- PreOps, X =:= versioninc ]),
    NumNone =       length([ X || X <- PreOps, X =:= none ]),

    if (4 =:= NumVersionInc) -> ok;
       (4 =:= NumVersionDec) -> ok;
       (4 =:= NumNone) -> ok;
       (1 =:= NumReadlock andalso NumNone =:= 3) -> ok;
       (1 =:= NumWritelock andalso NumNone =:= 3) -> ok;
       (1 =:= NumVersionDec andalso NumNone =:= 3) -> ok;
       (1 =:= NumVersionInc andalso 3 =:= NumNone) -> ok_or_abort;

       (3 =:= NumVersionDec andalso 1 =:= NumReadlock) -> ok_or_abort;
       (3 =:= NumVersionDec andalso 1 =:= NumWritelock) -> ok_or_abort;
       (3 =:= NumVersionDec andalso 1 =:= NumVersionInc) -> ok_or_abort;
       (3 =:= NumVersionDec andalso 1 =:= NumNone) -> ok_or_abort;
       (3 =:= NumVersionInc andalso 1 =:= NumReadlock) -> ok_or_abort;
       (3 =:= NumVersionInc andalso 1 =:= NumVersionDec) -> ok;
       (3 =:= NumVersionInc andalso 1 =:= NumNone) -> ok;
       (3 =:= NumVersionInc andalso 1 =:= NumWritelock) -> ok;

       true -> {fail, abort}
    end.

calc_rc_outcome(PreOps) ->
    %% DB is static over whole tx.
    %% Read phase and validation phase may operate on different
    %% replica subsets.
    NumWritelock =  length([ X || X <- PreOps, X =:= writelock ]),
    NumVersionInc = length([ X || X <- PreOps, X =:= versioninc ]),

    if (0 =:= NumWritelock) -> ok;
       (1 =:= NumWritelock andalso 1 =/= NumVersionInc) -> ok;
       (1 =:= NumWritelock andalso 1 =:= NumVersionInc) -> ok_or_abort;

       true -> {fail, abort}
    end.

calc_rmc_outcome(PreOps) ->
    NumReadlock =   length([ X || X <- PreOps, X =:= readlock ]),
    NumWritelock =  length([ X || X <- PreOps, X =:= writelock ]),
    NumVersionDec = length([ X || X <- PreOps, X =:= versiondec ]),
    NumVersionInc = length([ X || X <- PreOps, X =:= versioninc ]),
    NumNone =       length([ X || X <- PreOps, X =:= none ]),

    if (4 =:= (NumReadlock + NumNone)) -> ok;
       (3 =:= NumReadlock) -> ok;
       (3 =:= NumReadlock + NumVersionDec + NumNone) -> ok;
       (2 =< NumReadlock + NumNone)
       andalso (0 =:= NumWritelock + NumVersionInc) -> ok;
       (1 =:= NumWritelock) -> {fail, abort};
       (3 =:= NumVersionDec) -> ok;
       (4 =:= NumVersionDec) -> ok;

       true -> {fail, abort}
    end.

calc_wmc_outcome(PreOps) ->
    NumReadlock =   length([ X || X <- PreOps, X =:= readlock ]),
    NumWritelock =  length([ X || X <- PreOps, X =:= writelock ]),
    NumVersionDec = length([ X || X <- PreOps, X =:= versiondec ]),
    NumVersionInc = length([ X || X <- PreOps, X =:= versioninc ]),
    NumNone =       length([ X || X <- PreOps, X =:= none ]),

    if (NumVersionInc >= 2) -> {fail, abort};
       (NumVersionDec >= 2) -> {fail, abort};
       (NumReadlock >= 2) -> {fail, abort};
       (NumReadlock =:= 1 andalso NumNone =:= 3) -> ok;
       (NumWritelock =:= 1 andalso NumNone =:= 3) -> ok;
       (NumVersionDec =:= 1 andalso NumNone =:= 3) -> ok;
       (NumNone =:= 4) -> ok;
       (NumVersionInc =:= 1 andalso NumNone =:= 3) -> ok;

       true -> {fail, abort}
    end.

get_db_entries(Keys) ->
    [ begin
          lookup:unreliable_lookup(X, {get_key_entry, comm:this(), X}),
          receive
              {get_key_entry_reply, Entry} -> Entry
          end
      end
      || X <- Keys ].
