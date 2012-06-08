% @copyright 2009-2012 Zuse Institute Berlin,
%            2009 onScale solutions GmbH

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
%% @author Nico Kruber <kruber@zib.de>
%% @doc    API for transactions on replicated DHT items.
%% @version $Id$
-module(rdht_tx).
-author('schintke@zib.de').
-author('kruber@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-export([req_list/3]).
-export([check_config/0]).
-export([encode_value/1, decode_value/1]).

%% export to silence dialyzer
-export([decode_result/1]).

-include("scalaris.hrl").
-include("client_types.hrl").

-ifdef(with_export_type_support).
-export_type([req_id/0]).
-endif.

-type req_id() :: uid:global_uid().
-type request_on_key() :: api_tx:request_on_key().
-type results() :: [ api_tx:result() ].

%% @doc Perform several requests inside a transaction.
-spec req_list(tx_tlog:tlog(), [api_tx:request()], EnDecode::boolean()) -> {tx_tlog:tlog(), results()}.
req_list([], [{commit}], _EnDecode) -> {[], [{ok}]};
req_list(TLog, ReqList, EnDecode) ->
    %% PRE: TLog is sorted by key, implicitly given, as
    %%      we only generate sorted TLogs.
    ?TRACE("rdht_tx:req_list(~p, ~p, ~p)~n", [TLog, ReqList, EnDecode]),

    %% (0) Check TLog? Consts performance, may save some requests

    %% (1) Ensure commit is only at end of req_list (otherwise abort),
    %% (2) encode write values to ?DB:value format
    %% (3) drop {commit} request at end &and remember whether to
    %%     commit or not
    {ReqList1, OKorAbort, FoundCommitAtEnd} =
        rl_chk_and_encode(ReqList, [], ok),

    case OKorAbort of
        abort -> tlog_and_results_to_abort(TLog, ReqList);
        ok ->
            TLog2 = upd_tlog_via_rdht(TLog, ReqList1),

            %% perform all requests based on TLog to compute result
            %% entries
            {NewClientTLog, Results} = do_reqs_on_tlog(TLog2, ReqList1, EnDecode),

            %% do commit (if requested) and append the commit result
            %% to result list
            case FoundCommitAtEnd of
                true ->
                    CommitRes = commit(NewClientTLog),
                    {tx_tlog:empty(), Results ++ [CommitRes]};
                false ->
                    {NewClientTLog, Results}
            end
    end.

%% @doc Check whether commit is only at end (OKorAbort).
%%      Encode all values of write requests.
%%      Cut commit at end and inform caller via boolean (CommitAtEnd).
-spec rl_chk_and_encode(
        InTodo::[api_tx:request()], Acc::[api_tx:request()], ok|abort)
                       -> {Acc::[request_on_key()], ok|abort, CommitAtEnd::boolean()}.
rl_chk_and_encode([], Acc, OKorAbort) ->
    {lists:reverse(Acc), OKorAbort, false};
rl_chk_and_encode([{commit}], Acc, OKorAbort) ->
    {lists:reverse(Acc), OKorAbort, true};
rl_chk_and_encode([Req | RL], Acc, OKorAbort) ->
    case Req of
        {write, _Key, _Value} = Write ->
            rl_chk_and_encode(RL, [Write | Acc], OKorAbort);
        {commit} = Commit ->
            log:log(info, "Commit not at end of a req_list. Deciding abort."),
            rl_chk_and_encode(RL, [Commit | Acc], abort);
        Op ->
            rl_chk_and_encode(RL, [Op | Acc], OKorAbort)
    end.

%% @doc Fill all fields with {fail, abort} information.
-spec tlog_and_results_to_abort(tx_tlog:tlog(), [api_tx:request()]) ->
                                       {tx_tlog:tlog(), results()}.
tlog_and_results_to_abort(TLog, ReqList) ->
    NewTLog = lists:foldl(fun(Req, AccTLog) ->
                                  case Req of
                                      {commit} -> AccTLog;
                                      _ ->
                                          tx_tlog:add_or_update_status_by_key(
                                            AccTLog,
                                            req_get_key(Req),
                                            {fail, abort})
                                  end end, TLog, ReqList),
    {NewTLog, [ case element(1, X) of
                    read -> {fail, not_found};
                    write -> {ok};
                    add_del_on_list -> {ok};
                    add_on_nr -> {ok};
                    test_and_set -> {ok};
                    commit -> {fail, abort, []}
                end || X <- ReqList ]}.

%% @doc Send requests to the DHT, gather replies and merge TLogs.
-spec upd_tlog_via_rdht(tx_tlog:tlog(), [request_on_key()]) -> tx_tlog:tlog().
upd_tlog_via_rdht(TLog, ReqList) ->
    %% what to get from rdht? (also check old TLog)
    USReqList = lists:ukeysort(2, ReqList),
    ReqListonRDHT = first_req_per_key_not_in_tlog(TLog, USReqList),

    %% perform RDHT operations to collect missing TLog entries
    %% rdht requests for independent keys are processed in parallel.
    ReqListWithReqIds = initiate_rdht_ops(ReqListonRDHT),

    RTLog = collect_replies(tx_tlog:empty(), ReqListWithReqIds),

    %% merge TLogs (insert fail, abort, when version mismatch
    %% in reads for same key is detected)
    _MTLog = merge_tlogs(TLog, tx_tlog:sort_by_key(RTLog)).

%% @doc Requests, that need information from the DHT.
-spec first_req_per_key_not_in_tlog(tx_tlog:tlog(), [request_on_key()]) ->
                                           [request_on_key()].
first_req_per_key_not_in_tlog(SortedTLog, USortedReqList) ->
    %% PRE: no {commit} requests in SortedReqList
    %% POST: returns requests in reverse order, but thats sufficient.
    first_req_per_key_not_in_tlog_iter(SortedTLog, USortedReqList, []).

%% @doc Helper to acquire requests, that need information from the DHT.
-spec first_req_per_key_not_in_tlog_iter(tx_tlog:tlog(),
                                         [request_on_key()],
                                         [request_on_key()]) ->
                                                [request_on_key()].
first_req_per_key_not_in_tlog_iter(_, [], Acc) -> Acc;
first_req_per_key_not_in_tlog_iter([], USortedReqList, Acc) ->
    USortedReqList ++ Acc;
first_req_per_key_not_in_tlog_iter([TEntry | TTail] = USTLog,
                                   [Req | RTail] = USReqList,
                                   Acc) ->
    TKey = tx_tlog:get_entry_key(TEntry),
    case TKey =:= req_get_key(Req) of
        true ->
            %% key is in TLog, rdht op not necessary
            case tx_tlog:get_entry_operation(TEntry) of
                write ->
                    first_req_per_key_not_in_tlog_iter(USTLog, RTail, Acc);
                _ ->
                    case req_get_op(Req) of
                        %% when operation needs the value (read) and
                        %% TLog is optimized, we need the op anyway to
                        %% calculate the result entry.
                        read ->
                            first_req_per_key_not_in_tlog_iter
                              (USTLog, RTail, [Req | Acc]);
                        test_and_set ->
                            first_req_per_key_not_in_tlog_iter
                              (USTLog, RTail, [Req | Acc]);
                        add_on_nr ->
                            first_req_per_key_not_in_tlog_iter
                              (USTLog, RTail, [Req | Acc]);
                        add_del_on_list ->
                            first_req_per_key_not_in_tlog_iter
                              (USTLog, RTail, [Req | Acc]);
                        %%first_req_per_key_not_in_tlog_iter(USTLog, RTail, Acc);
                        _ ->
                            first_req_per_key_not_in_tlog_iter(USTLog, RTail, Acc)
                    end
            end;
        false ->
            case TKey < req_get_key(Req) of
                true ->
                    %% jump to next Tlog entry
                    first_req_per_key_not_in_tlog_iter(TTail, USReqList, Acc);
                false ->
                    first_req_per_key_not_in_tlog_iter(USTLog, RTail, [Req | Acc])
            end
    end.

%% @doc Trigger operations for the DHT.
-spec initiate_rdht_ops([request_on_key()]) -> [{req_id(), request_on_key()}].
initiate_rdht_ops(ReqList) ->
    ?TRACE("rdht_tx:initiate_rdht_ops(~p)~n", [ReqList]),
    %% @todo should choose a dht_node in the local VM at random or even
    %% better round robin.
    [ begin
          NewReqId = uid:get_global_uid(), % local id not sufficient
          OpModule = case req_get_op(Entry) of
                         read -> rdht_tx_read;
                         write -> rdht_tx_write;
                         test_and_set -> rdht_tx_read;
                         add_del_on_list -> rdht_tx_read;
                         add_on_nr -> rdht_tx_read
                     end,
          case OpModule of
              rdht_tx_read ->
                  rdht_tx_read:work_phase(self(), NewReqId, Entry);
              rdht_tx_write ->
                  rdht_tx_write:work_phase(self(), NewReqId, Entry)
          end,
          {NewReqId, Entry}
      end || Entry <- ReqList ].

%% @doc Collect replies from the quorum DHT operations.
-spec collect_replies(tx_tlog:tlog(), [{req_id(), request_on_key()}]) -> tx_tlog:tlog().
collect_replies(TLog, []) -> TLog;
collect_replies(TLog, ReqIdsReqList) ->
    ?TRACE("rdht_tx:collect_replies(~p, ~p)~n", [TLog, ReqIdsReqList]),
    {_, ReqId, RdhtTlogEntry} = receive_answer(),
    case lists:keyfind(ReqId, 1, ReqIdsReqList) of
        false ->
            %% Drop outdated result...
            collect_replies(TLog, ReqIdsReqList);
        _ ->
            %% add TLog entry, as it is guaranteed a necessary entry
            NewTLog = [RdhtTlogEntry | TLog],
            NewReqIdsReqList = lists:keydelete(ReqId, 1, ReqIdsReqList),
            collect_replies(NewTLog, NewReqIdsReqList)
    end.

%% @doc Merge TLog entries, if same key. Check for version mismatch,
%%      take over values.
%%      SortedTlog is old TLog
%%      SortedRTlog is TLog received from newer RDHT operations
-spec merge_tlogs(tx_tlog:tlog(), tx_tlog:tlog()) -> tx_tlog:tlog().
merge_tlogs(SortedTLog, SortedRTLog) ->
    merge_tlogs_iter(SortedTLog, SortedRTLog, []).

-spec merge_tlogs_iter(tx_tlog:tlog(), tx_tlog:tlog(), tx_tlog:tlog()) ->
                              tx_tlog:tlog().
merge_tlogs_iter([], [], Acc)          -> Acc;
merge_tlogs_iter([], SortedRTLog, [])  -> SortedRTLog;
merge_tlogs_iter([], SortedRTLog, Acc) -> lists:reverse(Acc) ++ SortedRTLog;
merge_tlogs_iter(SortedTLog, [], Acc)  -> lists:reverse(Acc) ++ SortedTLog;
merge_tlogs_iter([TEntry | TTail] = SortedTLog,
                 [RTEntry | RTTail] = SortedRTLog,
                 Acc) ->
    TKey = tx_tlog:get_entry_key(TEntry),
    RTKey = tx_tlog:get_entry_key(RTEntry),
    case TKey =:= RTKey of
        true ->
            %% key was in TLog, new entry is newer and contains value
            %% for read?
            case tx_tlog:get_entry_operation(TEntry) of
                read ->
                    %% check versions: if mismatch -> change status to abort
                    NewTLogEntry =
                        case tx_tlog:get_entry_version(TEntry)
                            =/= tx_tlog:get_entry_version(RTEntry) of
                            true ->
                                tx_tlog:set_entry_status(RTEntry, {fail, abort});
                            false ->
                                Val = tx_tlog:get_entry_value(RTEntry),
                                tx_tlog:set_entry_value(TEntry, Val)
                        end,
                    merge_tlogs_iter(TTail, RTTail, [NewTLogEntry | Acc]);
                _ ->
                    log:log(warn,
                            "Duplicate key in TLog merge should not happen ~p ~p", [TEntry, RTEntry]),
                    merge_tlogs_iter(TTail, RTTail, [ RTEntry | Acc])
            end;
        false ->
            case TKey < RTKey of
                true  -> merge_tlogs_iter(TTail, SortedRTLog, [TEntry | Acc]);
                false -> merge_tlogs_iter(SortedTLog, RTTail, [RTEntry | Acc])
            end
    end.

%% @doc Perform all operations on the TLog and generate list of results.
-spec do_reqs_on_tlog(tx_tlog:tlog(), [request_on_key()], EnDecode::boolean()) ->
                             {tx_tlog:tlog(), results()}.
do_reqs_on_tlog(TLog, ReqList, EnDecode) ->
    do_reqs_on_tlog_iter(TLog, ReqList, [], EnDecode).

%% @doc Helper to perform all operations on the TLog and generate list
%%      of results.
-spec do_reqs_on_tlog_iter(tx_tlog:tlog(), [request_on_key()], results(), EnDecode::boolean()) ->
                                  {tx_tlog:tlog(), results()}.
do_reqs_on_tlog_iter(TLog, [], Acc, _EnDecode) ->
    {tlog_cleanup(TLog), lists:reverse(Acc)};
do_reqs_on_tlog_iter(TLog, [Req | ReqTail], Acc, EnDecode) ->
    Key = req_get_key(Req),
    Entry = tx_tlog:find_entry_by_key(TLog, Key),
    {NewTLogEntry, ResultEntry} =
        case Req of
            %% native functions first:
            {read, Key}           -> tlog_read(Entry, Key, EnDecode);
            {write, Key, Value}   -> tlog_write(Entry, Key, Value, EnDecode);
            %% non-native functions:
            {add_del_on_list, Key, ToAdd, ToDel} -> tlog_add_del_on_list(Entry, Key, ToAdd, ToDel, EnDecode);
            {add_on_nr, Key, X}           -> tlog_add_on_nr(Entry, Key, X, EnDecode);
            {test_and_set, Key, Old, New} -> tlog_test_and_set(Entry, Key, Old, New, EnDecode)
        end,
    NewTLog = tx_tlog:update_entry(TLog, NewTLogEntry),
    do_reqs_on_tlog_iter(NewTLog, ReqTail, [ResultEntry | Acc], EnDecode).

-spec tlog_cleanup(tx_tlog:tlog()) -> tx_tlog:tlog().
tlog_cleanup(TLog) ->
    [ case tx_tlog:get_entry_operation(TLogEntry) of
          read -> tx_tlog:set_entry_value(TLogEntry, '$empty');
          write -> TLogEntry
      end || TLogEntry <- TLog ].

%% @doc Get a result entry for a read from the given TLog entry.
-spec tlog_read(tx_tlog:tlog_entry(), client_key(), EnDecode::boolean()) ->
                       {tx_tlog:tlog_entry(), api_tx:read_result()}.
tlog_read(Entry, _Key, EnDecode) ->
    Res = case tx_tlog:get_entry_status(Entry) of
              value -> {ok, tx_tlog:get_entry_value(Entry)};
              %% try reading from a failed entry (type mismatch was the reason?)
              {fail, abort} -> {ok, tx_tlog:get_entry_value(Entry)};
              {fail, not_found} = R -> R %% not_found
          end,
    {Entry, ?IIF(EnDecode, decode_result(Res), Res)}.

%% @doc Get a result entry for a write from the given TLog entry.
%%      Update the TLog entry accordingly.
-spec tlog_write(tx_tlog:tlog_entry(), client_key(), client_value(), EnDecode::boolean()) ->
                       {tx_tlog:tlog_entry(), api_tx:write_result()}.
tlog_write(Entry, _Key, Value1, EnDecode) ->
    Value = ?IIF(EnDecode, encode_value(Value1), Value1),
    NewEntryAndResult =
        fun(FEntry, FValue) ->
                case tx_tlog:get_entry_operation(FEntry) of
                    write ->
                        {tx_tlog:set_entry_value(FEntry, FValue), {ok}};
                    read ->
                        E1 = tx_tlog:set_entry_operation(FEntry, write),
                        E2 = tx_tlog:set_entry_value(E1, FValue),
                        {E2, {ok}}
            end
        end,
    case tx_tlog:get_entry_status(Entry) of
        value ->
            NewEntryAndResult(Entry, Value);
        {fail, not_found} ->
            E1 = tx_tlog:set_entry_operation(Entry, write),
            E2 = tx_tlog:set_entry_value(E1, Value),
            E3 = tx_tlog:set_entry_status(E2, value),
            {E3, {ok}};
        {fail, abort} ->
            {Entry, {ok}}
    end.

%% @doc Simulate a change on a set via read and write requests.
%%      Update the TLog entry accordingly.
-spec tlog_add_del_on_list(tx_tlog:tlog_entry(), client_key(),
                      client_value(), client_value(), EnDecode::boolean()) ->
                       {tx_tlog:tlog_entry(), api_tx:write_result()}.
tlog_add_del_on_list(Entry, _Key, ToAdd, ToDel, true) when
      (not erlang:is_list(ToAdd)) orelse
      (not erlang:is_list(ToDel)) ->
    %% input type error
    {tx_tlog:set_entry_status(Entry, {fail, abort}), {fail, not_a_list}};
tlog_add_del_on_list(Entry, Key, ToAdd, ToDel, true) ->
    Status = tx_tlog:get_entry_status(Entry),
    {_, Res0} = tlog_read(Entry, Key, true),
    case Res0 of
        {ok, OldValue} when erlang:is_list(OldValue) ->
            %% types ok
            case ToAdd =:= [] andalso ToDel =:= [] of
                true -> {Entry, {ok}}; % no op
                _ ->
                    case value =:= Status
                        orelse {fail, not_found} =:= Status of
                        true ->
                            NewValue1 = lists:append(ToAdd, OldValue),
                            NewValue2 = util:minus_first(NewValue1, ToDel),
                            tlog_write(Entry, Key, NewValue2, true);
                        false -> %% TLog has abort, report ok for this op.
                            {Entry, {ok}}
                    end
            end;
        {fail, not_found} -> %% key creation
            NewValue2 = util:minus_first(ToAdd, ToDel),
            tlog_write(Entry, Key, NewValue2, true);
        {ok, _} -> %% value is not a list
            {tx_tlog:set_entry_status(Entry, {fail, abort}),
             {fail, not_a_list}}
    end;
tlog_add_del_on_list(Entry, Key, ToAdd, ToDel, false) ->
    % note: we can only work with decoded values here
    tlog_add_del_on_list(Entry, Key, decode_value(ToAdd), decode_value(ToDel), true).

-spec tlog_add_on_nr(tx_tlog:tlog_entry(), client_key(),
                     client_value(), EnDecode::boolean()) ->
                             {tx_tlog:tlog_entry(), api_tx:write_result()}.
tlog_add_on_nr(Entry, _Key, X, true) when (not erlang:is_number(X)) ->
    %% check type of input data
    {tx_tlog:set_entry_status(Entry, {fail, abort}),
     {fail, not_a_number}};
tlog_add_on_nr(Entry, Key, X, true) ->
    Status = tx_tlog:get_entry_status(Entry),
    {_, Res0} = tlog_read(Entry, Key, true),
    case Res0 of
        {ok, OldValue} when erlang:is_number(OldValue) ->
            %% types ok
            case X == 0 of %% also accepts 0.0
                true -> {Entry, {ok}}; % no op
                _ ->
                    case value =:= Status orelse
                        {fail, not_found} =:= Status of
                        true ->
                            NewValue = OldValue + X,
                            tlog_write(Entry, Key, NewValue, true);
                        false -> %% TLog has abort, report ok for this op.
                            {Entry, {ok}}
                    end
            end;
        {ok, _} ->
            {tx_tlog:set_entry_status(Entry, {fail, abort}),
             {fail, not_a_number}};
        {fail, not_found} -> %% key creation
            tlog_write(Entry, Key, X, true)
    end;
tlog_add_on_nr(Entry, Key, X, false) ->
    % note: we can only work with decoded values here
    tlog_add_on_nr(Entry, Key, decode_value(X), true).

-spec tlog_test_and_set(tx_tlog:tlog_entry(), client_key(),
                        client_value(), client_value(), EnDecode::boolean()) ->
                               {tx_tlog:tlog_entry(), api_tx:write_result()}.
tlog_test_and_set(Entry, Key, Old, New, EnDecode) ->
    {_, Res0} = tlog_read(Entry, Key, EnDecode),
    case Res0 of
        {ok, Old} ->
            tlog_write(Entry, Key, New, EnDecode);
        {ok, RealOldValue} when not EnDecode ->
            % maybe there is just a different compression scheme
            % -> test decoded values
            Res0Decoded = decode_value(RealOldValue),
            OldDecoded = decode_value(Old),
            if Res0Decoded =:= OldDecoded ->
                   tlog_write(Entry, Key, New, EnDecode);
               true ->
                   {tx_tlog:set_entry_status(Entry, {fail, abort}),
                    {fail, {key_changed, RealOldValue}}}
            end;
        {ok, RealOldValue} ->
            {tx_tlog:set_entry_status(Entry, {fail, abort}),
             {fail, {key_changed, RealOldValue}}};
        X when erlang:is_tuple(X) -> %% other previous error
            {Entry, X}
    end.

%% @doc Encode the given client value to its internal representation which is
%%      compressed for all values except atom, boolean, number or binary.
-spec encode_value(client_value()) -> ?DB:value().
encode_value(Value) when
      is_atom(Value) orelse
      is_boolean(Value) orelse
      is_number(Value) ->
    Value; %%  {nav}
encode_value(Value) when
      is_binary(Value) ->
    %% do not compress a binary
    erlang:term_to_binary(Value, [{minor_version, 1}]);
encode_value(Value) ->
    erlang:term_to_binary(Value, [{compressed, 6}, {minor_version, 1}]).

%% @doc Decodes the given internal representation of a client value.
-spec decode_value(?DB:value()) -> client_value().
decode_value(Value) when is_binary(Value) -> erlang:binary_to_term(Value);
decode_value(Value)                       -> Value.

-spec decode_result(api_tx:result()) -> api_tx:result().
decode_result({ok, Value}) -> {ok, decode_value(Value)};
decode_result(X)           -> X.

%% commit phase
-spec commit(tx_tlog:tlog()) ->  api_tx:commit_result().
commit(TLog) ->
    %% set steering parameters, we need for the transactions engine:
    %% number of retries, etc?
    %% some parameters are checked via the individual operations
    %% read, write which implement the behaviour tx_op_beh.
    case tx_tlog:is_sane_for_commit(TLog) of
        false -> {fail, abort, tx_tlog:get_insane_keys(TLog)};
        true ->
            Client = comm:this(),
            ClientsId = {?commit_client_id, uid:get_global_uid()},
            ?TRACE("rdht_tx:commit(Client ~p, ~p, TLog ~p)~n", [Client, ClientsId, TLog]),
            case pid_groups:find_a(tx_tm) of
                failed ->
                    Msg = io_lib:format("No tx_tm found.~n", []),
                    tx_tm_rtm:msg_commit_reply(Client, ClientsId, {abort, Msg});
                TM ->
                    tx_tm_rtm:commit(TM, Client, ClientsId, TLog)
            end,
            _Result =
                receive
                    ?SCALARIS_RECV(
                       {tx_tm_rtm_commit_reply, ClientsId, commit}, %% ->
                         {ok}  %% commit / abort;
                      );
                    ?SCALARIS_RECV(
                       {tx_tm_rtm_commit_reply, ClientsId, {abort, FailedKeys}}, %% ->
                         begin
                             {fail, abort, FailedKeys} %% commit / abort;
                         end
                       );
                    ?SCALARIS_RECV(
                       {tx_timeout, ClientsId}, %% ->
                       begin
                         log:log(error, "No result for commit received!"),
                         {fail, timeout}
                       end
                      )
                end
    end.

-spec receive_answer() -> {tx_tlog:tx_op(), req_id(), tx_tlog:tlog_entry()}.
receive_answer() ->
    receive
        ?SCALARIS_RECV(
           {tx_tm_rtm_commit_reply, _, _}, %%->
           %% probably an outdated commit reply: drop it.
             receive_answer()
          );
        ?SCALARIS_RECV(
           {tx_timeout, _}, %% ->
           %% probably an outdated commit reply: drop it.
             receive_answer()
          );
        ?SCALARIS_RECV(
           {Op, RdhtId, RdhtTlog}, %% ->
             {Op, RdhtId, RdhtTlog}
          )
    end.

req_get_op(Request) -> element(1, Request).
req_get_key(Request) -> element(2, Request).

%% @doc Checks whether used config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(tx_timeout) and
    config:cfg_is_greater_than_equal(tx_timeout, 1000).

