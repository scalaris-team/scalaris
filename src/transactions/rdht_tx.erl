% @copyright 2009-2014 Zuse Institute Berlin

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
-vsn('$Id$ ').

-compile({inline, [req_get_op/1, req_get_key/1]}).

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-export([req_list/3]).
-export([check_config/0]).
-export([encode_value/1, decode_value/1, is_encoded_value/1]).
-export([req_props/1,
         req_get_key/1, req_get_op/1]).

-include("scalaris.hrl").
-include("client_types.hrl").

-export_type([req_id/0, request_on_key/0, encoded_value/0]).

-type req_id() :: uid:global_uid().
-type request_on_key() :: api_tx:request_on_key().

-type encoded_value() :: atom() | boolean() | number() | binary().

%% @doc Perform several requests inside a transaction.
-spec req_list(tx_tlog:tlog_ext(), [api_tx:request()], EnDecode::true)      -> {tx_tlog:tlog_ext(), [api_tx:result()]};
              (tx_tlog:tlog_ext(), [api_tx:request_enc()], EnDecode::false) -> {tx_tlog:tlog_ext(), [api_tx:result()]}.
req_list([], [{commit}], _EnDecode) -> {[], [{ok}]};
req_list(TLog, ReqList, EnDecode) ->
    %% PRE: TLog is sorted by key, implicitly given, as
    %%      we only generate sorted TLogs.
    ?TRACE("rdht_tx:req_list(~p, ~p, ~p)~n", [TLog, ReqList, EnDecode]),

    %% (0) Check TLog? Costs performance, may save some requests

    %% (1) Ensure commit is only at end of req_list (otherwise abort),
    %% (2) drop {commit} request at end and remember whether to
    %%     commit or not
    case rl_chk(ReqList, []) of
        abort -> tlog_and_results_to_abort(TLog, ReqList);
        {ReqList1, FoundCommitAtEnd} ->
            TLog2 = upd_tlog_via_rdht(TLog, ReqList1),

            %% perform all requests based on TLog to compute result
            %% entries
%%            io:format("Doing work on TLog~n"),
            {NewClientTLog, Results} = do_reqs_on_tlog(TLog2, ReqList1, EnDecode),
%%            io:format("Doing commit on ~p~n", [NewClientTLog]),

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
%%      Cut commit at end and inform caller via boolean (CommitAtEnd).
-spec rl_chk(InTodo::[api_tx:request()], Acc::[api_tx:request()])
        -> abort | {Acc::[api_tx:request()], CommitAtEnd::boolean()}.
rl_chk([], Acc) ->
    {lists:reverse(Acc), false};
rl_chk([{commit}], Acc) ->
    {lists:reverse(Acc), true};
rl_chk([{commit} | _RL], _Acc) ->
    log:log(info, "Commit not at end of a req_list. Deciding abort."),
    abort;
rl_chk([Op | RL], Acc) ->
    rl_chk(RL, [Op | Acc]).

%% @doc Fill all fields with {fail, abort} information.
-spec tlog_and_results_to_abort(tx_tlog:tlog(), [api_tx:request()]) ->
                                       {tx_tlog:tlog(), [api_tx:result()]}.
tlog_and_results_to_abort(TLog, ReqList) ->
    tlog_and_results_to_abort_iter(TLog, ReqList, []).

-spec tlog_and_results_to_abort_iter(tx_tlog:tlog(), [api_tx:request()], [api_tx:result()])
        -> {tx_tlog:tlog(), [api_tx:result()]}.
tlog_and_results_to_abort_iter(TLog, [], AccRes) ->
    {TLog, lists:reverse(AccRes)};
tlog_and_results_to_abort_iter(TLog, [Req | ReqListT], AccRes) ->
    case Req of
        {commit} ->
            Res = {fail, abort, []},
            tlog_and_results_to_abort_iter(TLog, ReqListT, [Res | AccRes]);
        _ ->
            Res = case req_get_op(Req) of
                      read -> {fail, not_found};
                      write -> {ok};
                      add_del_on_list -> {ok};
                      add_on_nr -> {ok};
                      test_and_set -> {ok}
                  end,
            NewTLog = tx_tlog:add_or_update_status_by_key(
                        TLog, req_get_key(Req), ?fail),
            tlog_and_results_to_abort_iter(NewTLog, ReqListT, [Res | AccRes])
    end.

%% @doc Send requests to the DHT, gather replies and merge TLogs.
-spec upd_tlog_via_rdht(tx_tlog:tlog(), [request_on_key()]) -> tx_tlog:tlog().
upd_tlog_via_rdht(TLog, ReqList) ->
    %% what to get from rdht? (also check old TLog)
    SReqList = lists:keysort(2, ReqList),
    ReqListonRDHT = tx_tlog:first_req_per_key_not_in_tlog(TLog, SReqList),

    %% perform RDHT operations to collect missing TLog entries
    %% rdht requests for independent keys are processed in parallel.
    ReqIds = initiate_rdht_ops(ReqListonRDHT),

    RTLog = collect_replies(tx_tlog:empty(), ReqIds),

    %% merge TLogs (insert fail, abort, when version mismatch
    %% in reads for same key is detected)
    _MTLog = tx_tlog:merge(TLog, RTLog).

-spec req_props(Req::request_on_key()) -> {NeedsFullRead::boolean(), WorksAfterAnyPartialRead::boolean(), ProvidesFullRead::boolean()}.
req_props(Req) ->
    case req_get_op(Req) of
        read when tuple_size(Req) =:= 2 -> {true, false, true}; % full read
        read when tuple_size(Req) =:= 3 -> {false, false, false}; % partial read
        test_and_set -> {true, false, true};
        add_on_nr -> {true, false, true};
        add_del_on_list -> {true, false, true};
        write -> {false, true, true}
    end.

%% @doc Trigger operations for the DHT.
-spec initiate_rdht_ops([request_on_key()]) -> [req_id()].
initiate_rdht_ops(ReqList) ->
    ?TRACE("rdht_tx:initiate_rdht_ops(~p)~n", [ReqList]),
    %% @todo should choose a dht_node in the local VM at random or even
    %% better round robin.
    [ begin
          NewReqId = uid:get_global_uid(), % local id not sufficient
          case req_get_op(Entry) of
              write           -> rdht_tx_write:work_phase(self(), NewReqId, Entry);
              read            -> rdht_tx_read:work_phase(self(), NewReqId, Entry);
              test_and_set    -> rdht_tx_test_and_set:work_phase(self(), NewReqId, Entry);
              add_del_on_list -> rdht_tx_add_del_on_list:work_phase(self(), NewReqId, Entry);
              add_on_nr       -> rdht_tx_add_on_nr:work_phase(self(), NewReqId, Entry)
          end,
          NewReqId
      end || Entry <- ReqList ].

%% @doc Collect replies from the quorum DHT operations.
-spec collect_replies(tx_tlog:tlog(), [req_id()]) -> tx_tlog:tlog().
collect_replies(TLog, [_H | _T] = ReqIdsList) ->
    ?TRACE("rdht_tx:collect_replies(~p, ~p)~n", [TLog, ReqIdsList]),
    % receive only matching replies
    {ReqId, RdhtTlogEntry} = receive_answer(),
    case util:lists_take(ReqId, ReqIdsList) of
        false -> collect_replies(TLog, ReqIdsList);
        RemainingReqIds ->
            NewTLog = tx_tlog:add_entry(TLog, RdhtTlogEntry),
            collect_replies(NewTLog, RemainingReqIds)
    end;
collect_replies(TLog, []) ->
    %% Drop outdated results...
    receive_old_answers(),
%%    io:format("Tlog collected ~p~n", [TLog]),
    tx_tlog:sort_by_key(TLog).

%% @doc Perform all operations on the TLog and generate list of results.
-spec do_reqs_on_tlog(tx_tlog:tlog(), [request_on_key()], EnDecode::boolean()) ->
                             {tx_tlog:tlog(), [api_tx:result()]}.
do_reqs_on_tlog(TLog, ReqList, EnDecode) ->
    do_reqs_on_tlog_iter(TLog, ReqList, [], EnDecode).

%% @doc Helper to perform all operations on the TLog and generate list
%%      of results.
%%      TODO: sort the req list similar to the tlog list and parse through both at the same time!
-spec do_reqs_on_tlog_iter(tx_tlog:tlog(), [request_on_key()], [api_tx:result()], EnDecode::boolean())
        -> {tx_tlog:tlog(), [api_tx:result()]}.
do_reqs_on_tlog_iter(TLog, [Req | ReqTail], Acc, EnDecode) ->
    {NewTLogEntry, ResultEntry} =
        case Req of
            %% native functions first:
            {read, Key} ->
                Entry = tx_tlog:find_entry_by_key(TLog, Key),
                rdht_tx_read:extract_from_tlog(Entry, Key, read, EnDecode);
            {read, Key, Op} ->
                Entry = tx_tlog:find_entry_by_key(TLog, Key),
                rdht_tx_read:extract_from_tlog(Entry, Key, Op, EnDecode);
            {write, Key, Value} ->
                Entry = tx_tlog:find_entry_by_key(TLog, Key),
                rdht_tx_write:extract_from_tlog(Entry, Key, Value, EnDecode);
            %% non-native functions:
            {add_del_on_list, Key, ToAdd, ToDel} ->
                Entry = tx_tlog:find_entry_by_key(TLog, Key),
                rdht_tx_add_del_on_list:extract_from_tlog(Entry, Key, ToAdd, ToDel, EnDecode);
            {add_on_nr, Key, X} ->
                Entry = tx_tlog:find_entry_by_key(TLog, Key),
                rdht_tx_add_on_nr:extract_from_tlog(Entry, Key, X, EnDecode);
            {test_and_set, Key, Old, New} ->
                Entry = tx_tlog:find_entry_by_key(TLog, Key),
                rdht_tx_test_and_set:extract_from_tlog(Entry, Key, Old, New, EnDecode)
        end,
    NewTLog = tx_tlog:update_entry(TLog, NewTLogEntry),
    do_reqs_on_tlog_iter(NewTLog, ReqTail, [ResultEntry | Acc], EnDecode);
do_reqs_on_tlog_iter(TLog, [], Acc, _EnDecode) ->
    {tx_tlog:cleanup(TLog), lists:reverse(Acc)}.

-spec is_encoded_value(term()) -> boolean().
is_encoded_value(Value) when is_atom(Value) -> true;
is_encoded_value(Value) when is_boolean(Value) -> true;
is_encoded_value(Value) when is_number(Value) -> true;
is_encoded_value(Value) when is_binary(Value) ->
    try decode_value(Value) of _ -> true
    catch error:badarg -> false
    end;
is_encoded_value(_) -> false.

%% @doc Encode the given client value to its internal representation which is
%%      compressed for all values except atom, boolean, number or binary.
-spec encode_value(client_value()) -> encoded_value().
encode_value(Value) when is_atom(Value) -> Value;
encode_value(Value) when is_boolean(Value) -> Value;
encode_value(Value) when is_number(Value) -> Value;
encode_value(Value) when is_binary(Value) ->
    %% do not compress a binary
    erlang:term_to_binary(Value, [{minor_version, 1}]);
encode_value(Value) ->
    erlang:term_to_binary(Value, [{compressed, 6}, {minor_version, 1}]).

%% @doc Decodes the given internal representation of a client value.
%%      TODO: use the stronger variant (commented out for the bug in rdht_tx_read:extract_from_tlog/4)
-spec decode_value(encoded_value()) -> client_value().
%% decode_value(Value) when is_atom(Value) -> Value;
%% decode_value(Value) when is_boolean(Value) -> Value;
%% decode_value(Value) when is_number(Value) -> Value;
decode_value(Value) when is_binary(Value) -> erlang:binary_to_term(Value);
decode_value(Value)                       -> Value.

%% commit phase
-spec commit(tx_tlog:tlog()) -> api_tx:commit_result().
-ifdef(TXNEW).
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
            case pid_groups:find_a(tx_tm_new) of
                failed ->
                    Msg = io_lib:format("No tx_tm_new found.~n", []),
                    tx_tm:msg_commit_reply(Client, ClientsId, {abort, Msg});
                TM ->
                    tx_tm:commit(TM, Client, ClientsId, TLog)
            end,
            _Result =
                begin
                    trace_mpath:thread_yield(),
                    %% log:log("Got execution right and finished commit ~p", [self()]),
                    receive
                    ?SCALARIS_RECV(
                       {tx_tm_commit_reply, ClientsId, commit}, %% ->
                         {ok}  %% commit / abort;
                      );
                    ?SCALARIS_RECV(
                       {tx_tm_commit_reply, ClientsId, {abort, FailedKeys}}, %% ->
                         {fail, abort, FailedKeys} %% commit / abort;
                       )
                end end
    end.
-else.
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
                begin
                    trace_mpath:thread_yield(),
                    receive
                        ?SCALARIS_RECV(
                           {tx_tm_rtm_commit_reply, ClientsId, commit}, %% ->
                           {ok}  %% commit / abort;
                          );
                        ?SCALARIS_RECV(
                           {tx_tm_rtm_commit_reply, ClientsId, {abort, FailedKeys}}, %% ->
                           {fail, abort, FailedKeys} %% commit / abort;
                          )
                    end end
    end.
-endif.

-spec receive_answer() -> {req_id(), tx_tlog:tlog_entry()}.
-ifdef(TXNEW).
 receive_answer() ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
%%%% OLD TX
% {tx_tm_rtm_commit_reply, _, _}, %%->
           {tx_tm_commit_reply, _, _}, %%->
           %% probably an outdated commit reply: drop it.
             receive_answer()
          );
        ?SCALARIS_RECV(
           {_Op, ReqId, RdhtTlog}, %% ->
             {ReqId, RdhtTlog}
          )
    end.
-else.
receive_answer() ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
           {tx_tm_rtm_commit_reply, _, _}, %%->
           %% probably an outdated commit reply: drop it.
           receive_answer()
          );
        ?SCALARIS_RECV(
           {_Op, ReqId, RdhtTlog}, %% ->
           {ReqId, RdhtTlog}
          )
        end.
-endif.

-spec receive_old_answers() -> ok.
-ifdef(TXNEW).
receive_old_answers() ->
    % note: do not yield trace_mpath thread with "after 0"!
%%    receive
%%%%%% OLD TX
%%%   ?SCALARIS_RECV({tx_tm_rtm_commit_reply, _, _}, receive_old_answers());
%%        ?SCALARIS_RECV({tx_tm_commit_reply, _, _}, receive_old_answers());
%%        ?SCALARIS_RECV({_Op, _RdhtId, _RdhtTlog}, receive_old_answers())
%%    after 0 -> ok
%%    end.
    ok.
-else.
receive_old_answers() ->
    % note: do not yield trace_mpath thread with "after 0"!
    receive
        ?SCALARIS_RECV({tx_tm_commit_reply, _, _}, receive_old_answers());
        ?SCALARIS_RECV({_Op, _RdhtId, _RdhtTlog}, receive_old_answers())
    after 0 -> ok
    end.
-endif.

-spec req_get_op(api_tx:request_on_key())
                -> read | write | add_del_on_list | add_on_nr | test_and_set.
req_get_op(Request) -> element(1, Request).
-spec req_get_key(api_tx:request_on_key())
                 -> api_tx:client_key().
req_get_key(Request) -> element(2, Request).

%% @doc Checks whether used config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(tx_timeout) and
    config:cfg_is_greater_than_equal(tx_timeout, 1000).

