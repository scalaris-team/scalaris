% @copyright 2009, 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin,
%                 onScale solutions GmbH

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
%% @doc    API for transactions on replicated DHT items.
%% @version $Id$
-module(rdht_tx).
-author('schintke@onscale.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-export([process_request_list/2]).
-export([check_config/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

-ifdef(with_export_type_support).
-export_type([req_id/0, request/0, result_entry/0, result/0]).
-endif.

-type req_id() :: {rdht_req_id, util:global_uid()}.
-type request() :: {rdht_tx_read, client_key()} | {rdht_tx_write, client_key(), client_value()} | {commit}.
-type result_entry() :: any(). %% TODO: specify more strict.
-type result() :: [ result_entry() ].

-spec process_request_list(tx_tlog:tlog(), [request()]) ->
        {tx_tlog:tlog(), {results, result()}}.
%% single request and empty translog, done separately for optimization only
process_request_list([], [SingleReq]) ->
    RdhtOpWithReqId = initiate_rdht_ops([{1, SingleReq}]),
    {NewTLog, TmpResultList, [], [], []} =
        collect_results_and_do_translogops({[], [], RdhtOpWithReqId, [], []}),
    TransLogResult = NewTLog,
    [{_, ResultEntry}] = TmpResultList,
    {TransLogResult, {results, [ResultEntry]}};

process_request_list(TLog, PlainReqList) ->
    ?TRACE("rdht_tx:process_request_list(~p, ~p)~n", [TLog, PlainReqList]),
    %% PRE: no 'abort/tx_failed' in TLog
    %% rdht requests for independent keys are processed in parallel.
    %% requests for the same key are executed in order with
    %% accumulated translog.
    %% if translog entry exists, do work_phase inside this process to
    %% reduce overall latency.
    ReqList = lists:zip(lists:seq(1, length(PlainReqList)), PlainReqList),
    %% split into 'rdht', 'delayed' and 'translog based' operations
    {RdhtOps, Delayed, TransLogOps, Commit} = my_split_ops(TLog, ReqList),

    RdhtOpsWithReqIds = initiate_rdht_ops(RdhtOps),
    {NewTLog, TmpResultList, [], [], []} =
        collect_results_and_do_translogops({TLog, [], RdhtOpsWithReqIds,
                                            Delayed, TransLogOps}),
    %% Now all op lists are empty.
    %% @TODO only if TransLog is ok, do the validation here if requested
    CommitResult = case Commit of
                       []                -> none;
                       [{_Num,{commit}}] -> commit(NewTLog)
                   end,
    TransLogResult = NewTLog,
    %% Sort resultlist and eliminate numbering
    {_, Tmp2ResultList} = lists:unzip(
                        lists:sort(fun(A, B) ->
                                           element(1, A) =< element(1, B)
                                   end, TmpResultList)),
    %% append result entry for commit
    ResultList = case CommitResult of
                     none -> Tmp2ResultList;
                     _ -> lists:append(Tmp2ResultList, [CommitResult])
                 end,
    %% return the NewTLog and a result list
    {TransLogResult, {results, ResultList}}.

%% implementation
my_split_ops(TLog, ReqList) ->
    ?TRACE("rdht_tx:my_split_ops(~p, ~p)~n", [TLog, ReqList]),
    Splitter =
        fun(ReqEntry, {RdhtOps, Delayed, TransLogOps, Commit}) ->
          {_Num, Entry} = ReqEntry,
          case element(1, Entry) of
              commit ->
                  {RdhtOps, Delayed, TransLogOps, [ReqEntry | Commit]};
              _ -> case {lists:keymember(element(2, Entry), 2, TLog),
                         my_key_in_numbered_reqlist(element(2, Entry), RdhtOps)}
                   of
                       {true,_} ->
                           {RdhtOps, Delayed, [ ReqEntry | TransLogOps ],
                            Commit};
                       {false, true} ->
                           {RdhtOps, [ReqEntry | Delayed], TransLogOps, Commit};
                       {false, false} ->
                           {[ReqEntry | RdhtOps], Delayed, TransLogOps, Commit}
                   end
          end
        end,
    {A, B, C, D} = lists:foldl(Splitter, {[],[],[],[]}, ReqList),
    {lists:reverse(A), lists:reverse(B), lists:reverse(C), lists:reverse(D)}.

my_key_in_numbered_reqlist(_Key, []) -> false;
my_key_in_numbered_reqlist(Key, [{_Num, Entry} | Tail]) ->
    case Key =:= element(2, Entry) of
        true -> true;
        false -> my_key_in_numbered_reqlist(Key, Tail)
    end.

initiate_rdht_ops(ReqList) ->
    ?TRACE("rdht_tx:initiate_rdht_ops(~p)~n", [ReqList]),
    [ begin
          NewReqId = {rdht_req_id, util:get_global_uid()}, % local id not sufficient
          apply(element(1, Entry), work_phase, [self(), NewReqId, Entry]),
          {NewReqId, {Num, Entry}}
      end || {Num, Entry} <- ReqList ].

%% all ops done -> terminate!
collect_results_and_do_translogops({TLog, Results, [], [], []}) ->
    {TLog, Results, [], [], []};
%% single request and empty translog, done separately for optimization only
collect_results_and_do_translogops({[], [], [RdhtOpWithReqId], [], []}
                                   = Args) ->
    {_, RdhtId, RdhtTlog, RdhtResult} = receive_answer(),
    case lists:keyfind(RdhtId, 1, [RdhtOpWithReqId]) of
        false ->
            %% Drop outdated result...
            collect_results_and_do_translogops(Args);
        _ -> {[RdhtTlog], [{1, RdhtResult}], [], [], []}
    end;

%% all translogops done -> wait for a RdhtOpReply
collect_results_and_do_translogops({TLog, Results, RdhtOpsWithReqIds,
                                    Delayed, []} = Args) ->
    ?TRACE("rdht_tx:collect_results_and_do_translogops(~p)~n", [Args]),
    {_, TRdhtId, _, _} = TReply = receive_answer(),
    {_, RdhtId, RdhtTlog, RdhtResult} =
        case lists:keyfind(TRdhtId, 1, RdhtOpsWithReqIds) of
        false ->
            %% Drop outdated result...
            collect_results_and_do_translogops(Args);
        _ -> TReply
    end,
    %% add TLog entry, as it is guaranteed a new entry
    NewTLog = [RdhtTlog | TLog],
    %% lookup Num for Result entry and add that
    NumList = [ X || {TmpId, {X, _}} <- RdhtOpsWithReqIds,
                     TmpId =:= RdhtId],
    [ThisNum] = NumList,
    NewResults = [{ThisNum, RdhtResult} | Results],
    NewRdhtOpsWithReqIds =
        [ X || {ThisId, _} = X <- RdhtOpsWithReqIds, ThisId =/= RdhtId],
    %% release correspondig delayed ops to translogops
    Key = erlang:element(2, RdhtTlog),
    {NewTransLogOps, NewDelayed} =
        lists:partition(
          fun({_Num, Req}) -> Key =:= erlang:element(2, Req)
          end, Delayed),
    %% repeat tail recursively
    collect_results_and_do_translogops(
      {NewTLog, NewResults, NewRdhtOpsWithReqIds,
       NewDelayed, NewTransLogOps});
%% do translog ops
collect_results_and_do_translogops({TLog, Results, RdhtOpsWithReqIds,
                                    Delayed, TransLogOps} = _Args) ->
    ?TRACE("rdht_tx:collect_results_and_do_translogops(~p)~n", [_Args]),
    {NewTLog, TmpResults} = do_translogops(TransLogOps, {TLog, []}),
    collect_results_and_do_translogops(
      {NewTLog, TmpResults ++ Results, RdhtOpsWithReqIds, Delayed, []}).

do_translogops([], Results) -> Results;
do_translogops([{Num, Entry} | TransLogOpsTail], {TLog, OldResults}) ->
    %% do the translogops one by one, to always use the newest TLog
    ?TRACE("rdht_tx:do_translogops(~p, ~p)~n",
           [[{Num, Entry} | TransLogOpsTail], {TLog, OldResults}]),
    Key = element(2, Entry),
    TLogEntry = lists:keyfind(Key, 2, TLog),
    {TmpTLogEntry, Result} =
        apply(element(1, Entry), work_phase, [TLogEntry, {Num, Entry}]),
    %% which entry to use?
    NewTLog = case {element(1,TLogEntry), element(1, TmpTLogEntry)} of
                {rdht_tx_read, rdht_tx_read} -> TLog;
                {rdht_tx_write, rdht_tx_read} -> TLog;
                {rdht_tx_write, rdht_tx_write} ->
                    lists:keyreplace(Key, 2, TLog, TmpTLogEntry);
                {rdht_tx_read, rdht_tx_write} ->
                    lists:keyreplace(Key, 2, TLog, TmpTLogEntry)
              end,
    do_translogops(TransLogOpsTail, {NewTLog, [Result | OldResults]}).

%% commit phase
commit(TLog) ->
    %% set steering parameters, we need for the transactions engine:
    %% number of retries, etc?
    %% some parameters are checked via the individual operations
    %% rdht_tx_read, rdht_tx_write which implement the behaviour tx_op_beh.
    Client = comm:this(),
    ClientsId = {commit_client_id, util:get_global_uid()},
    ?TRACE("rdht_tx:commit(Client ~p, ~p, TLog ~p)~n", [Client, ClientsId, TLog]),
    case pid_groups:find_a(tx_tm) of
        failed ->
            Msg = io_lib:format("No tx_tm found.~n", []),
            tx_tm_rtm:msg_commit_reply(Client, ClientsId, {fail, Msg});
        TM ->
            tx_tm_rtm:commit(TM, Client, ClientsId, TLog)
    end,
    msg_delay:send_local_as_client(config:read(tx_timeout) div 1000,
                         self(), {tx_timeout, ClientsId}),
    _Result =
        receive
            {tx_tm_rtm_commit_reply, ClientsId, Decision} ->
                {Decision}; %% commit / abort;
            {tx_timeout, ClientsId} ->
                log:log(error, "No result for commit received!"),
                {failed, timeout}
        end.

receive_answer() ->
    receive
        {tx_tm_rtm_commit_reply, _, _} ->
            %% probably an outdated commit reply: drop it.
            receive_answer();
        {tx_timeout, _} ->
            %% probably an outdated commit reply: drop it.
            receive_answer();
        {_Op, _RdhtId, _RdhtTlog, _RdhtResult} = Reply -> Reply
    end.

%%% delete


%% @doc Checks whether used config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(tx_timeout) and
    config:is_greater_than_equal(tx_timeout, 1000).

