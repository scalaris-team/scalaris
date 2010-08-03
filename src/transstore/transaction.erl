%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%
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
%%%-------------------------------------------------------------------
%%% File    : transaction.erl
%%% Author  : Monika Moser <moser@zib.de>
%%% Description : transaction algorithm
%%%
%%% Created :  9 Jul 2007 by Monika Moser <moser@zib.de>
%%%-------------------------------------------------------------------
%% @author Monika Moser <moser@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(transaction).

-author('moser@zib.de').
-vsn('$Id$').

-include("trecords.hrl").
-include("scalaris.hrl").

-export([do_transaction/5,
         do_transaction_wo_readphase/6,
         generateTID/1,
         getRTMKeys/1,
         initRTM/2,
         write/3,
         read/2,
         delete/2,
         do_delete/3,
         parallel_reads/2,
         do_parallel_reads/4,
         parallel_quorum_reads/3,
         quorum_read/2,
         do_quorum_read/3
%        write_read_receive/3
         ]).


%%--------------------------------------------------------------------
%% Function: do_transaction(State,
%%                          TransFun,
%%                          SuccessMessage,
%%                          FailureFun,
%%                          Owner) -> {TID, TM}
%% Description: Starts the leading transaction manager with all
%%                 information it needs
%%--------------------------------------------------------------------
do_transaction(State, TransFun, SuccessMessage, FailureFun, Owner)->
    TID = transaction:generateTID(State),
    InstanceId = erlang:get(instance_id),
    TM = spawn(tmanager, start_manager, [TransFun, SuccessMessage, FailureFun, Owner,  TID, InstanceId]),
    {{tid, TID},{tm, TM}}.

%%--------------------------------------------------------------------
%% Function: do_transaction(State,
%%                          Items,
%%                          SuccessFunArgument,
%%                          SuccessFun,
%%                          FailureFun,
%%                          Owner) -> {TID, TM}
%% Description: Starts the leading transaction manager with all
%%                 information it needs, without a readphase
%%--------------------------------------------------------------------
do_transaction_wo_readphase(State, Items, _SuccessFunArgument, SuccessFun, FailureFun, Owner)->
    TID = transaction:generateTID(State),
    InstanceId = erlang:get(instance_id),
    TM = spawn(tmanager, start_manager_commit, [Items, SuccessFun, FailureFun, Owner, TID, InstanceId]),
    {{tid, TID},{tm, TM}}.

%%====================================================================
%% operations on data
%%====================================================================

%%--------------------------------------------------------------------
%% Function: write(Key, Value, TransLog) -> {{fail, not_found}, TransLog} |
%%                                          {{fail, timeout}, TransLog} |
%%                                          {{fail, fail}, TransLog} |
%%                                          {ok, NewTransLog}
%% Description: Executes a write operation
%%              - retrieves the version number remotely
%%              - adds the operation to TransLog
%%--------------------------------------------------------------------

write(Key, Value, TransLog)->
    read_or_write(Key, Value, TransLog, write).
read(Key, TransLog)->
    read_or_write(Key, 0, TransLog, read).

read_or_write(Key, Value, TransLog, Operation) ->
    ElementsInTransLog = txlog:filter_by_key(TransLog, Key),
    %% check whether we have already logged the item
    case ElementsInTransLog of
        [Element] ->
            %% retrieve the information from the log.
            %% for a write, update the value and the version number.
            case {txlog:get_entry_status(Element),Operation} of
                {ok, write} ->
                    {ok, txlog:update_entry(TransLog, Element, write, Value)};
                {ok, read} ->
                    {{value, txlog:get_entry_value(Element)}, TransLog};
                _ ->
                    {{fail, fail}, TransLog}
            end;
        [] ->
            %% we do not have any information for the key in the log read the
            %% information from remote
            ReplicaKeys = ?RT:get_keys_for_replicas(Key),
            [ lookup:unreliable_get_key(X) || X <- ReplicaKeys ],
            erlang:send_after(config:read(transaction_lookup_timeout), self(),
                              {write_read_receive_timeout, hd(ReplicaKeys)}),
            {Flag, Result} = write_read_receive(ReplicaKeys, Operation),
            if Flag =:= fail ->
                    NewTransLog =
                        txlog:add_entry(TransLog,
                          txlog:new_entry(Operation, Key, fail, 0, 0)),
                    {{fail, Result}, NewTransLog};
               true -> %% Flag == value
                    {ReadVal, Version} = Result,
                    {NewVersion, NewVal} = case Operation of
                                               write -> {Version + 1, Value};
                                               read -> {Version, ReadVal}
                                           end,
                    NewTransLog =
                      txlog:add_entry(TransLog,
                        txlog:new_entry(Operation, Key, ok, NewVal, NewVersion)),

                    case Operation of
                        write -> {ok, NewTransLog};
                        read -> {{value, NewVal}, NewTransLog}
                    end
            end;
        _ ->
            %% there should be only one entry per item
            {{fail, fail}, TransLog}
    end.

%%--------------------------------------------------------------------
%% Function: quorum_read(Key, SourcePID) -> ,fail |
%%                               {value, Value}
%% Description: Executes a read operation
%%              - retrieves value and version number remotely
%%--------------------------------------------------------------------
quorum_read(Key, SourcePID)->
    InstanceId = erlang:get(instance_id),
    spawn(transaction, do_quorum_read, [Key, SourcePID, InstanceId]).

do_quorum_read(Key, SourcePID, InstanceId)->
    erlang:put(instance_id, InstanceId),
    ReplicaKeys = ?RT:get_keys_for_replicas(Key),
    [ lookup:unreliable_get_key(X) || X <- ReplicaKeys ],
    erlang:send_after(config:read(transaction_lookup_timeout), self(),
                      {write_read_receive_timeout, hd(ReplicaKeys)}),
    {Flag, Result} = write_read_receive(ReplicaKeys, read),
    if
        Flag =:= fail ->
            comm:send(SourcePID, {single_read_return, {fail, Result}});
        true ->
            {Value, Version} = Result,
            comm:send(SourcePID, {single_read_return,{value, Value, Version}})
    end.

write_read_receive(ReplicaKeys, Operation)->
    write_read_receive(ReplicaKeys, Operation,
                       {config:read(replication_factor),
                        config:read(quorum_factor),
                        0, 0, {0,-1}}).

write_read_receive(ReplicaKeys, Operation, State)->
    {ReplFactor, Quorum, NumOk, NumFailed, Result} = State,
    if (NumOk >= Quorum) ->
            {value, Result};
       (NumFailed >= Quorum)
       andalso (Operation =:= write) ->
            % Assume a new key
            {value, {0, -1}};
       (NumFailed >= Quorum) ->
            {fail, not_found};
       true ->
            receive
                {get_key_response, Key, failed} ->
                    case lists:member(Key, ReplicaKeys) of
                        true ->
                            write_read_receive(ReplicaKeys, Operation,
                                               {ReplFactor, Quorum,
                                                NumOk, 1 + NumFailed, Result});
                        false ->
                            write_read_receive(ReplicaKeys, Operation, State)
                    end;
                {get_key_response, Key, {ok, Value, Versionnr}} ->
                    case lists:member(Key, ReplicaKeys) of
                        true ->
                            {_OldVal, OldVersnr} = Result,
                            NewResult = case (Versionnr >= OldVersnr) of
                                            true -> {Value, Versionnr};
                                            false -> Result
                                        end,
                            write_read_receive(ReplicaKeys, Operation,
                                               {ReplFactor, Quorum,
                                                NumOk + 1, NumFailed, NewResult});
                       false ->
                            write_read_receive(ReplicaKeys, Operation, State)
                    end;
                {write_read_receive_timeout, _Key} ->
                    {fail, timeout};
                Any ->
                    io:format(standard_error,
                              "transaction:write_read_receive: Oops, unknown message ~p~n", [Any]),
                    write_read_receive(ReplicaKeys, Operation, State)
            end
    end.

%% @doc Executes parallel reads and sends the result to SourcePid, i.e. a
%%      {parallel_reads_return, fail} message in case of failures, otherwise
%%      {parallel_reads_return, NewTransLog} with the new translog.
%%      Needs a TransLog to collect all operations that are part of the
%%      transaction. Use this function if you want to do parallel reads
%%      without a transaction.
%%--------------------------------------------------------------------
-spec parallel_quorum_reads(Keys::[iodata() | integer()], TransLog::any(), SourcePID::comm:mypid()) -> pid().
parallel_quorum_reads(Keys, TransLog, SourcePID) ->
    InstanceId = erlang:get(instance_id),
    spawn(transaction, do_parallel_reads, [Keys, SourcePID, TransLog, InstanceId]).

-spec do_parallel_reads(Keys::[iodata() | integer()], SourcePID::comm:mypid(), TransLog::any(), InstanceId::instanceid()) -> ok.
do_parallel_reads(Keys, SourcePID, TransLog, InstanceId)->
    erlang:put(instance_id, InstanceId),
    {Flag, NewTransLog} = parallel_reads(Keys, TransLog),
    if
        Flag =:= fail->
            comm:send(SourcePID, {parallel_reads_return, fail});
        true ->
            comm:send(SourcePID, {parallel_reads_return, NewTransLog})
    end.

%%--------------------------------------------------------------------
%% @doc Executes a parallel read operation
%%       - retrieves value and version number remotely
%%       - adds the operation to TransLog
-spec parallel_reads(Keys::[iodata() | integer()], TransLog) -> {success | fail, TransLog}.
parallel_reads(Keys, TransLog) ->
    TLogCheck = check_trans_log(Keys, TransLog, [], []),

    if TLogCheck =:= fail->
            {fail, TransLog};
       true ->
            {ok, {_Result, ToLookup}} = TLogCheck,
            %% we do not have any information for the key in the log
            %% read the information from remote
            %% get a list with all replica keys
            %% [[Key, RKey1, RKey2, ..., RKey3], [Key2, ....], ...]
            ReplicaKeysAll =
                [ ?RT:get_keys_for_replicas(Elem) ||
                    Elem <- ToLookup ],

            lists:map(fun(ReplicaKeys)->
                              [ lookup:unreliable_get_key(ReplicaKey) ||
                                  ReplicaKey <- ReplicaKeys ]
                      end,
                      ReplicaKeysAll),

            ResultsInit = [ {Key, [], undecided} ||
                              [Key | _RKeys] <- ReplicaKeysAll ],

            erlang:send_after(config:read(transaction_lookup_timeout), self(),
                              {write_read_receive_timeout, hd(hd(ReplicaKeysAll))}),
            {Flag, WRResult} = write_read_receive_parallel(ResultsInit, ReplicaKeysAll),
            if
                Flag =:= fail ->
                    {fail, TransLog};
                true ->
                    NewTranslog = build_translog(WRResult),
                    NewTranslog2 = lists:append(NewTranslog, TransLog),
                    {success, NewTranslog2}
            end
    end.

%%====================================================================
%% Helper functions for parallel reads:
%%                    *check whether there is already a result in
%%                         the translog
%%                    *retrieve the results from read/write
%%                    *filter the results
%%====================================================================

check_trans_log([], _TransLog, ResultAccum, ToLookupAccum)->
    {ok, {ResultAccum, ToLookupAccum}};

check_trans_log([Key | Rest], TransLog, ResultAccum, ToLookupAccum)->
    ElementsInTransLog = [ X || {_, ElemKey, _, _, _} = X <- TransLog,
                                Key =:= ElemKey ],
    NumInLog = length(ElementsInTransLog),
    %% check whether we have already logged the item
    if
        %% there should not be more than one entry for each item
        NumInLog > 1 ->
            %%something went wrong
            fail;
        %% retrieve the information from the log if already there
        %% for a read operation we do not have to add further info in the log
        %% as it has the same validation conditions than previous operations
        NumInLog =:= 1 ->
            [{_,_,Success,Value,_}] =  ElementsInTransLog,
            if
                Success =:= ok ->
                    NewResultAccum = [{Key, Value}|ResultAccum],
                    check_trans_log(Rest, TransLog, NewResultAccum, ToLookupAccum);
                true ->
                    fail
            end;
        true->
            NewToLookupAccum = [Key | ToLookupAccum],
            check_trans_log(Rest, TransLog, ResultAccum, NewToLookupAccum)
    end.

write_read_receive_parallel(Results, ReplicaKeys)->
    receive
        {get_key_response, Key, failed} ->
            NewResults = add_result(Results, Key, fail, Results),
            CRRes = check_results_parallel(NewResults, NewResults),
            case CRRes of
                continue ->
                    write_read_receive_parallel(NewResults, ReplicaKeys);
                _ -> % {found, TLog}
                    CRRes
            end;
        {get_key_response, Key, {ok, Value, Versionnr}} ->
            NewResults = add_result(Results, Key, {Value, Versionnr}, Results),
            CRRes = check_results_parallel(NewResults, NewResults),
            case CRRes of
                continue ->
                    write_read_receive_parallel(NewResults, ReplicaKeys);
                _ -> % {found, TLog}
                    CRRes
            end;
        {write_read_receive_timeout, _Key} ->
            {fail, timeout};
        Any ->
            io:format(standard_error, "transaction:write_read_receive_parallel: Oops, unknown message ~p~n", [Any]),
            write_read_receive_parallel(Results, ReplicaKeys)
    end.

add_result([Head | Results], Key, Result, AllResults)->
    {CurrKey, ResultsForKey, EndResult} = Head,
    OrigKey = ?RT:get_original_key(Key),
    if
        CurrKey =:= OrigKey ->
            NewAllResults = lists:delete(Head, AllResults),
            [{CurrKey, [Result | ResultsForKey], EndResult} | NewAllResults];
        true ->
            add_result(Results, Key, Result, AllResults)
    end;
add_result([], _Key, _Result, AllResults) ->
    AllResults.


check_results_parallel([], AllResults)->
    {found, AllResults};

check_results_parallel([Head |Results], AllResults)->
    {Key, ResKey, EndRes} = Head,
    if
        EndRes =/= undecided ->
            check_results_parallel(Results, AllResults);
        true ->
            TMPResults = [ Elem || Elem <- ResKey, Elem =/= fail ],
            TMPResultsFailed = [ Elem || Elem <- ResKey, Elem =:= fail ],

            ReplFactor = config:read(replication_factor),
            QuorumFactor = config:read(quorum_factor),
            NumSuccessfulResponses = length(TMPResults),
            NumFailedResponses = length(TMPResultsFailed),

            if
                NumSuccessfulResponses >= QuorumFactor ->
                    MaxElem = get_max_element(TMPResults, {0,-1}),
                    NewAllResults = lists:delete(Head, AllResults),
                    NewAllResults2 = [{Key, ResKey, MaxElem} | NewAllResults],
                    check_results_parallel(Results, NewAllResults2);
                NumFailedResponses >= (QuorumFactor)->
                    NewAllResults = lists:delete(Head, AllResults),
                    NewAllResults2 = [{Key, ResKey, {0, -1}} | NewAllResults],
                    check_results_parallel(Results, NewAllResults2);
                true ->
                    NumResponses = length(ResKey),
                    if
                        NumResponses =:= ReplFactor ->
                            NewAllResults = lists:delete(Head, AllResults),
                            NewAllResults2 = [{Key, ResKey, {0, -1}} | NewAllResults],
                            check_results_parallel(Results, NewAllResults2);
                        true ->
                            continue
                    end
            end
    end.

get_max_element([], {ValMaxElement, VersMaxElement})->
    {ValMaxElement, VersMaxElement};
get_max_element([{Value, Versionnr}|Rest], {ValMaxElement, VersMaxElement})->
    get_max_element(Rest, case (Versionnr > VersMaxElement) of
                              true -> {Value, Versionnr};
                              false -> {ValMaxElement, VersMaxElement}
                          end).

build_translog(Results)->
    EndResultAccum = [],
    lists:foldl(fun(Elem, Acc)->
                        {CurrKey, _ResultsForKey, {Value, Version}} = Elem,
                        if
                            Version =:= -1 ->
                                [{read, CurrKey, fail, Value, Version}| Acc];
                            true ->
                                [{read, CurrKey, ok, Value, Version}| Acc]
                        end
                end,
                EndResultAccum,
                Results).


%%====================================================================
%% functions needed to initialize transaction managers
%%====================================================================


generateTID(State)->
    Node = dht_node_state:get(State, node),
    {node:id(Node), now()}.



%%--------------------------------------------------------------------
%% Function: getRTMKeys(TID)-> [Keys]
%% Description: Get the keys for the nodes that will act as TMs
%%                  based on symmetric replication on the TID
%%--------------------------------------------------------------------


getRTMKeys(TID)->
    {Key, _} = TID,
    RKeys = ?RT:get_keys_for_replicas(Key),
    lists:delete(Key, RKeys).

%%--------------------------------------------------------------------
%% Function: initRTM(State, Message)-> State
%% Description: initializes rTMs using information included in Message
%%--------------------------------------------------------------------

initRTM(State, Message)->
    TransID = Message#tm_message.transaction_id,
    ERTMPID = spawn(tmanager, start_replicated_manager, [Message, erlang:get(instance_id)]),
    RTMPID = comm:make_global(ERTMPID),
    %% update transaction log: store mapping between transaction ID and local TM
    TransLog = dht_node_state:get(State, trans_log),
    New_TID_TM_Mapping = dict:store(TransID, RTMPID, TransLog#translog.tid_tm_mapping),
    NewTransLog = TransLog#translog{tid_tm_mapping = New_TID_TM_Mapping},
    dht_node_state:set_trans_log(State, NewTransLog).

%% @doc deletes all replicas of an item, but respects locks
%%      the return value is the number of successfully deleted items
%%      WARNING: this function can lead to inconsistencies
-spec delete(SourcePid::comm:mypid(), Key::iodata() | integer()) -> ok.
delete(SourcePID, Key) ->
    InstanceId = erlang:get(instance_id),
    spawn(transaction, do_delete, [Key, SourcePID, InstanceId]), 
    ok.

-spec do_delete(Key::iodata() | integer(), SourcePid::comm:mypid(), InstanceId::instanceid()) -> ok.
do_delete(Key, SourcePID, InstanceId)->
    erlang:put(instance_id, InstanceId),
    ReplicaKeys = ?RT:get_keys_for_replicas(Key),
    [ lookup:unreliable_lookup(Replica, {delete_key, comm:this(), Replica}) ||
        Replica <- ReplicaKeys],
    erlang:send_after(config:read(transaction_lookup_timeout), self(), {timeout}),
    delete_collect_results(ReplicaKeys, SourcePID, []).

%% @doc collect the response for the delete requests
-spec delete_collect_results(ReplicaKeys::[?RT:key()], SourcePid::comm:mypid(),
				             Results::[ok | locks_set | undef]) -> ok.
delete_collect_results([], SourcePID, Results) ->
    OKs = length([ok || R <- Results, R =:= ok]),
    comm:send(SourcePID, {delete_result, {ok, OKs, Results}});
delete_collect_results(ReplicaKeys, Source_PID, Results) ->
    receive
        {delete_key_response, Key, Result} ->
            case lists:member(Key, ReplicaKeys) of
                true ->
                    delete_collect_results(lists:delete(Key, ReplicaKeys),
                                           Source_PID, [Result | Results]);
                false ->
                    delete_collect_results(ReplicaKeys, Source_PID, Results)
            end;
        {timeout} ->
            OKs = length([ok || R <- Results, R =:= ok]),
            comm:send(Source_PID,
                      {delete_result, {fail, timeout, OKs, Results}})
    end.
