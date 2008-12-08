%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(transstore.transaction).

-author('moser@zib.de').
-vsn('$Id$ ').

-include("trecords.hrl").

-export([do_transaction/5, do_transaction_wo_readphase/6, generateTID/1, getRTMKeys/1, initRTM/2, write/3, read/2, parallel_reads/2, do_parallel_reads/4, parallel_quorum_reads/3, quorum_read/2, do_quorum_read/3, write_read_receive/3,
translog_new/0]).

-import(lists).
-import(dict).
-import(cs_symm_replication).
-import(timer).
-import(config).
-import(node).
-import(cs_lookup).
-import(cs_send).
-import(cs_state).
-import(erlang).
-import(process_dictionary).
-import(io).



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
    TM = spawn(transstore.tmanager, start_manager, [TransFun, SuccessMessage, FailureFun, Owner,  TID, InstanceId]),
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
    TM = spawn(transstore.tmanager, start_manager_commit, [Items, SuccessFun, FailureFun, Owner, TID, InstanceId]),
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
    ElementsInTransLog = lists:filter(fun({_, ElementKey, _, _, _})->
					      if
						  ElementKey == Key ->
						      true;
						  true ->
						      false
					      end end, TransLog),
    NumInLog = length(ElementsInTransLog),
    %% check whether we have already logged the item
    if
 	%% there should not be more than one entry for each item
 	NumInLog > 1 ->
 	    %%something went wrong
 	    {{fail, fail}, TransLog};
 	%% retrieve the information from the log if already there
 	%% for a write operation we have to update the value and the version number
 	NumInLog == 1 ->
 	    [Element] =  ElementsInTransLog,
 	    {LogOperation,LogKey,LogSuccess,_,LogVersion} = Element,
 	    if
 		LogSuccess == ok ->
 		    if 
 			LogOperation == write ->
 			    NewVersion = LogVersion;
 			true ->
 			    NewVersion = LogVersion + 1
 		    end,
 		    NewElement = {write, LogKey, LogSuccess, Value, NewVersion},
 		    NewTransLog1 = lists:delete(Element, TransLog),
 		    NewTransLog2 = lists:append([NewElement], NewTransLog1),
		    {ok, NewTransLog2};
 		true ->
		    {{fail, fail}, TransLog}
	    end;
 	%% we do not have any information for the key in the log
 	%% read the information from remote
	true ->
	    ReplicaKeys = cs_symm_replication:get_keys_for_replicas(Key),
	    lists:map(fun(ReplicaKey) ->
			      cs_lookup:unreliable_get_key(ReplicaKey)
		      end, ReplicaKeys),
	    erlang:send_after(config:transactionLookupTimeout(), self(), {write_read_receive_timeout, lists:nth(1, ReplicaKeys)}),
	    {Flag, Result} = write_read_receive([], ReplicaKeys, write),
	    if
		Flag == fail ->
		    NewTransLog = [{write, Key, fail, 0, 0}| TransLog],
		    {{fail, Result}, NewTransLog};		
		true -> %% Flag == value from {value, {Value, Version}}
		    {_Val, Version} = Result,
		    NewTransLog = [{write, Key, ok, Value, (Version + 1)} | TransLog],
		    {ok, NewTransLog}
	    end
    end.

%%--------------------------------------------------------------------
%% Function: read(Key, TransLog) -> {{fail, not_found}, TransLog} |
%%                                  {{fail, timeout}, TransLog} |
%%                                  {{fail, fail}, TransLog} |
%%                                  {{value, Value}, NewTransLog} 
%% Description: Executes a read operation 
%%              - retrieves value and version number remotely
%%              - adds the operation to TransLog
%%--------------------------------------------------------------------
	   
read(Key, TransLog)-> 
    ElementsInTransLog = lists:filter(fun({_, ElementKey, _, _, _})->
					      if
						  ElementKey == Key ->
						      true;
						  true ->
						      false
					      end end, TransLog),
    NumInLog = length(ElementsInTransLog),
    %% check whether we have already logged the item
    if
	%% there should not be more than one entry for each item
	NumInLog > 1 ->
	    %%something went wrong
	    {{fail, fail}, TransLog};
	%% retrieve the information from the log if already there
	%% for a read operation we do not have to add further info in the log
	%% as it has the same validation conditions than previous operations
	NumInLog == 1 ->
	    [{_,_,Success,Value,_}] =  ElementsInTransLog,
	    if
		Success == ok ->
		    {{value, Value}, TransLog};
		true ->
		    {{fail, fail}, TransLog}
	    end;
	%% we do not have any information for the key in the log
	%% read the information from remote
	true ->
	    ReplicaKeys = cs_symm_replication:get_keys_for_replicas(Key),
	    lists:map(fun(ReplicaKey) ->
			      cs_lookup:unreliable_get_key(ReplicaKey)
		      end, ReplicaKeys),
	    erlang:send_after(config:transactionLookupTimeout(), self(), {write_read_receive_timeout, lists:nth(1, ReplicaKeys)}),
	    {Flag, Result} = write_read_receive([], ReplicaKeys, read),
	    if
		Flag == fail ->
		    NewTransLog = [{read, Key, fail, 0, 0}| TransLog],
		    {{fail, Result}, NewTransLog};
		true -> %Flag == value
		    {Value, Version} = Result,
		    NewTransLog = [{read, Key, ok, Value, Version} | TransLog],
		    {{value, Value}, NewTransLog}
	    end
    end.


%%--------------------------------------------------------------------
%% Function: quorum_read(Key, SourcePID) -> ,fail |
%%                               {value, Value} 
%% Description: Executes a read operation 
%%              - retrieves value and version number remotely
%%--------------------------------------------------------------------
quorum_read(Key, SourcePID)->
    InstanceId = erlang:get(instance_id),
    spawn(transstore.transaction, do_quorum_read, [Key, SourcePID, InstanceId]).

do_quorum_read(Key, SourcePID, InstanceId)->
    erlang:put(instance_id, InstanceId),
    ReplicaKeys = cs_symm_replication:get_keys_for_replicas(Key),
    lists:map(fun(ReplicaKey) ->
			      cs_lookup:unreliable_get_key(ReplicaKey)
	      end, ReplicaKeys),
    erlang:send_after(config:transactionLookupTimeout(), self(), {write_read_receive_timeout, lists:nth(1, ReplicaKeys)}),
    {Flag, Result} = write_read_receive([], ReplicaKeys, read),
    if
	Flag == fail ->
	    cs_send:send(SourcePID, {single_read_return, {fail, Result}});
	true ->
	    {Value, Version} = Result,
	    cs_send:send(SourcePID, {single_read_return,{value, Value, Version}})
    end.

%%====================================================================
%% Helper functions : *retrieve the results from read/write
%%                    *filter the results  
%%====================================================================

write_read_receive(Results, ReplicaKeys, Operation)->
    receive
	{get_key_response, Key, Msg} ->
	    IsCurrentlySearchedKey = lists:member(Key, ReplicaKeys),
	    if 
		IsCurrentlySearchedKey == true ->
		    if 
			Msg == failed ->
			    NewResults = [fail | Results],
			    CRRes = check_results(NewResults, Operation),
			    if 
				CRRes == fail ->
				    {fail, not_found};
				CRRes == continue ->
				    write_read_receive(NewResults, ReplicaKeys, Operation);
				true ->
				    {value, CRRes}
			    end;
			true ->
			    {ok, Value, Versionnr} = Msg,
			    NewResults = [{Value, Versionnr} | Results],
			    CRRes = check_results(NewResults, Operation),
			    if 
				CRRes == fail ->
				    {fail, not_found};
				CRRes == continue ->
				    write_read_receive(NewResults, ReplicaKeys, Operation);
				true ->
				    {value, CRRes}
			    end
		    end;
		true ->
		    write_read_receive(Results, ReplicaKeys, Operation)
	    end;
	{write_read_receive_timeout, Key} ->
	    IsCurrentlySearchedKey = lists:member(Key, ReplicaKeys),
	    if
		IsCurrentlySearchedKey == true ->
		    {fail, timeout};
		true ->
		    write_read_receive(Results, ReplicaKeys, Operation)
	    end
    end.

check_results(Results, Operation)->
    TMPResults = lists:filter(fun(Elem) -> 
				      if
					  Elem == fail ->
					      false;
					  true ->
					      true
				      end
			      end, Results),
    % lookup also failed if a new key is inserted
    % when a majority is failed, we assume it is a new key 
    TMPResultsFailed = lists:filter(fun(Elem) -> 
				      if
					  Elem == fail ->
					      true;
					  true ->
					      false
				      end
			      end, Results),
    ReplFactor = config:replicationFactor(),
    Quorum = config:quorumFactor(),
    NumSuccessfulResponses = length(TMPResults),
    NumFailedResponses = length(TMPResultsFailed),
    
    if
	NumSuccessfulResponses >= Quorum ->
	    get_max_element(TMPResults, {0,-1});
	(NumFailedResponses >= (ReplFactor - Quorum)) and (Operation == write) ->
	    {0, -1};
	true ->
	    NumResponses = length(Results),
	    if 
		NumResponses == ReplFactor ->
		    fail;
		true ->
		    continue
	    end
    end.



get_max_element([], {ValMaxElement, VersMaxElement})->
    {ValMaxElement, VersMaxElement};
get_max_element([{Value, Versionnr}|Rest], {ValMaxElement, VersMaxElement})->
    if
	Versionnr > VersMaxElement ->
	    get_max_element(Rest, {Value, Versionnr});
	true ->
	    get_max_element(Rest, {ValMaxElement, VersMaxElement})
    end.





%%--------------------------------------------------------------------
%% Function: parallel_quorum_reads(Keys, TransLog, SourcePID) -> {fail, TransLog},
%%                                   {success, NewTransLog} 
%% Args: [Keys] - List with keys
%%       TransLog
%% Description: Needs a TransLog to collect all operations  
%%                  that are part of the transaction
%%              Use this function if you want to do parallel reads
%%                  without a transaction
%%                  e.g. in the read phase of wikipedia operations
%%--------------------------------------------------------------------
parallel_quorum_reads(Keys, TransLog, SourcePID)->
    InstanceId = erlang:get(instance_id),
    spawn(transstore.transaction, do_parallel_reads, [Keys, SourcePID, TransLog, InstanceId]).

do_parallel_reads(Keys, SourcePID, TransLog, InstanceId)->
    erlang:put(instance_id, InstanceId),
    {Flag, NewTransLog} = parallel_reads(Keys, TransLog),
    if
	Flag == fail->
	    cs_send:send(SourcePID, {parallel_reads_return, fail});
	true ->
	    cs_send:send(SourcePID, {parallel_reads_return, NewTransLog})
    end.


%%--------------------------------------------------------------------
%% Function: parallel_reads(Keys, TransLog) -> {fail, TransLog} |
%%                                  {success, NewTransLog} 
%% Description: Executes a read operation 
%%              - retrieves value and version number remotely
%%              - adds the operation to TransLog
%%--------------------------------------------------------------------
	   
parallel_reads(Keys, TransLog)-> 
    TLogCheck = check_trans_log(Keys, TransLog, [], []),
    
    if
	TLogCheck == fail->
	    {fail, TransLog};
	true ->
	    {ok, {_Result, ToLookup}} = TLogCheck,
	    %% we do not have nay information for the key in the log
	    %% read the information from remote
	    ReplicaKeysAccum = [],
	    
	    %% get a list with all replica keys
	    %% [[Key, RKey1, RKey2, ..., RKey3], [Key2, ....], ...]
	    ReplicaKeysAll = lists:foldl(fun(Elem, Acc)->
					      RKeys = cs_symm_replication:get_keys_for_replicas(Elem),
					      KeyAndRKeys = [Elem | RKeys],
					      [KeyAndRKeys | Acc]
				      end,
				      ReplicaKeysAccum,
				      ToLookup),
	    
	    lists:map(fun(ReplicaKeyList)->
			      [_Key | ReplicaKeys] = ReplicaKeyList,
			      lists:map(fun(ReplicaKey)->
						cs_lookup:unreliable_get_key(ReplicaKey)
					end, 
					ReplicaKeys)
			      end,
		      ReplicaKeysAll),
	    
	    ResultsInitAcc = [],
	    ResultsInit = lists:foldl(fun(Elem, Acc) ->
					      [Key|_RKeys] = Elem,
					      [{Key, [], undecided}|Acc]
				      end,
				      ResultsInitAcc,
				      ReplicaKeysAll),

	    erlang:send_after(config:transactionLookupTimeout(), 
			     self(), 
			     {write_read_receive_timeout, lists:nth(1, lists:nth(1, ReplicaKeysAll))}),
	    
		
	    {Flag, WRResult} = write_read_receive_parallel(ResultsInit, ReplicaKeysAll),
	    
 	    if
 		Flag == fail ->
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
    ElementsInTransLog = lists:filter(fun({_, ElementKey, _, _, _})->
					      if
						  ElementKey == Key ->
						      true;
						  true ->
						      false
					      end end, TransLog),
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
	NumInLog == 1 ->
	    [{_,_,Success,Value,_}] =  ElementsInTransLog,
	    if
		Success == ok ->
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
	{get_key_response, Key, Msg} ->
	    IsCurrentlySearchedKey = is_searched_key(ReplicaKeys, Key),
	    if 
		IsCurrentlySearchedKey == true ->
		    if 
			Msg == failed ->
			    NewResults = add_result(Results, Key, fail, Results),
			    CRRes = check_results_parallel(NewResults, NewResults),
			    if 
				CRRes == continue ->
				    write_read_receive_parallel(NewResults, ReplicaKeys);
				%% should not occur? only for write operations
				CRRes == fail ->
				    {fail, not_found};
				true -> % {found, TLog}
				    CRRes
			    end;
			true ->
			    {ok, Value, Versionnr} = Msg,
			    NewResults = add_result(Results, Key, {Value, Versionnr}, Results),
			    CRRes = check_results_parallel(NewResults, NewResults),
			    if 
				CRRes == fail ->
				    {fail, not_found};
				CRRes == continue ->
				    write_read_receive_parallel(NewResults, ReplicaKeys);
				true -> % {found, TLog}
				    CRRes
			    end
		    end;
		true ->
		    write_read_receive_parallel(Results, ReplicaKeys)
	    end;
	{write_read_receive_timeout, Key} ->
	    IsCurrentlySearchedKey = lists:member(Key, lists:flatten(ReplicaKeys)),
	    if
		IsCurrentlySearchedKey == true ->
		    {fail, timeout};
		true ->
		    write_read_receive_parallel(Results, ReplicaKeys)
	    end
    end.

is_searched_key([], _Key)->
    false;
is_searched_key([Head|Tail], Key) ->
    InList = lists:member(Key, Head),
    if
	InList == false->
	    is_searched_key(Tail, Key);
	true ->
	    true
    end.
    
add_result([Head | Results], Key, Result, AllResults)->
    {CurrKey, ResultsForKey, EndResult} = Head,
    OrigKey = cs_symm_replication:get_original_key(Key),
    if
	CurrKey == OrigKey ->
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
	EndRes /= undecided ->
	    check_results_parallel(Results, AllResults);
	true ->
	    TMPResults = lists:filter(fun(Elem) -> 
					      if
						  Elem == fail ->
						      false;
					  true ->
						      true
					      end
				      end, ResKey),
	    TMPResultsFailed = lists:filter(fun(Elem) -> 
						    if
							Elem == fail ->
							    true;
							true ->
							    false
						    end
					    end, Results),
	    
	    ReplFactor = config:replicationFactor(),
	    QuorumFactor = config:quorumFactor(),
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
			NumResponses == ReplFactor ->
			    NewAllResults = lists:delete(Head, AllResults),
			    NewAllResults2 = [{Key, ResKey, {0, -1}} | NewAllResults],
			    check_results_parallel(Results, NewAllResults2);
			true ->
			    continue
		    end
	    end
    end.

build_translog(Results)->
    EndResultAccum = [],
    lists:foldl(fun(Elem, Acc)->
			{CurrKey, _ResultsForKey, {Value, Version}} = Elem,
			if
			    Version == -1 ->
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
    Node = cs_state:me(State),
    {node:id(Node), now()}.



%%--------------------------------------------------------------------
%% Function: getRTMKeys(TID)-> [Keys] 
%% Description: Get the keys for the nodes that will act as TMs
%%                  based on symmetric replication on the TID
%%--------------------------------------------------------------------


getRTMKeys(TID)->
    {Key, _} = TID,
    RKeys = cs_symm_replication:get_keys_for_replicas(Key),
    KeyInList = lists:member(Key, RKeys),
    if
	KeyInList == true ->
	    lists:delete(Key, RKeys);
	true ->
	    %% this should only happen in the beginning
	    %% when there are arbitrary keys for the nodes
	    [_ | NewRKeys] = RKeys,
	    NewRKeys
    end.
    
%%--------------------------------------------------------------------
%% Function: initRTM(State, Message)-> State 
%% Description: initializes rTMs using information included in Message
%%--------------------------------------------------------------------

initRTM(State, Message)->
    {Leader, Items} = Message#tm_message.message,
    RKey = Message#tm_message.tm_key,
    TransID = Message#tm_message.transaction_id,
    spawn(transstore.tmanager, start_replicated_manager, [TransID, Items, Leader, RKey, erlang:get(instance_id), self()]),
    RTMPID = receive
		 {the_pid, X} -> X
	     end,
    cs_send:send(Leader, {rtm, RTMPID, RKey}),
    
    %% update transaction log: store mapping between transaction ID and local TM
    TransLog = cs_state:get_trans_log(State),
    New_TID_TM_Mapping = dict:store(TransID, RTMPID, TransLog#translog.tid_tm_mapping),
    NewTransLog = TransLog#translog{tid_tm_mapping = New_TID_TM_Mapping},
    cs_state:set_trans_log(State, NewTransLog).

%%====================================================================
%% Functions to manipulate TransLog's
%%====================================================================

%%-------------------------------------------------------------------
%% Function: new_translog/0
%% Purpose:  create an empty list
%% Returns:  empty list
%%-------------------------------------------------------------------
translog_new()->
    trecords:new_translog().
