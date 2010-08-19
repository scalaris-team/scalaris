%  Copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : transaction_api.erl
%%% Author  : Monika Moser <moser@zib.de>
%%% Description : API for transactions
%%%
%%% Created : 17 Sep 2007 by Monika Moser <moser@zib.de>
%%%-------------------------------------------------------------------
-module(transaction_api).

-author('moser@zib.de').
-vsn('$Id$').

-include("trecords.hrl").

-export([write/3, read/2, write2/3, read2/2, parallel_reads/2, abort/0, 
	 quorum_read/1, parallel_quorum_reads/2, single_write/2, 
	 do_transaction/3, do_transaction_wo_rp/3, commit/1, jWrite/3, 
     jRead/2, jParallel_reads/2, delete/2]).

%%===================================================================
%% HOWTO do a transaction:
%% To execute a transaction including a readphase use do_transaction
%%   TFun: provide a function that gets a transaction log as parameter
%%    it has to return:
%%        1) in case of success {{ok, Value}, NewTransLog}
%%        2) in case you want to abort the transaction after the 
%%              readphase {{abort, Value}, NewTranslog}
%%        3) in a failure case {{fail, Reason}, NewTransLog}
%%   
%%   After the readphase 
%%   For 2) the SuccessFun({user_abort, Value}) will be called 
%%   For 3) the FailureFun(not_found | timeout | fail) will be called 
%%
%%   For 1) the commit phase will be started
%%      it calls 
%%          1.1) SuccessFun({commit, Value}) if successful
%%          1.2) FailureFun(abort) if failed
%%
%% A transaction without a readphase behaves according to 1.1) and 1.2)
%%   where Value = empty
%%===================================================================


%%====================================================================
%% transaction API - using transactions from outside
%%====================================================================


%% Use this function to do a quorum read without a commit phase
%% returns:
%% {Value, Version} | {fail, not_found} | {fail, timeout} | 
%% {fail, fail} 
%% * for {fail, fail} the reason is currently unknown, should 
%%   not occur 
quorum_read(Key)->
%%    pid_groups:join(pid_groups:group_with(dht_node)),
    RTO = config:read(quorum_read_timeout),
    transaction:quorum_read(Key, comm:this()),
    receive
        {single_read_return, {value, empty_val, _Version}}->
            ?TLOG2("single read return fail", [Page]),
            {fail, not_found};
        {single_read_return, {value, Page, Version}}->
            ?TLOG2("read_page returned", [Page]),
            {Page, Version};
        {single_read_return, {fail, Reason}}->
            ?TLOG("single read return fail"),
            {fail, Reason};
        _X ->
            ?TLOG2("read_page got the message ~p~n", [_X]),
            {fail, fail}
    after RTO ->
            ?TLOG("single read return fail - timeout"),
            {fail, timeout}
    end.

%% Use this function to do parallel quorum reads on a list of keys with a commit phase
-spec parallel_quorum_reads(Keys::[iodata() | integer()], Par::any()) -> {fail} | {fail, timeout} | any().
parallel_quorum_reads(Keys, _Par)->
%    ?TLOGN("starting quorum reads on ~p", [Keys]),
    RTO = config:read(parallel_quorum_read_timeout),
    case pid_groups:find_a(dht_node) of
        failed -> fail;
        LocalDHTNode ->
            LocalDHTNode ! {parallel_reads, comm:this(), Keys, []},
            receive
                {parallel_reads_return, fail}        -> {fail};
                {parallel_reads_return, NewTransLog} -> NewTransLog
            after RTO -> {fail, timeout}
            end
    end.

%% returns:
%% commit | userabort | {fail, not_found} | {fail, timeout} | {fail, fail} | {fail, abort}
%% * for {fail, fail} the reason is currently unknown, should 
%%   not occur 
single_write(Key, Value)->
    %% ?TLOGN("starting a single write function on ~p, value ~p", [Key, Value]),
    TFun = fun(TLog)->
                   {Result, TLog2} = write(Key, Value, TLog),
                   case Result of
                       ok -> {{ok, Value}, TLog2};
                       _ -> {Result, TLog2}
                   end
           end,
    SuccessFun = fun({Result, _ReadPhaseResult}) -> Result end,
    FailureFun = fun(Reason) -> {fail, Reason} end,
    do_transaction(TFun, SuccessFun, FailureFun).

%% use this function to do a full transaction including a readphase
do_transaction(TFun, SuccessFun, FailureFun)->
    LocalDHTNodes = pid_groups:find_all(dht_node),
    LocalDHTNode = lists:nth(random:uniform(length(LocalDHTNodes)), LocalDHTNodes),
    LocalDHTNode ! {do_transaction, TFun, SuccessFun, FailureFun, comm:this()},
    receive
        {trans, Message}->
            Message
    end.

%% Use this function to do a transaction without a read phase
%% Thus it is necessary to provide a proper list with items for TMItems
do_transaction_wo_rp([], _SuccessFun, _FailureFun)->
    {ok};
do_transaction_wo_rp(TMItems, SuccessFun, FailureFun)->
    LocalDHTNode = pid_groups:find_a(dht_node),
    LocalDHTNode ! {do_transaction_wo_rp, TMItems, nil, SuccessFun, FailureFun, comm:this()},
    receive
        {trans, Message}->
            %% ?TLOGN(" received ~p~n", [Message]),
            Message
    end.

%%--------------------------------------------------------------------
%% Function: commit(TransLog) -> {ok} | {fail, Reason}
%% Description: Commits all operations contained in the TransLog.
%%--------------------------------------------------------------------
commit(TransLog)->
    SuccessFun = fun(_) ->
        {ok} end,
    FailureFun = fun(X) ->
        {fail, X} end,
    do_transaction_wo_rp(trecords:create_items(TransLog), SuccessFun, FailureFun).

%%====================================================================
%% transaction API - Functions that can be used within a Transaction Fun
%%====================================================================

%%--------------------------------------------------------------------
%% Function: write(Key, Value, TransLog) -> {{fail, not_found}, TransLog} |
%%                                          {{fail, timeout}, TransLog} |
%%                                          {{fail, fail}, TransLog} |
%%                                          {ok, NewTransLog} 
%% Description: Needs a TransLog to collect all operations  
%%                  that are part of the transaction
%%--------------------------------------------------------------------
write(Key, Value, TransLog) ->
    transaction:write(Key, Value, TransLog).

%%--------------------------------------------------------------------
%% Function: read(Key, TransLog) -> {{fail, not_found}, TransLog} |
%%                                  {{fail, timeout}, TransLog} |
%%                                  {{fail, fail}, TransLog} |
%%                                  {{value, Value}, NewTransLog} 
%% Description: Needs a TransLog to collect all operations  
%%                  that are part of the transaction
%%--------------------------------------------------------------------
read(Key, TransLog)->
    TLog = transaction:read(Key, TransLog),
    case TLog of
        {{value, empty_val}, NTlog} -> {{fail, not_found}, NTlog};
        _ -> TLog
    end.


%%--------------------------------------------------------------------
%% Function: parallel_reads(Keys, TransLog) -> {fail, NewTransLog},
%%                                   {[{value, Value}], NewTransLog} 
%% Args: [Keys] - List with keys
%%       TransLog
%% Description: Needs a TransLog to collect all operations  
%%                  that are part of the transaction
%%--------------------------------------------------------------------
parallel_reads(Keys, TransLog)->
    transaction:parallel_reads(Keys, TransLog).


% @spec read2(translog(), term()) -> {term(), translog()}
% @throws {abort, {fail, translog()}}
read2(TransLog, Key) ->
    case read(Key, TransLog) of
	{{fail, _Details}, _TransLog1} = Result ->
	    throw({abort, Result});
	{{value, Value}, TransLog1} ->
	    {Value, TransLog1}
    end.

% @spec write2(translog(), term(), term()) -> translog()
% @throws {abort, {fail, translog()}}
write2(TransLog, Key, Value) ->
    case write(Key, Value, TransLog) of
	{{fail, _Reason}, _TransLog1} = Result ->
	    throw ({abort, Result});
	{ok, TransLog1} ->
	    TransLog1
    end.

%%--------------------------------------------------------------------
%% Function: abort() -> {abort} 
%% Description: Signals a user decision to abort a transaction
%%              PREMATURE
%%--------------------------------------------------------------------
abort()->
    abort.

%% @doc tries to delete the given key and returns the number of 
%%      replicas successfully deleted.
%%      WARNING: this function can lead to inconsistencies
-spec(delete/2 :: (any(), pos_integer()) -> {ok, pos_integer(), list()} | 
						{fail, timeout} | 
						{fail, timeout, pos_integer(), list()} | 
						{fail, node_not_found}).
delete(Key, Timeout) ->
    case pid_groups:find_a(dht_node) of
        failed ->
	    {fail, node_not_found};
	LocalDHTNode ->
	    LocalDHTNode ! {delete, comm:this(), Key},
	    receive
		{delete_result, Result} ->
		    Result
	    after
		Timeout ->
		    {fail, timeout}
	    end
    end.

%%====================================================================
%% transaction API - Wrapper functions to be used from the java transaction api
%%====================================================================

%%--------------------------------------------------------------------
%% Function: jWrite(Key, Value, TransLog) -> {{fail, not_found}, TransLog} |
%%                                          {{fail, timeout}, TransLog} |
%%                                          {{fail, fail}, TransLog} |
%%                                          {ok, NewTransLog} 
%% Description: Needs a TransLog to collect all operations  
%%                  that are part of the transaction
%%--------------------------------------------------------------------
jWrite(Key, Value, TransLog)->
    write(Key, Value, TransLog).

%%--------------------------------------------------------------------
%% Function: jRead(Key, TransLog) -> {{fail, not_found}, TransLog} |
%%                                  {{fail, timeout}, TransLog} |
%%                                  {{fail, fail}, TransLog} |
%%                                  {{value, Value}, NewTransLog} 
%% Description: Needs a TransLog to collect all operations  
%%                  that are part of the transaction
%%--------------------------------------------------------------------
jRead(Key, TransLog)->
    read(Key, TransLog).

%%--------------------------------------------------------------------
%% Function: jParallel_reads(Keys, TransLog) -> {fail, NewTransLog},
%%                                   {[{value, Value}], NewTransLog} 
%% Args: [Keys] - List with keys
%%       TransLog
%% Description: Needs a TransLog to collect all operations  
%%                  that are part of the transaction
%%--------------------------------------------------------------------
jParallel_reads(Keys, TransLog)->
%%     io:format("~p~n", [TransLog]),
    A = parallel_reads(Keys, TransLog),
%%     io:format("~p~n", [A]),
    A.
