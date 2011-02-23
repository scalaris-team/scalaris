%  Copyright 2007-2011 Zuse Institute Berlin
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

-include("scalaris.hrl").
-include("client_types.hrl").
-include("trecords.hrl").

-export([write/3, read/2, write2/3, read2/2, parallel_reads/2, abort/0, 
	 quorum_read/1, parallel_quorum_reads/2, single_write/2, 
	 do_transaction/3, do_transaction_wo_rp/3, commit/1, jWrite/3, 
     jRead/2, jParallel_reads/2, delete/2]).

-type fail_reason() :: not_found | timeout | fail | abort.

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


%% @doc Performs a quorum read without a commit phase. Returns:
%% {Value, Version} | {fail, not_found} | {fail, timeout} | {fail, fail} 
%% -> for {fail, fail} the reason is currently unknown, should not occur 
-spec quorum_read(Keys::[client_key()])
        -> {client_value(), client_version()} |
           {fail, not_found} | {fail, timeout} | {fail, fail}.
quorum_read(Key) ->
    RTO = config:read(quorum_read_timeout),
    case pid_groups:find_a(dht_node) of
        failed -> {fail, fail};
        LocalDHTNode ->
            LocalDHTNode ! {read, comm:this(), Key},
            receive
                {single_read_return, {value, empty_val, _Version}} ->
                    ?TLOG2("single read return fail", [Page]),
                    {fail, not_found};
                {single_read_return, {value, Page, Version}} ->
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
            end
    end.

%% @doc Performs parallel quorum reads on a list of keys with a commit phase.
-spec parallel_quorum_reads(Keys::[client_key()], Par::any()) -> {fail} | {fail, timeout} | any().
parallel_quorum_reads(Keys, _Par) ->
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
%% -> for {fail, fail} the reason is currently unknown, should not occur
-spec single_write(client_key(), client_value())
        -> commit | userabort | {fail, fail_reason()}.
single_write(Key, Value) ->
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

%% @doc Performs a full transaction including a readphase.
-spec do_transaction(TFun::fun((TLog) -> {Result::any(), TLog}),
                           SuccessFun::fun(({Result::any(), ReadPhaseResult::any()}) -> OverallResultSucc),
                           FailureFun::fun((Reason::fail_reason()) -> OverallResultFail))
        -> OverallResultSucc | OverallResultFail.
do_transaction(TFun, SuccessFun, FailureFun)->
    LocalDHTNodes = pid_groups:find_all(dht_node),
    LocalDHTNode = lists:nth(random:uniform(length(LocalDHTNodes)), LocalDHTNodes),
    LocalDHTNode ! {do_transaction, TFun, SuccessFun, FailureFun, comm:this()},
    receive
        {trans, Message} -> Message
    end.

%% @doc Performs a transaction without a read phase.
%% Thus it is necessary to provide a proper list with items for TMItems
-spec do_transaction_wo_rp(TMItems::[any()],
                           SuccessFun::fun(({Result::any(), ReadPhaseResult::any()}) -> OverallResultSucc),
                           FailureFun::fun((Reason::fail_reason()) -> OverallResultFail))
        -> OverallResultSucc | OverallResultFail.
do_transaction_wo_rp([], _SuccessFun, _FailureFun)->
    {ok};
do_transaction_wo_rp(TMItems, SuccessFun, FailureFun)->
    LocalDHTNode = pid_groups:find_a(dht_node),
    LocalDHTNode ! {do_transaction_wo_rp, TMItems, nil, SuccessFun, FailureFun, comm:this()},
    receive
        {trans, Message} -> Message
    end.

%% @doc Commits all operations contained in the TransLog.
-spec commit(TransLog::any()) -> {ok} | {fail, fail_reason()}.
commit(TransLog) ->
    SuccessFun = fun(_) -> {ok} end,
    FailureFun = fun(X) -> {fail, X} end,
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
-spec write(client_key(), client_value(), TransLog)
        -> {{fail, not_found | timeout | fail} | ok, TransLog}.
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
-spec read(client_key(), TransLog)
        -> {{fail, not_found | timeout | fail} | {value, client_value()}, TransLog}.
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
-spec parallel_reads([client_key()], TransLog)
        -> {fail | [{value, client_value()}], TransLog}.
parallel_reads(Keys, TransLog)->
    transaction:parallel_reads(Keys, TransLog).


% @spec read2(translog(), term()) -> {term(), translog()}
% @throws {abort, {fail, translog()}}
-spec read2(TransLog, client_key())
        -> {client_value(), TransLog}.
read2(TransLog, Key) ->
    case read(Key, TransLog) of
        {{fail, _Details}, _TransLog1} = Result ->
            throw({abort, Result});
        {{value, Value}, TransLog1} ->
            {Value, TransLog1}
    end.

% @spec write2(translog(), term(), term()) -> translog()
% @throws {abort, {fail, translog()}}
-spec write2(TransLog, client_key(), client_value()) -> TransLog.
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
-spec abort() -> abort.
abort() -> abort.

%% @doc tries to delete the given key and returns the number of 
%%      replicas successfully deleted.
%%      WARNING: this function can lead to inconsistencies
-spec delete(client_key(), Timeout::pos_integer())
        -> {ok, ResultsOk::pos_integer(), ResultList::[ok | undef]} |
           {fail, timeout} |
           {fail, timeout, ResultsOk::pos_integer(), ResultList::[ok | undef]} |
           {fail, node_not_found}.
delete(Key, Timeout) ->
    case pid_groups:find_a(dht_node) of
        failed ->
            {fail, node_not_found};
        LocalDHTNode ->
            LocalDHTNode ! {delete, comm:this(), Key},
            receive {delete_result, Result} -> Result
            after Timeout -> {fail, timeout}
	    end
    end.

%%====================================================================
%% transaction API - Wrapper functions to be used from the java transaction api
%%====================================================================

%% @doc Java wrapper to write/3. Needs a TransLog to collect all operations
%%      that are part of the transaction.
-spec jWrite(client_key(), client_value(), TransLog)
        -> {{fail, not_found | timeout | fail} | ok, TransLog}.
jWrite(Key, Value, TransLog) ->
    write(Key, Value, TransLog).

%% @doc Java wrapper to read/2. Needs a TransLog to collect all operations  
%%      that are part of the transaction.
-spec jRead(client_key(), TransLog)
        -> {{fail, not_found | timeout | fail} | {value, client_value()}, TransLog}.
jRead(Key, TransLog)->
    read(Key, TransLog).

%% @doc Java wrapper to parallel_reads/2. Needs a TransLog to collect all  
%%      operations that are part of the transaction.
-spec jParallel_reads([client_key()], TransLog)
        -> {fail | [{value, client_value()}], TransLog}.
jParallel_reads(Keys, TransLog)->
    parallel_reads(Keys, TransLog).
