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
%%% File    : transaction_test.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : test cases for transactions
%%%
%%% Created :  26 June 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: transaction_test.erl 463 2008-05-05 11:14:22Z schuett $
-module(transstore.transaction_test).

-author('schuett@zib.de').
-vsn('$Id: transaction_test.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([run_test_increment/2, quorum_read/1, test/1, test1/0, test3/0, test4/0,
 test5/0, test6/0, run_test_write/3, run_test_write_5/2,
 run_test_write_20/2, run_test_read_5/2, run_test_read_20/2, transtest_write_read/1, do_transtest_write_read/2]).

-export([test_reads_writes/2, do_test_reads_writes_init/2, writer/3, reader/2, initiator_writer/4, initiator_reader/3]).

-import(cs_send).
-import(util).
-import(boot_server).
-import(boot_logger).
-import(io).
-import(io_lib).
-import(lists).
-import(ioutils).
-import(timer).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Used by the web server to send test messages to one node 
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test(NumElems) ->
    send_to_node({transtest, cs_send:this(), NumElems}),
    receive
	{trans, X} -> X
    after 
	1000 -> fail
    end.


test1() ->
    send_to_node({test1, cs_send:this()}),
    receive
	{trans, X} -> X
    end.

test3() ->
    %send_to_node({test3, self()}),
    send_to_node({transtest, cs_send:this(), 20}),
    receive
	{trans, X} ->
	    X
    end.

test4() ->
    send_to_node({test4, cs_send:this()}),
    receive
	{trans, X} ->
	    X
    end.
test5() ->
    send_to_node({test5, cs_send:this()}),
    receive
	{trans, X} ->
	    X
    end.
test6() ->
    send_to_node({test6, cs_send:this()}),
    receive
	{trans, X} ->
	    X
    end.

quorum_read(Key)->
    io:format("Quorum read on key: ~p ~n", [Key]),
    send_to_node({single_read, cs_send:this(), Key}),
    receive
	{single_read_return, fail}->
	    fail;
	{single_read_return, X} ->
	    X
    end.

    
send_to_node(Message) ->
    Nodes = boot_server:node_list(),
    cs_send:send(util:randomelem(Nodes), Message).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Transaction Tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


run_test_increment(State, Source_PID)->
    boot_logger:log_to_file(io_lib:format("runing increment transaction test", [])),
    
    TFun = fun(TransLog)->
		   Key = "Increment",
		   {Result, TransLog1} = transaction_api:read(Key, TransLog),
		   {Result2, TransLog2} = 
		       if
			   Result == fail ->
			       Value = 0,
			       transaction_api:write(Key, Value, TransLog);
			   true ->
			       {value, Val} = Result,
			       Value = Val + 1,
			       transaction_api:write(Key, Value, TransLog1)
		   end,
		   if
		       Result2 == ok ->
			   {{ok, Value}, TransLog2};
		       true ->
			   {{fail, abort}, TransLog2}
		   end
	   end,
    SuccessFun = fun(X) ->
			 {success, X}
		 end,
    FailureFun = fun(Reason)->
			 {failure, "test increment failed", Reason}
		 end,
    
    transaction:do_transaction(State, TFun, SuccessFun, FailureFun, Source_PID),
    ok.

run_test_write(State, Source_PID, NumElems)->    
    TestName = io_lib:format("test_write_~p", [NumElems]),
    boot_logger:log_to_file(io_lib:format("running transaction ~s", [TestName])),
    TFun = fun (TransLog) ->
		   WriteList = random_list(NumElems),
		   NewTLog = lists:foldl(fun(Element, TLogAcc)->
						{_Result, NewTLogAcc} = transaction_api:write(Element, TestName, TLogAcc),
						NewTLogAcc
					end,
					TransLog,
					WriteList),
		   {{ok, writesuccess}, NewTLog}
	   end,
    SuccessFun = fun(_)-> {trans, {success, io_lib:format("transaction ~s: success", [TestName])}} end,
    FailureFun = fun(Reason)->
			 {trans, {failure, io_lib:format("transaction ~s: fail ", [TestName]), Reason}}
		 end,

    transaction:do_transaction(State, 
				      TFun, 
				      SuccessFun,
				      FailureFun,
				      Source_PID),
    ok.
   

run_test_write_5(State, Source_PID)->
    boot_logger:log_to_file(io_lib:format("running test_write_5", [])),
    
    
    TFun = fun (TransLog) ->
		   WriteList = ["Aword", "EWord", "IWord", "MWord", "QWord"],
		   NewTLog = lists:foldl(fun(Element, TLogAcc)->
						{_Result, NewTLogAcc} = transaction_api:write(Element, "run_test_write_5", TLogAcc),
						NewTLogAcc
					end,
					TransLog,
					WriteList),
		   {{ok, write_success}, NewTLog}
	   end,
    SuccessFun = fun(_)-> {trans, {success, "transaction test_write_5 was successful"}} end,
    FailureFun = fun(Reason)->
			 {trans, {failure, "transaction test_write_5 failed", Reason}}
		 end,
    
    transaction:do_transaction(State, 
				      TFun, 
				      SuccessFun,
				      FailureFun,
				      Source_PID),
    ok.


run_test_write_20(State, Source_PID)->
    boot_logger:log_to_file(io_lib:format("running test_write_20", [])),
    TFun = fun (TransLog) ->
		   WriteList = ["Aword", "BWord", "CWord", "DWord", "EWord", "Fword", "GWord", "HWord", "IWord", "JWord", "Kword", "LWord", "MWord", "NWord", "OWord", "Pword", "QWord", "RWord", "SWord", "TWord"],
		   NewTLog = lists:foldl(fun(Element, TLogAcc)->
						 {_Result, NewTLogAcc} = transaction_api:write(Element, "run_test_write20", TLogAcc),
						 NewTLogAcc
					 end,
					 TransLog,
					 WriteList),
		   {{ok, writesuccess}, NewTLog}
	   end,
  
    SuccessFun = fun(_)-> {trans, {success, "transaction test_write_20 was successful"}} end,
    FailureFun = fun(Reason)->
			 {trans, {failure, "transaction test_write_20 failed", Reason}}
		 end,

    transaction:do_transaction(State, 
				  TFun, 
				  SuccessFun,
				  FailureFun,
				  Source_PID),
    ok.

run_test_read_5(State, Source_PID)->
    boot_logger:log_to_file(io_lib:format("running test_read_5", [])),
    
    TFun = fun (TransLog) ->
		   ReadList = ["Aword", "EWord", "IWord", "MWord", "QWord"],
		   NewTLog = lists:foldl(fun(Element, TLogAcc)->
						 {_Result, NewTLogAcc} = transaction_api:read(Element, TLogAcc),
						 NewTLogAcc
					 end,
					 TransLog,
					 ReadList),
		   {{ok, read}, NewTLog}
	   end,
    
    SuccessFun = fun(_)-> {trans, {success, "transaction test_read_5 was successful"}} end,
    FailureFun = fun(Reason)->
			 {trans, {failure, "transaction test_read_5 failed", Reason}}
		 end,

    transaction:do_transaction(State, 
				  TFun, 
				  SuccessFun,
				  FailureFun,
				  Source_PID),
    ok.

run_test_read_20(State, Source_PID)->
    boot_logger:log_to_file(io_lib:format("running test_read_20", [])),
   
       TFun = fun (TransLog) ->
		   ReadList = ["Aword", "BWord", "CWord", "DWord", "EWord", "Fword", "GWord", "HWord", "IWord", "JWord", "Kword", "LWord", "MWord", "NWord", "OWord", "Pword", "QWord", "RWord", "SWord", "TWord"],
		   NewTLog = lists:foldl(fun(Element, TLogAcc)->
						 {_Result, NewTLogAcc} = transaction_api:read(Element, TLogAcc),
						 NewTLogAcc
					 end,
					 TransLog,
					 ReadList),
		   {{ok, read},  NewTLog}
	   end,
  
    
    SuccessFun = fun(_)-> {trans, {success, "transaction test_read_20 was successful"}} end,
        FailureFun = fun(Reason)->
			 {trans, {failure, "transaction test_read_20 failed", Reason}}
		 end,

    transaction:do_transaction(State, 
				  TFun, 
				  SuccessFun,
				  FailureFun,
				  Source_PID),
    ok.


random_list(NumElements)->
    Dict = ioutils:for_each_line_in_file("../data/SQLiteWords.txt", fun(Line, Accum) -> [Line|Accum] end, [read], []),
    random_list_select(NumElements, Dict, []).

random_list_select(0, _DictIn, Outlist)->
    Outlist;
random_list_select(NumElements, DictIn, Outlist) ->
    RandElem = util:randomelem(DictIn),
    NewOutList = [RandElem | Outlist],
    NewDictIn = lists:subtract(DictIn, [RandElem]),
    random_list_select((NumElements - 1), NewDictIn, NewOutList).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Transaction Tests - Using Transaction API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%transstore.transaction_test:transtest_write_read(1).
transtest_write_read(Value)->
%    InstanceId = erlang:get(instance_id),
    InstanceId = cs_send:this(),
    spawn(transstore.transaction_test, do_transtest_write_read, [InstanceId, Value]).


do_transtest_write_read(InstanceId, Value)->
    %erlang:put(instance_id, InstanceId),
    io:format("My parent: ~p, me: ~p ~n", [InstanceId, self()]),
    io:format("running test_write_20~n", []),
    WriteList = ["Aword", "BWord", "CWord", "DWord", "EWord", "Fword", "GWord", "HWord", "IWord", "JWord", "Kword", "LWord", "MWord", "NWord", "OWord", "Pword", "QWord", "RWord", "SWord", "TWord"],
    TFun = fun (TransLog) ->	   
		   NewTLog = lists:foldl(fun(Element, TLogAcc)->
						 {_Result, NewTLogAcc} = transaction_api:write(Element, Value, TLogAcc),
						 NewTLogAcc
					 end,
					 TransLog,
					 WriteList),
		   {{ok, NewTLog}, NewTLog}
	   end,
  
    SuccessFun = fun(_Result)->  
			  {success, 
			   {io_lib:format("transaction test was successful~n", [])}
			  }
		 end,
    FailureFun = fun(Reason)->
			  {failure, 
			   {"transaction test failed", Reason}
			  }
		 end,

    {Result, _Message} = transaction_api:do_transaction(TFun, 
							SuccessFun,
							FailureFun),
    
    io:format("Transtest_write_read write result: ~p~n", [Result]),
    
    
    if 
	Result == success->
	    io:format("transtest_write_read: writing was successful~n", []),
	    ReadResult = transaction_api:parallel_quorum_reads(WriteList, nothing),
	    io:format("transtest_write_read: read  ~p ~n", [ReadResult]);
	true ->
	    io:format("transtest_write_read: writing failed~n", [])
    end.


%transstore.transaction_test:test_reads_writes(100, 1).
%transstore.transaction_test:test_reads_writes(1, 1).

%lists:map(fun(E)->I=process_info(E), io:format("Proc ~p ~n", [I]) end, processes()).            
test_reads_writes(Rate, Value)->
    spawn(transstore.transaction_test, do_test_reads_writes_init, [Rate, Value]).

do_test_reads_writes_init(Rate, Value)->
    Dict = ioutils:for_each_line_in_file("../data/SQLiteWords.txt", fun(Line, Accum) -> [Line|Accum] end, [read], []),
    spawn(transstore.transaction_test, initiator_writer, [Dict, Value, self(), Rate]),
    timer:send_after(400000, self(), {tresult}),
    Result = test_reads_writes_loop([], Dict, tresult),
    SuccList = lists:filter(fun({S,_It})-> if S == success-> true; true -> false end end, Result),
    FailList = lists:filter(fun({S,_It})-> 
				    if
					S == success->
					    false;
					true ->
					    true
				    end
			    end,
			    Result),
    SNr = length(SuccList),
    FNr = length(FailList),
    WA = length(Dict),
    io:format("Number of write attempts: ~p~n", [WA]),
    io:format("Number successful writes: ~p and failed writes: ~p ~n, FailedItems:~p~n", [SNr, FNr, FailList]),
    
    SuccWords = lists:map(fun({_, {Key, _}})-> Key end, SuccList),

    spawn(transstore.transaction_test, initiator_reader, [SuccWords, self(), Rate]),

    timer:send_after(400000, self(), {trresult}),
    ReadResult = test_reads_writes_loop([], Dict, trresult),
    ReadSuccList = lists:filter(fun({S,_It})-> if
					      S==success->
						  true;
					      true ->
						  false
					  end
			    end,
			    ReadResult),
    ReadFailList = lists:filter(fun({S,_RR})-> if
					      S==success->
						  false;
					      true ->
						  true
					  end
			    end,
			    ReadResult),
    RSNr = length(ReadSuccList),
    RFNr = length(ReadFailList),
    RWA = length(SuccWords),
    io:format("Number of read attempts: ~p~n", [RWA]),
    io:format("Number successful reads: ~p and failed reads: ~p ~n, FailedItems:~p~n", [RSNr, RFNr, ReadFailList]).

initiator_writer(WordList, Iteration, Owner, Rate)->
     lists:foreach(fun(Elem)->
			  timer:sleep(Rate),
			  spawn_writer(Elem, Iteration, Owner)
		  end,
		  WordList).
initiator_reader(SuccWords, Owner, Rate)->
        lists:foreach(fun(ElR)-> 
			  timer:sleep(Rate),
			  spawn_reader(ElR, Owner)
		  end,
		  SuccWords).

test_reads_writes_loop(Accum, [], _Atom) ->
    Accum;
test_reads_writes_loop(Accum, ToReceive, Atom)->
    receive
	{Atom, Result, Message, Word}->
	    {Accum2, ToReceive2} = accumulate_res(Result, Message, Word, ToReceive, Accum),
	    test_reads_writes_loop(Accum2, ToReceive2, Atom);
	{Atom} ->
	    Rest = length(ToReceive),
	    io:format("Got timeout, did not receive results from ~p transactions~n", [Rest]),
	    Accum
    end.

accumulate_res(Result, Message, Word, ToReceive, Accum)->
    TR2 = lists:delete(Word, ToReceive),
    {[{Result, Message}|Accum], TR2}.

spawn_writer(Word, Iteration, Owner)->
    spawn(transstore.transaction_test, writer, [Word, Iteration, Owner]).

writer(Word, Iteration, Owner)->
%%     TFun =
%%  	fun(TLog)->
%% 		{Res, TLog2} = transaction_api:write(Word, Iteration, TLog),
%% 		if
%% 		    Res == ok ->
%% 			{{ok, Iteration}, TLog2};
%% 		    true ->
%% 			{Res, TLog2}
%% 		end
%% 	end,
%%     SuccessFun = 
%% 	fun(_Res)->
%% 		{success, {Word, Iteration}}
%% 	end,
%%     FailureFun =
%% 	fun(Reason)->
%% 		{failure, {Word, Iteration, Reason}}
%% 	end,
%%     {Result, Message} = transaction_api:do_transaction(TFun, SuccessFun, FailureFun),
%%     Owner ! {trresult, Result, Message, Word}
    Result = transaction_api:single_write(Word, Iteration),
    if
	Result == commit ->
	    Owner ! {tresult, success, {Word, Iteration}, Word};
	Result == userabort ->
	    Owner ! {tresult, userabort, {Word, Iteration}, Word};
	true->
	    {_F, Reason} = Result,
	    Owner ! {tresult, failure, {Word, Iteration, Reason}, Word}
    end.
    
spawn_reader(Word, Owner)->
    spawn(transstore.transaction_test, reader, [Word,  Owner]).

reader(Word, Owner)->
    {RF, RR} = transaction_api:quorum_read(Word),
    if
	RF == fail->
	    Result = fail,
	    Message = RR;
	true->
	    Result = success,
	    Message = {RF, RR}
    end,
    Owner ! {trresult, Result, Message, Word}.
    
    
    
	
    
    
