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
%%% File    : util.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Utility Functions
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: util.erl 479 2008-06-12 06:38:54Z schuett $
-module(util).

-author('schuett@zib.de').
-vsn('$Id: util.erl 479 2008-06-12 06:38:54Z schuett $ ').

-export([escape_quotes/1, is_between/3, is_between_stab/3, is_between_closed/3, 
	 trunc/2, min/2, max/2, lengthX/1, randomelem/1, logged_exec/1, 
	 wait_for_unregister/1, get_stacktrace/0, ksplit/2, dump/0, dump2/0, find/2, logger/0, dump3/0]).
                          
escape_quotes(String) ->
	lists:reverse(lists:foldl(fun escape_quotes_/2, [], String)).

escape_quotes_($", Rest) -> [$",$\\|Rest];
escape_quotes_(Ch, Rest) -> [Ch|Rest].


is_between(X, _, X) ->
    true;
is_between(minus_infinity, _, plus_infinity) ->
    true;
is_between(minus_infinity, plus_infinity, _) ->
    false;
is_between(plus_infinity, plus_infinity, _) ->
    false;
is_between(plus_infinity, minus_infinity, _) ->
    true;
is_between(Begin, Id, End) when Begin < End ->
    (Begin < Id) and (Id =< End);
is_between(Begin, Id, End) ->
    (Begin < Id) or (Id =< End).


is_between_stab(Begin, Id, End) ->
    if
	Begin < End -> 
	    (Begin < Id) and (Id < End);
	Begin == End ->
	    true;
	true -> 
	    (Begin < Id) or (Id < End)
    end.

is_between_closed(Begin, Id, End) ->
    if
	Begin < End -> 
	    (Begin < Id) and (Id < End);
	Begin == End ->
	    Id /= End;
	true -> 
	    (Begin < Id) or (Id < End)
    end.


trunc(L, K) ->
    if
	length(L) =< K ->
	    L;
	true ->
	    {Trunc, _} = lists:split(K, L),
	    Trunc
    end.
    
max(A, B) ->
    if
	A > B ->
	    A;
	true ->
	    B
    end.

min(A, B) ->
    if
	A < B ->
	    A;
	true ->
	    B
    end.

lengthX([]) ->
    0;
lengthX([_ | Rest]) ->
    1 + lengthX(Rest).

randomelem(List)->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Length= length(List),
    RandomNum = random:uniform(Length),
    lists:nth(RandomNum, List).

logged_exec(Cmd) ->
    Output = os:cmd(Cmd),
    OutputLength = lengthX(Output),
    if
	OutputLength > 10 ->
	    log:log("exec", Cmd),
	    log:log("exec", Output);
	true ->
	    ok
    end.

wait_for_unregister(PID) ->
    case whereis(PID) of
	undefined ->
	    ok;
	_ ->
	    wait_for_unregister(PID)
    end.

get_stacktrace() ->
    {'EXIT',{badarg,Stacktrace}} = (catch abs(x)),
    Stacktrace.


ksplit(List, K) ->
    N = length(List),
    PartitionSizes = lists:duplicate(N rem K, N div K + 1) ++ lists:duplicate(K - (N rem K), N div K),
    {Result, []} = lists:foldl(fun(Size, {Result, RestList}) -> 
				       {SubList, RestList2} = lists:split(Size, RestList),
				       {[SubList|Result], RestList2}
			end, {[], List}, PartitionSizes),
    Result.

dump() ->
    lists:reverse(
      lists:keysort(2, dict:to_list(lists:foldl(fun (X, Accum) -> 
							dict:merge(fun (_K, V1, V2) -> 
									   V1 + V2 
								   end, 
								   Accum, 
								   dict:store(
								     lists:keysearch(current_function, 
										     1, 
										     erlang:process_info(X, current_function)
										    ), 
								     1, 
								     dict:new()
								    )
								  ) 
						end, dict:new(), processes())))
     ).

dump2() ->
    lists:map(fun ({PID, {memory, Size}}) -> 
		      {_, Fun} = erlang:process_info(PID, current_function), 
		      {PID, Size, Fun} 
	      end, 
	      lists:reverse(lists:keysort(2, lists:map(fun (X) -> 
							       {X, process_info(X, memory)} 
						       end, 
						       processes())))).

dump3() ->
    lists:reverse(lists:keysort(2, lists:map(fun (X) -> 
						     {memory, Mem} = process_info(X, memory),
						     {current_function, CurFun} = process_info(X, current_function),
						     {message_queue_len, Msgs} = process_info(X, message_queue_len),
						     %{binary, Bin} = process_info(X, binary),
						     {stack_size, Stack} = process_info(X, stack_size),
						     {heap_size, Heap} = process_info(X, heap_size),
						     {messages, Messages} = process_info(X, messages),
						     {X, Mem, Msgs, Stack, Heap, lists:map(fun(Y) -> element(1, Y) end, Messages), CurFun}
					     end, processes()))).

find(Elem, [Elem | _]) ->
    1;
find(Elem, [_ | Tail]) ->
    1 + find(Elem, Tail).

logger() ->
    spawn(fun () -> log() end).

log() ->
    {ok, F} = file:open("mem.log", write),
    log(F).

log(F) ->
    io:format(F, "~p: ~p~n", [dump3(), time()]),
    timer:sleep(300000),
    log(F).
