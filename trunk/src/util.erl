%  Copyright 2007-2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%% @version $Id$
-module(util).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([escape_quotes/1, is_between/3, is_between_stab/3, is_between_closed/3,
         trunc/2, min/2, max/2, randomelem/1, logged_exec/1,
         wait_for_unregister/1, get_stacktrace/0, ksplit/2, dump/0, dump2/0,
         find/2, logger/0, dump3/0, uniq/1, get_nodes/0, minus/2,
         sleep_for_ever/0, shuffle/1, get_proc_in_vms/1,random_subset/2,
         gb_trees_largest_smaller_than/2, gb_trees_foldl/3, pow/2, parameterized_start_link/2]).
-export([sup_worker_desc/3, sup_worker_desc/4, sup_supervisor_desc/4]).

parameterized_start_link(Module, Parameters) ->
    apply(Module, start_link, Parameters).

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
    case length(L) =< K of
        true -> L;
        false -> element(1, lists:split(K, L))
    end.

max(plus_infinity, _) -> plus_infinity;
max(_, plus_infinity) -> plus_infinity;
max(minus_infinity, X) -> X;
max(X, minus_infinity) -> X;
max(A, B) ->
    case A > B of
        true -> A;
        false -> B
    end.

min(minus_infinity, _) -> minus_infinity;
min(_, minus_infinity) -> minus_infinity;
min(plus_infinity, X) -> X;
min(X, plus_infinity) -> X;
min(A, B) ->
    case A < B of
        true -> A;
        false -> B
    end.

randomelem(List)->
    Length= length(List),
    RandomNum = randoms:rand_uniform(1, Length),
    lists:nth(RandomNum, List).

logged_exec(Cmd) ->
    Output = os:cmd(Cmd),
    OutputLength = length(Output),
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
    erlang:get_stacktrace().

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
      lists:keysort(
        2, dict:to_list(
             lists:foldl(
               fun (X, Accum) ->
                       dict:merge(fun (_K, V1, V2) ->
                                          V1 + V2
                                  end,
                                  Accum,
                                  dict:store(
                                    lists:keysearch(current_function,
                                                    1,
                                                    [erlang:process_info(X, current_function)]
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
     %% {binary, Bin} = process_info(X, binary),
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
    {ok, F} = file:open(config:mem_log_file(), [write]),
    log(F).

log(F) ->
    io:format(F, "~p: ~p~n", [dump3(), time()]),
    timer:sleep(300000),
    log(F).

%% @doc minus(M,N) : { x | x in M and x notin N}
minus([],_N) ->
    [];
minus([H|T],N) ->
    case lists:member(H,N) of
        true -> minus(T,N);
        false -> [H]++minus(T,N)
    end.

%% @doc omit repeated entries in a sorted list
-spec(uniq/1 :: (list()) -> list()).
uniq([First | Rest]) ->
    lists:reverse(uniq(First, Rest, [First]));
uniq([]) ->
    [].

uniq(Current, [Current | Rest], Uniq) ->
    uniq(Current, Rest, Uniq);
uniq(_, [Head | Rest], Uniq) ->
    uniq(Head, Rest, [Head | Uniq]);
uniq(_, [], Uniq) ->
    Uniq.

-spec(get_nodes/0 :: () -> list()).
get_nodes() ->
    get_proc_in_vms(bench_server).

-spec(get_proc_in_vms/1 :: (atom()) -> list()).
get_proc_in_vms(Proc) ->
    boot_server:node_list(),
    Nodes =
        receive
            {get_list_response,X} ->
            X
        after 2000 ->
            {failed}
        end,
    lists:usort([cs_send:get(Proc, CSNode) || CSNode <- Nodes]).

sleep_for_ever() ->
    timer:sleep(5000),
    sleep_for_ever().

-spec(random_subset/2 :: (pos_integer(), list()) -> list()).
random_subset(Size, List) -> shuffle(List, [], Size).

%% @doc Fisher-Yates shuffling for lists
-spec(shuffle/1 :: (list()) -> list()).
shuffle(List) -> shuffle(List, [], length(List)).

shuffle([], Acc, _Size) -> Acc;
shuffle(_List, Acc, 0) -> Acc;
shuffle(List, Acc, Size) ->
    {Leading, [H | T]} = lists:split(random:uniform(length(List)) - 1, List),
    shuffle(Leading ++ T, [H | Acc], Size - 1).

-ifdef(types_are_builtin).
-spec(gb_trees_largest_smaller_than/2 :: (any(), gb_tree()) -> {value, any(), any()} | nil).
-else.
-spec(gb_trees_largest_smaller_than/2 :: (any(), gb_trees:gb_tree()) -> {value, any(), any()} | nil).
-endif.
gb_trees_largest_smaller_than(_Key, {0, _Tree}) ->
    nil;
gb_trees_largest_smaller_than(MyKey, {_Size, InnerTree} = Tree) ->
    case largest_smaller_than_iter(MyKey, InnerTree) of
        {value, _, _} = Value ->
            Value;
        nil ->
            {LargestKey, LargestValue} = gb_trees:largest(Tree),
            {value, LargestKey, LargestValue}
    end.

% find largest in subtree smaller than MyKey
largest_smaller_than_iter(_MyKey, nil) ->
    nil;
largest_smaller_than_iter(MyKey, {Key, Value, Smaller, Bigger}) ->
    case Key < MyKey of
        true ->
            case largest_smaller_than_iter(MyKey, Bigger) of
                {value, _, _} = AValue ->
                    AValue;
                nil ->
                    {value, Key, Value}
            end;
        false ->
            largest_smaller_than_iter(MyKey, Smaller)
    end.

-ifdef(types_are_builtin).
-spec(gb_trees_foldl/3 :: (any(), any(), gb_tree()) -> any()).
-else.
-spec(gb_trees_foldl/3 :: (any(), any(), gb_trees:gb_tree()) -> any()).
-endif.
gb_trees_foldl(_F, Accu, {0, _Tree}) ->
    Accu;
gb_trees_foldl(F, Accu, {_, Tree}) ->
    gb_trees_foldl_iter(F, Accu, Tree).

gb_trees_foldl_iter(_F, Accu, nil) ->
    Accu;
gb_trees_foldl_iter(F, Accu, {Key, Value, Smaller, Bigger}) ->
    Res1 = gb_trees_foldl_iter(F, Accu, Smaller),
    Res2 = F(Key, Value, Res1),
    gb_trees_foldl_iter(F, Res2, Bigger).

pow(_X, 0) ->
    1;
pow(X, 1) ->
    X;
pow(X, 2) ->
    X * X;
pow(X, 3) ->
    X * X * X;
pow(X, Y) ->
    case Y rem 2 of
        0 ->
            Half = pow(X, Y div 2),
            Half * Half;
        1 ->
            Half = pow(X, Y div 2),
            Half * Half * X
    end.

sup_worker_desc(Name, Module, Function) ->
    sup_worker_desc(Name, Module, Function, []).

sup_worker_desc(Name, Module, Function, Options) ->
    {Name, {Module, Function, Options},
      permanent,
      brutal_kill,
      worker,
      []
     }.

sup_supervisor_desc(Name, Module, Function, Options) ->
    {Name, {Module, Function, Options},
      permanent,
      brutal_kill,
      supervisor,
      []
     }.
