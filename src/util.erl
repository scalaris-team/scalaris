% @copyright 2007-2015 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Utility Functions.
%% @end
%% @version $Id$
-module(util).

-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export_type([time_utc/0, us_timestamp/0]).

-export([escape_quotes/1,
         kahan_sum/3,
         min/2, max/2, log/2, log2/1, log1p/1, ceil/1, floor/1, pow1p/2,
         logged_exec/1,
         randomelem/1, randomelem_and_length/1,
         pop_randomelem/1, pop_randomelem/2, pop_randomsubset/2,
         get_stacktrace/0, get_linetrace/0, get_linetrace/1,
         do_throw/1,
         extract_from_list_may_not_exist/2,
         minus_all/2, minus_first/2,
         map_with_nr/3,
         par_map/2, par_map/3,
         lists_take/2, lists_takewith/2,
         lists_split/2, lists_keystore2/5,
         lists_partition3/2,
         lists_remove_at_indices/2,
         sublist/3, lists_index_of/2,
         lists_check_min_length/2,
         sleep_for_ever/0, shuffle/1, get_proc_in_vms/1,random_subset/2,
         gb_trees_largest_smaller_than/2, gb_trees_foldl/3, pow/2,
         zipfoldl/5, safe_split/2, '=:<'/2,
         split_unique/2, split_unique/3, split_unique/4,
         ssplit_unique/2, ssplit_unique/3, ssplit_unique/4,
         smerge2/2, smerge2/3, smerge2/4, smerge2/6,
         is_unittest/0, make_filename/1,
         app_get_env/2,
         app_check_running/1,
         timestamp2us/1, us2timestamp/1,
         time_plus_s/2, time_plus_ms/2, time_plus_us/2,
         readable_utc_time/1,
         for_to/3, for_to_ex/3, for_to_ex/4, for_to_fold/5,
         collect_while/1]).
-export([list_set_nth/3]).
-export([debug_info/0, debug_info/1]).
-export([print_bits/2, bin_xor/2, bin_or/2, bin_and/2]).
-export([if_verbose/1, if_verbose/2]).
-export([tc/3, tc/2, tc/1]).
-export([wait_for/1, wait_for/2,
         wait_for_process_to_die/1,
         wait_for_ets_table_to_disappear/2,
         ets_tables_of/1]).
-export([round/2]).

-export([repeat/3, repeat/4, parallel_run/5]).

-export([empty/1]).

-export([extint2atom/1]).

% RRD helpers which don't belong to the rrd datastructure
-export([ rrd_combine_timing_slots/3
         , rrd_combine_timing_slots/4
         , rrd_combine_gauge_slots/3
         , rrd_combine_gauge_slots/4
         , rrd_combine_slots/6
    ]).

% feeder for tester
-export([log_feeder/2, log2_feeder/1, log1p_feeder/1, pow1p_feeder/2]).
-export([readable_utc_time_feeder/1]).
-export([map_with_nr_feeder/3]).
-export([par_map_feeder/2, par_map_feeder/3]).
-export([lists_takewith_feeder/2]).
-export([lists_partition3_feeder/2]).

-export([sets_map/2, sets_equal/2]).

-type us_timestamp() :: non_neg_integer(). % micro seconds since Epoch

-type time_utc() :: {{1970..10000, 1..12, 1..31}, {0..23, 0..59, 0..59}}.

-type args() :: [term()].
-type accumulatorFun(T, U) :: fun((T, U) -> U).
-type repeat_params() :: parallel |
                         collect |
                         {accumulate, accumulatorFun(any(), R), R}. %{accumulate, fun, accumulator init value}

-dialyzer([{[no_opaque, no_contracts], gb_trees_largest_smaller_than/2},
           {no_return, lists_takewith_iter_feeder/3}]).

%% @doc Executes wait_for/2 with a WaitTime of 10ms.
-spec wait_for(fun(() -> boolean())) -> ok.
wait_for(F) -> wait_for(F, 10).

%% @doc Waits for F/0 to become true and checks every WaitTime Milliseconds.
%%      Uses (send_local_after/2 and receive) or timer:sleep/1 to wait to
%%      return control flow to e.g. proto_sched depending on whether the caller
%%      is a gen_component or not.
-spec wait_for(fun(() -> boolean()), WaitTimeInMs::pos_integer()) -> ok.
wait_for(F, WaitTime) ->
    case gen_component:is_gen_component(self()) of
        true  -> wait_for2(F, WaitTime);
        false -> wait_for1(F, WaitTime)
    end.

%% @doc Waits for F/0 to become true and checks every WaitTime Milliseconds.
%%      Uses send_local_after/2 and receive to wait to return control flow to
%%       e.g. proto_sched and is thus _NOT_ suitable for gen_components.
-spec wait_for1(fun(() -> boolean()), WaitTimeInMs::pos_integer()) -> ok.
wait_for1(F, WaitTime) ->
    case F() of
        true  -> ok;
        false ->
            WaitID = uid:get_pids_uid(),
            comm:send_local_after(WaitTime, self(), {continue_wait, WaitID}),
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV({continue_wait, WaitID},% ->
                               wait_for1(F, WaitTime))
            end
    end.

%% @doc Waits for F/0 to become true and checks every WaitTime Milliseconds.
%%      Uses timer:sleep/1 to wait and is thus suitable for gen_components.
-spec wait_for2(fun(() -> boolean()), WaitTimeInMs::pos_integer()) -> ok.
wait_for2(F, WaitTime) ->
    case F() of
        true  -> ok;
        false -> timer:sleep(WaitTime),
                 wait_for2(F, WaitTime)
    end.

%% @doc Waits for the given process (name or pid) to die.
-spec wait_for_process_to_die(pid() | atom()) -> ok.
wait_for_process_to_die(Name) when is_atom(Name) ->
    wait_for(fun() ->
                     case erlang:whereis(Name) of
                         undefined -> true;
                         Pid       -> not is_process_alive(Pid)
                     end
             end);
wait_for_process_to_die(Pid) when is_pid(Pid) ->
    wait_for(fun() -> not is_process_alive(Pid) end).

%% @doc Waits for the given ets table to disappear.
-spec wait_for_ets_table_to_disappear(Pid::pid(), ets:tid() | atom()) -> ok.
wait_for_ets_table_to_disappear(Pid, Table) ->
    wait_for(fun() ->
                     case ets:info(Table, owner) of
                         undefined -> true;
                         Pid -> false;
                         _ -> true
                     end
             end).

-spec ets_tables_of(pid()) -> [ets:tid() | atom()].
ets_tables_of(Pid) ->
    Tabs = ets:all(),
    [ Tab || Tab <- Tabs, ets:info(Tab, owner) =:= Pid ].

%% @doc Escapes quotes in the given string.
-spec escape_quotes(String::string()) -> string().
escape_quotes(String) ->
    lists:foldr(fun escape_quotes_/2, [], String).

%-spec escape_quotes_(String::string(), Rest::string()) -> string().
-spec escape_quotes_(char(), string()) -> string().
escape_quotes_($", Rest) -> [$\\, $" | Rest];
escape_quotes_(Ch, Rest) -> [Ch | Rest].

%% @doc Variant of erlang:max/2 also taking ?PLUS_INFINITY_TYPE and
%%      ?MINUS_INFINITY_TYPE into account, e.g. for comparing keys.
%% @end
%%-spec max(?PLUS_INFINITY_TYPE, any()) -> ?PLUS_INFINITY_TYPE;
%%         (any(), ?PLUS_INFINITY_TYPE) -> ?PLUS_INFINITY_TYPE;
%%         (T | ?MINUS_INFINITY_TYPE, T | ?MINUS_INFINITY_TYPE) -> T.
-spec max(any(), any()) -> any().
max(?PLUS_INFINITY, _) -> ?PLUS_INFINITY;
max(_, ?PLUS_INFINITY) -> ?PLUS_INFINITY;
max(?MINUS_INFINITY, X) -> X;
max(X, ?MINUS_INFINITY) -> X;
max(A, B) when A > B -> A;
max(_A, B) -> B.

%% @doc Variant of erlang:min/2 also taking ?PLUS_INFINITY_TYPE and
%%      ?MINUS_INFINITY_TYPE into account, e.g. for comparing keys.
%% @end
%%-spec min(?MINUS_INFINITY_TYPE, any()) -> ?MINUS_INFINITY_TYPE;
%%         (any(), ?MINUS_INFINITY_TYPE) -> ?MINUS_INFINITY_TYPE;
%%         (T | ?PLUS_INFINITY_TYPE, T | ?PLUS_INFINITY_TYPE) -> T.
-spec min(any(), any()) -> any().
min(?MINUS_INFINITY, _) -> ?MINUS_INFINITY;
min(_, ?MINUS_INFINITY) -> ?MINUS_INFINITY;
min(?PLUS_INFINITY, X) -> X;
min(X, ?PLUS_INFINITY) -> X;
min(A, B) when A < B -> A;
min(_A, B) -> B.

%% @doc Implements the Kahan summation algorithm for a low numerical error
%%      when adding a sequence of finite precision floating point numbers.
%%      (ref. [https://en.wikipedia.org/wiki/Kahan_summation_algorithm])
-spec kahan_sum([number()], AccSum::float(), AccCompensation::float()) ->
          {Sum::float(), Compensation::float()}.
kahan_sum([], AccSum, AccCompensation) ->
    {AccSum, AccCompensation};
kahan_sum([X | Rest], Sum, C) ->
    Y = X - C,
    T = Sum + Y,
    kahan_sum(Rest, T, (T - Sum) - Y).

-spec pow(integer(), non_neg_integer()) -> integer();
         (float(), non_neg_integer()) -> number().
pow(_X, 0) ->
    1;
pow(X, 1) ->
    X;
pow(X, 2) ->
    X * X;
pow(X, 3) ->
    X * X * X;
pow(X, Y) when (Y rem 2) =:= 0 ->
    Half = pow(X, Y div 2),
    Half * Half;
pow(X, Y) ->
    Half = pow(X, Y div 2),
    Half * Half * X.

-spec log_feeder(X::number(), Base::number()) -> {number(), number()}.
log_feeder(X0, B0) ->
    X = case X0 of
            0   -> 1;   % not allowed
            0.0 -> 1.0; % not allowed
            X1  -> erlang:abs(X1)
        end,
    B = case erlang:abs(B0) of
           1    -> 2;   % 1 not allowed
           1.0  -> 2.0; % 1 not allowed
           0    -> 2;   % 0 not allowed
           0.0  -> 2.0; % 0 not allowed
           B1   -> B1
        end,
    {X, B}.

%% @doc Logarithm of X to the base of Base.
-spec log(X::number(), Base::number()) -> float().
log(X, B) -> math:log10(X) / math:log10(B).

-spec log2_feeder(X::number()) -> {number()}.
log2_feeder(0)   -> {1}; % 0 not allowed
log2_feeder(0.0) -> {1.0}; % 0 not allowed
log2_feeder(X) when X < 0 -> {-X};
log2_feeder(X)   -> {X}.

%% @doc Logarithm of X to the base of 2.
-spec log2(X::number()) -> float().
log2(X) -> math:log10(X) / 0.3010299956639812. % use hard-coded math:log10(2)

-spec log1p_feeder(X::number()) -> {number()}.
log1p_feeder(-1)   -> {0}; % -1 not allowed
log1p_feeder(-1.0) -> {0.0}; % -1 not allowed
log1p_feeder(X) when X < -1 -> {-X};
log1p_feeder(X)    -> {X}.

%% @doc More precise version of ln(1+x) for small x.
%%
%% from: David Goldberg. 1991. What every computer scientist should know
%%       about floating-point arithmetic. ACM Comput. Surv. 23, 1
%%       (March 1991), 5-48. DOI=<a href="http://dx.doi.org/10.1145/103162.103163">10.1145/103162.103163</a>
-spec log1p(X::number()) -> float().
log1p(X) ->
    W = 1 + X,
    if W == 1 -> float(X);
       true   -> X * math:log(W) / (W-1)
    end.

-spec pow1p_feeder(X0::float(), Y0::float()) -> {X::float(), Y::float()}.
pow1p_feeder(X0, Y0) ->
    X = if X0 < 0 -> -X0;
           true   -> X0
        end,
    {X, Y0}.

%% @doc Calculates (1 - X^Y) more exactly, especially for X^Y near 1
%%      (only really usefull for 0 &lt; X &lt; 1 - for the rest, use math:pow/2).
%%      Uses the series representation of 1 - X^Y
%%      1-X^Y = sum_(i=1)^infinity (- Y^i * log^i(X) / (i!))
%% from: <a href="http://www.wolframalpha.com/input/?i=series+x^y">Wolfram Alpha for x^y</a>
-spec pow1p(X::float(), Y::float()) -> float().
pow1p(X, Y) when X >= 0 andalso Y == 0 ->
    0.0;
pow1p(X, Y) when X == 0 andalso Y /= 0 ->
    1.0;
pow1p(X, Y) when X > 0 andalso Y /= 0 ->
    % the difference between the terms for i and (i+1) is ln(X)/((i+1)*N)
    YxLnX = Y * math:log(X),
    pow1p_(-YxLnX, YxLnX, 2, -YxLnX).

%% @doc Helper for pow1p/2.
-spec pow1p_(Prev::float(), YxLnX::float(), I::pos_integer(), PrevSum::float())
        -> float().
pow1p_(Prev, YxLnX, CurI, Sum) ->
    Cur = Prev * YxLnX / CurI,
    NextSum = Sum + Cur,
    if NextSum == Sum ->
           % log:pal("end at ~B", [CurI]),
           Sum;
       true ->
           % log:pal("cur ~p ~p", [Sum, NextSum]),
           pow1p_(Cur, YxLnX, CurI + 1, NextSum)
    end.

%% @doc Returns the largest integer not larger than X.
-spec floor(X::number()) -> integer().
floor(X) ->
    T = erlang:trunc(X),
    if X < T -> T - 1;
       true  -> T
    end.

%% @doc Returns the smallest integer not smaller than X.
-spec ceil(X::number()) -> integer().
ceil(X) ->
    T = erlang:trunc(X),
    if X > T -> T + 1;
       true  -> T
    end.

-spec logged_exec(Cmd::string() | atom()) -> ok.
logged_exec(Cmd) ->
    Output = os:cmd(Cmd),
    OutputLength = length(Output),
    if
        OutputLength > 10 ->
            log:log(info, "exec", Cmd),
            log:log(info, "exec", Output),
            ok;
        true ->
            ok
    end.

%% @doc Gets the current stack trace. Use this method in order to get a stack
%%      trace if no exception was thrown.
-spec get_stacktrace() -> [{Module::atom(), Function::atom(), ArityOrArgs::byte() | [term()]} |
                           {Module::atom(), Function::atom(), ArityOrArgs::byte() | [term()], Sources::[term()]}].
get_stacktrace() ->
    % throw an exception for erlang:get_stacktrace/0 to return the actual stack trace
    case (try erlang:exit(a)
          catch exit:_ -> erlang:get_stacktrace()
          end) of
        % erlang < R15 : {util, get_stacktrace, 0}
        % erlang >= R15: {util, get_stacktrace, 0, _}
        %% drop head element as it was generated just above
        [T | ST] when erlang:element(1, T) =:= util andalso
                          erlang:element(2, T) =:= get_stacktrace andalso
                          erlang:element(3, T) =:= 0 -> ok;
        ST -> ST % just in case
    end,
    ST.

-spec get_linetrace() -> term() | undefined.
get_linetrace() ->
    erlang:get(test_server_loc).

-spec get_linetrace(Pid::pid()) -> term() | undefined.
get_linetrace(Pid) ->
    {dictionary, Dict} = erlang:process_info(Pid, dictionary),
    extract_from_list_may_not_exist(Dict, test_server_loc).

-spec do_throw(term()) -> no_return().
do_throw(Exception) ->
    log:log("Exception ~p at ~.0p", [Exception, get_stacktrace()]),
    erlang:throw(Exception).

%% @doc Extracts a given ItemInfo from an ItemList or returns 'undefined' if
%%      there is no such item.
-spec extract_from_list_may_not_exist
        ([{Item::term(), Info::any()}], ItemInfo::term()) -> Info::any() | undefined.
extract_from_list_may_not_exist(List, Key) ->
    case lists:keyfind(Key, 1, List) of
        false -> undefined;
        X     -> element(2, X)
    end.

%% @doc minus_all(M,N) : { x | x in M and x notin N}
-spec minus_all(List::[T], Excluded::[T]) -> [T]
    when is_subtype(T, any()).
minus_all([_|_] = L, [Excluded]) ->
    [E || E <- L, E =/= Excluded];
minus_all([_|_] = L, ExcludeList) ->
    ExcludeSet = sets:from_list(ExcludeList),
    [E || E <- L, not sets:is_element(E, ExcludeSet)];
minus_all([], _ExcludeList) ->
    [].

%% @doc Deletes the first occurrence of each element in Excluded from List.
%%      Similar to lists:foldl(fun lists:delete/2, NewValue1, ToDel) but more
%%      performant for out case.
-spec minus_first(List::[T], Excluded::[T]) -> [T]
    when is_subtype(T, any()).
minus_first([_|_] = L, [Excluded]) ->
    lists:delete(Excluded, L);
minus_first([_|_] = L, ExcludeList) ->
    minus_first2(L, ExcludeList);
minus_first([], _ExcludeList) ->
    [].

%% @doc Removes every item in Excluded only once from List.
-spec minus_first2(List::[T], Excluded::[T]) -> [T]
    when is_subtype(T, any()).
minus_first2([H | T], [_|_] = Excluded) ->
    case lists_take(H, Excluded) of
        false     -> [H | minus_first2(T, Excluded)];
        Excluded2 -> minus_first2(T, Excluded2)
    end;
minus_first2([], _Excluded) ->
    [];
minus_first2(L, []) ->
    L.

-spec get_proc_in_vms(atom()) -> [comm:mypid()].
get_proc_in_vms(Proc) ->
    mgmt_server:node_list(),
    Nodes =
        begin
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV({get_list_response, X}, X)
        after 2000 ->
            log:log(error,"[ util ] Timeout getting node list from mgmt server"),
            throw('mgmt_server_timeout')
        end end,
    lists:usort([comm:get(Proc, DHTNode) || DHTNode <- Nodes]).

-spec sleep_for_ever() -> no_return().
sleep_for_ever() ->
    timer:sleep(5000),
    sleep_for_ever().

%% @doc Returns a random element from the given (non-empty!) list according to
%%      a uniform distribution.
-spec randomelem(List::[X,...]) -> X
    when is_subtype(X, any()).
randomelem([X]) -> X;
randomelem(List) ->
    element(1, randomelem_and_length(List)).

%% @doc Returns a random element from the given (non-empty!) list according to
%%      a uniform distribution (also returns the list's length).
-spec randomelem_and_length(List::[X,...]) -> {X, Length::pos_integer()}
    when is_subtype(X, any()).
randomelem_and_length([X]) -> {X, 1};
randomelem_and_length(List) ->
    Length = length(List) + 1,
    RandomNum = randoms:rand_uniform(1, Length),
    {lists:nth(RandomNum, List), Length - 1}.

%% @doc Removes a random element from the (non-empty!) list and returns the
%%      resulting list and the removed element.
-spec pop_randomelem(List::[X,...]) -> {NewList::[X], PoppedElement::X}
    when is_subtype(X, any()).
pop_randomelem([X]) -> {[], X};
pop_randomelem(List) ->
    pop_randomelem(List, length(List)).

%% @doc Removes a random element from the first Size elements of a (non-empty!)
%%      list and returns the resulting list and the removed element.
%%      If Size is 0, the first element will be popped.
%%      Size must not exceed the length of the list!
-spec pop_randomelem(List::[X,...], Size::non_neg_integer())
        -> {NewList::[X], PoppedElement::X}
    when is_subtype(X, any()).
pop_randomelem([X | TL], 0) -> {TL, X};
pop_randomelem([X | TL], 1) -> {TL, X};
pop_randomelem(List, Size) ->
    {Leading, [H | T]} = lists:split(randoms:rand_uniform(0, Size), List),
    {lists:append(Leading, T), H}.

%% @doc Removes a random subset of Size elements from the given list and returns
%%      the resulting list and the removed subset.
%%      If Size is larger than the list length, all elements will be returned
%%      (in random order).
-spec pop_randomsubset(Size::pos_integer(), [X])
        -> {NewList::[X], PoppedSet::[X]}
    when is_subtype(X, any()).
pop_randomsubset(0, List) -> {List, []};
pop_randomsubset(_Size, []) -> {[], []};
pop_randomsubset(_Size, [X]) -> {[], [X]};
pop_randomsubset(Size0, List) ->
    A = array:fix(array:from_list(List)),
    Size = erlang:min(Size0, array:size(A)),
    LNew = array:to_list(shuffle_helperA(A, Size, 0)),
    {PoppedSet, Rest} = lists:split(Size, LNew),
    {Rest, PoppedSet}.

%% @doc Returns a random subset of Size elements from the given list.
-spec random_subset(Size::pos_integer(), [T]) -> [T]
    when is_subtype(T, any()).
random_subset(0, _List) -> [];
random_subset(_Size, []) -> [];
random_subset(_Size, [X]) -> [X];
random_subset(Size0, List) ->
    A = array:fix(array:from_list(List)),
    Size = erlang:min(Size0, array:size(A)),
    A2 = shuffle_helperA(A, Size, 0),
    array:to_list(array:resize(Size, A2)).

%% @doc Fisher-Yates shuffling for lists.
-spec shuffle([T]) -> [T]
    when is_subtype(T, any()).
shuffle([]) -> [];
shuffle([X]) -> [X];
shuffle(List) ->
    A = array:fix(array:from_list(List)),
    A2 = shuffle_helperA(A, array:size(A), 0),
    array:to_list(A2).

%% @doc Fisher-Yates shuffling for lists helper function: creates a shuffled
%%      list of length ShuffleSize.
%%      PreCond: a non-empty array, ShuffleSize &lt; size(Array)
-spec shuffle_helperA(array:array(T), ShuffleSize::non_neg_integer(),
                      StartPos::non_neg_integer()) -> array:array(T)
    when is_subtype(T, any()).
shuffle_helperA(Array, CurPos, CurPos) ->
    Array;
shuffle_helperA(Array, ShuffleSize, N1) ->
    E1 = array:get(N1, Array),
    N2 = randoms:rand_uniform(N1, array:size(Array)),
    E2 = array:get(N2, Array),
    A2 = array:set(N1, E2, array:set(N2, E1, Array)),
    shuffle_helperA(A2, ShuffleSize, N1 + 1).

%% @doc Find the largest key in GBTree that is smaller than Key.
%%      Note: gb_trees offers only linear traversal or lookup of exact keys -
%%      we implement a more flexible binary search here despite gb_tree being
%%      defined as opaque.
-spec gb_trees_largest_smaller_than(Key, gb_trees:tree(Key, Value))
        -> {value, Key, Value} | nil
    when is_subtype(Key, any()),
         is_subtype(Value, any()).
gb_trees_largest_smaller_than(_Key, {0, _Tree}) ->
    nil;
gb_trees_largest_smaller_than(MyKey, {_Size, InnerTree}) ->
    gb_trees_largest_smaller_than_iter(MyKey, InnerTree, true).

-spec gb_trees_largest_smaller_than_iter(Key, {Key, Value, Smaller::term(), Bigger::term()}, RightTree::boolean()) -> {value, Key, Value} | nil
    when is_subtype(Key, any()),
         is_subtype(Value, any()).
gb_trees_largest_smaller_than_iter(SearchKey, {Key, Value, _Smaller, nil},
                                   true) when Key < SearchKey ->
    % we reached the right end of the whole tree
    % -> there is no larger item than the current item
    {value, Key, Value};
gb_trees_largest_smaller_than_iter(SearchKey, {Key, Value, _Smaller, Bigger},
                                   RightTree) when Key < SearchKey ->
    case gb_trees_largest_smaller_than_iter(SearchKey, Bigger, RightTree) of
        {value, _, _} = AValue -> AValue;
        nil -> {value, Key, Value}
    end;
gb_trees_largest_smaller_than_iter(SearchKey, {_Key, _Value, Smaller, _Bigger},
                                   _RightTree) ->
  gb_trees_largest_smaller_than_iter(SearchKey, Smaller, false);
gb_trees_largest_smaller_than_iter(_SearchKey, nil, _RightTree) ->
    nil.

%% @doc Foldl over gb_trees.
-spec gb_trees_foldl(fun((Key, Value, Acc) -> Acc), Acc, gb_trees:tree(Key, Value)) -> Acc
    when is_subtype(Acc, any()),
         is_subtype(Key, any()),
         is_subtype(Value, any()).
gb_trees_foldl(F, Acc, GBTree) ->
    gb_trees_foldl_iter(F, Acc, gb_trees:next(gb_trees:iterator(GBTree))).

-spec gb_trees_foldl_iter(fun((Key, Value, Acc) -> Acc), Acc,
                          {Key, Value, Iter::gb_trees:iter(Key, Value)} | none) -> Acc.
gb_trees_foldl_iter(_F, Acc, none) ->
    Acc;
gb_trees_foldl_iter(F, Acc, {Key, Val, Iter}) ->
    gb_trees_foldl_iter(F, F(Key, Val, Acc), gb_trees:next(Iter)).

%% @doc Measures the execution time (in microseconds) for an MFA
%%      (does not catch exceptions as timer:tc/3 in older Erlang versions).
-spec tc(module(), atom(), list()) -> {integer(), term()}.
tc(M, F, A) ->
    Before = os:timestamp(),
    Val = apply(M, F, A),
    After = os:timestamp(),
    {timer:now_diff(After, Before), Val}.

%% @doc Measures the execution time (in microseconds) for Fun(Args)
%%      (does not catch exceptions as timer:tc/3 in older Erlang versions).
-spec tc(Fun::fun(), Args::list()) -> {integer(), term()}.
tc(Fun, Args) ->
    Before = os:timestamp(),
    Val = apply(Fun, Args),
    After = os:timestamp(),
    {timer:now_diff(After, Before), Val}.

%% @doc Measures the execution time (in microseconds) for Fun()
%%      (does not catch exceptions as timer:tc/3 in older Erlang versions).
-spec tc(Fun::fun()) -> {integer(), term()}.
tc(Fun) ->
    Before = os:timestamp(),
    Val = Fun(),
    After = os:timestamp(),
    {timer:now_diff(After, Before), Val}.

-spec zipfoldl(ZipFun::fun((X, Y) -> Z), FoldFun::fun((Z, Acc) -> Acc), L1::[X], L2::[Y], Acc) -> Acc.
zipfoldl(ZipFun, FoldFun, [L1H | L1R], [L2H | L2R], AccIn) ->
    zipfoldl(ZipFun, FoldFun, L1R, L2R, FoldFun(ZipFun(L1H, L2H), AccIn));
zipfoldl(_ZipFun, _FoldFun, [], [], AccIn) ->
    AccIn.

%% @doc Sorts like erlang:'=&lt;'/2 but also defines the order of integers/floats
%%      representing the same value.
-spec '=:<'(T, T) -> boolean()
                     when is_subtype(T, any()).
'=:<'(T1, T2) ->
    case (T1 == T2) andalso (T1 =/= T2) of
        true when erlang:is_number(T1) andalso erlang:is_number(T2) ->
            erlang:is_integer(T1);
        true when erlang:is_tuple(T1) andalso erlang:is_tuple(T2) ->
            '=:<'(erlang:tuple_to_list(T1), erlang:tuple_to_list(T2));
        true when erlang:is_list(T1) andalso erlang:is_list(T2) ->
            % recursively check '=<'
            '=:<_lists'(T1, T2);
        _ -> erlang:'=<'(T1, T2)
    end.

%% @doc Compare two lists which are equal based on erlang:'=='/2.
-spec '=:<_lists'(T::list(), T::list()) -> boolean().
'=:<_lists'([H1 | R1], [H2 | R2]) ->
    case (H1 == H2) andalso (H1 =/= H2) of
        true  -> '=:<'(H1, H2);
        false -> '=:<_lists'(R1, R2)
    end;
'=:<_lists'([], []) -> true.

%% @doc Splits off N elements from List. If List is not large enough, the whole
%%      list is returned.
-spec safe_split(non_neg_integer(), [T]) -> {FirstN::[T], Rest::[T]}
    when is_subtype(T, any()).
safe_split(N, List) when is_integer(N), N >= 0, is_list(List) ->
    safe_split(N, List, []).

-spec safe_split(non_neg_integer(), [T], [T]) -> {FirstN::[T], Rest::[T]}
    when is_subtype(T, any()).
safe_split(0, L, R) ->
    {lists:reverse(R, []), L};
safe_split(N, [H | T], R) ->
    safe_split(N - 1, T, [H | R]);
safe_split(_N, [], R) ->
    {lists:reverse(R, []), []}.

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that both lists share and a list of elements unique to L2.
%%      Returned lists are sorted and contain no duplicates.
-spec split_unique(L1::[X], L2::[X]) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}
    when is_subtype(X, any()).
split_unique(L1, L2) ->
    split_unique(L1, L2, fun erlang:'=<'/2).

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that are equal in both lists (according to the ordering
%%      function Lte) and a list of elements unique to L2.
%%      When two elements compare equal, the element from List1 is picked.
%%      Lte(A, B) should return true if A compares less than or equal to B in
%%      the ordering, false otherwise.
%%      Returned lists are sorted according to Lte and contain no duplicates.
-spec split_unique(L1::[X], L2::[X], Lte::fun((X, X) -> boolean())) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}
    when is_subtype(X, any()).
split_unique(L1, L2, Lte) ->
    split_unique(L1, L2, Lte, fun(E1, _E2) -> E1 end).

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that are equal in both lists (according to the ordering
%%      function Lte) and a list of elements unique to L2.
%%      When two elements compare equal, EqSelect(element(L1), element(L2))
%%      chooses which of them to take.
%%      Lte(A, B) should return true if A compares less than or equal to B in
%%      the ordering, false otherwise.
%%      Returned lists are sorted according to Lte and contain no duplicates.
-spec split_unique(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> X)) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}
    when is_subtype(X, any()).
split_unique(L1, L2, Lte, EqSelect) ->
    L1Sorted = lists:usort(Lte, L1),
    L2Sorted = lists:usort(Lte, L2),
    ssplit_unique_helper(L1Sorted, L2Sorted, Lte, EqSelect, {[], [], []}).

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that both lists share and a list of elements unique to L2.
%%      Both lists must be sorted. Returned lists are sorted as well.
-spec ssplit_unique(L1::[X], L2::[X]) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}
    when is_subtype(X, any()).
ssplit_unique(L1, L2) ->
    ssplit_unique(L1, L2, fun erlang:'=<'/2).

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that are equal in both lists (according to the ordering
%%      function Lte) and a list of elements unique to L2.
%%      When two elements compare equal, the element from List1 is picked.
%%      Both lists must be sorted according to Lte. Lte(A, B) should return
%%      true if A compares less than or equal to B in the ordering, false
%%      otherwise.
%%      Returned lists are sorted according to Lte.
-spec ssplit_unique(L1::[X], L2::[X], Lte::fun((X, X) -> boolean())) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}
    when is_subtype(X, any()).
ssplit_unique(L1, L2, Lte) ->
    ssplit_unique(L1, L2, Lte, fun(E1, _E2) -> E1 end).

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that are equal in both lists (according to the ordering
%%      function Lte) and a list of elements unique to L2.
%%      When two elements compare equal, EqSelect(element(L1), element(L2))
%%      chooses which of them to take.
%%      Both lists must be sorted according to Lte. Lte(A, B) should return true
%%      if A compares less than or equal to B in the ordering, false otherwise.
%%      Returned lists are sorted according to Lte.
-spec ssplit_unique(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> X)) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}
    when is_subtype(X, any()).
ssplit_unique(L1, L2, Lte, EqSelect) ->
    ssplit_unique_helper(L1, L2, Lte, EqSelect, {[], [], []}).

%% @doc Helper function for ssplit_unique/4.
-spec ssplit_unique_helper(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> X), {UniqueOldL1::[X], SharedOld::[X], UniqueOldL2::[X]}) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}
    when is_subtype(X, any()).
ssplit_unique_helper(L1 = [H1 | T1], L2 = [H2 | T2], Lte, EqSelect, {UniqueL1, Shared, UniqueL2}) ->
    LteH1H2 = Lte(H1, H2),
    LteH2H1 = Lte(H2, H1),
    case LteH1H2 andalso LteH2H1 of
        true ->
            ssplit_unique_helper(T1, L2, Lte, EqSelect, {UniqueL1, [EqSelect(H1, H2) | Shared], UniqueL2});
        false when LteH1H2 ->
            ssplit_unique_helper(T1, L2, Lte, EqSelect, {[H1 | UniqueL1], Shared, UniqueL2});
        false when LteH2H1 ->
            % the top of the shared list could be the same as the top of L2!
            case (Shared =:= []) orelse not (Lte(hd(Shared), H2) andalso Lte(H2, hd(Shared))) of
                true  -> ssplit_unique_helper(L1, T2, Lte, EqSelect, {UniqueL1, Shared, [H2 | UniqueL2]});
                false -> ssplit_unique_helper(L1, T2, Lte, EqSelect, {UniqueL1, Shared, UniqueL2})
            end
    end;
ssplit_unique_helper(L1, [], _Lte, _EqSelect, {UniqueL1, Shared, UniqueL2}) ->
    {lists:reverse(UniqueL1, L1), lists:reverse(Shared), lists:reverse(UniqueL2)};
ssplit_unique_helper([], L2 = [H2 | T2], Lte, EqSelect, {UniqueL1, Shared, UniqueL2}) ->
    % the top of the shared list could be the same as the top of L2 since
    % elements are only removed from L2 if an element of L1 is larger
    case Shared =:= [] orelse not (Lte(hd(Shared), H2) andalso Lte(H2, hd(Shared))) of
        true  ->
            {lists:reverse(UniqueL1), lists:reverse(Shared), lists:reverse(UniqueL2, L2)};
        false ->
            ssplit_unique_helper([], T2, Lte, EqSelect, {UniqueL1, Shared, UniqueL2})
    end.

%% @doc Merges two unique sorted lists into a single list.
-spec smerge2(L1::[X], L2::[X]) -> MergedList::[X]
    when is_subtype(X, any()).
smerge2(L1, L2) ->
    smerge2(L1, L2, fun erlang:'=<'/2).

%% @doc Merges two unique Lte-sorted lists into a single list.
-spec smerge2(L1::[X], L2::[X], Lte::fun((X, X) -> boolean())) -> MergedList::[X]
    when is_subtype(X, any()).
smerge2(L1, L2, Lte) ->
    smerge2(L1, L2, Lte, fun(E1, _E2) -> [E1] end).

%% @doc Merges two unique Lte-sorted lists into a single list.
-spec smerge2(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> [X])) -> MergedList::[X]
    when is_subtype(X, any()).
smerge2(L1, L2, Lte, EqSelect) ->
    smerge2(L1, L2, Lte, EqSelect, fun(X) -> [X] end, fun(X) -> [X] end).

%% @doc Merges two unique Lte-sorted lists into a single list.
-spec smerge2(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> [X]),
              FirstExist::fun((X) -> [X]), SecondExist::fun((X) -> [X])) -> MergedList::[X]
    when is_subtype(X, any()).
smerge2(L1, L2, Lte, EqSelect, FirstExist, SecondExist) ->
    smerge2_helper(L1, L2, Lte, EqSelect, FirstExist, SecondExist).

%% @doc Helper function for merge2/4.
-spec smerge2_helper(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()),
        EqSelect::fun((X, X) -> [X]), FirstExist::fun((X) -> [X]),
        SecondExist::fun((X) -> [X])) -> MergedList::[X]
    when is_subtype(X, any()).
smerge2_helper(L1 = [H1 | T1], L2 = [H2 | T2], Lte, EqSelect, FirstExist, SecondExist) ->
    LteH1H2 = Lte(H1, H2),
    LteH2H1 = Lte(H2, H1),
    % note: need to reverse the results of EqSelect, FirstExist, SecondExist since ML is reversed
    if LteH1H2 andalso LteH2H1 ->
           EqSelect(H1, H2) ++ smerge2_helper(T1, T2, Lte, EqSelect, FirstExist, SecondExist);
       LteH1H2 ->
           FirstExist(H1) ++ smerge2_helper(T1, L2, Lte, EqSelect, FirstExist, SecondExist);
       LteH2H1 ->
           SecondExist(H2) ++ smerge2_helper(L1, T2, Lte, EqSelect, FirstExist, SecondExist)
    end;
smerge2_helper(L1, [], _Lte, _EqSelect, FirstExist, _SecondExist) ->
    lists:flatmap(FirstExist, L1);
smerge2_helper([], L2, _Lte, _EqSelect, _FirstExist, SecondExist) ->
    lists:flatmap(SecondExist, L2).

%% @doc Try to check whether common-test is running.
-spec is_unittest() -> boolean().
is_unittest() ->
    case erlang:get({util, is_unittest}) of
        undefined ->
            Pid = self(),
            % old erlang versions, e.g. R14B04, may not clean up old DOWN messages in
            % demonitor and thus pollute the message queue and cause 'unknown message'
            % warnings in gen_components
            % -> spawn the ct:get_status() call into a separate process
            % Note: no comm:send_local and no SCALARIS_RECV needed (we are not
            % interested in tracing this)
            spawn(fun () ->
                           case catch ct:get_status() of
                               no_tests_running -> Pid ! {is_unittest, false};
                               {error, _} -> Pid ! {is_unittest, false};
                               {'EXIT', {undef, _}} -> Pid ! {is_unittest, false};
                               _ -> Pid ! {is_unittest, true}
                           end
                  end),
            receive
                {is_unittest, Result} ->
                    erlang:put({util, is_unittest}, Result),
                    Result
            end;
        Value -> Value
  end.

-spec make_filename([byte()]) -> string().
make_filename(Name) ->
    re:replace(Name, "[^a-zA-Z0-9\-_@\.]", "_", [{return, list}, global]).

%% @doc Get an application environment variable for the 'scalaris' application.
%%      If the variable is undefined, Default is returned.
-spec app_get_env(Var::atom(), Default::T) -> T
    when is_subtype(T, any()).
app_get_env(Var, Default) ->
    case application:get_env(scalaris, Var) of
        {ok, Val} -> Val;
        _         -> app_check_known(),
                     Default
    end.

-spec app_check_known() -> ok.
app_check_known() ->
    case application:get_application() of
        {ok, scalaris } -> ok;
        undefined ->
            case is_unittest() of
                true -> ok;
                _    ->
                    %% log:log("undefined application but no unittest~n"),
                    ok
            end;
        {ok, App} ->
            log:log("unknown application: ~.0p~n", [App]),
            ok
    end.

%% @doc check if App is allready running.
-spec app_check_running(App::atom()) -> boolean().
app_check_running(App) ->
    Running = application:which_applications(),
    case lists:keyfind(App, 1, Running) of
        false ->
            false;
        _Tuple ->
            true
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% time calculations
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc convert os:timestamp() to microsecs
% See http://erlang.org/pipermail/erlang-questions/2008-December/040368.html
-spec timestamp2us(erlang_timestamp()) -> us_timestamp().
timestamp2us({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

% @doc convert microsecs to os:timestamp()
-spec us2timestamp(us_timestamp()) -> erlang_timestamp().
us2timestamp(Time) ->
    MicroSecs = Time rem 1000000,
    Time2 = (Time - MicroSecs) div 1000000,
    Secs = Time2 rem 1000000,
    MegaSecs = (Time2 - Secs) div 1000000,
    {MegaSecs, Secs, MicroSecs}.

-spec time_plus_us(Time::erlang_timestamp(), Delta_MicroSeconds::non_neg_integer()) -> erlang_timestamp().
time_plus_us({MegaSecs, Secs, MicroSecs}, Delta) ->
    MicroSecs1 = MicroSecs + Delta,
    NewMicroSecs = MicroSecs1 rem 1000000,
    Secs1 = Secs + (MicroSecs1 div 1000000),
    NewSecs = Secs1 rem 1000000,
    MegaSecs1 = MegaSecs + (Secs1 div 1000000),
    NewMegaSecs = MegaSecs1 rem 1000000,
    {NewMegaSecs, NewSecs, NewMicroSecs}.

-spec time_plus_ms(Time::erlang_timestamp(), Delta_MilliSeconds::non_neg_integer()) -> erlang_timestamp().
time_plus_ms(Time, Delta) ->
    time_plus_us(Time, Delta * 1000).

-spec time_plus_s(Time::erlang_timestamp(), Delta_Seconds::non_neg_integer()) -> erlang_timestamp().
time_plus_s({MegaSecs, Secs, MicroSecs}, Delta) ->
    Secs1 = Secs + Delta,
    NewSecs = Secs1 rem 1000000,
    MegaSecs1 = MegaSecs + (Secs1 div 1000000),
    NewMegaSecs = MegaSecs1 rem 1000000,
    {NewMegaSecs, NewSecs, MicroSecs}.

-spec readable_utc_time_feeder({0..1000, 0..1000, 0..1000}) -> {erlang_timestamp()}.
readable_utc_time_feeder({A, B, C}) ->
    {{A, B, C}}.

-spec readable_utc_time(erlang_timestamp()) -> tuple().
readable_utc_time(TimeTriple) ->
    DateTime = calendar:now_to_universal_time(TimeTriple),
    erlang:append_element(DateTime, element(3, TimeTriple)).

%% acc=AccIn, for(i; I<=n; i++) { acc=AccFun(fun(i), acc) }
-spec for_to_fold(integer(), integer(), fun((integer()) -> X),
                  AccFun::fun((X, Acc) -> Acc), AccIn::Acc) -> Acc.
for_to_fold(I, N, Fun, AccFun, AccIn) when I =< N ->
    AccOut = AccFun(Fun(I), AccIn),
    for_to_fold(I + 1, N, Fun, AccFun, AccOut);
for_to_fold(_I, _N, _Fun, _AccFun, AccIn) ->
    AccIn.

%% @doc for(i; i &lt;= n; i++) { fun(i) }
-spec for_to(integer(), integer(), fun((integer()) -> any())) -> ok.
for_to(I, N, Fun) when I =< N ->
    Fun(I),
    for_to(I + 1, N, Fun);
for_to(_I, _N, _Fun) ->
    ok.

%% @doc for(i; i &lt;= n; i++) { Acc = [fun(i) | Acc] }
%%      (equal to <tt>lists:map(Fun, lists:seq(N,I,-1)) ++ Acc</tt>).
-spec for_to_ex(integer(), integer(), fun((integer()) -> T), [T]) -> [T].
for_to_ex(I, N, Fun, Acc) ->
    for_to_fold(I, N, Fun, fun(X, XAcc) -> [X | XAcc] end, Acc).

%% @doc for(i; i &lt;= n; i++) { Acc = [fun(i) | Acc] }
%%      (equal to <tt>lists:map(Fun, lists:seq(N,I,-1))</tt>).
-spec for_to_ex(integer(), integer(), fun((integer()) -> T)) -> [T].
for_to_ex(I, N, Fun) ->
    for_to_ex(I, N, Fun, []).

-spec map_with_nr_feeder(1..2, [number()], integer()) -> {Fun::fun((number(), integer()) -> number()), List::[number()], integer()}.
map_with_nr_feeder(1, List, StartNr) ->
    {fun(X, I) -> X * I end, List, StartNr};
map_with_nr_feeder(2, List, StartNr) ->
    {fun(X, I) -> X + I end, List, StartNr}.

%% @doc Similar to lists:map/2 but also passes the current number to the fun:
%%      <tt>[a, b, c,...]</tt> maps to
%%      <tt>[fun(a, StartNr), fun(b, StartNr+1), fun(c, StartNr+2),...]</tt>
-spec map_with_nr(fun((A, integer()) -> B), List::[A], StartNr::integer()) -> [B]
     when is_subtype(A, any()),
          is_subtype(B, any()).
map_with_nr(F, [H | T], Nr) ->
    [F(H, Nr) | map_with_nr(F, T, Nr + 1)];
map_with_nr(F, [], _Nr) when is_function(F, 2) -> [].

-type try_catch_result() :: ok | throw | error | exit.

%% @doc Helper for par_map/2.
-spec par_map_recv(Id::term(), {try_catch_result(), [B]}) -> {try_catch_result(), [B]}
    when is_subtype(B, any()).
par_map_recv(E, {ErrorX, ListX}) ->
    trace_mpath:thread_yield(),
    receive ?SCALARIS_RECV({parallel_result, E, {ok, ResultY}},
                           {ErrorX, [ResultY | ListX]});
            ?SCALARIS_RECV({parallel_result, E, ErrorY},
                           {ErrorY, ListX})
    end.

-spec par_map_feeder(1..2, [number()]) -> {Fun::fun((number()) -> number()), List::[number()]}.
par_map_feeder(1, List) ->
    {fun(X) -> X * X end, List};
par_map_feeder(2, List) ->
    {fun(X) -> X + X end, List}.

%% @doc Parallel version of lists:map/2. Spawns a new process for each element
%%      in the list!
-spec par_map(Fun::fun((A) -> B), List::[A]) -> [B]
     when is_subtype(A, any()),
          is_subtype(B, any()).
par_map(Fun, [E]) -> [Fun(E)];
par_map(Fun, [_|_] = List) ->
    _ = [erlang:spawn(?MODULE, parallel_run, [self(), Fun, [E], true, E]) || E <- List],
    case lists:foldr(fun par_map_recv/2, {{ok, ok}, []}, List) of
        {{ok, ok}, Result}   -> Result;
        {{Level, Reason}, _} -> erlang:Level(Reason) % throw the error here again
    end;
par_map(Fun, []) when is_function(Fun, 1)-> [].

%% @doc Helper for par_map/3.
-spec par_map_recv2(ListElem::term(), {try_catch_result(), [B], Id::non_neg_integer()})
        -> {try_catch_result(), [B], Id::non_neg_integer()}
     when is_subtype(B, any()).
par_map_recv2(_E, {ErrorX, ListX, Id}) ->
    trace_mpath:thread_yield(),
    receive ?SCALARIS_RECV({parallel_result, Id, {ok, ResultY}},
                           {ErrorX, lists:reverse(ResultY, ListX), Id + 1});
            ?SCALARIS_RECV({parallel_result, Id, ErrorY},
                           {ErrorY, ListX, Id + 1})
    end.

-spec par_map_feeder(1..2, [number()], 1..50) -> {Fun::fun((number()) -> number()), List::[number()], 1..50}.
par_map_feeder(FunNr, List, MaxThreads) ->
    {Fun, List} = par_map_feeder(FunNr, List),
    {Fun, List, MaxThreads}.

%% @doc Parallel version of lists:map/2 with the possibility to limit the
%%      maximum number of processes being spawned.
-spec par_map(Fun::fun((A) -> B), List::[A], MaxThreads::pos_integer()) -> [B]
     when is_subtype(A, any()),
          is_subtype(B, any()).
par_map(Fun, [E], _MaxThreads) -> [Fun(E)];
par_map(Fun, [_|_] = List, 1) -> lists:map(Fun, List);
par_map(Fun, [_|_] = List, MaxThreads) ->
    SplitList = lists_split(List, MaxThreads),
    lists:foldl(
      fun(E, Id) ->
              erlang:spawn(?MODULE, parallel_run,
                           [self(), fun(X) -> lists:map(Fun, X) end, [E], true, Id]),
              Id + 1
      end, 0, SplitList),
    % note: lists are reversed!
    case lists:foldl(fun par_map_recv2/2, {{ok, ok}, [], 0}, SplitList) of
        {{ok, ok}, Result, _}   -> Result;
        {{Level, Reason}, _, _} -> erlang:Level(Reason) % throw the error here again
    end;
par_map(Fun, [], _MaxThreads) when is_function(Fun, 1) -> [].

%% @doc Delete an element from a list (once). When not found, return false.
-spec lists_take(T, [T]) -> [T] | false when is_subtype(T, any()).
lists_take(Elem, L) ->
    lists_take_iter(Elem, L, []).

-spec lists_take_iter(T, [T], [T]) -> [T] | false
   when is_subtype(T, any()).
lists_take_iter(Elem, [Elem|T], Acc) -> lists:reverse(Acc, T);
lists_take_iter(Elem, [H|T], Acc)    -> lists_take_iter(Elem, T, [H|Acc]);
lists_take_iter(_Elem, [], _Acc)     -> false.

-spec lists_takewith_feeder(Elem::T, [T]) -> {fun((T) -> boolean()), [T]}
     when is_subtype(T, any()).
lists_takewith_feeder(Elem, L) ->
    {fun(X) -> X =:= Elem end, L}.

%% @doc Delete an element from a list (once) based on a predicate. When not
%%      found, return false.
-spec lists_takewith(fun((T) -> boolean()), [T]) -> {T, [T]} | false
     when is_subtype(T, any()).
lists_takewith(Pred, L) ->
    lists_takewith_iter(Pred, L, []).

-compile({nowarn_unused_function, {lists_takewith_iter_feeder, 3}}).
-spec lists_takewith_iter_feeder(Elem::T, [T], [T]) -> {fun((T) -> boolean()), [T], [T]}
     when is_subtype(T, any()).
lists_takewith_iter_feeder(Elem, L, Acc) ->
    {fun(X) -> X =:= Elem end, L, Acc}.

-spec lists_takewith_iter(fun((T) -> boolean()), [T], [T]) -> {T, [T]} | false
   when is_subtype(T, any()).
lists_takewith_iter(Pred, [Elem | T], Acc) ->
    case Pred(Elem) of
        true  -> {Elem, lists:reverse(Acc, T)};
        false -> lists_takewith_iter(Pred, T, [Elem | Acc])
    end;
lists_takewith_iter(_Pred, [], _Acc) ->
    false.

%% @doc Splits the given list into several partitions, returning a list of parts
%%      of the original list. Both the parts and their contents are reversed
%%      compared to the original list!
-spec lists_split([A], Partitions::pos_integer()) -> [[A]]
     when is_subtype(A, any()).
lists_split([X], _Partitions) -> [[X]];
lists_split([_|_] = List, 1) -> [lists:reverse(List)];
lists_split([_|_] = List, Partitions) ->
    BlockSize = length(List) div Partitions,
    case BlockSize =< 1 of
        true -> lists:foldl(fun(E, Acc) -> [[E] | Acc] end, [], List);
        _    -> lists_split(List, BlockSize, 0, [], [])
    end;
lists_split([], _Partitions) -> [].

%% @doc Helper for lists_split/2.
-spec lists_split([A], BlockSize::pos_integer(), CurBlockSize::non_neg_integer(), [A], [[A]]) -> [[A]]
     when is_subtype(A, any()).
lists_split([_|_] = List, BlockSize, BlockSize, CurBlock, Result) ->
    lists_split(List, BlockSize, 0, [], [CurBlock | Result]);
lists_split([H | T], BlockSize, CurBlockSize, CurBlock, Result) ->
    lists_split(T, BlockSize, CurBlockSize + 1, [H | CurBlock], Result);
lists_split([], _BlockSize, _CurBlockSize, CurBlock, Result) ->
    [CurBlock | Result].

-spec lists_keystore2(Key::term(), NC::pos_integer(), List::[tuple()],
                      NS::pos_integer(), NewValue::term()) -> [tuple()].
lists_keystore2(Key, NC, [H | T], NS, NewValue) when element(NC, H) == Key ->
    [setelement(NS, H, NewValue) | T];
lists_keystore2(Key, NC, [H | T], NS, NewValue) ->
    [H | lists_keystore2(Key, NC, T, NS, NewValue)];
lists_keystore2(_Key, _N, [], _NS, _NewValue) ->
    [].

-spec lists_partition3_feeder(will_fill_pred, [integer()])
        -> {fun((integer()) -> 1..3), [integer()]}.
lists_partition3_feeder(will_fill_pred, List) ->
    {fun(I) -> abs(I rem 3) + 1 end, List}.

-spec lists_partition3(Pred::fun((Elem :: T) -> 1..3), List::[T])
    -> {Pred1::[T], Pred2::[T], Pred3::[T]}
     when is_subtype(T, any()).
lists_partition3(Pred, L) ->
    lists_partition3(Pred, L, [], [], []).

-compile({nowarn_unused_function, {lists_partition3_feeder, 5}}).
-spec lists_partition3_feeder(will_fill_pred, [integer()],
                              [integer()], [integer()], [integer()])
        -> {fun((integer()) -> 1..3), [integer()], [integer()], [integer()], [integer()]}.
lists_partition3_feeder(will_fill_pred, List, As, Bs, Cs) ->
    {fun(I) -> abs(I rem 3) + 1 end, List, As, Bs, Cs}.

-spec lists_partition3(Pred::fun((Elem :: T) -> 1..3), List::[T],
                      Acc1::[T], Acc2::[T], Acc3::[T])
                     ->  {Pred1::[T], Pred2::[T], Pred3::[T]}
     when is_subtype(T, any()).
lists_partition3(Pred, [H | T], As, Bs, Cs) ->
    case Pred(H) of
        1 -> lists_partition3(Pred, T, [H | As], Bs, Cs);
        2 -> lists_partition3(Pred, T, As, [H | Bs], Cs);
        3 -> lists_partition3(Pred, T, As, Bs, [H | Cs])
    end;
lists_partition3(Pred, [], As, Bs, Cs) when is_function(Pred, 1) ->
    {lists:reverse(As), lists:reverse(Bs), lists:reverse(Cs)}.

-spec lists_remove_at_indices([any(),...], [non_neg_integer(),...]) -> [any()].
lists_remove_at_indices([_|_] = List, [_|_] = Indices) ->
    lists_remove_at_indices(List, Indices, 0).

% PRED: Indices list should be non-empty
-spec lists_remove_at_indices([T], Indices::[non_neg_integer()], non_neg_integer())
        -> [T] when is_subtype(T, any()).
lists_remove_at_indices(List, [], _CurrentIndex) ->
    List;
lists_remove_at_indices([_|ListTail], [CurrentIndex|IndexTail], CurrentIndex) ->
    lists_remove_at_indices(ListTail, IndexTail, CurrentIndex + 1);
lists_remove_at_indices([X|L], Indices, CurrentIndex) ->
    [X | lists_remove_at_indices(L, Indices, CurrentIndex + 1)].

%% @doc A more flexible sublist function than lists:sublist/3.
%%      Extracts a sublist of length Length starting at Start.
%%      If Start is negative, we count from the end, e.g. -1 is the last
%%      element, -2 the second last.
%%      If Length is negative, the sublist is created in reversed
%%      direction, e.g. sublist([a,b,c], -1, -2) gets [c, b].
%%      If Start is less than -ListLength and Length is non-negative, it will be
%%      set to 1. If Length is negative in this case, an empty sublist will be
%%      returned.
%%      If Start is greater than ListLength and Length is non-negative, an empty
%%      sublist will be returned. If Length is negative in this case, it will
%%      be set to ListLength.
%%      Note: sublists never wrap between start and end, i.e.
%%      sublist([a,b,c], 1, -2) gets []!
%%      Examples:
%%       * first 10: sublist(L, 1, 10) | sublist(L, 10, -10) (reverse order)
%%       * last 10 : sublist(L, -10, 10) | sublist(L, -1, -10) (reverse order)
-spec sublist(List::[X,...], Start::pos_integer() | neg_integer(), Length::integer()) -> {[X], Length::non_neg_integer()}
     when is_subtype(X, any()).
sublist(List, Start, Length) ->
    ListLen = length(List),
    NewStart = if Start > ListLen andalso Length >= 0  -> 0;
                  Start > ListLen andalso Length < 0   -> ListLen;
                  Start >= 1                           -> Start;
                  Start < -ListLen andalso Length >= 0 -> 1;
                  Start < -ListLen andalso Length < 0  -> 0;
                  Start =< -1                          -> ListLen + Start + 1
               end,
    sublist_(List, ListLen, NewStart, Length).

-compile({nowarn_unused_function, {sublist__feeder, 4}}).
-spec sublist__feeder(List::[X,...], ListLength::non_neg_integer(), Start::non_neg_integer(), Length::integer())
        -> {List::[X,...], ListLength::non_neg_integer(), Start::non_neg_integer(), Length::integer()}
     when is_subtype(X, any()).
sublist__feeder(List, _, Start, Length) ->
    ListLength = length(List),
    {List, ListLength, erlang:min(Start, ListLength), Length}.

%% @doc Helper for sublist/3.
%%      Pre: ListLength =:= length(List), 0 =&lt; Start =&lt; ListLength
-spec sublist_(List::[X,...], ListLength::non_neg_integer(), Start::non_neg_integer(), Length::integer()) -> {[X], Length::non_neg_integer()}
     when is_subtype(X, any()).
sublist_(_List, ListLength, 0, _Length) ->
    {[], ListLength};
sublist_(List, ListLength, Start, Length) when Length >= 0 ->
    {lists:sublist(List, Start, Length), ListLength};
sublist_(List, ListLength, Start, Length) when Length < 0 ->
    RevList = lists:reverse(List),
    NewStart = ListLength - Start + 1, % note: reverse order!
    {lists:sublist(RevList, NewStart, -Length), ListLength}.

%% @doc If Element is in List, its index is returned (1..length(List) as in lists:nth/2),
%%      otherwise 'not_found'.
-spec lists_index_of(Element::T, List::[T]) -> pos_integer() | not_found
     when is_subtype(T, any()).
lists_index_of(Element, List) when is_list(List) ->
    lists_index_of_(Element, List, 1).

%% @doc Helper for lists_index_of/2
-spec lists_index_of_(Element::T, List::[T], pos_integer()) -> pos_integer() | not_found
     when is_subtype(T, any()).
lists_index_of_(_E, [], _N)      -> not_found;
lists_index_of_(E, [E | _TL], N) -> N;
lists_index_of_(E, [_H | TL], N) -> lists_index_of_(E, TL, N + 1).

%% @doc Checks whether the given list is at least the given length long without
%%      going through the whole list like erlang:length/1.
-spec lists_check_min_length(List::[any()], non_neg_integer()) -> boolean().
lists_check_min_length([], I) when I > 0 -> false;
lists_check_min_length(_, 0) -> true;
lists_check_min_length([_|T], I) ->
    lists_check_min_length(T, I - 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% repeat
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Sequencial (default) or parallel run of function FUN with arguments ARGS TIMES-fold.
%%      Options as list/propertylist: collect, parallel, accumulate
%%          * collect (atom): all results of FUN will returned as a list
%%          * accumulate (tuple): {accumulate, accFun, accInit}
%%                                all results will be accumulated with accFun
%%          * parallel (atom): FUN will be called TIMES-fold in parallel.
%%                             Combination with collect and accumulate is supported.
%% @end
-spec repeat(fun(), args(), pos_integer()) -> ok.
repeat(Fun, Args, Times) ->
    NoAccFun = fun(_, _) -> ok end,
    i_repeat(Fun, Args, Times, NoAccFun, ok).
-spec repeat(fun(), args(), pos_integer(), [repeat_params()]) -> ok | any().
repeat(Fun, Args, Times, Params) ->
    NoAccFun = fun(_, _) -> ok end,
    case lists:member(collect, Params) of
        true ->
            AccFun = fun(I, R) -> [I | R] end,
            AccInit = [],
            ok;
        false ->
            case lists:keyfind(accumulate, 1, Params) of
                false ->
                    AccFun = NoAccFun,
                    AccInit = ok;
                {accumulate, AccFun, AccInit} ->
                    ok
            end
    end,
    case lists:member(parallel, Params) of
        true ->
            repeat(fun spawn/3, [?MODULE, parallel_run,
                                 [self(), Fun, Args, AccFun =/= NoAccFun, ok]],
                   Times),
            case AccFun of
                NoAccFun -> ok;
                _ -> parallel_collect(Times, AccFun, AccInit)
            end;
        _ -> i_repeat(Fun, Args, Times, AccFun, AccInit)
    end.

-spec i_repeat(fun(), args(), non_neg_integer(), accumulatorFun(any(), R), R) -> R.
i_repeat(_, _, 0, _AccFun, Acc) -> Acc;
i_repeat(Fun, Args, Times, AccFun, Acc) ->
    R = apply(Fun, Args),
    i_repeat(Fun, Args, Times - 1, AccFun, AccFun(R, Acc)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% parallel repeat helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec parallel_run(pid(), fun(), args(), boolean(), Id::any()) -> ok.
parallel_run(SrcPid, Fun, Args, DoAnswer, Id) ->
    Res = try {ok, apply(Fun, Args)}
          catch Level:Reason -> {Level, Reason}
          end,
    case DoAnswer of
        true -> comm:send_local(SrcPid, {parallel_result, Id, Res});
        _ -> ok
    end,
    ok.

-spec parallel_collect(non_neg_integer(), accumulatorFun(any(), U), U) -> U.
parallel_collect(0, _, Accumulator) ->
    Accumulator;
parallel_collect(ExpectedResults, AccuFun, Accumulator) ->
    trace_mpath:thread_yield(),
    receive ?SCALARIS_RECV({parallel_result, ok, {ok, Result}}, ok);
            ?SCALARIS_RECV({parallel_result, ok, Result}, ok) % TODO: throw the error here again?
    end,
    parallel_collect(ExpectedResults - 1, AccuFun, AccuFun(Result, Accumulator)).

-spec collect_while(GatherFun::fun((non_neg_integer()) -> {boolean(), T} | boolean())) -> [T].
collect_while(GatherFun) ->
    collect_while(GatherFun, 0).

-spec collect_while(GatherFun::fun((non_neg_integer()) -> {boolean(), T} | boolean()), non_neg_integer()) -> [T].
collect_while(GatherFun, Count) ->
    case GatherFun(Count) of
        {true, Data}  -> [Data | GatherFun(Count + 1)];
        {false, Data} -> [Data];
        true          -> GatherFun(Count + 1);
        false         -> []
    end.

-spec list_set_nth([A], pos_integer(), B) -> [A | B]
     when is_subtype(A, any()),
          is_subtype(B, any()).
list_set_nth(L, Pos, Val) ->
    list_set_nth(L, Pos, Val, 1).

-spec list_set_nth([A], pos_integer(), B, pos_integer()) -> [A | B]
     when is_subtype(A, any()),
          is_subtype(B, any()).
list_set_nth([_H | T], Pos, Val, Pos) ->
    [Val | T];
list_set_nth([H | T], Pos, Val, Cur) ->
    [H | list_set_nth(T, Pos, Val, Cur + 1)];
list_set_nth([], _Pos, _Val, _Cur) -> [].

-spec debug_info() -> [[{string(), string() | number()}]].
debug_info() ->
    [ [ debug_info(Y) || Y <- pid_groups:members(X)] || X <- pid_groups:groups()].

-spec debug_info(pid()) -> [{string(), term()}];
                (atom()) -> [[{string(), term()}]].
debug_info(PidName) when is_atom(PidName) ->
    [ debug_info(X) || X <- pid_groups:find_all(PidName)];
debug_info(Pid) when is_pid(Pid) ->
    {GenCompDesc, GenCompInfo} =
        case gen_component:is_gen_component(Pid) of
            true ->
                {Grp, Name} = pid_groups:group_and_name_of(Pid),
                comm:send_local(Pid , {web_debug_info, self()}),
                trace_mpath:thread_yield(),
                receive
                    ?SCALARIS_RECV({web_debug_info_reply, LocalKVs}, %% ->
                                   {[{"pidgroup", pid_groups:group_to_string(Grp)},
                                     {"pidname", webhelpers:safe_html_string("~p", [Name])}],
                                    LocalKVs})
                after 1000 -> {[], []}
                end;
            false -> {[], []}
        end,
    [{_, Memory}, {_, Reductions}, {_, QueueLen}] =
        process_info(Pid, [memory, reductions, message_queue_len]),
    [{"pid", pid_to_list(Pid)}]
        ++   GenCompDesc
        ++ [{"memory", Memory},
            {"reductions", Reductions},
            {"message_queue_len", QueueLen}]
        ++ GenCompInfo.

%% empty shell_prompt_func
-spec empty(any()) -> [].
empty(_) -> "".

-spec print_bits(fun((string(), [term()]) -> Result), binary()) -> Result.
print_bits(FormatFun, Binary) ->
    BitSize = erlang:bit_size(Binary),
    <<BinNr:BitSize>> = Binary,
    NrBits = lists:flatten(io_lib:format("~B", [BitSize])),
    FormatFun("~" ++ NrBits ++ ".2B", [BinNr]).

-spec if_verbose(string()) -> ok.
if_verbose(String) ->
    case app_get_env(verbose, false) of
        true ->  io:format(String);
        false -> ok
    end.

-spec if_verbose(string(), list()) -> ok.
if_verbose(String, Fmt) ->
    case app_get_env(verbose, false) of
        true ->  io:format(String, Fmt);
        false -> ok
    end.

%% @doc Binary XOR for the two bitstrings, even for big bitstrings where the
%%      conversion to an integer fails.
%%      Note: 0's are appended if the sizes do not match.
-spec bin_xor(bitstring(), bitstring()) -> bitstring().
bin_xor(Binary1, Binary2) ->
    bin_op(Binary1, Binary2, fun erlang:'bxor'/2).

%% @doc Binary OR for the two bitstrings, even for big bitstrings where the
%%      conversion to an integer fails.
%%      Note: 0's are appended if the sizes do not match.
-spec bin_or(bitstring(), bitstring()) -> bitstring().
bin_or(Binary1, Binary2) ->
    bin_op(Binary1, Binary2, fun erlang:'bor'/2).

%% @doc Binary AND for the two bitstrings, even for big bitstrings where the
%%      conversion to an integer fails.
%%      Note: 0's are appended if the sizes do not match.
-spec bin_and(bitstring(), bitstring()) -> bitstring().
bin_and(Binary1, Binary2) ->
    bin_op(Binary1, Binary2, fun erlang:'band'/2).

%% @doc Generic binary operations for the two bitstrings, even for big
%%      bitstrings where the conversion to an integer fails.
%%      Note: 0's are appended if the sizes do not match.
-spec bin_op(bitstring(), bitstring(), fun((integer(), integer()) -> integer()))
        -> bitstring().
bin_op(Binary1, Binary2, BinOp) ->
    BitSize1 = erlang:bit_size(Binary1),
    BitSize2 = erlang:bit_size(Binary2),
    ResSize = erlang:max(BitSize1, BitSize2),
    % up to (at least) Erlang 18.3, there is an upper limit of converting
    % binaries to integers or if this works the following bxor/2 will fail
    if ResSize =< 16#1FFFFC0 ->
           <<BinNr1:BitSize1/little>> = Binary1,
           <<BinNr2:BitSize2/little>> = Binary2,
           ResNr = BinOp(BinNr1, BinNr2),
           <<ResNr:ResSize/little>>;
       BitSize1 =:= BitSize2 ->
           % split the binary and bxor each part
           RestSize = BitSize1 rem 16#1FFFFC0,
           <<BinNr1:RestSize/little, Bin1TL/binary>> = Binary1,
           <<BinNr2:RestSize/little, Bin2TL/binary>> = Binary2,
           ResNr = BinOp(BinNr1, BinNr2),
           bin_op(Bin1TL, Bin2TL, BinOp, <<ResNr:RestSize/little>>);
       true ->
           % first bring the binaries to the same size, then try again:
           Bin1Large = <<Binary1/bitstring, 0:(ResSize - BitSize1)/little>>,
           Bin2Large = <<Binary2/bitstring, 0:(ResSize - BitSize2)/little>>,
           bin_op(Bin1Large, Bin2Large, BinOp)
    end.

%% @doc Helper for bin_op/3.
%%      Note: We cannot use erlang:list_to_binary/1 either since that suffers
%%            from the same problem with big binaries.
-spec bin_op(binary(), binary(), fun((integer(), integer()) -> integer()),
              ResultAcc::bitstring()) -> bitstring().
bin_op(<<>>, <<>>, _BinOp, Acc) ->
    Acc;
bin_op(Binary1, Binary2, BinOp, Acc) ->
    <<BinNr1:16#1FFFFC0/little, Bin1TL/binary>> = Binary1,
    <<BinNr2:16#1FFFFC0/little, Bin2TL/binary>> = Binary2,
    ResNr = BinOp(BinNr1, BinNr2),
    bin_op(Bin1TL, Bin2TL, BinOp, <<Acc/bitstring, ResNr:16#1FFFFC0/little>>).

-ifdef(enable_debug).
-spec extint2atom(atom()) -> atom().
extint2atom(X) when is_atom(X) -> X.
-else.
-spec extint2atom(atom() | integer()) -> atom().
extint2atom(X) when is_atom(X) -> X;
extint2atom(X) when is_integer(X) ->
    case X of
        %% lookup
        ?lookup_aux -> ?lookup_aux_atom;
        ?lookup_fin -> ?lookup_fin_atom;
        %% comm
        ?send_to_group_member -> ?send_to_group_member_atom;
        ?send_to_registered_proc -> ?send_to_registered_proc_atom;
        ?deliver -> ?deliver_atom;
        ?unpack_msg_bundle -> ?unpack_msg_bundle_atom;
        ?quiet -> ?quiet_atom;
        %% dht_node
        ?get_key_with_id_reply -> ?get_key_with_id_reply_atom;
        ?get_key -> ?get_key_atom;
        ?read_op -> ?read_op_atom;
        ?read_op_with_id_reply -> ?read_op_with_id_reply_atom;
        %% paxos
        ?proposer_accept -> ?proposer_accept_atom;
        ?acceptor_accept -> ?acceptor_accept_atom;
        ?paxos_id -> ?paxos_id_atom;
        ?proposer_initialize -> ?proposer_initialize_atom;
        ?proposer_deleteids -> ?proposer_deleteids_atom;
        %% transactions
        ?register_TP -> ?register_TP_atom;
        ?tx_tm_rtm_init_RTM -> ?tx_tm_rtm_init_RTM_atom;
        ?tp_do_commit_abort -> ?tp_do_commit_abort_atom;
        ?tx_tm_rtm_delete -> ?tx_tm_rtm_delete_atom;
        ?tp_committed -> ?tp_committed_atom;
        ?tx_state -> ?tx_state_atom;
        ?tx_id -> ?tx_id_atom;
        ?tx_item_id -> ?tx_item_id_atom;
        ?tx_item_state -> ?tx_item_state_atom;
        ?commit_client_id -> ?commit_client_id_atom;
        ?undecided -> ?undecided_atom;
        ?prepared -> ?prepared_atom;
        ?commit -> ?commit_atom;
        ?abort -> ?abort_atom;
        ?value -> ?value_atom;
        ?partial_value -> ?partial_value_atom;
        ?read -> ?read_atom;
        ?write -> ?write_atom;
        ?init_TP -> ?init_TP_atom;
        ?tp_do_commit_abort_fwd -> ?tp_do_commit_abort_fwd_atom;
        ?random_from_list -> ?random_from_list_atom;
        ?sublist -> ?sublist_atom;
        ?ok -> ?ok_atom;
        ?fail -> ?fail_atom;
        ?not_found -> ?not_found_atom;
        ?empty_list -> ?empty_list_atom;
        ?not_a_list -> ?not_a_list_atom;
        %% rrepair
        ?check_nodes -> ?check_nodes_atom;
        ?check_nodes_response -> ?check_nodes_response_atom;
        ?key_upd -> ?key_upd_atom
    end.
-endif.

-spec sets_map(Fun::fun((V) -> X), Set::sets:set(V)) -> [X]
     when is_subtype(V, any()),
          is_subtype(X, any()).
sets_map(Fun, Set) ->
    lists:reverse(sets:fold(fun (El, Acc) ->
                [Fun(El) | Acc]
        end, [], Set)).

%% @doc Compare two sets for equality.
-spec sets_equal(sets:set(V), sets:set(V)) -> boolean()
     when is_subtype(V, any()).
sets_equal(Set1, Set2) ->
    sets:is_subset(Set1, Set2) andalso sets:is_subset(Set2, Set1).

%% @doc Combine the last N slots from a dump into one tuple. The number of slots to
%% combine is determined by Interval (in us): Take as many slots as needed to look
%% Interval-Epsilon microseconds back into the past.

-spec rrd_combine_timing_slots(DB :: rrd:rrd(),
                               CurrentTS :: erlang_timestamp(),
                               Interval :: non_neg_integer()) ->
    {Sum :: number(), SquaresSum :: number(), Count :: non_neg_integer(),
     Min :: number(), Max :: number()} | undefined.
rrd_combine_timing_slots(DB, CurrentTS, Interval) ->
    rrd_combine_timing_slots(DB, CurrentTS, Interval, 0). % Epsilon = 0ms

-spec rrd_combine_timing_slots(DB :: rrd:rrd(),
                               CurrentTS :: erlang_timestamp(),
                               Interval :: non_neg_integer(),
                               Epsilon :: non_neg_integer()) ->
    {Sum :: number(), SquaresSum :: number(), Count :: non_neg_integer(),
     Min :: number(), Max :: number()} | undefined.
rrd_combine_timing_slots(DB, CurrentTS, Interval, Epsilon) ->
    InitialValue = fun({SlotSum,SlotSquared,SlotCount,SlotMin,SlotMax,_}) ->
            {SlotSum, SlotSquared, SlotCount, SlotMin, SlotMax }
    end,
    UpdateValue = fun({Sum, SquaresSum, Count, Min, Max} = _Acc,
                      {SlotSum,SlotSquared,SlotCount,SlotMin,SlotMax,_}) ->
            { Sum+SlotSum
             , SquaresSum + SlotSquared
             , Count + SlotCount
             , erlang:min(Min, SlotMin)
             , erlang:max(Max, SlotMax)
            }
    end,
    rrd_combine_slots(DB, CurrentTS, Interval, Epsilon, InitialValue, UpdateValue).

%% @doc This function does the same as rrd_combine_timing_slots, but for RRDs of type
%% gauge. It sums up slots until enough slots have been read.
-spec rrd_combine_gauge_slots(DB :: rrd:rrd(),
                               CurrentTS :: erlang_timestamp(),
                               Interval :: non_neg_integer()) -> Value :: number() | undefined.
rrd_combine_gauge_slots(DB, CurrentTS, Interval) ->
    rrd_combine_gauge_slots(DB, CurrentTS, Interval, 0). % Epsilon = 0ms

-spec rrd_combine_gauge_slots(DB :: rrd:rrd(),
                               CurrentTS :: erlang_timestamp(),
                               Interval :: non_neg_integer(),
                               Epsilon :: non_neg_integer()) -> Value :: number() | undefined.
rrd_combine_gauge_slots(DB, CurrentTS, Interval, Epsilon) ->
    InitialValue = fun(Value) -> Value end,
    UpdateValue = fun(AccValue, CurrentValue) -> AccValue + CurrentValue end,
    rrd_combine_slots(DB, CurrentTS, Interval, Epsilon, InitialValue, UpdateValue).

%% @doc Accumulates data over RRD slots. This function allows to reduce a number of slots
%% into a single accumulator value. The amount of slots is determined by Interval and
%% Epsilon: it reads as many slots as needed to cover the last Interval - Epsilon us.
-spec rrd_combine_slots(DB :: rrd:rrd(),
                        CurrentTS :: erlang_timestamp(),
                        Interval :: non_neg_integer(),
                        Epsilon :: non_neg_integer(),
                        InitialValue :: fun((rrd:data_type()) -> Acc),
                        UpdateValue :: fun((Acc, rrd:data_type()) -> Acc)
                       ) -> term() | undefined.
rrd_combine_slots(DB, CurrentTS, Interval, Epsilon, InitialValue, UpdateValue) ->
    Slots = rrd:dump(DB),
    CalcStepLength = fun(Current, From, To) ->
            case timer:now_diff(Current,From) >= 0
                andalso timer:now_diff(To, Current) >= 0 of
                true  -> timer:now_diff(Current, From);
                false -> timer:now_diff(To, From)
            end
    end,
    Acc = lists:foldl(
            fun
                (_, {RemainingUS, _} = Acc) when (RemainingUS - Epsilon) =< 0 -> Acc;
                ({From, To, Value}, {RemainingUS, Acc}) ->
                    StepLength = CalcStepLength(CurrentTS, From, To),
                    {RemainingUS - StepLength, UpdateValue(Acc, Value)};
                ({From, To, Value}, {RemainingUS}) ->
                    StepLength = CalcStepLength(CurrentTS, From, To),
                    {RemainingUS - StepLength, InitialValue(Value)}
                end, {Interval}, Slots),
        case Acc of
            {_, Results} -> Results;
            {Interval} -> undefined
        end.

%% @doc Rounds a float according to some precision
-spec round(Number::float(), Precision::pos_integer()) -> float().
round(Number, Precision) ->
    P = pow(10, Precision),
    erlang:round(Number * P) / P.
