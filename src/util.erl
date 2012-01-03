% @copyright 2007-2011 Zuse Institute Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Utility Functions.
%%% @end
%% @version $Id$
-module(util).

-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([global_uid/0, time/0, time_utc/0]).
-endif.
-export([escape_quotes/1,
         min/2, max/2, log/2, log2/1, ceil/1, floor/1,
         logged_exec/1,
         randomelem/1, pop_randomelem/1, pop_randomelem/2,
         first_matching/2,
         get_stacktrace/0, get_linetrace/0, get_linetrace/1,
         dump/0, dump2/0, dump3/0,
         minus_all/2, minus_first/2,
         sleep_for_ever/0, shuffle/1, get_proc_in_vms/1,random_subset/2,
         gb_trees_largest_smaller_than/2, gb_trees_foldl/3, pow/2,
         zipfoldl/5, safe_split/2, '=:<'/2,
         split_unique/2, split_unique/3, split_unique/4,
         ssplit_unique/2, ssplit_unique/3, ssplit_unique/4,
         smerge2/2, smerge2/3, smerge2/4,
         is_unittest/0, make_filename/1,
         app_get_env/2,
         time_plus_s/2, time_plus_ms/2, time_plus_us/2,
         for_to/3, for_to_ex/3,
         collect_while/1]).
-export([sup_worker_desc/3,
         sup_worker_desc/4,
         sup_supervisor_desc/3,
         sup_supervisor_desc/4,
         tc/3, tc/2, tc/1]).
-export([supervisor_terminate/1,
         supervisor_terminate_childs/1,
         wait_for/1, wait_for/2,
         wait_for_process_to_die/1,
         wait_for_table_to_disappear/1,
         ets_tables_of/1]).

-export([get_pids_uid/0, get_global_uid/0, is_my_old_uid/1]).
-export([s_repeat/3, s_repeatAndCollect/3, s_repeatAndAccumulate/5,
         p_repeat/3, p_repeatAndCollect/3, p_repeatAndAccumulate/5,
         parallel_run/4]).
-export([proplist_get_value/2, proplist_get_value/3]).

-export([empty/1]).

-opaque global_uid() :: {pos_integer(), comm:mypid()}.

-type time() :: {MegaSecs::non_neg_integer(),
                 Secs::non_neg_integer(),
                 MicroSecs::non_neg_integer()}.

-type time_utc() :: {{1970..10000, 1..12, 1..31}, {0..23, 0..59, 0..59}}.

-type args() :: [term()].
-type accumulatorFun(T, U) :: fun((T, U) -> U).
%-type anyFun(T) :: fun((...) -> T). %will not work with R14B02 dialyzer
-type anyFun(T) :: 
    fun((any()) -> T) | 
    fun((any(), any()) -> T).


%% @doc Creates a worker description for a supervisor.
-spec sup_worker_desc(Name::atom() | string(), Module::module(), Function::atom())
        -> {Name::atom() | string(), {Module::module(), Function::atom(), Options::[]},
            permanent, brutal_kill, worker, []}.
sup_worker_desc(Name, Module, Function) ->
    sup_worker_desc(Name, Module, Function, []).

%% @doc Creates a worker description for a supervisor.
-spec sup_worker_desc(Name::atom() | string(), Module::module(), Function::atom(), Options::list())
        -> {Name::atom() | string(), {Module::module(), Function::atom(), Options::list()},
            permanent, brutal_kill, worker, []}.
sup_worker_desc(Name, Module, Function, Options) ->
    {Name, {Module, Function, Options}, permanent, brutal_kill, worker, []}.

%% @doc Creates a supervisor description for a supervisor.
-spec sup_supervisor_desc(Name::atom() | string(), Module::module(), Function::atom())
        -> {Name::atom() | string(), {Module::module(), Function::atom(), Options::[]},
            permanent, brutal_kill, supervisor, []}.
sup_supervisor_desc(Name, Module, Function) ->
    sup_supervisor_desc(Name, Module, Function, []).

%% @doc Creates a supervisor description for a supervisor.
-spec sup_supervisor_desc(Name::atom() | string(), Module::module(), Function::atom(), Options::list())
        -> {Name::atom() | string(), {Module::module(), Function::atom(), Options::list()},
            permanent, brutal_kill, supervisor, []}.
sup_supervisor_desc(Name, Module, Function, Args) ->
    {Name, {Module, Function, Args}, permanent, brutal_kill, supervisor, []}.


-spec supervisor_terminate(Supervisor::pid() | atom()) -> ok.
supervisor_terminate(SupPid) ->
    supervisor_terminate_childs(SupPid),
    case is_pid(SupPid) of
        true -> exit(SupPid, kill);
        false -> exit(whereis(SupPid), kill)
    end,
    wait_for_process_to_die(SupPid),
    ok.

-spec supervisor_terminate_childs(Supervisor::pid() | atom()) -> ok.
supervisor_terminate_childs(SupPid) ->
    ChildSpecs = supervisor:which_children(SupPid),
    _ = [ begin
              case Type of
                  supervisor ->
                      supervisor_terminate_childs(Pid);
                  _ -> ok
              end,
              Tables = ets_tables_of(Pid),
              _ = supervisor:terminate_child(SupPid, Id),
              wait_for_process_to_die(Pid),
              _ = [ wait_for_table_to_disappear(Tab) || Tab <- Tables ],
              supervisor:delete_child(SupPid, Id)
          end ||  {Id, Pid, Type, _Module} <- ChildSpecs,
                  Pid =/= undefined ],
    ok.

-spec wait_for(fun(() -> any())) -> ok.
wait_for(F) -> wait_for(F, 10).

-spec wait_for(fun(() -> any()), WaitTimeInMs::pos_integer()) -> ok.
wait_for(F, WaitTime) ->
    case F() of
        true  -> ok;
        false -> timer:sleep(WaitTime),
                 wait_for(F)
    end.

-spec wait_for_process_to_die(pid() | atom()) -> ok.
wait_for_process_to_die(Name) when is_atom(Name) ->
    wait_for(fun() ->
                     case erlang:whereis(Name) of
                         undefined -> true;
                         Pid       -> not is_process_alive(Pid)
                     end
             end);
wait_for_process_to_die(Pid) ->
    wait_for(fun() -> not is_process_alive(Pid) end).

-spec wait_for_table_to_disappear(tid() | atom()) -> ok.
wait_for_table_to_disappear(Table) ->
    wait_for(fun() -> ets:info(Table) =:= undefined end).

-spec ets_tables_of(pid()) -> list().
ets_tables_of(Pid) ->
    Tabs = ets:all(),
    [ Tab || Tab <- Tabs, ets:info(Tab, owner) =:= Pid ].

%% @doc Escapes quotes in the given string.
-spec escape_quotes(String::string()) -> string().
escape_quotes(String) ->
    lists:foldr(fun escape_quotes_/2, [], String).

-spec escape_quotes_(String::string(), Rest::string()) -> string().
escape_quotes_($", Rest) -> [$\\, $" | Rest];
escape_quotes_(Ch, Rest) -> [Ch | Rest].

%% @doc Variant of erlang:max/2 also taking ?PLUS_INFINITY_TYPE and
%%      ?MINUS_INFINITY_TYPE into account, e.g. for comparing keys.
-spec max(?PLUS_INFINITY_TYPE, any()) -> ?PLUS_INFINITY_TYPE;
         (any(), ?PLUS_INFINITY_TYPE) -> ?PLUS_INFINITY_TYPE;
         (T | ?MINUS_INFINITY_TYPE, T | ?MINUS_INFINITY_TYPE) -> T.
max(?PLUS_INFINITY, _) -> ?PLUS_INFINITY;
max(_, ?PLUS_INFINITY) -> ?PLUS_INFINITY;
max(?MINUS_INFINITY, X) -> X;
max(X, ?MINUS_INFINITY) -> X;
max(A, B) ->
    case A > B of
        true -> A;
        false -> B
    end.

%% @doc Variant of erlang:min/2 also taking ?PLUS_INFINITY_TYPE and
%%      ?MINUS_INFINITY_TYPE into account, e.g. for comparing keys.
-spec min(?MINUS_INFINITY_TYPE, any()) -> ?MINUS_INFINITY_TYPE;
         (any(), ?MINUS_INFINITY_TYPE) -> ?MINUS_INFINITY_TYPE;
         (T | ?PLUS_INFINITY_TYPE, T | ?PLUS_INFINITY_TYPE) -> T.
min(?MINUS_INFINITY, _) -> ?MINUS_INFINITY;
min(_, ?MINUS_INFINITY) -> ?MINUS_INFINITY;
min(?PLUS_INFINITY, X) -> X;
min(X, ?PLUS_INFINITY) -> X;
min(A, B) ->
    case A < B of
        true -> A;
        false -> B
    end.

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
pow(X, Y) ->
    case Y rem 2 of
        0 ->
            Half = pow(X, Y div 2),
            Half * Half;
        1 ->
            Half = pow(X, Y div 2),
            Half * Half * X
    end.

%% @doc Logarithm of X to the base of Base.
-spec log(X::number(), Base::number()) -> float().
log(X, B) -> math:log10(X) / math:log10(B).

%% @doc Logarithm of X to the base of 2.
-spec log2(X::number()) -> float().
log2(X) -> log(X, 2).

%% @doc Returns the largest integer not larger than X. 
-spec floor(X::number()) -> integer().
floor(X) when X >= 0 ->
    erlang:trunc(X);
floor(X) ->
    T = erlang:trunc(X),
    case T == X of
        true -> T;
        _    -> T - 1
    end.

%% @doc Returns the smallest integer not smaller than X. 
-spec ceil(X::number()) -> integer().
ceil(X) when X < 0 ->
    erlang:trunc(X);
ceil(X) ->
    T = erlang:trunc(X),
    case T == X of
        true -> T;
        _    -> T + 1
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
        [{util, get_stacktrace, 0} | ST] -> ok; % erlang < R15
        [{util, get_stacktrace, 0, _} | ST] -> ok; % erlang >= R15
        ST -> ST % just in case
    end,
    ST.

-spec get_linetrace() -> term() | undefined.
get_linetrace() ->
    erlang:get(test_server_loc).

-spec get_linetrace(Pid::pid()) -> term() | undefined.
get_linetrace(Pid) ->
    {dictionary, Dict} = erlang:process_info(Pid, dictionary),
    dump_extract_from_list_may_not_exist(Dict, test_server_loc).

%% @doc Extracts a given ItemInfo from an ItemList that has been returned from
%%      e.g. erlang:process_info/2 for the dump* methods.
-spec dump_extract_from_list
        ([{Item::atom(), Info::term()}], ItemInfo::memory | message_queue_len | stack_size | heap_size) -> non_neg_integer();
        ([{Item::atom(), Info::term()}], ItemInfo::messages) -> [tuple()];
        ([{Item::atom(), Info::term()}], ItemInfo::current_function) -> Fun::mfa();
        ([{Item::atom(), Info::term()}], ItemInfo::dictionary) -> [{Key::term(), Value::term()}].
dump_extract_from_list(List, Key) ->
    element(2, lists:keyfind(Key, 1, List)).

%% @doc Extracts a given ItemInfo from an ItemList or returns 'undefined' if
%%      there is no such item.
-spec dump_extract_from_list_may_not_exist
        ([{Item::term(), Info}], ItemInfo::term()) -> Info | undefined.
dump_extract_from_list_may_not_exist(List, Key) ->
    case lists:keyfind(Key, 1, List) of
        false -> undefined;
        X     -> element(2, X)
    end.

%% @doc Returns a list of all currently executed functions and the number of
%%      instances for each of them.
-spec dump() -> [{Fun::mfa(), FunExecCount::pos_integer()}].
dump() ->
    Info = [element(2, Fun) || X <- processes(),
                               Fun <- [process_info(X, current_function)],
                               Fun =/= undefined],
    FunCnt = dict:to_list(lists:foldl(fun(Fun, DictIn) ->
                                              dict:update_counter(Fun, 1, DictIn)
                                      end, dict:new(), Info)),
    lists:reverse(lists:keysort(2, FunCnt)).

%% @doc Returns information about all processes' memory usage.
-spec dump2() -> [{PID::pid(), Mem::non_neg_integer(), Fun::mfa()}].
dump2() ->
    dumpX([memory, current_function, dictionary],
          fun(K, Value) ->
                  case K of
                      dictionary -> dump_extract_from_list_may_not_exist(Value, test_server_loc);
                      _          -> Value
                  end
          end).

%% @doc Returns various data about all processes.
-spec dump3() -> [{PID::pid(), Mem::non_neg_integer(), MsgQLength::non_neg_integer(),
                   StackSize::non_neg_integer(), HeapSize::non_neg_integer(),
                   Messages::[atom()], Fun::mfa()}].
dump3() ->
    dumpX([memory, message_queue_len, stack_size, heap_size, messages, current_function],
          fun(K, Value) ->
                  case K of
                      messages -> [element(1, V) || V <- Value];
                      _        -> Value
                  end
          end).

%% @doc Returns various data about all processes.
-spec dumpX([ItemInfo::atom(),...], ValueFun::fun((atom(), term()) -> term())) -> [tuple(),...].
dumpX(Keys, ValueFun) ->
    Info = 
        [begin
             Values =
                 [ValueFun(Key, dump_extract_from_list(Data, Key)) || Key <- Keys],
             erlang:list_to_tuple([Pid, Values])
         end || Pid <- processes(),
                Data <- [process_info(Pid, Keys)],
                Data =/= undefined],
    lists:reverse(lists:keysort(2, Info)).

%% @doc minus_all(M,N) : { x | x in M and x notin N}
-spec minus_all(List::[T], Excluded::[T]) -> [T].
minus_all([], _ExcludeList) ->
    [];
minus_all([_|_] = L, [Excluded]) ->
    [E || E <- L, E =/= Excluded];
minus_all([_|_] = L, ExcludeList) ->
    ExcludeSet = sets:from_list(ExcludeList),
    [E || E <- L, not sets:is_element(E, ExcludeSet)].

%% @doc Deletes the first occurrence of each element in Excluded from List.
%%      Similar to lists:foldl(fun lists:delete/2, NewValue1, ToDel) but more
%%      performant for out case.
-spec minus_first(List::[T], Excluded::[T]) -> [T].
minus_first([], _ExcludeList) ->
    [];
minus_first([_|_] = L, [Excluded]) ->
    lists:delete(Excluded, L);
minus_first([_|_] = L, ExcludeList) ->
    minus_first2(L, ExcludeList, []).

%% @doc Removes every item in Excluded only once from List.
-spec minus_first2(List::[T], Excluded::[T], Result::[T]) -> [T].
minus_first2([], _Excluded, Result) ->
    lists:reverse(Result);
minus_first2(L, [], Result) ->
    lists:reverse(Result, L);
minus_first2([H | T], Excluded, Result) ->
    case delete_if_exists(Excluded, H, []) of
        {true,  Excluded2} -> minus_first2(T, Excluded2, Result);
        {false, Excluded2} -> minus_first2(T, Excluded2, [H | Result])
    end.

%% @doc Removes Del from List if it is found. Stops on first occurrence.
-spec delete_if_exists(List::[T], Del::T, Result::[T]) -> {Found::boolean(), [T]}.
delete_if_exists([], _Del, Result) ->
    {false, lists:reverse(Result)};
delete_if_exists([Del | T], Del, Result) ->
    {true, lists:reverse(Result, T)};
delete_if_exists([H | T], Del, Result) ->
    delete_if_exists(T, Del, [H | Result]).

-spec get_proc_in_vms(atom()) -> [comm:mypid()].
get_proc_in_vms(Proc) ->
    mgmt_server:node_list(),
    Nodes =
        receive
            {get_list_response, X} -> X
        after 2000 ->
            log:log(error,"[ util ] Timeout getting node list from mgmt server"),
            throw('mgmt_server_timeout')
        end,
    lists:usort([comm:get(Proc, DHTNode) || DHTNode <- Nodes]).

-spec sleep_for_ever() -> no_return().
sleep_for_ever() ->
    timer:sleep(5000),
    sleep_for_ever().

%% @doc Returns a random element from the given (non-empty!) list according to
%%      a uniform distribution.
-spec randomelem(List::[X,...]) -> X.
randomelem(List)->
    Length = length(List) + 1,
    RandomNum = randoms:rand_uniform(1, Length),
    lists:nth(RandomNum, List).
    
%% @doc Removes a random element from the (non-empty!) list and returns the
%%      resulting list and the removed element.
-spec pop_randomelem(List::[X,...]) -> {NewList::[X], PoppedElement::X}.
pop_randomelem(List) ->
    pop_randomelem(List, length(List)).
    
%% @doc Removes a random element from the first Size elements of a (non-empty!)
%%      list and returns the resulting list and the removed element. 
-spec pop_randomelem(List::[X,...], Size::non_neg_integer()) -> {NewList::[X], PoppedElement::X}.
pop_randomelem(List, Size) ->
    {Leading, [H | T]} = lists:split(randoms:rand_uniform(0, Size), List),
    {lists:append(Leading, T), H}.

%% @doc Returns a random subset of Size elements from the given list.
-spec random_subset(Size::pos_integer(), [T]) -> [T].
random_subset(0, _List) ->
    % having this special case here prevents unnecessary calls to erlang:length()
    [];
random_subset(Size, List) ->
    ListSize = length(List),
    shuffle_helper(List, [], Size, ListSize).

%% @doc Fisher-Yates shuffling for lists.
-spec shuffle([T]) -> [T].
shuffle(List) ->
    ListSize = length(List),
    shuffle_helper(List, [], ListSize, ListSize).

%% @doc Fisher-Yates shuffling for lists helper function: creates a shuffled
%%      list of length ShuffleSize.
-spec shuffle_helper(List::[T], AccResult::[T], ShuffleSize::non_neg_integer(), ListSize::non_neg_integer()) -> [T].
shuffle_helper([], Acc, _Size, _ListSize) ->
    Acc;
shuffle_helper([_|_] = _List, Acc, 0, _ListSize) ->
    Acc;
shuffle_helper([_|_] = List, Acc, Size, ListSize) ->
    {Leading, [H | T]} = lists:split(randoms:rand_uniform(0, ListSize), List),
    shuffle_helper(lists:append(Leading, T), [H | Acc], Size - 1, ListSize - 1).

-spec first_matching(List::[T], Pred::fun((T) -> boolean())) -> {ok, T} | failed.
first_matching([], _Pred) -> failed;
first_matching([H | R], Pred) ->
    case Pred(H) of
        true -> {ok, H};
        _    -> first_matching(R, Pred)
    end.

%% @doc Find the largest key in GBTree that is smaller than Key.
%%      Note: gb_trees offers only linear traversal or lookup of exact keys -
%%      we implement a more flexible binary search here despite gb_tree being
%%      defined as opaque.
-spec gb_trees_largest_smaller_than(Key, gb_tree()) -> {value, Key, Value::any()} | nil.
gb_trees_largest_smaller_than(_Key, {0, _Tree}) ->
    nil;
gb_trees_largest_smaller_than(MyKey, {_Size, InnerTree}) ->
    gb_trees_largest_smaller_than_iter(MyKey, InnerTree, true).

-spec gb_trees_largest_smaller_than_iter(Key, {Key, Value, Smaller::term(), Bigger::term()}, RightTree::boolean()) -> {value, Key, Value} | nil.
gb_trees_largest_smaller_than_iter(_SearchKey, nil, _RightTree) ->
    nil;
gb_trees_largest_smaller_than_iter(SearchKey, {Key, Value, Smaller, Bigger}, RightTree) ->
    case Key < SearchKey of
        true when RightTree andalso Bigger =:= nil ->
            % we reached the right end of the whole tree
            % -> there is no larger item than the current item
            {value, Key, Value};
        true ->
            case gb_trees_largest_smaller_than_iter(SearchKey, Bigger, RightTree) of
                {value, _, _} = AValue -> AValue;
                nil -> {value, Key, Value}
            end;
        _ ->
            gb_trees_largest_smaller_than_iter(SearchKey, Smaller, false)
    end.

%% @doc Foldl over gb_trees.
-spec gb_trees_foldl(fun((Key::any(), Value::any(), Acc) -> Acc), Acc, gb_tree()) -> Acc.
gb_trees_foldl(F, Acc, GBTree) ->
    gb_trees_foldl_iter(F, Acc, gb_trees:next(gb_trees:iterator(GBTree))).

-spec gb_trees_foldl_iter(fun((Key, Value, Acc) -> Acc), Acc,
                          {Key, Value, Iter::term()} | none) -> Acc.
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

-spec get_pids_uid() -> pos_integer().
get_pids_uid() ->
    Result = case erlang:get(pids_uid_counter) of
                 undefined ->
                     %% Same pid may be reused in the same VM, so we
                     %% get a VM unique offset to start
                     %% It is not completely safe, but safe enough
                     element(1, erlang:statistics(reductions));
                 Any -> Any + 1
             end,
    erlang:put(pids_uid_counter, Result),
    Result.

-spec get_global_uid() -> global_uid().
get_global_uid() ->
    _Result = {get_pids_uid(), comm:this()}
    %% , term_to_binary(_Result, [{minor_version, 1}])
    .

%% @doc Checks whether the given GUID is an old incarnation of a GUID from
%%      my node.
-spec is_my_old_uid(pos_integer() | global_uid()) -> boolean() | remote.
is_my_old_uid({LocalUid, Pid}) ->
    case comm:this() of
        Pid -> is_my_old_uid(LocalUid);
        _   -> remote
    end;
is_my_old_uid(Id) when is_integer(Id) ->
    LastUid = case erlang:get(pids_uid_counter) of
                  undefined -> 0;
                  Any -> Any
              end,
    Id =< LastUid;
is_my_old_uid(_Id) ->
    false.

-spec zipfoldl(ZipFun::fun((X, Y) -> Z), FoldFun::fun((Z, Acc) -> Acc), L1::[X], L2::[Y], Acc) -> Acc.
zipfoldl(ZipFun, FoldFun, [L1H | L1R], [L2H | L2R], AccIn) ->
    zipfoldl(ZipFun, FoldFun, L1R, L2R, FoldFun(ZipFun(L1H, L2H), AccIn));
zipfoldl(_ZipFun, _FoldFun, [], [], AccIn) ->
    AccIn.

%% @doc Sorts like erlang:'=&lt;'/2 but also defines the order of integers/floats
%%      representing the same value.
-spec '=:<'(T, T) -> boolean().
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
'=:<_lists'([], []) -> true;
'=:<_lists'([H1 | R1], [H2 | R2]) ->
    case (H1 == H2) andalso (H1 =/= H2) of
        true  -> '=:<'(H1, H2);
        false -> '=:<_lists'(R1, R2)
    end.

%% @doc Splits off N elements from List. If List is not large enough, the whole
%%      list is returned.
-spec safe_split(non_neg_integer(), [T]) -> {FirstN::[T], Rest::[T]}.
safe_split(N, List) when is_integer(N), N >= 0, is_list(List) ->
    safe_split(N, List, []).

safe_split(0, L, R) ->
    {lists:reverse(R, []), L};
safe_split(N, [H | T], R) ->
    safe_split(N - 1, T, [H | R]);
safe_split(_N, [], R) ->
    {lists:reverse(R, []), []}.

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that both lists share and a list of elements unique to L2.
%%      Returned lists are sorted and contain no duplicates.
-spec split_unique(L1::[X], L2::[X]) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}.
split_unique(L1, L2) ->
    split_unique(L1, L2, fun erlang:'=<'/2).

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that are equal in both lists (according to the ordering
%%      function Lte) and a list of elements unique to L2.
%%      When two elements compare equal, the element from List1 is picked.
%%      Lte(A, B) should return true if A compares less than or equal to B in
%%      the ordering, false otherwise.
%%      Returned lists are sorted according to Lte and contain no duplicates.
-spec split_unique(L1::[X], L2::[X], Lte::fun((X, X) -> boolean())) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}.
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
-spec split_unique(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> X)) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}.
split_unique(L1, L2, Lte, EqSelect) ->
    L1Sorted = lists:usort(Lte, L1),
    L2Sorted = lists:usort(Lte, L2),
    ssplit_unique_helper(L1Sorted, L2Sorted, Lte, EqSelect, {[], [], []}).

%% @doc Splits L1 into a list of elements that are not contained in L2, a list
%%      of elements that both lists share and a list of elements unique to L2.
%%      Both lists must be sorted. Returned lists are sorted as well.
-spec ssplit_unique(L1::[X], L2::[X]) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}.
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
-spec ssplit_unique(L1::[X], L2::[X], Lte::fun((X, X) -> boolean())) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}.
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
-spec ssplit_unique(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> X)) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}.
ssplit_unique(L1, L2, Lte, EqSelect) ->
    ssplit_unique_helper(L1, L2, Lte, EqSelect, {[], [], []}).

%% @doc Helper function for ssplit_unique/4.
-spec ssplit_unique_helper(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> X), {UniqueOldL1::[X], SharedOld::[X], UniqueOldL2::[X]}) -> {UniqueL1::[X], Shared::[X], UniqueL2::[X]}.
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

-spec smerge2(L1::[X], L2::[X]) -> MergedList::[X].
smerge2(L1, L2) ->
    smerge2(L1, L2, fun erlang:'=<'/2).

-spec smerge2(L1::[X], L2::[X], Lte::fun((X, X) -> boolean())) -> MergedList::[X].
smerge2(L1, L2, Lte) ->
    smerge2(L1, L2, Lte, fun(E1, _E2) -> [E1] end).

-spec smerge2(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> [X])) -> MergedList::[X].
smerge2(L1, L2, Lte, EqSelect) ->
    smerge2_helper(L1, L2, Lte, EqSelect, []).

%% @doc Helper function for merge2/4.
-spec smerge2_helper(L1::[X], L2::[X], Lte::fun((X, X) -> boolean()), EqSelect::fun((X, X) -> [X]), OldMergedList::[X]) -> MergedList::[X].
smerge2_helper(L1 = [H1 | T1], L2 = [H2 | T2], Lte, EqSelect, ML) ->
    LteH1H2 = Lte(H1, H2),
    LteH2H1 = Lte(H2, H1),
    case LteH1H2 andalso LteH2H1 of
        true ->
            smerge2_helper(T1, L2, Lte, EqSelect, lists:reverse(EqSelect(H1, H2)) ++ ML);
        false when LteH1H2 ->
            smerge2_helper(T1, L2, Lte, EqSelect, [H1 | ML]);
        false when LteH2H1 ->
            % the top of ML could be equal to the top of L2 (if so, the decision
            % about H2 has already been made and we omit it here, otherwise H2
            % needs to be added)
            case (ML =:= []) orelse not (Lte(hd(ML), H2) andalso Lte(H2, hd(ML))) of
                true  -> smerge2_helper(L1, T2, Lte, EqSelect, [H2 | ML]);
                false -> smerge2_helper(L1, T2, Lte, EqSelect, ML) 
            end
    end;
smerge2_helper(L1, [], _Lte, _EqSelect, ML) ->
    lists:reverse(ML, L1);
smerge2_helper([], L2 = [H2 | T2], Lte, EqSelect, ML) ->
    % The top of ML could be equal to the top of L2 (if so, the decision about
    % H2 has already been made and we omit it here, otherwise H2 needs to be
    % added).
    % This is because elements are only removed from L2 if an element of L1 is
    % larger.
    case ML =:= [] orelse not (Lte(hd(ML), H2) andalso Lte(H2, hd(ML))) of
        true  -> lists:reverse(ML, L2);
        false -> smerge2_helper([], T2, Lte, EqSelect, ML)
    end.

%% @doc Try to check whether common-test is running.
-spec is_unittest() -> boolean().
is_unittest() ->
    Pid = self(),
    spawn(fun () ->
                  case catch ct:get_status() of
                      no_tests_running -> Pid ! {is_unittest, false};
                      {error, _} -> Pid ! {is_unittest, false};
                      {'EXIT', {undef, _}} -> Pid ! {is_unittest, false};
                      _ -> Pid ! {is_unittest, true}
                  end
          end),
    receive
        {is_unittest, Result} -> Result
    end.

-spec make_filename(string()) -> string().
make_filename(Name) ->
    re:replace(Name, "[^a-zA-Z0-9\-_@\.]", "_", [{return, list}, global]).

%% @doc Get an application environment variable. If it is undefined, Default is
%%      returned.
-spec app_get_env(Var::atom(), Default::T) -> T.
app_get_env(Var, Default) ->
    case application:get_env(Var) of
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
                    error_logger:warning_msg("undefined application but no unittest~n"),
                    ok
            end;
        {ok, App} ->
            error_logger:warning_msg("unknown application: ~.0p~n", [App]),
            ok
    end.

-spec time_plus_us(Time::time(), Delta_MicroSeconds::integer()) -> time().
time_plus_us({MegaSecs, Secs, MicroSecs}, Delta) ->
    MicroSecs1 = MicroSecs + Delta,
    NewMicroSecs = MicroSecs1 rem 1000000,
    Secs1 = Secs + (MicroSecs1 div 1000000),
    NewSecs = Secs1 rem 1000000,
    MegaSecs1 = MegaSecs + (Secs1 div 1000000),
    NewMegaSecs = MegaSecs1 rem 1000000,
    {NewMegaSecs, NewSecs, NewMicroSecs}.

-spec time_plus_ms(Time::time(), Delta_MilliSeconds::integer()) -> time().
time_plus_ms(Time, Delta) ->
    time_plus_us(Time, Delta * 1000).

-spec time_plus_s(Time::time(), Delta_Seconds::integer()) -> time().
time_plus_s({MegaSecs, Secs, MicroSecs}, Delta) ->
    Secs1 = Secs + Delta,
    NewSecs = Secs1 rem 1000000,
    MegaSecs1 = MegaSecs + (Secs1 div 1000000),
    NewMegaSecs = MegaSecs1 rem 1000000,
    {NewMegaSecs, NewSecs, MicroSecs}.

%% for(i; I<=n; i++) { fun(i) }
-spec for_to(integer(), integer(), fun()) -> ok.
for_to(I, N, Fun) ->
    if I =< N -> Fun(I), for_to(I+1, N, Fun);
       true -> ok
    end.

%% for(i; I<=n; i++) { Acc = [fun(i)|Acc] }
for_to_ex(N, N, Fun, Acc) ->
    [Fun(N)|Acc];
for_to_ex(I, N, Fun, Acc) ->
    R = Fun(I),
    for_to_ex(I+1, N, Fun, [R|Acc]).

-spec for_to_ex(integer(), integer(), anyFun(T)) -> [T].
for_to_ex(I, N, Fun) ->
    for_to_ex(I, N, Fun, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% sequential repeat
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Simple sequential function repetition
-spec s_repeat(fun(), args(), pos_integer()) -> ok.
s_repeat(Fun, Args, 1) ->    
    apply(Fun, Args),
    ok;
s_repeat(Fun, Args, Times) ->
    apply(Fun, Args),
    s_repeat(Fun, Args, Times - 1).

%% @doc Simple sequential function repetiton with collection of their results
%%      returns a list of results of "times" function calls
%% @end
-spec s_repeatAndCollect(fun(), args(), pos_integer()) -> [any()].
s_repeatAndCollect(Fun, Args, Times) ->    
    s_repeatAndAccumulate(Fun, Args, Times, fun(R, Y) -> [R | Y] end, []).

%% @doc Sequential repetion of function FUN with arguments ARGS TIMES-fold.
%%      Results will be accumulated with an accumulator function ACCUFUN 
%%      in register ACCUMULATOR.
%% @end
-spec s_repeatAndAccumulate(anyFun(T), args(), pos_integer(), accumulatorFun(T, U), U) -> U.
s_repeatAndAccumulate(Fun, Args, 1, AccuFun, Accumulator) ->
    R1 = apply(Fun, Args),
    AccuFun(R1, Accumulator);
s_repeatAndAccumulate(Fun, Args, Times, AccuFun, Accumulator) ->
    R1 = apply(Fun, Args),
    s_repeatAndAccumulate(Fun, Args, Times - 1, AccuFun, AccuFun(R1, Accumulator)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% parallel repeat
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec p_repeat(fun(), args(), pos_integer()) -> ok.
p_repeat(Fun, Args, Times) ->
    p_repeat(Fun, Args, Times, false).

-spec p_repeatAndCollect(fun(), args(), pos_integer()) -> [any()].
p_repeatAndCollect(Fun, Args, Times) ->
    p_repeatAndAccumulate(Fun, Args, Times, fun(X,Y) -> [X|Y] end, []).

-spec p_repeatAndAccumulate(anyFun(T), args(), pos_integer(), accumulatorFun(T, U), U) -> U.
p_repeatAndAccumulate(Fun, Args, Times, AccuFun, Accumulator) ->
    p_repeat(Fun, Args, Times, true),
    parallel_collect(Times, AccuFun, Accumulator).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% parallel repeat helper functions 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec p_repeat(fun(), args(), pos_integer(), boolean()) -> ok.
p_repeat(Fun, Args, Times, DoAnswer) ->
    s_repeat(fun spawn/3, [?MODULE, parallel_run, [self(), Fun, Args, DoAnswer]], Times).

-spec parallel_run(pid(), fun(), args(), boolean()) -> ok.
parallel_run(SrcPid, Fun, Args, DoAnswer) ->
    Res = (catch apply(Fun, Args)),
    case DoAnswer of
        true -> comm:send_local(SrcPid, {parallel_result, Res});
        _ -> ok 
    end,
    ok.

-spec parallel_collect(non_neg_integer(), accumulatorFun(any(), U), U) -> U.
parallel_collect(0, _, Accumulator) ->
    Accumulator;
parallel_collect(ExpectedResults, AccuFun, Accumulator) -> 
    Result = receive
                {parallel_result, R} -> R
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

% @doc Gets the value of a property list - proplist:get_value/2 is 10x slower than this
-spec proplist_get_value(atom(), [{atom(), any()}]) -> any().
proplist_get_value(Key, List) ->
    case lists:keyfind(Key, 1, List) of
        {_K, V} -> V;
        _ -> undefined
    end.

% @doc If List contains a key-value pair named key, the value will be returned
%      otherwise default.
-spec proplist_get_value(atom(), [{atom(), any()}], any()) -> any().
proplist_get_value(Key, List, Default) ->
    case proplist_get_value(Key, List) of 
        undefined -> Default;
        V -> V
    end.
    
%% empty shell_prompt_func
-spec empty(any()) -> [].
empty(_) -> "".
