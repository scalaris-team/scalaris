%  @copyright 2010-2015 Zuse Institute Berlin

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
%% @doc    test generator
%% @end
%% @version $Id$
-module(tester).
-author('schuett@zib.de').
-vsn('$Id$').

-export([test/4, test/5, test_log/4,
         register_type_checker/3, unregister_type_checker/1,
         register_value_creator/4, unregister_value_creator/1]).

% pseudo proc for the value_creator to create pids
-export([start_pseudo_proc/0, pseudo_proc_fun/0]).

-export([type_check_module/4]).

-include("tester.hrl").
-include("unittest.hrl").

-type test_option() :: multi_threaded | {threads, pos_integer()} | with_feeder.
-type test_options() :: [test_option()].

-spec test(module(), atom(), non_neg_integer(), non_neg_integer()) -> ok.
test(Module, Func, Arity, Iterations) ->
    test(Module, Func, Arity, Iterations, []).

-spec test(module(), atom(), non_neg_integer(), non_neg_integer(), test_options()) -> ok.
test(Module, Func, Arity, Iterations, Options) ->
    EmptyParseState = tester_parse_state:new_parse_state(),
    ParseState = tester_parse_state:find_fun_info(Module, Func, Arity, EmptyParseState),
    Threads = proplists:get_value(threads, Options, case proplists:get_bool(multi_threaded, Options) of
                                                       true -> erlang:system_info(schedulers);
                                                       false -> 1
                                                    end),
    run_test(Module, Func, Arity, Iterations, ParseState, Threads, Options),
    ok.

-spec test_log(module(), atom(), non_neg_integer(), non_neg_integer()) -> ok.
test_log(Module, Func, Arity, Iterations) ->
    EmptyParseState = tester_parse_state:new_parse_state(),
    ParseState = tester_parse_state:find_fun_info(Module, Func, Arity, EmptyParseState),
    io:format(""),
    _ = run(Module, Func, Arity, Iterations, ParseState, [], 1),
    ok.

-spec pseudo_proc_fun() -> no_return().
pseudo_proc_fun() ->
    receive
        _Msg -> ok
    end,
    pseudo_proc_fun().

-spec start_pseudo_proc() -> pid().
start_pseudo_proc() ->
    Pid = erlang:spawn_link(?MODULE, pseudo_proc_fun, []),
    erlang:register(tester_pseudo_proc, Pid),
    Pid.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% run tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec run(module(), atom(), non_neg_integer(), non_neg_integer(),
          tester_parse_state:state(), test_options(),
          Thread::non_neg_integer()) -> ok | any().
run(Module, Func, Arity, Iterations, ParseState, Options, Thread) ->
    FeederFun = list_to_atom(atom_to_list(Func) ++ "_feeder"),
    case proplists:get_bool(with_feeder, Options) of
        true ->
            % get spec from feeder
            case tester_parse_state:lookup_fun_type({'fun', Module,
                                                     FeederFun, Arity},
                                                    ParseState) of
                {value, FeederFunType} ->
                    % get spec from tested-fun
                    {value, FunType} = tester_parse_state:lookup_fun_type({'fun', Module,
                                                                           Func, Arity},
                                                                          ParseState),
                    run_helper(Module, Func, Arity, Iterations, FunType,
                               FeederFunType, ParseState, Options, Thread);
                none ->
                   {fail, no_result, no_result_type, feeder_fun_type_not_found,
                    tester_parse_state,
                    lookup_type,
                    {'fun', Module,
                     FeederFun, Arity},
                    'maybe_not_exported_or_no_spec_or...', erlang:get_stacktrace(),
                    util:get_linetrace()}
            end;
        false ->
            FeederFunType = {var_type, [], {union_fun, []}},
            % get spec from tested-fun
            {value, FunType} = tester_parse_state:lookup_fun_type({'fun', Module,
                                                                   Func, Arity},
                                                                  ParseState),
            run_helper(Module, Func, Arity, Iterations, FunType, FeederFunType,
                       ParseState, Options, Thread)
    end.

-spec run_helper(Module::module(), Fun::atom(), Arity::non_neg_integer(),
                 Iterations::non_neg_integer(),
                 Fun::{var_type, [], {union_fun, [test_fun_type(),...]}},
                 FeederFun::{var_type, [], {union_fun, [test_fun_type()]}},
                 tester_parse_state:state(), test_options(),
                 Thread::non_neg_integer()) -> ok | any().
run_helper(_Module, _Func, _Arity, 0, _FunType, _FeederFunType, _TypeInfos, _Options, _Thread) ->
    ok;
run_helper(Module, Func, Arity, Iterations, FunType, FeederFunType, TypeInfos, Options, Thread) ->
    %% ct:pal("Calling: ~.0p:~.0p(~.0p)...~p to call", [Module, Func, Options,
    %%                                                  Iterations -1]),
    case run_test_ttt(Module, Func, FunType, FeederFunType, TypeInfos, Options, Thread) of
        ok ->
            run_helper(Module, Func, Arity, Iterations - 1, FunType, FeederFunType,
                       TypeInfos, Options, Thread);
        Error ->
            Error
    end.

-spec get_arg_and_result_type(Fun::{var_type, [], {union_fun, [test_fun_type(),...]}},
                              FeederFun::{var_type, [], {union_fun, [test_fun_type(),...]}},
                              test_options()) ->
                                     {var_fun, [], test_fun_type()}.
get_arg_and_result_type({var_type, [], {union_fun, FunTypes}} = _FunType,
                        {var_type, [], {union_fun, FeederFunTypes}} = _FeederFunType, Options) ->
    case proplists:get_bool(with_feeder, Options) of
                                        true ->
                                            {var_fun, [], util:randomelem(FeederFunTypes)};
                                       false ->
                                           {var_fun, [], util:randomelem(FunTypes)}
                                    end.


-spec run_test_ttt(Module::module(), Fun::atom(),
                   Fun::{var_type, [], {union_fun, [test_fun_type(),...]}},
                   FeederFun::{var_type, [], {union_fun, [test_fun_type(),...]}},
                   tester_parse_state:state(), test_options(),
                   Thread::non_neg_integer()) -> any().
run_test_ttt(Module, Func,
             {var_type, [], {union_fun, FunTypes}} = FunType,
             {var_type, [], {union_fun, _FeederFunTypes}} = FeederFunType,
             TypeInfos, Options, Thread) ->
    Fun = get_arg_and_result_type(FunType, FeederFunType, Options),
    {var_fun, VarList, {'fun', ArgType, ResultType}} = Fun,
    Size = 30,
    GenArgs = try
               {ok, tester_value_creator:create_value({var_type, VarList, ArgType},
                                                      Size, TypeInfos)}
           catch
               Error:{error, Reason} ->
                   print_error(Reason),
                   %ct:pal("Reason: ~p~n", [Reason]),
                   {fail, {fail, no_result, no_result_type, Error, tester_value_creator,
                           create_value,
                           [ArgType, Size, typeInfos], %TypeInfos
                           Reason, erlang:get_stacktrace(), util:get_linetrace()}}
           end,
    case GenArgs of
        {ok, Args} ->
            case proplists:get_bool(with_feeder, Options) of
                true ->
                                                % result is a tuple
                    Result = apply_feeder(Module, Func, Args, ResultType, TypeInfos),
                    case Result of
                        {ok, FeededArgs} ->
                            FunResultTypes =
                                [InnerResultType
                                 || {'fun', InnerArgType, InnerResultType} <- FunTypes,
                                    tester_type_checker:check(FeededArgs, InnerArgType, TypeInfos) =:= true],
                            case FunResultTypes of
                                [] ->
                                    {fail, no_result, no_result_type,
                                     type_check_failed_feeder_result_is_not_valid_input_for_fun,
                                     Module, Func, Args, none, erlang:get_stacktrace(),
                                     util:get_linetrace()};
                                _ ->
                                    apply_args(Module, Func, tuple_to_list(FeededArgs),
                                               {union, FunResultTypes}, TypeInfos, Thread)
                            end;
                        FeederError ->
                            FeederError
                    end;
                false ->
                    apply_args(Module, Func, Args, ResultType, TypeInfos, Thread)
            end;
        {fail, ErrorDesc} ->
            ErrorDesc
    end.

% @doc called before the actual test to convert the input values. Can
% be used to implement types which cannot be expressed by type-specs
apply_feeder(Module, Func, Args, ResultType, TypeInfos) ->
    FeederFun = list_to_atom(atom_to_list(Func) ++ "_feeder"),
    try
        Result = apply(Module, FeederFun, Args),
        case tester_type_checker:check(Result, ResultType, TypeInfos) of
            true ->
                {ok, Result};
            {false, ErrMsg} ->
                tester_type_checker:log_error(ErrMsg),
                {fail, no_result, ResultType, type_check_failed_of_feeder_result, Module,
                 FeederFun,
                 Args,
                 consult_output_for_detailed_type_check_report,
                 erlang:get_stacktrace(), util:get_linetrace()}
        end
    catch
        Error:Reason ->
            ct:pal("Reason: ~p~n", [Reason]),
            {fail, no_result, no_result_type, Error, Module,
             FeederFun,
             Args,
             Reason, erlang:get_stacktrace(), util:get_linetrace()}
    end.

apply_args(Module, Func, Args, ResultType, TypeInfos, Thread) ->
    %% ct:pal("Calling: ~.0p:~.0p(~.0p)", [Module, Func, Args]),
    try
        tester_global_state:set_last_call(Thread, Module, Func, Args),
        Result = erlang:apply(Module, Func, Args),
        %% ct:pal("Result: ~.0p ~n~.0p", [Result, ResultType]),
        case tester_type_checker:check(Result, ResultType, TypeInfos) of
            true ->
                ok;
            {false, ErrorMsg} ->
                tester_type_checker:log_error(ErrorMsg),
                {fail, Result, ResultType, type_check_failed_on_fun_result, Module, Func,
                 Args, consult_output_for_detailed_type_check_report,
                 no_stacktrace, util:get_linetrace()}
        end
    catch
        exit:{test_case_failed, Reason} ->
            {fail, no_result, no_result_type, test_case_failed, Module, Func,
             Args, Reason, erlang:get_stacktrace(), util:get_linetrace()};
        Error:Reason ->
            {fail, no_result, no_result_type, Error, Module, Func, Args, Reason,
             erlang:get_stacktrace(), util:get_linetrace()}
    end.

-spec run_test(module(), atom(), non_neg_integer(), non_neg_integer(),
               tester_parse_state:state(), integer(), test_options()) -> ok.
run_test(Module, Func, Arity, Iterations, ParseState, Threads, Options) ->
    Master = self(),
    Dict = erlang:get(),
    _Pids = [spawn_link(
               fun() ->
                       Name = list_to_atom("run_test:" ++ integer_to_list(Thread)),
                       catch(erlang:register(Name, self())),
                       %% copy the dictionary of the original tester process
                       %% to worker threads (allows to join a pid_group if
                       %% necessary for a test
                       _ = [ erlang:put(K, V) || {K, V} <- Dict ],
                       unittest_global_state:register_thread(Thread),
                       Result = run(Module, Func, Arity,
                                    Iterations div Threads, ParseState, Options,
                                    Thread),
                       Master ! {result, Result, self()},
                       case Result of
                           ok -> tester_global_state:reset_last_call(Thread);
                           _  -> ok
                       end
               end) || Thread <- lists:seq(1, Threads)],
    Results = [receive {result, Result, ThreadPid} -> {Result, ThreadPid} end || _ <- lists:seq(1, Threads)],
    _ = [fun ({Result, ThreadPid}) ->
                 case Result of
                     {fail, ResultValue, ResultType, Error, _Module, _Func, Args, Term,
                      StackTrace, LineTrace} ->
                         ArgsStr = case lists:flatten([io_lib:format(", ~1000p", [Arg]) || Arg <- Args]) of
                                       [$,, $ | X] -> X;
                                       X -> X
                                   end,
                         ct:pal("Failed (in ~.0p)~n"
                                " Message    ~p in ~1000p:~1000p(~s):~n"
                                "            ~p~n"
                                " Result     ~p~n"
                                " ResultType ~p~n"
                                " Stacktrace ~p~n"
                                " Linetrace  ~p~n",
                                [ThreadPid, Error, Module, Func, ArgsStr, Term, ResultValue,
                                 ResultType, StackTrace, LineTrace]),
                         ?ct_fail("~.0p in ~.0p:~.0p(~.0p): ~.0p",
                                  [Error, Module, Func, Args, Term]);
                     ok -> ok
                 end
         end(XResult) || XResult <- Results],
    %ct:pal("~w~n", [Results]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% type check a module
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec type_check_module(module(),
                        ExcludeExported::[{Fun::atom(), Arity::non_neg_integer()}],
                        ExcludePrivate::[{Fun::atom(), Arity::non_neg_integer()}],
                       Count::pos_integer()) -> ok.
type_check_module(Module, ExcludeExported, ExcludePrivate, Count) ->
    ExpFuncs = Module:module_info(exports),
    ExcludeList = [{module_info, 0}, {module_info, 1} | ExcludeExported],

    %% only excluded exported functions?
    ErrList = [ {Module, X} || X <- ExcludeList,
                               not lists:member(X, ExpFuncs) ],
    case ErrList of
        [] -> ok;
        [_|_] ->
            ct:pal("Excluded non-existing or non-exported functions: ~p~n", [ErrList]),
            throw(error)
    end,

    %% perform the actual tests
    %% >= R15 generates behaviour_info without a type spec so
    %% tester cannot find it. Erlang < R15 checks behaviour_info
    %% itself, so no own tests necessary here.
    %% Silently drop it for modules that export it.
    ResList = type_check_module_funs(
                Module, ExpFuncs, [{behaviour_info, 1} | ExcludeList], Count),

    case cover:modules() of
        [] ->
            type_check_private_funs(Module, ExcludePrivate, Count);
        _ ->
            %% code reloading is not supported for cover analysis (see
            %% docs of cover:compile/2)
            ct:pal("*** Detected cover analysis, have to skip tests of private funs."),
            ok
    end,
    %% Was there even anything left to test?
    case [] =:= ExcludeExported orelse
             lists:member(ok, lists:flatten(ResList)) of
        true -> ok;
        _ ->
            ct:pal("Excluded all exported functions for module ~p?!",
                   [Module]),
            catch ct:comment("Excluded all exported functions for module ~p?!",
                             [Module]),
            ok
    end,

    ok.

-spec type_check_module_funs(
        module(), FunList::[{Fun::atom(), Arity::non_neg_integer()}],
        ExcludeList::[{Fun::atom(), Arity::non_neg_integer()}],
        Count::pos_integer()) -> [Status | [Status]]
        when is_subtype(Status, ok | skipped | feeder).
type_check_module_funs(Module, FunList, ExcludeList, Count) ->
    Dict = erlang:get(),
    util:par_map(
      fun({Fun, Arity} = FA) ->
              _ = [ erlang:put(K, V) || {K, V} <- Dict ],
              %% test all non excluded funs with std. settings
              Res1 = case lists:member(FA, ExcludeList) of
                         false ->
                             ct:pal("Testing ~p:~p/~p", [Module, Fun, Arity]),
                             test(Module, Fun, Arity, Count, [{threads, 2}]);
                         true  -> skipped
                     end,

              %% if Fun is a feeder, crosscheck existence of tested fun,
              %% but do not trigger tests for that.
              FunString = atom_to_list(Fun),
              Res2 = case lists:suffix("_feeder", FunString) of
                         true ->
                             TestedFunString =
                                 lists:sublist(FunString, length(FunString) - 7),
                             try lists:member({list_to_existing_atom(TestedFunString), Arity},
                                              FunList) of
                                 true -> feeder;
                                 false ->
                                     ct:pal("Found feeder, but no target fun ~p:~s/~p",
                                            [Module, TestedFunString, Arity]),
                                     throw(error)
                             catch _:_ ->
                                       ct:pal("Found feeder, but no target fun ~p:~s/~p",
                                              [Module, TestedFunString, Arity]),
                                       throw(error)
                             end;
                         false -> Res1
                     end,

              %% if a feeder is found, test with feeder and ignore the
              %% exclude list, as a feeder is expected to feed the
              %% tested fun appropriately (will type check feeder
              %% results for required input types anyhow).
              FeederFun = list_to_atom(FunString ++ "_feeder"),
              case lists:member({FeederFun, Arity}, FunList) of
                  true ->
                      ct:pal("Testing with feeder ~p:~p/~p",
                             [Module, Fun, Arity]),
                      [Res2, test(Module, Fun, Arity, Count, [{threads, 2}, with_feeder])];
                  false -> Res2
              end
      end, FunList, 2).

-spec type_check_private_funs(
        module(), ExcludePrivate::[{Fun::atom(), Arity::non_neg_integer()}],
        Count::pos_integer()) -> ok.
type_check_private_funs(Module, ExcludePrivate, Count) ->
    ExportedFuns = Module:module_info(exports),

    tester_helper:load_with_export_all(Module),
    AllFuns = Module:module_info(exports),

    PrivateFuns = [ X || X <- AllFuns, not lists:member(X, ExportedFuns)],

    ct:pal("*** Private funs of ~p:~n~.0p", [Module, PrivateFuns]),

    %% only excluded existing functions?
    ErrList = [ {Module, X} || X <- ExcludePrivate,
                               not lists:member(X, PrivateFuns) ],
    case ErrList of
        [] -> ok;
        [_|_] ->
            ct:pal("Excluded non-existing private functions: ~p~n", [ErrList]),
            throw(error)
    end,

    _ = type_check_module_funs(Module, PrivateFuns, ExcludePrivate, Count),

    tester_helper:load_without_export_all(Module).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% handle global state, e.g. specific handlers
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec register_type_checker(type_spec(), module(), atom()) ->
                                   true.
register_type_checker(Type, Module, Function) ->
    tester_global_state:register_type_checker(Type, Module, Function).

-spec unregister_type_checker(type_spec()) -> true | ok.
unregister_type_checker(Type) ->
    tester_global_state:unregister_type_checker(Type).

-spec register_value_creator(type_spec(), module(), atom(), non_neg_integer()) -> true.
register_value_creator(Type, Module, Function, Arity) ->
    tester_global_state:register_value_creator(Type, Module, Function, Arity).

-spec unregister_value_creator(type_spec()) -> true | ok.
unregister_value_creator(Type) ->
    tester_global_state:unregister_value_creator(Type).

print_error(Msgs) ->
    ct:pal("error in value creator. could not create a value of type:\n"
           ++ lists:flatten(print_error_(lists:reverse(Msgs), "  "))).

print_error_([], _Prefix) ->
    "";
print_error_([Msg|Msgs], Prefix) ->
    io_lib:format("~s~s~n", [Prefix, Msg]) ++ print_error_(Msgs, " in ").
