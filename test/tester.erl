%  @copyright 2010-2012 Zuse Institute Berlin
%  @end
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
%%% File    tester.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    test generator
%%% @end
%%% Created :  30 March 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(tester).

-author('schuett@zib.de').
-vsn('$Id$').

-export([test/4, test/5, test_log/4,
         test_with_scheduler/3, test_with_scheduler/4]).

-include("tester.hrl").
-include("unittest.hrl").

-type test_option() :: multi_threaded | {threads, pos_integer()}.
-type test_options() :: [test_option()].

-spec test/4 :: (module(), atom(), non_neg_integer(), non_neg_integer()) -> ok.
test(Module, Func, Arity, Iterations) ->
    test(Module, Func, Arity, Iterations, []).

-spec test/5 :: (module(), atom(), non_neg_integer(), non_neg_integer(), test_options()) -> ok.
test(Module, Func, Arity, Iterations, Options) ->
    EmptyParseState = tester_parse_state:new_parse_state(),
    ParseState = try tester_collect_function_info:collect_fun_info(Module, Func, Arity,
                                      EmptyParseState)
    catch
        throw:Term2 -> ?ct_fail("exception (throw) in ~p:~p(): ~p~n",
                                [Module, Func,
                                 {exception, {Term2, erlang:get_stacktrace(),
                                              util:get_linetrace()}}]);
        % special handling for exits that come from a ct:fail() call:
        exit:{test_case_failed, Reason2} ->
            ?ct_fail("error ~p:~p/~p failed with ~p~n",
                     [Module, Func, Arity, {Reason2, erlang:get_stacktrace(),
                                     util:get_linetrace()}]);
        exit:Reason2 -> ?ct_fail("exception (exit) in ~p:~p(): ~p~n",
                                 [Module, Func,
                                  {exception, {Reason2, erlang:get_stacktrace(),
                                               util:get_linetrace()}}]);
        error:Reason2 -> ?ct_fail("exception (error) in ~p:~p(): ~p~n",
                                  [Module, Func,
                                   {exception, {Reason2, erlang:get_stacktrace(),
                                                util:get_linetrace()}}])
    end,
    Threads = proplists:get_value(threads, Options, case proplists:get_bool(multi_threaded, Options) of
                                                       true -> erlang:system_info(schedulers);
                                                       false -> 1
                                                    end),
    run_test(Module, Func, Arity, Iterations, ParseState, Threads),
    ok.

-spec test_log/4 :: (module(), atom(), non_neg_integer(), non_neg_integer()) -> ok.
test_log(Module, Func, Arity, Iterations) ->
    EmptyParseState = tester_parse_state:new_parse_state(),
    ParseState = try tester_collect_function_info:collect_fun_info(Module, Func, Arity,
                                      EmptyParseState)
    catch
        throw:Term2 -> ?ct_fail("exception (throw) in ~p:~p(): ~p~n",
                                [Module, Func,
                                 {exception, {Term2, erlang:get_stacktrace(),
                                              util:get_linetrace()}}]);
        % special handling for exits that come from a ct:fail() call:
        exit:{test_case_failed, Reason2} ->
            ?ct_fail("error ~p:~p() failed with ~p~n", [Module, Func,
                                                        {Reason2, erlang:get_stacktrace(),
                                                         util:get_linetrace()}]);
        exit:Reason2 -> ?ct_fail("exception (exit) in ~p:~p(): ~p~n",
                                 [Module, Func,
                                  {exception, {Reason2, erlang:get_stacktrace(),
                                               util:get_linetrace()}}]);
        error:Reason2 -> ?ct_fail("exception (error) in ~p:~p(): ~p~n",
                                  [Module, Func,
                                   {exception, {Reason2, erlang:get_stacktrace(),
                                                util:get_linetrace()}}])
    end,
    io:format(""),
    _ = run(Module, Func, Arity, Iterations, ParseState),
    ok.

% @doc options are white_list and seed
-spec test_with_scheduler(list(module()), fun(), list()) -> any().
test_with_scheduler(Modules, F, Options) ->
    test_with_scheduler(Modules, F, Options, 1).

-spec test_with_scheduler(list(module()), fun(), list(), number()) -> any().
test_with_scheduler(Modules, F, Options, Repetitions) ->
    _InstrumentRes = [tester_scheduler:instrument_module(Module) || Module <- Modules],
    Processes = unittest_helper:get_processes(),
    Res = repeat(fun () ->
                         {ok, Pid} = tester_scheduler:start(Options),
                         (catch register(usscheduler, Pid)),
                         Res = (catch F()),
                         unittest_helper:kill_new_processes(Processes, [quiet]),
                         (catch exit(Pid)),
                         (catch unregister(usscheduler)),
                         Res
                 end, Repetitions),
    _DeleteRes = [code:delete(Module) || Module <- Modules],
    Res.

repeat(F, 1) ->
    F();
repeat(F, Repetitions) ->
    _Res = F(),
    repeat(F, Repetitions - 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% run tests
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec run/5 :: (module(), atom(), non_neg_integer(), non_neg_integer(),
                tester_parse_state:state()) -> any().
run(Module, Func, Arity, Iterations, ParseState) ->
    {value, FunType} = tester_parse_state:lookup_type({'fun', Module, Func, Arity},
                                                      ParseState),
    run_helper(Module, Func, Arity, Iterations, FunType, ParseState).

-spec run_helper/6 :: (Module::module(), Fun::atom(), Arity::non_neg_integer(),
                       Iterations::non_neg_integer(),
                       FunType | {union_fun, [FunType,...]},
                       tester_parse_state:state()) -> any()
        when is_subtype(FunType, {'fun', type_spec(), type_spec()}).
run_helper(_Module, _Func, _Arity, 0, _FunType, _TypeInfos) ->
    ok;
run_helper(Module, Func, 0, Iterations, {union_fun, FunTypes} = FunType,
           TypeInfos) ->
    CurrentFunType = util:randomelem(FunTypes),
    {'fun', _ArgType, _ResultType} = CurrentFunType,
    apply_args(Module, Func, 0, [], Iterations, CurrentFunType, FunType,
               TypeInfos);
run_helper(Module, Func, Arity, Iterations, {union_fun, FunTypes} = FunType,
           TypeInfos) ->
    Size = 30,
    CurrentFunType = util:randomelem(FunTypes),
    {'fun', ArgType, _ResultType} = CurrentFunType,
    Args = try tester_value_creator:create_value(ArgType, Size, TypeInfos)
           catch
               Error:Reason ->
                   {fail, no_result, no_result_type, Error, tester_value_creator,
                    create_value,
                    [ArgType, Size, TypeInfos],
                    Reason, erlang:get_stacktrace(), util:get_linetrace()}
    end,
    apply_args(Module, Func, Arity, Args, Iterations, CurrentFunType, FunType,
               TypeInfos).

apply_args(Module, Func, Arity, Args, Iterations,
           {'fun', _ArgType, ResultType}, FunTypes, TypeInfos) ->
    %ct:pal("~w:~w ~w", [Module, Func, Args]),
    try erlang:apply(Module, Func, Args) of
        Result ->
            %ct:pal("~w ~n~w", [Result, ResultType]),
            case tester_type_checker:check(Result, ResultType, TypeInfos) of
                true ->
                    run_helper(Module, Func, Arity, Iterations - 1, FunTypes,
                               TypeInfos);
                {false, ErrorMsg} ->
                    % @todo give good error message
                    {fail, Result, ResultType, test_case_failed, Module, Func,
                     Args, ErrorMsg, no_stacktrace, util:get_linetrace()}
            end
    catch
        exit:{test_case_failed, Reason} ->
            {fail, no_result, no_result_type, test_case_failed, Module, Func,
             Args, Reason, erlang:get_stacktrace(), util:get_linetrace()};
        Error:Reason ->
            {fail, no_result, no_result_type, Error, Module, Func, Args, Reason,
             erlang:get_stacktrace(), util:get_linetrace()}
    end.

-spec run_test/6 :: (module(), atom(), non_neg_integer(), non_neg_integer(),
                     tester_parse_state:state(), integer()) -> ok.
run_test(Module, Func, Arity, Iterations, ParseState, Threads) ->
    Master = self(),
    _Pids = [spawn(fun () ->
                           Result = run(Module, Func, Arity,
                                        Iterations div Threads, ParseState),
                           Master ! {result, Result}
                   end) || _ <- lists:seq(1, Threads)],
    Results = [receive {result, Result} -> Result end || _ <- lists:seq(1, Threads)],
    %ct:pal("~w~n", [Results]),
    _ = [fun (Result) ->
                 case Result of
                     {fail, ResultValue, ResultType, Error, Module, Func, Args, Term,
                      StackTrace, LineTrace} ->
                         ArgsStr = case lists:flatten([io_lib:format(", ~1000p", [Arg]) || Arg <- Args]) of
                                       [$,, $ | X] -> X;
                                       X -> X
                                   end,
                         ct:pal("Failed~n"
                                " Message    ~p in ~1000p:~1000p(~s):~n"
                                "            ~p~n"
                                " Result     ~p~n"
                                " ResultType ~p~n"
                                " Stacktrace ~p~n"
                                " Linetrace  ~p~n",
                                [Error, Module, Func, ArgsStr, Term, ResultValue,
                                 ResultType, StackTrace, LineTrace]),
                         ?ct_fail("~.0p in ~.0p:~.0p(~.0p): ~.0p",
                                  [Error, Module, Func, Args, Term]);
                     ok -> ok
                 end
         end(XResult) || XResult <- Results],
    %ct:pal("~w~n", [Results]),
    ok.
