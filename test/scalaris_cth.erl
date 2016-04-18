%  @copyright 2011-2013 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Common test hooks for Scalaris unit tests.
%% @end
%% @version $Id$
-module(scalaris_cth).
-author('kruber@zib.de').
-vsn('$Id$').

-include("types.hrl").

%% Callbacks
-export([id/1, init/2, terminate/1]).
-export([pre_init_per_suite/3, post_init_per_suite/4,
         pre_end_per_suite/3, post_end_per_suite/4]).
-export([pre_init_per_testcase/3, post_end_per_testcase/4]).
-export([on_tc_fail/3, on_tc_skip/3]).

-record(state, { processes = [] :: [unittest_helper:process_info()] | undefined,
                 suite          :: atom() | undefined,
                 logger_pid     :: pid() | undefined,
                 tc_start = []  :: [{comm:erl_local_pid(), erlang_timestamp()}] | []
               }).
-type state() :: #state{} | {ok, #state{}}. % the latter for erlang =< R14B03
-type id() :: atom().

%% @doc Return a unique id for this CTH.
-spec id(Opts::term()) -> id().
id(_Opts) ->
    ?MODULE.

%% @doc Always called before any other callback function. Use this to initiate
%% any common state.
-spec init(Id::id(), Opts::term()) -> {ok, State::state()}.
init(_Id, _Opts) ->
    % note: erlang =< R14B03 requires only #state{}
    % -> provide alternative handlers for all functions below catching the
    % additional {ok, state()} tuple
    {ok, #state{}}.

%% @doc Called before init_per_suite is called.

-spec pre_init_per_suite(SuiteName::atom(), Config::unittest_helper:kv_opts(), CTHState::state())
        -> {Return::unittest_helper:kv_opts() | {fail, Reason::term()} | {skip, Reason::term()},
            NewCTHState::state()}.
pre_init_per_suite(Suite, Config, {ok, State}) ->
    pre_init_per_suite(Suite, Config, State);
pre_init_per_suite(Suite, Config, State) when is_record(State, state) ->
    Processes = unittest_helper:get_processes(),
    set_config_file_paths(),
    % initiate the logging process (which needs the config)
    {TempPid, _} = unittest_helper:start_process(
          fun() ->
                  {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
                  ConfOptions = unittest_helper:prepare_config(
                                  [{config, [{log_path, PrivDir}]}]),
                  config:init(ConfOptions)
          end),
    % need to start the logger from a non-linked process so that it is
    % available until the end of the suite
    {LoggerPid, _} = unittest_helper:start_process(
                       fun() -> {ok, _LogPid} = log:start_link(),
                                exit(TempPid, kill)
                       end),
    ct:pal("Starting unittest ~p~n", [ct:get_status()]),
    randoms:start(),
    % note: on R14B02, post_end_per_testcase was also called for init_per_suite
    % -> we thus need a valid time
    {Config, State#state{processes = Processes, suite = Suite,
                         logger_pid = LoggerPid,
                         tc_start = [{self(), os:timestamp()}]}}.

%% @doc Called after init_per_suite.
-spec post_init_per_suite(SuiteName::atom(), Config::unittest_helper:kv_opts(), Return, CTHState::state())
        -> {Return, NewCTHState::state()}
    when is_subtype(Return, unittest_helper:kv_opts() | {fail, Reason::term()} | {skip, Reason::term()} | term()).
post_init_per_suite(Suite, Config, Return, {ok, State}) ->
    post_init_per_suite(Suite, Config, Return, State);
post_init_per_suite(_Suite, _Config, Return, State) when is_record(State, state) ->
    {Return, State}.

%% @doc Called before end_per_suite.
-spec pre_end_per_suite(SuiteName::atom(), Config::unittest_helper:kv_opts(), CTHState::state())
        -> {Return::unittest_helper:kv_opts() | {fail, Reason::term()} | {skip, Reason::term()},
            NewCTHState::state()}.
pre_end_per_suite(_Suite, Config, {ok, State}) ->
    pre_end_per_suite(_Suite, Config, State);
pre_end_per_suite(_Suite, Config, State) when is_record(State, state) ->
    {Config, State}.

%% @doc Called after end_per_suite.
-spec post_end_per_suite(SuiteName::atom(), Config::unittest_helper:kv_opts(), Return, CTHState::state())
        -> {Return, NewCTHState::state()}
    when is_subtype(Return, unittest_helper:kv_opts() | {fail, Reason::term()} | {skip, Reason::term()} | term()).
post_end_per_suite(Suite, Config, Return, {ok, State}) ->
    post_end_per_suite(Suite, Config, Return, State);
post_end_per_suite(_Suite, _Config, Return, State) when is_record(State, state) ->
    ct:pal("Stopping unittest ~p~n", [ct:get_status()]),
    unittest_helper:stop_ring(),
    % the following might still be running in case there was no ring:
    error_logger:delete_report_handler(error_logger_log4erl_h),
    log:set_log_level(none),
    exit(State#state.logger_pid, kill),
    randoms:stop(),
    _ = inets:stop(),
    unittest_global_state:delete(),
    unittest_helper:kill_new_processes(State#state.processes),
    {Return, State#state{processes = [], suite = undefined, tc_start = [] } }.

%% @doc Called before each test case.
-spec pre_init_per_testcase(TestcaseName::atom(), Config::unittest_helper:kv_opts(), CTHState::state())
        -> {Return::unittest_helper:kv_opts() | {fail, Reason::term()} | {skip, Reason::term()},
            NewCTHState::state()}.
pre_init_per_testcase(TC, Config, {ok, State}) ->
    pre_init_per_testcase(TC, Config, State);
pre_init_per_testcase(TC, Config, State) when is_record(State, state) ->
    ct:pal("Start ~p:~p~n"
           "####################################################",
           [State#state.suite, TC]),
    {Config, State#state{tc_start = [{self(), os:timestamp()} | State#state.tc_start]}}.

%% @doc Called after each test case.
-spec post_end_per_testcase(TestcaseName::atom(), Config::unittest_helper:kv_opts(), Return, CTHState::state())
        -> {Return, NewCTHState::state()}
    when is_subtype(Return, unittest_helper:kv_opts() | {fail, Reason::term()} |
                        {error, Reason::term()} | {testcase_aborted, Reason::term()} |
                        {skip, Reason::term()} | {timetrap_timeout, integer()}).
post_end_per_testcase(TC, Config, Return, {ok, State}) ->
    post_end_per_testcase(TC, Config, Return, State);
post_end_per_testcase(TC, Config, Return, State) when is_record(State, state) ->
    case Return of
        {timetrap_timeout, TimeTrapTime_ms} ->
            print_debug_data(),
            Suite = State#state.suite,
            try Suite:end_per_testcase(TC, Config)
            catch
                exit:undef ->
                    ok;
                error:undef ->
                    ok;
                Error:Reason ->
                    ct:pal("Caught ~p:~p while trying to clean up Ring after
                           timeout~n", [Error, Reason])
            end,
            ok;
        {skip, {failed, {_FailedMod, _FailedFun, {timetrap_timeout, TimeTrapTime_ms}}}} ->
            ok;
        {fail, _Reason} ->
            TimeTrapTime_ms = 0,
            print_debug_data();
        {error, _} ->
            % from thrown exceptions or ct:fail/*
            TimeTrapTime_ms = 0,
            print_debug_data();
        {testcase_aborted, _} ->
            % e.g. from an exception in a gen_component (Reason = exception_throw)
            TimeTrapTime_ms = 0,
            print_debug_data();
        _ ->
            TimeTrapTime_ms = 0,
            ok
    end,
    case proplists:get_value(stop_ring, Config, false) of
        true  -> unittest_helper:stop_ring();
        false -> ok
    end,
    {Start, NewTcStart} =  case lists:keytake(self(), 1, State#state.tc_start) of
                               {value, {_, Val}, List} -> {Val, List};
                               false -> { failed, State#state.tc_start }
                           end,
    TCTime_ms = case Start of
                    failed -> TimeTrapTime_ms;
                    _      -> timer:now_diff(os:timestamp(), Start) / 1000
                end,
    ct:pal("####################################################~n"
           "End ~p:~p -> ~.0p (after ~fs)",
           [State#state.suite, TC, Return, TCTime_ms / 1000]),
    {Return, State#state{tc_start = NewTcStart} }.

-spec print_debug_data() -> ok.
print_debug_data() ->
    case (catch config:read(no_print_ring_data)) of
        true -> ok;
        _ -> unittest_helper:print_ring_data()
    end,
    try proto_sched:get_infos() of
        [] ->
            % proto_sched or its stats do not exist anymore -> print collected stats
            case unittest_global_state:lookup(proto_sched_stats) of
                failed -> ok;
                Stats -> ct:pal("Proto scheduler stats (collected): ~.2p",
                                [Stats])
            end;
        Stats ->
            ct:pal("Proto scheduler stats: ~.2p", [Stats])
    catch _:_ ->
              % pid_groups not available (anymore)? -> check whether we recorded some:
              case unittest_global_state:lookup(proto_sched_stats) of
                  failed -> ok;
                  Stats -> ct:pal("Proto scheduler stats (collected): ~.2p",
                                  [Stats])
              end
    end,
    catch tester_global_state:log_last_calls(),
    ok.

%% @doc Called after post_init_per_suite, post_end_per_suite, post_init_per_group,
%% post_end_per_group and post_end_per_testcase if the suite, group or test case failed.
-spec on_tc_fail(TestcaseName::init_per_suite | end_per_suite | init_per_group | end_per_group | atom(),
                 Reason::{error, FailInfo} |
                         {error, {RunTimeError, StackTrace}} |
                         {timetrap_timeout, integer()} |
                         {failed, {Suite::atom(), end_per_testcase, FailInfo}},
                 CTHState::state()) -> NewCTHState::#state{}
    when is_subtype(FailInfo, {timetrap_timeout, integer()} | {RunTimeError, StackTrace} | UserTerm),
         is_subtype(RunTimeError, term()),
         is_subtype(StackTrace, list()),
         is_subtype(UserTerm, term()).
on_tc_fail(TC, Reason, {ok, State}) ->
    on_tc_fail(TC, Reason, State);
on_tc_fail(TC, Reason, State) when is_record(State, state) ->
    ct:pal("~p:~p failed with ~p.~n", [State#state.suite, TC, Reason]),
    State.

%% @doc Called when a test case is skipped by either user action
%% or due to an init function failing.
-spec on_tc_skip(TestcaseName::end_per_suite | init_per_group | end_per_group | atom(),
                 Reason::{tc_auto_skip, {Suite, Func::atom(), Reason}} |
                         {tc_user_skip, {Suite, TestCase::atom(), Comment::string()}},
                 CTHState::state()) -> NewCTHState::#state{}
    when is_subtype(Suite, atom()),
         is_subtype(Reason, {failed, FailReason} | {require_failed_in_suite0, RequireInfo}),
         is_subtype(FailReason, {Suite, ConfigFunc, FailInfo} | {Suite, FailedCaseInSequence}),
         is_subtype(RequireInfo, {not_available, atom()}),
         is_subtype(ConfigFunc, init_per_suite | init_per_group),
         is_subtype(FailInfo, {timetrap_timeout,integer()} | {RunTimeError, StackTrace} | bad_return | UserTerm),
         is_subtype(FailedCaseInSequence, atom()),
         is_subtype(RunTimeError, term()),
         is_subtype(StackTrace, list()),
         is_subtype(UserTerm, term()).
on_tc_skip(TC, Reason, {ok, State}) ->
    on_tc_skip(TC, Reason, State);
on_tc_skip(TC, Reason, State) when is_record(State, state) ->
    ct:pal("~p:~p skipped with ~p.~n", [State#state.suite, TC, Reason]),
    State.

%% @doc Called when the scope of the CTH is done
-spec terminate(State::state()) -> ok.
terminate({ok, State}) ->
    terminate(State);
terminate(State) when is_record(State, state) ->
    ok.

%% @doc Sets the app environment to point to the correct config file paths
%%      assuming that the current working directory is a sub-dir of
%%      our top-level, e.g. "test". This is needed in order for the config
%%      process to find its (default) config files.
-spec set_config_file_paths() -> ok.
set_config_file_paths() ->
    application:set_env(scalaris, config, "../bin/scalaris.cfg"),
    application:set_env(scalaris, local_config, "../bin/scalaris.local.cfg").
