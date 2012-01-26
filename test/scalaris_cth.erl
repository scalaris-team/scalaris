%  @copyright 2011, 2012 Zuse Institute Berlin

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
-vsn('$Id$ ').

%% Callbacks
-export([id/1, init/2, terminate/1]).
-export([pre_init_per_suite/3, post_init_per_suite/4,
         pre_end_per_suite/3, post_end_per_suite/4]).
-export([pre_init_per_testcase/3, post_end_per_testcase/4]).
-export([on_tc_fail/3, on_tc_skip/3]).

-record(state, { processes = [] :: [unittest_helper:process_info()] | undefined,
                 suite          :: atom() | undefined,
                 tc_start = []  :: [{comm:erl_local_pid(), util:time()}] | [] 
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
    ct:pal("Starting unittest ~p~n", [ct:get_status()]),
    randoms:start(),
    % note: on R14B02, post_end_per_testcase was also called for init_per_suite
    % -> we thus need a valid time
    {Config, State#state{processes = Processes, suite = Suite, tc_start = [{self(), os:timestamp()}]}}.

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
    error_logger:tty(false),
    randoms:stop(),
    _ = inets:stop(),
    error_logger:tty(true),
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
    when is_subtype(Return, unittest_helper:kv_opts() | {fail, Reason::term()} | {skip, Reason::term()} | {timetrap_timeout, integer()} | term()).
post_end_per_testcase(TC, Config, Return, {ok, State}) ->
    post_end_per_testcase(TC, Config, Return, State);
post_end_per_testcase(TC, _Config, Return, State) when is_record(State, state) ->
    {Start, NewTcStart} =  case lists:keytake(self(), 1, State#state.tc_start) of
                               {value, {_, Val}, List} -> {Val, List};
                               false -> { os:timestamp(), State#state.tc_start }
                           end,
    TCTime_us = timer:now_diff(os:timestamp(), Start),
    ct:pal("####################################################~n"
           "End ~p:~p -> ~.0p (after ~fs)",
           [State#state.suite, TC, Return, TCTime_us / 1000000]),
    case Return of
        {timetrap_timeout, _} ->
            case (catch config:read(no_print_ring_data)) of
                true -> ok;
                _ -> unittest_helper:print_ring_data()
            end;
        _ -> ok
    end,
    {Return, State#state{tc_start = NewTcStart} }.

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
