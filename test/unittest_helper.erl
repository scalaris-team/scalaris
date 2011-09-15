%  Copyright 2008-2011 Zuse Institute Berlin
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
%%% File    : unittest_helper.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Helper functions for Unit tests
%%%
%%% Created :  27 Aug 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(unittest_helper).
-author('schuett@zib.de').
-vsn('$Id$').

%%-define(TRACE_RING_DATA(), print_ring_data()).
-define(TRACE_RING_DATA(), ok).

-export([fix_cwd/0,
         get_scalaris_port/0, get_yaws_port/0,
         make_ring_with_ids/1, make_ring_with_ids/2, make_ring/1, make_ring/2,
         stop_ring/0, stop_ring/1,
         stop_pid_groups/0,
         check_ring_size/1, check_ring_size_fully_joined/1,
         wait_for_stable_ring/0, wait_for_stable_ring_deep/0,
         start_process/1, start_process/2,
         start_subprocess/1, start_subprocess/2,
         get_all_children/1,
         get_processes/0, kill_new_processes/1, kill_new_processes/2,
         init_per_suite/1, end_per_suite/1,
         create_ct_all/1, create_ct_groups/2,
         init_per_group/2, end_per_group/2,
         get_ring_data/0, print_ring_data/0,
         macro_equals/4, macro_equals/5,
         expect_no_message_timeout/1,
         prepare_config/1,
         start_minimal_procs/3, stop_minimal_procs/1]).

-include("scalaris.hrl").

%% @doc Sets the current working directory to "../bin" if it does not end with
%%      "/bin" yet. Assumes that the current working directory is a sub-dir of
%%      our top-level, e.g. "test". This is needed in order for the config
%%      process to find its (default) config files.
-spec fix_cwd() -> ok | {error, Reason::file:posix()}.
fix_cwd() ->
    case file:get_cwd() of
        {ok, CurCWD} ->
            case string:rstr(CurCWD, "/bin") =/= (length(CurCWD) - 4 + 1) of
                true -> file:set_cwd("../bin");
                _    -> ok
            end;
        Error -> Error
    end.

-spec get_port(EnvName::string(), Default::pos_integer()) -> pos_integer().
get_port(EnvName, Default) ->
    case os:getenv(EnvName) of
        false -> Default;
        X ->
            try erlang:list_to_integer(X)
            catch
                _:_ -> Default
            end
    end.

-spec get_scalaris_port() -> pos_integer().
get_scalaris_port() ->
    get_port("SCALARIS_UNITTEST_PORT", 14195).

-spec get_yaws_port() -> pos_integer().
get_yaws_port() ->
    get_port("SCALARIS_UNITTEST_YAWS_PORT", 8000).

%% @doc Adds unittest-specific config parameters to the given key-value list.
%%      The following parameters are added7changed:
%%       - SCALARIS_UNITTEST_PORT environment variable to specify the port to
%%         listen on
%%       - SCALARIS_UNITTEST_YAWS_PORT environment variable to specify the port
%%         yaws listens on
%%       - adds only a single known_hosts node
%%       - specifies the node to be empty
-spec add_my_config(ConfigKVList::[{atom(), term()}])
    -> NewKVList::[{atom(), term()}].
add_my_config(KVList) ->
    % add empty_node to the end (so it is not overwritten)
    % but add known_hosts to the beginning so it
    % can be overwritten by the Options
    ScalarisPort = get_scalaris_port(),
    YawsPort = get_yaws_port(),
    KVList1 = [{known_hosts, [{{127,0,0,1}, ScalarisPort, service_per_vm}]},
               {mgmt_server, {{127,0,0,1}, ScalarisPort, mgmt_server}},
               {port, ScalarisPort},
               {yaws_port, YawsPort} | KVList],
    lists:append(KVList1, [{start_mgmt_server, true}]).

%% @doc Adds unittest specific ports from the environment to the list of
%%      options for the config process.
-spec prepare_config(Options::[{atom(), term()}]) -> NewOptions::[{atom(), term()}].
prepare_config(Options) ->
    prepare_config_helper(Options, false, []).

-spec prepare_config_helper(Options, ConfigFound::boolean(), Options) -> Options when is_subtype(Options, [{atom(), term()}]).
prepare_config_helper([], false, OldOptions) ->
    lists:reverse(OldOptions, [{config, add_my_config([])}]);
prepare_config_helper([], true, OldOptions) ->
    lists:reverse(OldOptions);
prepare_config_helper([Option | Rest], OldConfigFound, OldOptions) ->
    {NewOption, ConfigFound} =
        case Option of
            {config, KVList} -> {{config, add_my_config(KVList)}, true};
            X                -> {X, OldConfigFound}
        end,
    prepare_config_helper(Rest, ConfigFound, [NewOption | OldOptions]).

%% @doc Creates a ring with the given IDs (or IDs returned by the IdFun).
-spec make_ring_with_ids([?RT:key()] | fun(() -> [?RT:key()])) -> pid().
make_ring_with_ids(Ids) ->
    make_ring_with_ids(Ids, []).

%% @doc Creates a ring with the given IDs (or IDs returned by the IdFun).
%%      Passes Options to the supervisor, e.g. to set config variables, specify
%%      a {config, [{Key, Value},...]} option.
-spec make_ring_with_ids([?RT:key(),...] | fun(() -> [?RT:key(),...]), Options::[tuple()]) -> pid().
make_ring_with_ids(Ids, Options) when is_list(Ids) ->
    make_ring_with_ids(fun () -> Ids end, Options);
make_ring_with_ids(IdsFun, Options) when is_function(IdsFun, 0) ->
    % note: do not call IdsFun before the initial setup
    %       (it might use config or another process)
    _ = fix_cwd(),
    error_logger:tty(true),
    case ets:info(config_ets) of
        undefined -> ok;
        _         -> ct:fail("Trying to create a new ring although there is already one.")
    end,
    Pid = start_process(
            fun() ->
                    ct:pal("Trying to build Scalaris with ids~n"),
                    erlang:register(ct_test_ring, self()),
                    randoms:start(),
                    {ok, _GroupsPid} = pid_groups:start_link(),
                    NewOptions = prepare_config(Options),
                    _ = sup_scalaris:start_link(NewOptions),
                    mgmt_server:connect(),
                    Ids = IdsFun(), % config may be needed
                    _ = admin:add_node([{first}, {{dht_node, id}, hd(Ids)}]),
                    _ = [admin:add_node_at_id(Id) || Id <- tl(Ids)],
                    ok
            end),
%%     timer:sleep(1000),
    % need to call IdsFun again (may require config process or others
    % -> can not call it before starting the scalaris process)
    Ids = IdsFun(),
    Size = length(Ids),
    check_ring_size(Size),
    wait_for_stable_ring(),
    check_ring_size(Size),
    ct:pal("Scalaris booted with ~p node(s)...~n", [Size]),
    ?TRACE_RING_DATA(),
    Pid.

%% @doc Creates a ring with Size random IDs.
-spec make_ring(Size::pos_integer()) -> pid().
make_ring(Size) ->
    make_ring(Size, []).

%% @doc Creates a ring with Size rangom IDs.
%%      Passes Options to the supervisor, e.g. to set config variables, specify
%%      a {config, [{Key, Value},...]} option.
-spec make_ring(Size::pos_integer(), Options::[tuple()]) -> pid().
make_ring(Size, Options) ->
    _ = fix_cwd(),
    error_logger:tty(true),
    case ets:info(config_ets) of
        undefined -> ok;
        _         -> ct:fail("Trying to create a new ring although there is already one.")
    end,
    Pid = start_process(
            fun() ->
                    ct:pal("Trying to build Scalaris~n"),
                    erlang:register(ct_test_ring, self()),
                    randoms:start(),
                    {ok, _GroupsPid} = pid_groups:start_link(),
                    NewOptions = prepare_config(Options),
                    _ = sup_scalaris:start_link(NewOptions),
                    mgmt_server:connect(),
                    _ = admin:add_node([{first}]),
                    _ = admin:add_nodes(Size - 1),
                    ok
            end),
%%     timer:sleep(1000),
    check_ring_size(Size),
    wait_for_stable_ring(),
    check_ring_size(Size),
    ct:pal("Scalaris has booted with ~p node(s)...~n", [Size]),
    ?TRACE_RING_DATA(),
    Pid.

%% @doc Stops a ring previously started with make_ring/1 or make_ring_with_ids/1.
-spec stop_ring() -> ok.
stop_ring() ->
    case erlang:whereis(ct_test_ring) of
        undefined -> ok;
        Pid       -> stop_ring(Pid)
    end.

%% @doc Stops a ring previously started with make_ring/1 or make_ring_with_ids/1
%%      when the process' pid is known.
-spec stop_ring(pid()) -> ok.
stop_ring(Pid) ->
    try
        begin
            error_logger:tty(false),
            error_logger:delete_report_handler(error_logger_log4erl_h),
            log:set_log_level(none),
            util:supervisor_terminate(main_sup),
            catch exit(Pid, kill),
            util:wait_for_process_to_die(Pid),
            stop_pid_groups(),
            catch(unregister(ct_test_ring)),
            error_logger:tty(true),
            ct:pal("Scalaris ring stopped.~n"),
            ok
        end
    catch
        throw:Term ->
            ct:pal("exception in stop_ring: ~p~n", [Term]),
            throw(Term);
        exit:Reason ->
            ct:pal("exception in stop_ring: ~p~n", [Reason]),
            throw(Reason);
        error:Reason ->
            ct:pal("exception in stop_ring: ~p ~p~n", [Reason, erlang:get_stacktrace()]),
            throw(Reason)
    end.

-spec stop_pid_groups() -> ok.
stop_pid_groups() ->
    gen_component:kill(pid_groups),
    util:wait_for_table_to_disappear(pid_groups),
    util:wait_for_table_to_disappear(pid_groups_hidden),
    catch unregister(pid_groups),
    ok.


-spec wait_for_stable_ring() -> ok.
wait_for_stable_ring() ->
    util:wait_for(fun() ->
                     R = admin:check_ring(),
                     ct:pal("CheckRing: ~p~n", [R]),
                     R =:= ok
             end, 500).

-spec wait_for_stable_ring_deep() -> ok.
wait_for_stable_ring_deep() ->
    util:wait_for(fun() ->
                     R = admin:check_ring_deep(),
                     ct:pal("CheckRingDeep: ~p~n", [R]),
                     R =:= ok
             end, 500).

-spec check_ring_size(non_neg_integer(), CheckFun::fun((DhtNodeState::term()) -> boolean())) -> ok.
check_ring_size(Size, CheckFun) ->
    DhtModule = config:read(dht_node),
    util:wait_for(
      fun() ->
              % note: we use a single VM in unit tests, therefore no
              % mgmt_server is needed - if one exists though, then check
              % the correct size
              BootSize =
                  try
                      mgmt_server:number_of_nodes(),
                      receive {get_list_length_response, L} -> L end
                  catch _:_ -> Size
                  end,
              erlang:whereis(config) =/= undefined andalso
                  BootSize =:= Size andalso
                  Size =:= erlang:length(
                [P || P <- pid_groups:find_all(DhtModule),
                      CheckFun(gen_component:get_state(P))])
      end, 500).

-spec check_ring_size(Size::non_neg_integer()) -> ok.
check_ring_size(Size) ->
    check_ring_size(Size, fun(State) ->
                                  DhtModule = config:read(dht_node),
                                  DhtModule:is_alive(State)
                          end).

%% @doc Checks whether Size nodes have fully joined the ring (including
%%      finished join-related slides).
-spec check_ring_size_fully_joined(Size::non_neg_integer()) -> ok.
check_ring_size_fully_joined(Size) ->
    check_ring_size(Size, fun(State) ->
                                  DhtModule = config:read(dht_node),
                                  DhtModule:is_alive_no_slide(State)
                          end).

-spec start_process(StartFun::fun(() -> any())) -> pid().
start_process(StartFun) ->
    start_process(StartFun, fun() -> receive {done} -> ok end end).

-spec start_process(StartFun::fun(() -> any()), RunFun::fun(() -> any())) -> pid().
start_process(StartFun, RunFun) ->
    Owner = self(),
    Node = spawn(
             fun() ->
                     StartFun(),
                     Owner ! {started, self()},
                     RunFun()
             end),
    receive
        {started, Node} -> Node
    end.

-spec start_subprocess(StartFun::fun(() -> any())) -> pid().
start_subprocess(StartFun) ->
    start_subprocess(StartFun, fun() -> receive {done} -> ok end end).

-spec start_subprocess(StartFun::fun(() -> any()), RunFun::fun(() -> any())) -> pid().
start_subprocess(StartFun, RunFun) ->
    process_flag(trap_exit, true),
    Owner = self(),
    Node = spawn_link(
             fun() ->
                     StartFun(),
                     Owner ! {started, self()},
                     RunFun()
             end),
    receive
        {started, Node} -> Node
    end.

%% @doc Starts the minimal number of processes in order for non-ring unit tests
%%      to be able to execute (pid_groups, config, log).
-spec start_minimal_procs(CTConfig, ConfigOptions::[{atom(), term()}],
                          StartCommServer::boolean()) -> CTConfig when is_subtype(CTConfig, list()).
start_minimal_procs(CTConfig, ConfigOptions, StartCommServer) ->
    ok = unittest_helper:fix_cwd(),
    Pid = unittest_helper:start_process(
            fun() ->
                    {ok, _GroupsPid} = pid_groups:start_link(),
                    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, CTConfig),
                    ConfigOptions2 = unittest_helper:prepare_config(
                                       [{config, [{log_path, PrivDir} | ConfigOptions]}]),
                    {ok, _ConfigPid} = config:start_link2(ConfigOptions2),
                    {ok, _LogPid} = log:start_link(),
                    case StartCommServer of
                        true ->
                            {ok, _CommPid} = sup_comm_layer:start_link(),
                            Port = unittest_helper:get_scalaris_port(),
                            comm_server:set_local_address({127,0,0,1}, Port);
                        false -> ok
                    end
            end),
    [{wrapper_pid, Pid} | CTConfig].

%% @doc Stops the processes started by start_minimal_procs/3 given the
%%      ct config term.
-spec stop_minimal_procs(term()) -> ok.
stop_minimal_procs(CTConfig)  ->
    case lists:keyfind(wrapper_pid, 1, CTConfig) of
        {wrapper_pid, Pid} ->
            error_logger:tty(false),
            log:set_log_level(none),
            exit(Pid, kill),
            unittest_helper:stop_pid_groups(),
            error_logger:tty(true);
        false -> ok
    end.

-spec get_all_children(Supervisor::pid()) -> [pid()].
get_all_children(Supervisor) ->
    AllChilds = [X || X = {_, Pid, _, _} <- supervisor:which_children(Supervisor),
                      Pid =/= undefined],
    WorkerChilds = [Pid ||  {_Id, Pid, worker, _Modules} <- AllChilds],
    SupChilds = [Pid || {_Id, Pid, supervisor, _Modules} <- AllChilds],
    lists:flatten([WorkerChilds | [get_all_children(S) || S <- SupChilds]]).

-type process_info() ::
    {pid(), InitCall::mfa(), CurFun::mfa(), Info::term | failed | no_further_infos}.

-spec get_processes() -> [process_info()].
get_processes() ->
    [begin
         InitCall = element(2, lists:keyfind(initial_call, 1, Data)),
         CurFun = element(2, lists:keyfind(current_function, 1, Data)),
         Info =
             case {InitCall, CurFun} of
                 {{gen_component, _, _}, {gen_component, _, _}} ->
                     gen_component:get_component_state(X);
                 {_, {file_io_server, _, _}} ->
                     case file:pid2name(X) of
                         undefined -> {file, undefined};
                         {ok, FileName} -> {file, FileName};
                         %% case occuring in older Erlang versions:
                         FileName -> {file, FileName}
                     end;
                 {_, {gen_server, _, _}} ->
                     % may throw exit due to a killed process
                     try sys:get_status(X)
                     catch _:_ -> no_further_infos
                     end;
                 _ -> no_further_infos
             end,
         {X, InitCall, CurFun, Info}
     end
     || X <- processes(),
        Data <- [process_info(X, [current_function, initial_call])],
        Data =/= undefined].

-spec kill_new_processes(OldProcesses::[process_info()]) -> ok.
kill_new_processes(OldProcesses) ->
    kill_new_processes(OldProcesses, []).

-spec kill_new_processes(OldProcesses::[process_info()], Options::list()) -> ok.
kill_new_processes(OldProcesses, Options) ->
    NewProcesses = get_processes(),
    {_OnlyOld, _Both, OnlyNew} =
        util:split_unique(OldProcesses, NewProcesses,
                          fun(P1, P2) ->
                                  element(1, P1) =< element(1, P2)
                          end, fun(_P1, P2) -> P2 end),
%%     ct:pal("Proc-Old: ~.0p~n", [_OnlyOld]),
%%     ct:pal("Proc-Both: ~.0p~n", [_Both]),
%%     ct:pal("Proc-New: ~.0p~n", [_OnlyNew]),
    Killed = [begin
%%                   ct:pal("killing ~.0p~n", [Proc]),
                  Tabs = util:ets_tables_of(X),
                  try erlang:exit(X, kill) of
                      true -> util:wait_for_process_to_die(X),
                              _ = [ util:wait_for_table_to_disappear(Tab) || Tab <- Tabs ],
                              {ok, Proc}
                  catch _:_ -> {fail, Proc}
                  end
              end || {X, InitCall, CurFun, _Info} = Proc <- OnlyNew,
                     not (InitCall =:= {test_server_sup, timetrap, 3} andalso
                              CurFun =:= {test_server_sup, timetrap, 3}),
                     not (InitCall =:= {test_server_sup, timetrap, 2} andalso
                              CurFun =:= {test_server_sup, timetrap, 2}),
                     X =/= self(),
                     element(1, CurFun) =/= file_io_server],
    case lists:member(quiet, Options) of
        false ->
            ct:pal("Killed processes: ~.0p~n", [Killed]);
        true -> ok
    end.

%% @doc Generic init_per_suite for all unit tests. Prints current state
%%      information and stores information about all running processes.
%%      Also starts the crypto application needed for unit tests using the
%%      tester.
-spec init_per_suite([tuple()]) -> [tuple()].
init_per_suite(Config) ->
    Processes = get_processes(),
    ct:pal("Starting unittest ~p~n", [ct:get_status()]),
    randoms:start(),
    [{processes, Processes} | Config].

%% @doc Generic end_per_suite for all unit tests. Tries to stop a scalaris ring
%%      started with make_ring* (if there is one) and stops the inets and
%%      crypto applications needed for some unit tests. Then gets the list of
%%      processes stored in init_per_suite/1 and kills all but a selected few
%%      of processes which are now running but haven't been running before.
%%      Thus allows a clean start of succeeding test suites.
%%      Prints information about the processes that have been killed.
-spec end_per_suite([tuple()]) -> [tuple()].
end_per_suite(Config) ->
    ct:pal("Stopping unittest ~p~n", [ct:get_status()]),
    unittest_helper:stop_ring(),
    % the following might still be running in case there was no ring:
    randoms:stop(),
    _ = inets:stop(),
    {processes, OldProcesses} = lists:keyfind(processes, 1, Config),
    kill_new_processes(OldProcesses),
    error_logger:tty(true),
    Config.

-type ct_group_props() ::
    [parallel | sequence | shuffle | {shuffle, {integer(), integer(), integer()}} |
     {repeat | repeat_until_all_ok | repeat_until_all_fail | repeat_until_any_ok | repeat_until_any_fail, integer() | forever}].

-spec testcase_to_groupname(TestCase::atom()) -> atom().
testcase_to_groupname(TestCase) ->
    erlang:list_to_atom(erlang:atom_to_list(TestCase) ++ "_grp").

-spec create_ct_all(TestCases::[atom()]) -> [{group, atom()}].
create_ct_all(TestCases) ->
    [{group, testcase_to_groupname(TestCase)} || TestCase <- TestCases].

-spec create_ct_groups(TestCases::[atom()], SpecialOptions::[{atom(), ct_group_props()}])
        -> [{atom(), ct_group_props(), [atom()]}].
create_ct_groups(TestCases, SpecialOptions) ->
    [begin
         Options =
             case lists:keyfind(TestCase, 1, SpecialOptions) of
                 false -> [sequence, {repeat, 1}];
                 {_, X} -> X
             end,
         {testcase_to_groupname(TestCase), Options, [TestCase]}
     end || TestCase <- TestCases].

-spec init_per_group(atom(), [tuple()]) -> [tuple()].
init_per_group(_Group, Config) -> Config.

-spec end_per_group(atom(), [tuple()]) -> {return_group_result, ok | failed}.
end_per_group(_Group, Config) ->
    Status = test_server:lookup_config(tc_group_result, Config),
    case proplists:get_value(failed, Status) of
        [] ->                                   % no failed cases 
            {return_group_result, ok};
        _Failed ->                              % one or more failed
            {return_group_result, failed}
    end.

-spec get_ring_data() -> [{pid(), 
                           {intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()}, 
                           ?DB:db_as_list(), 
                           {prred, comm:erl_pid_plain()},
                           {succc, comm:erl_pid_plain()},
                           ok | timeout}].
get_ring_data() ->
    DHTNodes = pid_groups:find_all(dht_node),
    lists:sort(
      fun(E1, E2) ->
              erlang:element(2, E1) =< erlang:element(2, E2)
      end,
      [begin
           comm:send_local(DhtNode,
                           {unittest_get_bounds_and_data, comm:this()}),
           receive
               {unittest_get_bounds_and_data_response, Bounds, Data, Pred, Succ} ->
                   {DhtNode, Bounds, Data,
                    {pred, comm:make_local(node:pidX(Pred))},
                    {succ, comm:make_local(node:pidX(Succ))}, ok}
           % note: no timeout possible - can not leave messages in queue!
%%            after 500 -> {DhtNode, empty, [], null, null, timeout}
           end
       end || DhtNode <- DHTNodes]).

-spec print_ring_data() -> ok.
print_ring_data() ->
    DataAll = unittest_helper:get_ring_data(),
    ct:pal("~.0p~n", [DataAll]).

-include("unittest.hrl").

-spec macro_equals(Actual::any(), ExpectedVal::any(), ActualStr::string(), ExpectedStr::string()) -> ok | no_return().
macro_equals(Actual, ExpectedVal, ActualStr, ExpectedStr) ->
    case Actual of
        ExpectedVal -> ok;
        Any ->
            ct:pal("Failed: Stacktrace ~p~n", [util:get_stacktrace()]),
            ?ct_fail("~s evaluated to \"~.0p\" which is "
                         "not the expected ~s that evaluates to \"~.0p\"~n",
                         [ActualStr, Any, ExpectedStr, ExpectedVal])
    end.

-spec macro_equals(Actual::any(), ExpectedVal::any(), ActualStr::string(), ExpectedStr::string(), Note::string()) -> ok | no_return().
macro_equals(Actual, ExpectedVal, ActualStr, ExpectedStr, Note) ->
    case Actual of
        ExpectedVal -> ok;
        Any ->
            ct:pal("Failed: Stacktrace ~p~n",
                   [util:get_stacktrace()]),
            ?ct_fail("~s evaluated to \"~.0p\" which is "
                         "not the expected ~s that evaluates to \"~.0p\"~n"
                             "(~s)~n",
                             [ActualStr, Any, ExpectedStr, ExpectedVal, lists:flatten(Note)])
    end.

-spec expect_no_message_timeout(Timeout::pos_integer()) -> ok | no_return().
expect_no_message_timeout(Timeout) ->
    receive
        ActualMessage ->
            ?ct_fail("expected no message but got \"~.0p\"~n", [ActualMessage])
    after Timeout -> ok
end.
