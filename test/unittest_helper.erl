%  @copyright 2008-2015 Zuse Institute Berlin

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
%% @doc    Helper functions for Unit tests
%% @end
%% @version $Id$
-module(unittest_helper).
-author('schuett@zib.de').
-vsn('$Id$').

%%-define(TRACE_RING_DATA(), print_ring_data()).
-define(TRACE_RING_DATA(), ok).

-export([get_scalaris_port/0, get_yaws_port/0,
         make_ring_with_ids/1, make_ring_with_ids/2, make_ring/1, make_ring/2, make_ring_recover/1,
         make_symmetric_ring/0, make_symmetric_ring/1,
         stop_ring/0, stop_ring/1,
         stop_pid_groups/0,
         check_ring_size/1, check_ring_size_fully_joined/1,
         wait_for_stable_ring/0, wait_for_stable_ring_deep/0,
         start_process/1, start_process/2,
         start_subprocess/1, start_subprocess/2,
         get_processes/0, kill_new_processes/1, kill_new_processes/2,
         create_ct_all/1, create_ct_groups/2,
         init_per_group/2, end_per_group/2,
         get_ring_data/1, print_ring_data/0,
         print_proto_sched_stats/1, print_proto_sched_stats/2,
         macro_equals/5, macro_compare/7,
         macro_equals_failed/6,
         expect_no_message_timeout/1,
         prepare_config/1,
         start_minimal_procs/3, stop_minimal_procs/1,
         check_ring_load/1, check_ring_data/0, check_ring_data/2,
         build_interval/2,
         db_entry_not_null/1, scrub_data/1]).

-export_type([process_info/0, kv_opts/0, ring_data/1]).

-include("scalaris.hrl").
-include("client_types.hrl").
-include("unittest.hrl").

-dialyzer({no_match, get_processes/0}).

-type kv_opts() :: [{Key::atom(), Value::term()}].

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
               {yaws_port, YawsPort},
               {start_type, first_nostart} | KVList],
    lists:append(KVList1, [{start_mgmt_server, true}]).

%% @doc Adds unittest specific ports from the environment to the list of
%%      options for the config process.
-spec prepare_config(Options::[{atom(), term()}]) -> NewOptions::[{atom(), term()}].
prepare_config(Options) ->
    prepare_config_helper(Options, false).

-spec prepare_config_helper(Options, ConfigFound::boolean()) -> Options when is_subtype(Options, [{atom(), term()}]).
prepare_config_helper([], false) ->
    [{config, add_my_config([])}];
prepare_config_helper([], true) ->
    [];
prepare_config_helper([Option | Rest], OldConfigFound) ->
    {NewOption, ConfigFound} =
        case Option of
            {config, KVList} -> {{config, add_my_config(KVList)}, true};
            X                -> {X, OldConfigFound}
        end,
    [NewOption | prepare_config_helper(Rest, ConfigFound)].

%% @doc Creates a symmetric ring.
-spec make_symmetric_ring() -> pid().
make_symmetric_ring() ->
    make_symmetric_ring([]).

%% @doc Creates a symmetric ring.
-spec make_symmetric_ring(Options::kv_opts()) -> pid().
make_symmetric_ring(Options) ->
    ct:pal("Trying to make a symmetric ring"),
    NodeAddFun =
        fun() ->
                Ids = case lists:keyfind(scale_ring_size_by, 1, Options) of
                          {scale_ring_size_by, Scale} ->
                              api_rt:get_evenly_spaced_keys(
                                Scale * config:read(replication_factor));
                          false ->
                              api_rt:get_evenly_spaced_keys(
                                config:read(replication_factor))
                      end,
                [admin:add_node([{first}, {{dht_node, id}, hd(Ids)}])
                 | [admin:add_node_at_id(Id) || Id <- tl(Ids)]]
        end,
    Pid = make_ring_generic(Options, NodeAddFun),
    Size = case lists:keyfind(scale_ring_size_by, 1, Options) of
               {scale_ring_size_by, Scale} ->
                   Scale * config:read(replication_factor);
               false ->
                   config:read(replication_factor)
           end,
    check_ring_size(Size),
    wait_for_stable_ring(),
    check_ring_size(Size),
    ct:pal("Scalaris booted with ~p node(s)...~n", [Size]),
    TimeTrap = test_server:timetrap(3000 + Size * 1000),
    test_server:timetrap_cancel(TimeTrap),
    Pid.

%% @doc Creates a ring with the given IDs (or IDs returned by the IdFun).
-spec make_ring_with_ids([?RT:key()]) -> pid().
make_ring_with_ids(Ids) ->
    make_ring_with_ids(Ids, []).

%% @doc Creates a ring with the given IDs (or IDs returned by the IdFun).
%%      Passes Options to the supervisor, e.g. to set config variables, specify
%%      a {config, [{Key, Value},...]} option.
-spec make_ring_with_ids([?RT:key(),...], Options::kv_opts()) -> pid().
make_ring_with_ids(Ids, Options) when is_list(Ids) ->
  ct:pal("Trying to make ring with Ids, containing ~p nodes.",[erlang:length(Ids)]),
  NodeAddFun =
        fun() ->
                [admin:add_node([{first}, {{dht_node, id}, hd(Ids)}])
                 | [admin:add_node_at_id(Id) || Id <- tl(Ids)]]
        end,
  Size = erlang:length(Ids),
  TimeTrap = test_server:timetrap(3000 + Size * 1000),
  Pid = make_ring_generic(Options, NodeAddFun),
  check_ring_size(Size),
  wait_for_stable_ring(),
  check_ring_size(Size),
  ct:pal("Scalaris booted with ~p node(s)...~n", [Size]),
  test_server:timetrap_cancel(TimeTrap),
  Pid.

-spec make_ring_recover(Options::kv_opts()) -> pid().
make_ring_recover(Options) ->
  ct:pal("Trying to recover ring."),
  TimeTrap = test_server:timetrap(60000),
  Pid = make_ring_generic(Options, fun() -> [] end),
  ct:pal("ring restored"),
  test_server:timetrap_cancel(TimeTrap),
  Pid.

%% @doc Creates a ring with Size random IDs.
-spec make_ring(Size::pos_integer()) -> pid().
make_ring(Size) ->
    make_ring(Size, []).

%% @doc Creates a ring with Size rangom IDs.
%%      Passes Options to the supervisor, e.g. to set config variables, specify
%%      a {config, [{Key, Value},...]} option.
-spec make_ring(Size::pos_integer(), Options::kv_opts()) -> pid().
make_ring(Size, Options) ->
  ct:pal("Trying to make ring with ~p nodes.",[Size]),
    NodeAddFun =
        fun() ->
                First = admin:add_node([{first}]),
                {RestSuc, RestFailed} = admin:add_nodes(Size - 1),
                [First | RestSuc ++ RestFailed]
        end,
    TimeTrap = test_server:timetrap(3000 + Size * 1000),
    Pid = make_ring_generic(Options, NodeAddFun),
    check_ring_size(Size),
    wait_for_stable_ring(),
    check_ring_size(Size),
    ct:pal("Scalaris booted with ~p node(s)...~n", [Size]),
    test_server:timetrap_cancel(TimeTrap),
    Pid.

-spec make_ring_generic(Options::kv_opts(),
                        NodeAddFun::fun(() -> [pid_groups:groupname() | {error, term()}]))
        -> pid().
make_ring_generic(Options, NodeAddFun) ->
    case ets:info(config_ets) of
        undefined -> ok;
        _         -> ct:fail("Trying to create a new ring although there is already one.")
    end,
    {Pid, StartRes} =
        start_process(
          fun() ->
                  erlang:register(ct_test_ring, self()),
                  randoms:start(),
                  {ok, _GroupsPid} = pid_groups:start_link(),
                  NewOptions = prepare_config(Options),
                  config:init(NewOptions),
                  tester:start_pseudo_proc(),
                  {ok, _} = sup_scalaris:start_link(NewOptions),
                  NodeAddFun()
          end),
    FailedNodes = [X || X = {error, _ } <- StartRes],
    case FailedNodes of
        [] -> ok;
        [_|_] -> stop_ring(Pid),
                 ?ct_fail("adding nodes failed: ~.0p", [FailedNodes])
    end,
    true = erlang:is_process_alive(Pid),
%%     timer:sleep(1000),
    % need to call IdsFun again (may require config process or others
    % -> can not call it before starting the scalaris process)
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
    LogLevel = config:read(log_level),
    try
        begin
            ct:pal("unittest_helper:stop_ring start."),
            log:set_log_level(none),
            sup:sup_terminate(main_sup),
            catch exit(Pid, kill),
            util:wait_for_process_to_die(Pid),
            stop_pid_groups(),
            sup_scalaris:stop_first_services(),
            log:set_log_level(LogLevel),
            ct:pal("unittest_helper:stop_ring done."),
            ok
        end
    catch
        Level:Reason ->
            ct:pal("~s in stop_ring: ~p~n~.0p", [Level, Reason, erlang:get_stacktrace()]),
            erlang:Level(Reason)
    after
            catch(unregister(ct_test_ring)),
            log:set_log_level(LogLevel)
    end.

-spec stop_pid_groups() -> ok.
stop_pid_groups() ->
    case whereis(pid_groups) of
        PidGroups when is_pid(PidGroups) ->
            gen_component:kill(PidGroups),
            util:wait_for_ets_table_to_disappear(PidGroups, pid_groups),
            util:wait_for_ets_table_to_disappear(PidGroups, pid_groups_hidden),
            catch unregister(pid_groups),
            ok;
        _ -> ok
    end.


-spec wait_for_stable_ring() -> ok.
wait_for_stable_ring() ->
    util:wait_for(fun() ->
                          R = admin:check_ring(),
                          %% ct:pal("CheckRing: ~p~n", [R]),
                          R =:= ok
             end, 100).

-spec wait_for_stable_ring_deep() -> ok.
wait_for_stable_ring_deep() ->
    util:wait_for(fun() ->
                     R = admin:check_ring_deep(),
                     ct:pal("CheckRingDeep: ~p~n", [R]),
                     R =:= ok
             end, 100).

-spec check_ring_size(non_neg_integer(), CheckFun::fun((DhtNodeState::term()) -> boolean())) -> ok.
check_ring_size(Size, CheckFun) ->
    util:wait_for(
      fun() ->
              % note: we use a single VM in unit tests, therefore no
              % mgmt_server is needed - if one exists though, then check
              % the correct size
              BootSize =
                  try
                      mgmt_server:number_of_nodes(),
                      trace_mpath:thread_yield(),
                      receive ?SCALARIS_RECV({get_list_length_response, L}, L) end
                  catch _:_ -> Size
                  end,
              BootSize =:= Size andalso
                  Size =:= erlang:length(
                             [P || P <- pid_groups:find_all(dht_node),
                                   CheckFun(gen_component:get_state(P))])
      end, 50).

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

%% @doc Starts a process which executes the given function and then waits forever.
-spec start_process(StartFun::fun(() -> StartRes)) -> {pid(), StartRes}.
start_process(StartFun) ->
    start_process(StartFun, fun() -> receive {done} -> ok end end).

-spec start_process(StartFun::fun(() -> StartRes), RunFun::fun(() -> any())) -> {pid(), StartRes}.
start_process(StartFun, RunFun) ->
    start_process(StartFun, RunFun, false, spawn).

-spec start_process(StartFun::fun(() -> StartRes), RunFun::fun(() -> any()),
                    TrapExit::boolean(), Spawn::spawn | spawn_link)
        -> {pid(), StartRes}.
start_process(StartFun, RunFun, TrapExit, Spawn) ->
    process_flag(trap_exit, TrapExit),
    Owner = self(),
    Node = erlang:Spawn(
             fun() ->
                     try Res = StartFun(),
                         Owner ! {started, self(), Res}
                     catch Level:Reason ->
                               Owner ! {killed},
                               erlang:Level(Reason)
                     end,
                     RunFun()
             end),
    receive
        {started, Node, Res} -> {Node, Res};
        {killed} -> ?ct_fail("start_process(~.0p, ~.0p, ~.0p, ~.0p) failed",
                             [StartFun, RunFun, TrapExit, Spawn])
    end.

%% @doc Starts a sub-process which executes the given function and then waits forever.
-spec start_subprocess(StartFun::fun(() -> StartRes)) -> {pid(), StartRes}.
start_subprocess(StartFun) ->
    start_subprocess(StartFun, fun() -> receive {done} -> ok end end).

-spec start_subprocess(StartFun::fun(() -> StartRes), RunFun::fun(() -> any())) -> {pid(), StartRes}.
start_subprocess(StartFun, RunFun) ->
    start_process(StartFun, RunFun, true, spawn_link).

%% @doc Starts the minimal number of processes in order for non-ring unit tests
%%      to be able to execute (pid_groups, config, log).
-spec start_minimal_procs(CTConfig, ConfigOptions::[{atom(), term()}],
                          StartCommServer::boolean()) -> CTConfig when is_subtype(CTConfig, list()).
start_minimal_procs(CTConfig, ConfigOptions, StartCommServer) ->
    {Pid, _} =
        start_process(
          fun() ->
                  {ok, _GroupsPid} = pid_groups:start_link(),
                  {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, CTConfig),
                  ConfigOptions2 = prepare_config(
                                     [{config, [{log_path, PrivDir} | ConfigOptions]}]),
                  config:init(ConfigOptions2),
                  tester:start_pseudo_proc(),
                  case StartCommServer of
                      true ->
                          {ok, _CommPid} = sup:sup_start(
                                             {local, no_name},
                                             sup_comm_layer, []),
                          Port = get_scalaris_port(),
                          comm_server:set_local_address({127,0,0,1}, Port);
                      false -> ok
                  end
          end),
    [{wrapper_pid, Pid} | CTConfig].

%% @doc Stops the processes started by start_minimal_procs/3 given the
%%      ct config term.
-spec stop_minimal_procs(CTConfig) -> CTConfig when is_subtype(CTConfig, list()).
stop_minimal_procs(CTConfig)  ->
    case lists:keytake(wrapper_pid, 1, CTConfig) of
        {value, {wrapper_pid, Pid}, CTConfig1} ->
            LogLevel = config:read(log_level),
            log:set_log_level(none),
            exit(Pid, kill),
            stop_pid_groups(),
            log:set_log_level(LogLevel),
            CTConfig1;
        false -> CTConfig
    end.

-type process_info() ::
    {pid(), InitCall::mfa(), CurFun::mfa(),
     {RegName::atom() | not_registered, Info::term() | failed | no_further_infos}}.

-spec get_processes() -> [process_info()].
get_processes() ->
    [begin
         InitCall =
             case proc_lib:initial_call(X) of
                 false -> element(2, lists:keyfind(initial_call, 1, Data));
                 {Module, Function, Args} -> {Module, Function, length(Args)}
             end,
         CurFun = element(2, lists:keyfind(current_function, 1, Data)),
         RegName = case lists:keyfind(registered_name, 1, Data) of
                       false -> not_registered;
                       RegNameTpl -> element(2, RegNameTpl)
                   end,
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
         {X, InitCall, CurFun, {RegName, Info}}
     end
     || X <- processes(),
        Data <- [process_info(X, [current_function, initial_call, registered_name])],
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
%%     ct:pal("Proc-New: ~.0p~n", [OnlyNew]),
    Killed = [begin
%%                   ct:pal("killing ~.0p~n", [Proc]),
                  Tabs = util:ets_tables_of(X),
                  try erlang:exit(X, kill) of
                      true ->
                          util:wait_for_process_to_die(X),
                          _ = [begin
%%                                    ct:pal("waiting for table ~.0p to disappear~n~p",
%%                                           [Tab, ets:info(Tab)]),
                                   util:wait_for_ets_table_to_disappear(X, Tab)
                               end || Tab <- Tabs ],
                              {ok, Proc}
                  catch _:_ -> {fail, Proc}
                  end
              end || {X, InitCall, CurFun, {RegName, _Info}} = Proc <- OnlyNew,
                     not (InitCall =:= {test_server_sup, timetrap, 3} andalso
                              CurFun =:= {test_server_sup, timetrap, 3}),
                     not (InitCall =:= {test_server_sup, timetrap, 2} andalso
                              CurFun =:= {test_server_sup, timetrap, 2}),
                     not (InitCall =:= {test_server_gl, init, 1}),
                     not (CurFun =:= {prim_file, drv_get_response, 1}), % spawned by R16 for io:put_chars/3
                     RegName =/= cover_server,
                     X =/= self(),
                     X =/= whereis(timer_server),
                     element(1, CurFun) =/= file_io_server],
    case lists:member({?quiet}, Options) of
        false ->
            ct:pal("Killed processes: ~.0p~n", [Killed]);
        true -> ok
    end.

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

-spec init_per_group(atom(), Config) -> Config when is_subtype(Config, kv_opts()).
init_per_group(_Group, Config) -> Config.

-spec end_per_group(atom(), kv_opts()) -> {return_group_result, ok | failed}.
end_per_group(_Group, Config) ->
    Status = test_server:lookup_config(tc_group_result, Config),
    case proplists:get_value(failed, Status) of
        [] ->                                   % no failed cases
            {return_group_result, ok};
        _Failed ->                              % one or more failed
            {return_group_result, failed}
    end.

-type ring_data(DBType) ::
          [{pid(),
            {intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()},
            DBType,
            {pred, comm:erl_local_pid_plain()},
            {succ, comm:erl_local_pid_plain()},
            ok | timeout}] |
          {exception, Level::throw | error | exit, Reason::term(), Stacktrace::term()}.

-spec get_ring_data(full) -> ring_data(db_dht:db_as_list());
                   (kv) -> ring_data([{?RT:key(), client_version()}]).
get_ring_data(Type) ->
    Self = self(),
    erlang:spawn(
      fun() ->
              try
                  DHTNodes = pid_groups:find_all(dht_node),
                  Data =
                      lists:sort(
                        fun(E1, E2) ->
                                erlang:element(2, E1) =< erlang:element(2, E2)
                        end,
                        [begin
                             comm:send_local(DhtNode,
                                             {unittest_get_bounds_and_data, comm:this(), Type}),
                             receive
                                 {unittest_get_bounds_and_data_response, Bounds, Data, Pred, Succ} ->
                                     {DhtNode, Bounds, Data,
                                      {pred, comm:make_local(node:pidX(Pred))},
                                      {succ, comm:make_local(node:pidX(Succ))}, ok}
                             % we are in a separate process, any message in the
                             % message box should not influence the calling process
                                 after 750 -> {DhtNode, empty, [], {pred, null}, {succ, null}, timeout}
                             end
                         end || DhtNode <- DHTNodes]),
                  Self ! {data, Data}
              catch
                  Level:Reason ->
                      Result =
                          case erlang:whereis(ct_test_ring) of
                              undefined -> [];
                              _         -> {exception, Level, Reason, erlang:get_stacktrace()}
                          end,
                      Self ! {data, Result}
              end
      end),
    receive {data, Data} -> Data
    end.

-spec print_ring_data() -> ok.
print_ring_data() ->
    DataAll = get_ring_data(full),
    ct:pal("Scalaris ring data:~n~.0p~n", [DataAll]).

-spec print_proto_sched_stats(now | now_short | at_end_if_failed | at_end_if_failed_append) -> ok.
print_proto_sched_stats(When) ->
    % NOTE: keep the default tag in sync!
    print_proto_sched_stats(When, default).

-spec print_proto_sched_stats(now | now_short | at_end_if_failed | at_end_if_failed_append,
                              TraceId::term()) -> ok.
print_proto_sched_stats(now, TraceId) ->
    ct:pal("Proto scheduler stats: ~.2p", [proto_sched:get_infos(TraceId)]);
print_proto_sched_stats(now_short, TraceId) ->
    ct:pal("Proto scheduler stats: ~.2p",
           [proto_sched:info_shorten_messages(proto_sched:get_infos(TraceId), 200)]);
print_proto_sched_stats(at_end_if_failed, TraceId) ->
    unittest_global_state:insert(proto_sched_stats, proto_sched:get_infos(TraceId)),
    ok;
print_proto_sched_stats(at_end_if_failed_append, TraceId) ->
    Current = case unittest_global_state:lookup(proto_sched_stats) of
                  failed -> [];
                  X when is_list(X) -> X
              end,
    unittest_global_state:insert(proto_sched_stats,
                                 Current ++ proto_sched:get_infos(TraceId)),
    ok.

-spec macro_equals(Actual::any(), ExpectedVal::any(), ActualStr::string(),
                   ExpectedStr::string(), Note::term()) -> true | no_return().
macro_equals(Actual, ExpectedVal, ActualStr, ExpectedStr, Note) ->
    macro_compare(fun erlang:'=:='/2, Actual, ExpectedVal, "=:=", ActualStr, ExpectedStr, Note).

-spec macro_compare(CompFun::fun((T, T) -> boolean()), Actual::any(), ExpectedVal::any(),
                    CompFunStr::string(), ActualStr::string(), ExpectedStr::string(),
                    Note::term()) -> true | no_return().
macro_compare(CompFun, Actual, ExpectedVal, CompFunStr, ActualStr, ExpectedStr, Note) ->
    case CompFun(Actual, ExpectedVal) of
        true  -> true;
        false -> macro_equals_failed(Actual, ExpectedVal, CompFunStr, ActualStr, ExpectedStr, Note)
    end.

-spec macro_equals_failed(ActualVal::any(), ExpectedVal::any(), CompFunStr::string(),
                          ActualStr::string(), ExpectedStr::string(), Note::term())
        -> no_return().
macro_equals_failed(ActualVal, ExpectedVal, CompFunStr, ActualStr, ExpectedStr, Note) ->
    log:log(error, "Failed (in ~.0p)~n"
           " Call:        ~s~n"
           " Returns:     ~.0p~n"
           " is not (~s)~n"
           " Exp. term:   ~s~n"
           " Exp. return: ~.0p~n"
           " Note:        ~.0p~n"
           " Stacktrace:  ~p~n"
           " Linetrace:   ~p~n",
           [self(), ActualStr, ActualVal, CompFunStr, ExpectedStr, ExpectedVal, Note,
            util:get_stacktrace(), util:get_linetrace()]),
    ?ct_fail("~s returned ~.0p which is not ~s the expected ~s that returns ~.0p. Note: ~.0p",
             [ActualStr, ActualVal, CompFunStr, ExpectedStr, ExpectedVal, Note]).

-spec expect_no_message_timeout(Timeout::pos_integer()) -> true | no_return().
expect_no_message_timeout(Timeout) ->
    receive
        ActualMessage ->
            ?ct_fail("expected no message but got \"~.0p\"~n", [ActualMessage])
    after Timeout -> true
end.

-spec check_ring_load(ExpLoad::pos_integer()) -> true | no_return().
check_ring_load(ExpLoad) ->
    Ring = statistics:get_ring_details(),
    Load = statistics:get_total_load(load, Ring),
    ?equals(Load, ExpLoad).

-spec check_ring_data() -> boolean().
check_ring_data() ->
    check_ring_data(250, 8).

-spec check_ring_data(Timeout::pos_integer(), Retries::non_neg_integer()) -> boolean().
check_ring_data(Timeout, Retries) ->
    Data = lists:flatmap(
             fun({_Pid, _Interval, Data, {pred, _PredPid}, {succc, _SuccPid}, _Result}) -> Data;
                (_) -> []
             end,
             get_ring_data(full)),
    case Retries < 1 of
        true ->
            check_ring_data_all(Data, true);
        _ ->
            case check_ring_data_all(Data, false) of
                true -> true;
                _    -> timer:sleep(Timeout),
                        check_ring_data(Timeout, Retries - 1)
            end
    end.

check_ring_data_all(Data, FailOnError) ->
    check_ring_data_unique(Data, FailOnError) andalso
        check_ring_data_repl_fac(Data, FailOnError) andalso
        check_ring_data_lock_free(Data, FailOnError) andalso
        check_ring_data_repl_exist(Data, FailOnError).

-spec check_ring_data_unique(Data::[db_entry:entry()], FailOnError::boolean()) -> boolean().
check_ring_data_unique(Data, FailOnError) ->
    UniqueData = lists:usort(fun(A, B) ->
                                     db_entry:get_key(A) =< db_entry:get_key(B)
                             end, Data),
    DataDiff = lists:subtract(Data, UniqueData),
    case FailOnError of
        true ->
            ?equals_w_note(DataDiff, [], "duplicate elements detected"),
            true;
        _ when DataDiff =:= [] ->
            true;
        _ ->
            ct:pal("check_ring_data: duplicate elements detected: ~p~n", [DataDiff]),
            false
    end.

-spec check_ring_data_repl_fac(Data::[db_entry:entry()], FailOnError::boolean()) -> boolean().
check_ring_data_repl_fac(Data, FailOnError) ->
    ReplicaKeys0 = ?RT:get_replica_keys(?RT:hash_key("0")),
    DataLen = length(Data),
    ReplicaLen = length(ReplicaKeys0),
    ReplicaRem = DataLen rem ReplicaLen,
    case FailOnError of
        true ->
            ?equals(ReplicaRem, 0);
        _ when ReplicaRem =:= 0 ->
            true;
        _ ->
            ct:pal("check_ring_data: length(Data) = ~p not multiple of ~p~n", [DataLen, ReplicaLen]),
            false
    end.

-spec check_ring_data_lock_free(Data::[db_entry:entry()], FailOnError::boolean()) -> boolean().
check_ring_data_lock_free(Data, FailOnError) ->
    Locked = [E || E <- Data, db_entry:is_locked(E)],
    case FailOnError of
        true ->
            ?equals_w_note(Locked, [], "ring is not lock-free"),
            true;
        _ when Locked =:= [] ->
            true;
        _ ->
            ct:pal("check_ring_data: locked elements found: ~p~n", [Locked]),
            false
    end.

-spec check_ring_data_repl_exist(Data::[db_entry:entry()], FailOnError::boolean()) -> boolean().
check_ring_data_repl_exist(Data, FailOnError) ->
    ElementsNotAllReplicas =
        [E || E <- Data, not data_contains_all_replicas(Data, db_entry:get_key(E))],
    case FailOnError of
        true ->
            ?equals_w_note(ElementsNotAllReplicas, [], "missing replicas found"),
            true;
        _ when ElementsNotAllReplicas =:= [] ->
            true;
        _ ->
            ct:pal("check_ring_data: elements with missing replicas:", [ElementsNotAllReplicas]),
            false
    end.

data_contains_all_replicas(Data, Key) ->
    Keys = ?RT:get_replica_keys(Key),
    Replicas = [E || E <- Data, lists:member(db_entry:get_key(E), Keys)],
    length(Keys) =:= length(Replicas).

% @doc returns only left-open intervals or intervals:all() 
%      rrepair relies on this
-spec build_interval(intervals:key(), intervals:key()) -> intervals:interval().
build_interval(?MINUS_INFINITY, ?PLUS_INFINITY) -> intervals:all();
build_interval(?PLUS_INFINITY, ?MINUS_INFINITY) -> intervals:all();
build_interval(A, A) -> intervals:all();
build_interval(A, B) when A < B -> intervals:new('(', A, B, ']');
build_interval(A, B) when A > B -> intervals:new('(', B, A, ']').

%% @doc can be used to filter empty entries out of a list of db_entries
-spec db_entry_not_null(db_entry:entry()) -> boolean().
db_entry_not_null(Entry) ->
    not db_entry:is_null(Entry).

%% @doc removes duplicates and db_entries that are null
-spec scrub_data(db_dht:db_as_list()) -> db_dht:db_as_list().
scrub_data(Data) ->
    lists:filter(
        fun db_entry_not_null/1, 
        lists:usort(fun(A, B) ->
                 db_entry:get_key(A) =< db_entry:get_key(B)
            end, lists:reverse(Data))).
