%  Copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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

-export([fix_cwd/0,
         make_ring_with_ids/1, make_ring_with_ids/2, make_ring/1, make_ring/2,
         stop_ring/0, stop_ring/1,
         stop_pid_groups/0,
         check_ring_size/1,
         wait_for_stable_ring/0, wait_for_stable_ring_deep/0,
         wait_for_process_to_die/1,
         start_process/1, start_process/2,
         start_subprocess/1, start_subprocess/2,
         get_all_children/1,
         get_processes/0, init_per_suite/1, end_per_suite/1,
         get_ring_data/0, print_ring_data/0]).

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
    fix_cwd(),
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
                    pid_groups:start_link(),
                    sup_scalaris:start_link(boot, [{boot_server, empty} | Options]),
                    boot_server:connect(),
                    Ids = IdsFun(), % config may be needed
                    admin:add_node([{first}, {{dht_node, id}, hd(Ids)}]),
                    [admin:add_node_at_id(Id) || Id <- tl(Ids)],
                    ok
            end),
%%     timer:sleep(1000),
    % need to call IdsFun again (may require config process or others
    % -> can not call it before starting the scalaris process)
    Ids = IdsFun(),
    check_ring_size(length(Ids)),
    wait_for_stable_ring(),
    Size = check_ring_size(length(Ids)),
    ct:pal("Scalaris has booted with ~p node(s)...~n", [Size]),
    print_ring_data(),
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
    fix_cwd(),
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
                    pid_groups:start_link(),
                    sup_scalaris:start_link(boot, Options),
                    boot_server:connect(),
                    admin:add_nodes(Size - 1),
                    ok
            end),
%%     timer:sleep(1000),
    check_ring_size(Size),
    wait_for_stable_ring(),
    check_ring_size(Size),
    ct:pal("Scalaris has booted with ~p node(s)...~n", [Size]),
    print_ring_data(),
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
            log:set_log_level(none),
            exit(Pid, kill),
            wait_for_process_to_die(Pid),
            stop_pid_groups(),
            % do not stop the randoms and inets apps since we do not know
            % whether we started them in make_ring* and whether other processes
            % still rely on it
%%             randoms:stop(),
%%             inets:stop(),
            catch unregister(ct_test_ring),
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
            ct:pal("exception in stop_ring: ~p~n", [Reason]),
            throw(Reason)
    end.

-spec stop_pid_groups() -> ok.
stop_pid_groups() ->
    gen_component:kill(pid_groups),
    wait_for_table_to_disappear(pid_groups),
    catch unregister(pid_groups),
    ok.

-spec wait_for_process_to_die(pid()) -> ok.
wait_for_process_to_die(Pid) ->
    case is_process_alive(Pid) of
        true -> timer:sleep(100),
                wait_for_process_to_die(Pid);
        _    -> ok
    end.

-spec wait_for_table_to_disappear(tid() | atom()) -> ok.
wait_for_table_to_disappear(Table) ->
    case ets:info(Table) of
        undefined -> ok;
        _         -> timer:sleep(100),
                     wait_for_table_to_disappear(Table)
    end.

-spec wait_for_stable_ring() -> ok.
wait_for_stable_ring() ->
    R = admin:check_ring(),
    ct:pal("CheckRing: ~p~n",[R]),
    case R of
        ok -> ok;
        _  -> timer:sleep(500),
              wait_for_stable_ring()
    end.

-spec wait_for_stable_ring_deep() -> ok.
wait_for_stable_ring_deep() ->
    R = admin:check_ring_deep(),
    ct:pal("CheckRingDeep: ~p~n",[R]),
    case R of
        ok -> ok;
        _  -> timer:sleep(500),
              wait_for_stable_ring_deep()
    end.

-spec check_ring_size(non_neg_integer()) -> ok.
check_ring_size(Size) ->
    boot_server:number_of_nodes(),
    RSize = receive {get_list_length_response, L} -> L
            end,
    case (RSize =:= Size) of
        true -> Size;
        _    -> timer:sleep(500),
                check_ring_size(Size)
    end.

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

-spec get_all_children(Supervisor::pid()) -> [pid()].
get_all_children(Supervisor) ->
    AllChilds = [X || X = {_, Pid, _, _} <- supervisor:which_children(Supervisor),
                      Pid =/= undefined],
    WorkerChilds = [Pid ||  {_Id, Pid, worker, _Modules} <- AllChilds],
    SupChilds = [Pid || {_Id, Pid, supervisor, _Modules} <- AllChilds],
    lists:flatten([WorkerChilds | [get_all_children(S) || S <- SupChilds]]).

-spec get_processes() -> [{pid(), InitCall::mfa(), CurFun::mfa(), Info::term | failed | no_further_infos}].
get_processes() ->
    _Processes = [begin
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
                                      FileName -> {file, FileName}
                                  end;
                              {_, {gen_server, _, _}} ->
                                  sys:get_status(X);
                              _ -> no_further_infos
                          end,
                      {X, InitCall, CurFun, Info}
                  end
                  || X <- processes(),
                     Data <- [process_info(X, [current_function, initial_call])],
                     Data =/= undefined].

%% @doc Generic init_per_suite for all unit tests. Prints current state
%%      information and stores information about all running processes.
%%      Also starts the crypto application needed for unit tests using the
%%      tester.
-spec init_per_suite([tuple()]) -> [tuple()].
init_per_suite(Config) ->
    Processes = get_processes(),
    ct:pal("Starting unittest ~p", [ct:get_status()]),
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
    ct:pal("Stopping unittest ~p", [ct:get_status()]),
    %error_logger:tty(false),
    unittest_helper:stop_ring(),
    % the following might still be running in case there was no ring:
    randoms:stop(),
    inets:stop(),
    {processes, OldProcesses} = lists:keyfind(processes, 1, Config),
    NewProcesses = get_processes(),
    {_OnlyOld, _Both, OnlyNew} = util:split_unique(OldProcesses, NewProcesses,
                                                 fun(P1, P2) ->
                                                         element(1, P1) =< element(1, P2)
                                                 end, fun(_P1, P2) -> P2 end),
%%     ct:pal("Old: ~.0p~n", [OnlyOld]),
%%     ct:pal("Both: ~.0p~n", [Both]),
%%     ct:pal("New: ~.0p~n", [OnlyNew]),
    Killed = [begin
%%                   ct:pal("killing ~.0p", [Proc]),
                  try erlang:exit(X, kill) of
                      true -> wait_for_process_to_die(X),
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
    ct:pal("Killed processes: ~.0p", [Killed]),
    Config.

-spec get_ring_data() -> [{{intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()}, ?DB:db_as_list(), ok | timeout}].
get_ring_data() ->
    DHTNodes = pid_groups:find_all(dht_node),
    lists:sort(
      [begin
           comm:send_local(DhtNode,
                           {unittest_get_bounds_and_data, comm:this()}),
           % note: no timeout possible - can not leave messages in queue!
           receive
               {unittest_get_bounds_and_data_response, Bounds, Data} ->
                   {Bounds, Data, ok}
%%            after 500 -> {empty, [], timeout}
           end
       end || DhtNode <- DHTNodes]).

-spec print_ring_data() -> ok.
print_ring_data() ->
    DataAll = unittest_helper:get_ring_data(),
    ct:pal("~.0p", [DataAll]).
