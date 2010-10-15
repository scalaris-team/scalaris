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
         make_ring_with_ids/1, make_ring/1, stop_ring/0, stop_ring/1,
         stop_pid_groups/0,
         check_ring_size/1,
         wait_for_stable_ring/0, wait_for_stable_ring_deep/0,
         wait_for_process_to_die/1,
         start_process/1, start_process/2,
         start_subprocess/1, start_subprocess/2,
         get_all_children/1]).

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

-spec make_ring_with_ids([?RT:key()] | fun(() -> [?RT:key()])) -> pid().
make_ring_with_ids(Ids) when is_list(Ids) ->
    make_ring_with_ids(fun () -> Ids end);
make_ring_with_ids(IdsFun) when is_function(IdsFun, 0) ->
    % note: do not call IdsFun before the initial setup
    %       (it might use config or another process)
    fix_cwd(),
    error_logger:tty(true),
    undefined = ets:info(config_ets),
    Pid = start_process(
            fun() ->
                    ct:pal("Trying to build Scalaris~n"),
                    erlang:register(ct_test_ring, self()),
                    randoms:start(),
                    pid_groups:start_link(),
                    sup_scalaris:start_link(boot, [{boot_server, empty}]),
                    boot_server:connect(),
                    Ids = IdsFun(),
                    admin:add_node([{first}, {{idholder, id}, hd(Ids)}]),
                    [admin:add_node_at_id(Id) || Id <- tl(Ids)],
                    ok
            end),
%%     timer:sleep(1000),
    Ids = IdsFun(),
    check_ring_size(length(Ids)),
    wait_for_stable_ring(),
    Size = check_ring_size(length(Ids)),
    ct:pal("Scalaris has booted with ~p nodes...~n", [Size]),
    Pid.

-spec make_ring(pos_integer()) -> pid().
make_ring(Size) ->
    fix_cwd(),
    error_logger:tty(true),
    undefined = ets:info(config_ets),
    Pid = start_process(
            fun() ->
                    ct:pal("Trying to build Scalaris~n"),
                    erlang:register(ct_test_ring, self()),
                    randoms:start(),
                    pid_groups:start_link(),
                    sup_scalaris:start_link(boot),
                    boot_server:connect(),
                    admin:add_nodes(Size - 1),
                    ok
            end),
%%     timer:sleep(1000),
    check_ring_size(Size),
    wait_for_stable_ring(),
    check_ring_size(Size),
    ct:pal("Scalaris has booted with ~p node(s)...~n", [Size]),
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
