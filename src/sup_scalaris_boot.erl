%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    sup_scalaris_boot.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Supervisor for a boot node (the first node that creates a Scalaris
%%%         network) that is responsible for keeping its processes running.
%%%
%%%         If one of the supervised processes fails, only the failed process
%%%         will be re-started!
%%% @end
%%% Created : 17 Jan 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(sup_scalaris_boot).
-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(supervisor).
-include("autoconf.hrl").
-export([start_link/0, init/1]).

-spec start_link() -> {ok, Pid::cs_send:erl_pid_plain()} | ignore | {error, Error::{already_started, Pid::cs_send:erl_pid_plain()} | term()}.
start_link() ->
    Link = supervisor:start_link({local, main_sup}, ?MODULE, []),
    case Link of
        {ok, _Pid} ->
            ok;
        ignore ->
            io:format("error in starting scalaris boot supervisor: supervisor should not return ignore~n");
        {error, Error} ->
            io:format("error in starting scalaris boot supervisor: ~p~n", [Error])
    end,
    sup_scalaris_node:scan_environment(),
    Link.

-ifdef(HAVE_TCERL).
start_tcerl() ->
    tcerl:start().
-else.
start_tcerl() ->
    ok.
-endif.

-spec init(any()) -> {ok, {{one_for_one, MaxRetries::pos_integer(), PeriodInSeconds::pos_integer()}, [ProcessDescr::any()]}}.
-ifdef(SIMULATION).
init(_Args) ->
    randoms:start(),
    InstanceId = string:concat("boot_server_", randoms:getRandomId()),
    %% error_logger:logfile({open, preconfig:cs_log_file()}),
    inets:start(),
    start_tcerl(),
    {ok, {{one_for_one, 10, 1},
          [ X || {Name, _, _, _, _, _} = X <- my_process_list(InstanceId),
                Name =/= tracer, Name =/= boot_xmlrpc,
                Name =/= bench_server, Name =/= comm_port]}}.
-else.
init(_Args) ->
    randoms:start(),
    InstanceId = string:concat("boot_server_", randoms:getRandomId()),
    %% error_logger:logfile({open, preconfig:cs_log_file()}),
    inets:start(),
    start_tcerl(),
    {ok, {{one_for_one, 10, 1},
          [ X || {Name, _, _, _, _, _} = X <- my_process_list(InstanceId),
                Name =/= tracer, Name =/= boot_xmlrpc]}}.
-endif.

my_process_list(InstanceId) ->
    Tracer =
        util:sup_worker_desc(tracer, tracer, start_link),
    FailureDetector =
        util:sup_worker_desc(fd, fd, start_link),
    Node =
        util:sup_worker_desc(boot_server, boot_server, start_link,
                             [InstanceId]),
    Config =
        util:sup_worker_desc(config, config, start_link,
                             [[preconfig:config(), preconfig:local_config()]]),
    XMLRPC =
        util:sup_worker_desc(boot_xmlrpc, boot_xmlrpc, start_link,
                             [InstanceId]),
    Logger =
        util:sup_worker_desc(logger, log, start_link),
    Service =
        util:sup_worker_desc(service_per_vm, service_per_vm, start_link),
    DHTNode =
        util:sup_supervisor_desc(dht_node, sup_dht_node, start_link, [[first]]),
    YAWS =
        util:sup_worker_desc(yaws, yaws_wrapper, start_link,
                             [ preconfig:docroot(),
                               [{port, preconfig:yaws_port()},
                                {listen, {0,0,0,0}}, {opaque, InstanceId}],
                               [{max_open_conns, 800}, {access_log, false},
                                {logdir, preconfig:log_path()}] ]),
    CommPort =
        util:sup_supervisor_desc(comm_port_sup, comm_port_sup, start_link),
    BenchServer =
        util:sup_worker_desc(bench_server, bench_server, start_link),
    AdminServer =
        util:sup_worker_desc(admin_server, admin, start_link),
    Ganglia =
        util:sup_worker_desc(ganglia_server, ganglia, start_link),
    MonitorTiming =
        util:sup_worker_desc(monitor_timing, monitor_timing, start_link),
    %% order in the following list is the start order
    [Config,
     Service,
     Logger,
     MonitorTiming,
     Tracer,
     CommPort,
     FailureDetector,
     AdminServer,
     XMLRPC,
     Node,
     YAWS,
     BenchServer,
     Ganglia,
     DHTNode].
