%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc    Supervisor for a boot node (the first node that
%%         creates a Scalaris network) or "ordinary" node (nodes joining an
%%         existing Scalaris network) that is responsible for keeping its
%%         processes running.
%%
%%         If one of the supervised processes fails, only the failed process
%%         will be re-started!
%% @end
%% @version $Id$
-module(sup_scalaris).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(supervisor).

-export([start_link/1, start_link/2, init/1, check_config/0]).

-ifdef(with_export_type_support).
-export_type([supervisor_type/0]).
-endif.

-type supervisor_type() :: boot | node.

-spec start_link(supervisor_type())
        -> {ok, Pid::pid()} | ignore |
           {error, Error::{already_started, Pid::pid()} | term()}.
start_link(SupervisorType) ->
    start_link(SupervisorType, []).

-spec start_link(supervisor_type(), list(tuple()))
        -> {ok, Pid::pid()} | ignore |
           {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link(SupervisorType, Options) ->
    Link = supervisor:start_link({local, main_sup}, ?MODULE, {SupervisorType, Options}),
    case Link of
        {ok, _Pid} ->
            ok;
        ignore ->
            error_logger:error_msg("error in starting scalaris ~p supervisor: supervisor should not return ignore~n",
                      [SupervisorType]);
        {error, Error} ->
            error_logger:error_msg("error in starting scalaris ~p supervisor: ~p~n",
                      [SupervisorType, Error])
    end,
    add_additional_nodes(),
    Link.

-spec init({supervisor_type(), [tuple()]})
        -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                  PeriodInSeconds::pos_integer()}, [ProcessDescr::any()]}}.
init({SupervisorType, Options}) ->
    randoms:start(),
    _ = config:start_link2(Options),
    ServiceGroup = pid_groups:new("basic_services_"),
    ErrorLoggerFile = filename:join(config:read(log_path),
                                    config:read(log_file_name_errorlogger)),
    case error_logger:logfile({open, ErrorLoggerFile}) of
        ok -> ok;
        {error, Reason} -> error_logger:error_msg("can not open logfile ~.0p: ~.0p",
                                                  [ErrorLoggerFile, Reason])
    end,
    _ = inets:start(),
    {ok, {{one_for_one, 10, 1}, my_process_list(SupervisorType, ServiceGroup, Options)}}.

-spec my_process_list/3 :: (supervisor_type(), pid_groups:groupname(), list(tuple())) -> [any()].
my_process_list(SupervisorType, ServiceGroup, Options) ->
    EmptyBootServer = util:app_get_env(empty, false) orelse
                          lists:member({boot_server, empty}, Options),
    
    AdminServer = util:sup_worker_desc(admin_server, admin, start_link),
    BenchServer = util:sup_worker_desc(bench_server, bench_server, start_link),
    BootServerOptions = case EmptyBootServer of
                            false -> [];
                            _     -> [{empty}]
                        end,
    BootServer = util:sup_worker_desc(boot_server, boot_server, start_link,
                                      [ServiceGroup, BootServerOptions]),
    BootServerDNCache =
        util:sup_worker_desc(deadnodecache, dn_cache, start_link,
                             [ServiceGroup]),
    CommLayer =
        util:sup_supervisor_desc(sup_comm_layer, sup_comm_layer, start_link),
    Config = util:sup_worker_desc(config, config, start_link2, [Options]),
    ClientsDelayer =
        util:sup_worker_desc(clients_msg_delay, msg_delay, start_link,
                             ["clients_group"]),
    DHTNodeFirstId = case util:app_get_env(first_id, random) of
                         random -> [];
                         Id     -> [{{dht_node, id}, Id}]
                     end,
    DHTNodeOptions = DHTNodeFirstId ++ [{first} | Options], % this is the first dht_node in this VM
    DHTNode =
        util:sup_supervisor_desc(dht_node, sup_dht_node, start_link,
                                 [[{my_sup_dht_node_id, dht_node} | DHTNodeOptions]]),
    FailureDetector = util:sup_worker_desc(fd, fd, start_link, [ServiceGroup]),
    Ganglia = util:sup_worker_desc(ganglia_server, ganglia, start_link),
    Logger = util:sup_supervisor_desc(logger, log, start_link),
    MonitorTiming =
        util:sup_worker_desc(monitor_timing, monitor_timing, start_link,
                             [ServiceGroup]),
    Service =
        util:sup_worker_desc(service_per_vm, service_per_vm, start_link,
                             [ServiceGroup]),
    YAWS =
        util:sup_worker_desc(yaws, yaws_wrapper, start_link,
                             [ config:read(docroot),
                               [{port, config:read(yaws_port)},
                                {listen, {0,0,0,0}}],
                               [{max_open_conns, 800},
                                {access_log, false},
                                {logdir, config:read(log_path)}]
                              ]),

    %% order in the following list is the start order
    PreBootServer = [Config,
                     Logger,
                     Service,
                     MonitorTiming,
                     CommLayer,
                     FailureDetector,
                     AdminServer,
                     ClientsDelayer],
    %% do we want to run an empty boot-server?
    PostBootServer = case EmptyBootServer of
                         true -> [YAWS, BenchServer, Ganglia];
                         _    -> [YAWS, BenchServer, Ganglia, DHTNode]
                     end,
    % check whether to start the boot server
    case SupervisorType of
        boot ->
            lists:flatten([PreBootServer, BootServerDNCache, BootServer, PostBootServer]);
        node ->
            lists:flatten([PreBootServer, PostBootServer])
    end.

-spec add_additional_nodes() -> ok.
add_additional_nodes() ->
    Size = config:read(nodes_per_vm),
    log:log(info, "Starting ~B nodes", [Size]),
    _ = admin:add_nodes(Size - 1),
    ok.

%% @doc Checks whether config parameters of the cyclon process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_string(log_path) and
    config:is_string(log_file_name_errorlogger) and
    config:test_and_error(log_path, fun(X) -> X =/= config:read(log_file_name_errorlogger) end,
                          "is not different from log_file_name_errorlogger") and
    config:is_integer(nodes_per_vm) and
    config:is_port(yaws_port) and
    config:is_string(docroot).
