%  @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc Supervisor for a mgmt server or/and "ordinary" node that is
%%      responsible for keeping its processes running.
%%
%%      If one of the supervised processes fails, only the failed
%%      process will be re-started!
%% @end
%% @version $Id$
-module(sup_scalaris).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(supervisor).

-export([start_link/0, start_link/1, init/1, check_config/0]).

-spec start_link()
        -> {ok, Pid::pid()} | ignore |
           {error, Error::{already_started, Pid::pid()} | term()}.
start_link() -> start_link([]).

-spec start_link(list(tuple()))
        -> {ok, Pid::pid()} | ignore |
           {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link(Options) ->
    Link = supervisor:start_link({local, main_sup}, ?MODULE, Options),
    case Link of
        {ok, _Pid} ->
            ok;
        ignore ->
            error_logger:error_msg("error in starting scalaris ~p supervisor: supervisor should not return ignore~n",
                      []);
        {error, Error} ->
            error_logger:error_msg("error in starting scalaris ~p supervisor: ~p~n",
                      [Error])
    end,
    add_additional_nodes(),
    Link.

-spec init([tuple()])
        -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                  PeriodInSeconds::pos_integer()}, [ProcessDescr::any()]}}.
init(Options) ->
    randoms:start(),
    _ = config:start_link2(Options),
    ServiceGroup = "basic_services",
    ErrorLoggerFile = filename:join(config:read(log_path),
                                    config:read(log_file_name_errorlogger)),
    case error_logger:logfile({open, ErrorLoggerFile}) of
        ok -> ok;
        {error, Reason} -> error_logger:error_msg("can not open logfile ~.0p: ~.0p",
                                                  [ErrorLoggerFile, Reason])
    end,
    _ = inets:start(),
    {ok, {{one_for_one, 10, 1}, my_process_list(ServiceGroup, Options)}}.

-spec my_process_list(pid_groups:groupname(), list(tuple())) -> [any()].
my_process_list(ServiceGroup, Options) ->
    StartMgmtServer = case config:read(start_mgmt_server) of
                          failed -> false;
                          X -> X
                      end,
    DHTNodeModule = case config:read(start_dht_node) of
                          failed -> false;
                          Y -> Y
                      end,

    AdminServer = util:sup_worker_desc(admin_server, admin, start_link),
    BenchServer = util:sup_worker_desc(bench_server, bench_server, start_link),
    MgmtServer = util:sup_worker_desc(mgmt_server, mgmt_server, start_link,
                                      [ServiceGroup, []]),
    MgmtServerDNCache =
        util:sup_worker_desc(deadnodecache, dn_cache, start_link,
                             [ServiceGroup]),
    CommLayer =
        util:sup_supervisor_desc(sup_comm_layer, sup_comm_layer, start_link),
    Config = util:sup_worker_desc(config, config, start_link2, [Options]),
    ClientsDelayer =
        util:sup_worker_desc(clients_msg_delay, msg_delay, start_link,
                             ["clients_group"]),
    ClientsMonitor =
        util:sup_worker_desc(clients_monitor, monitor, start_link, ["clients_group"]),
    DHTNodeJoinAt = case util:app_get_env(join_at, random) of
                         random -> [];
                         Id     -> [{{dht_node, id}, Id}]
                     end,
    DHTNodeOptions = DHTNodeJoinAt ++ [{first} | Options], % this is the first dht_node in this VM
    DHTNode = util:sup_supervisor_desc(dht_node, sup_dht_node, start_link,
                                       [[{my_sup_dht_node_id, dht_node}
                                         | DHTNodeOptions]]),
    FailureDetector = util:sup_worker_desc(fd, fd, start_link, [ServiceGroup]),
    Ganglia = util:sup_worker_desc(ganglia_server, ganglia, start_link),
    Logger = util:sup_supervisor_desc(logger, log, start_link),
    Monitor =
        util:sup_worker_desc(monitor, monitor, start_link, [ServiceGroup]),
    Service =
        util:sup_worker_desc(service_per_vm, service_per_vm, start_link,
                             [ServiceGroup]),
    YAWS =
        util:sup_supervisor_desc(yaws, sup_yaws, start_link, []),

    ServicePaxosGroup = util:sup_supervisor_desc(
                          sup_service_paxos_group, sup_paxos, start_link,
                          [ServiceGroup, []]),
    %% order in the following list is the start order
    BasicServers = [Config,
                    Logger,
                    ClientsMonitor,
                    Monitor,
                    Service,
                    CommLayer,
                    FailureDetector,
                    AdminServer,
                    ClientsDelayer,
                    ServicePaxosGroup],
    Servers = [YAWS, BenchServer, Ganglia],
    MgmtServers =
        case StartMgmtServer orelse util:is_unittest() of
            true -> [MgmtServerDNCache, MgmtServer];
            false -> []
        end,
    DHTNodeServer =
        case DHTNodeModule of
            false -> []; %% no dht node requested
            _ -> [DHTNode]
        end,
    lists:flatten([BasicServers, MgmtServers, Servers, DHTNodeServer]).

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
