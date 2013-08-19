%  @copyright 2007-2013 Zuse Institute Berlin

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
-export([init/1]).

-export([supspec/1, childs/1]).

-export([start_link/0, check_config/0]).

%% used in unittest_helper.erl
-export([start_link/1]).

-spec start_link() -> {ok, Pid::pid()}
                         | {error, Error::{already_started, Pid::pid()}
                                        | term()}.
start_link() -> start_link([]).

%% called by unittest_helper.erl
-spec start_link([tuple()])
        -> {ok, Pid::pid()} | 
           {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link(Options) ->
    ServiceGroup = "basic_services",
    Link = sup:sup_start({local, main_sup}, ?MODULE,
                         [{service_group, ServiceGroup} | Options]),
    case Link of
        {ok, SupRef} when is_pid(SupRef) ->
            add_additional_nodes(),
            case util:is_unittest() of
                true -> ok;
                _    -> io:format("Scalaris started successfully."
                                  " Hit <return> to see the erlang shell prompt.~n")
            end,
            ok;
%%        ignore ->
%%            error_logger:error_msg(
%%              "error in starting sup_scalaris supervisor:"
%%              " supervisor should not return ignore~n",
%%              []);
        {error, Error} ->
            error_logger:error_msg(
              "error in starting sup_scalaris supervisor: ~p~n",
              [Error])
   end,
   Link.

-spec init([tuple()])
        -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                  PeriodInSeconds::pos_integer()},
                 [ProcessDescr::supervisor:child_spec()]}}.
init(Options) ->
    start_first_services(Options),
    supspec(Options).

-spec supspec(any()) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                  PeriodInSeconds::pos_integer()}, []}}.
supspec(_) ->
    {ok, {{one_for_one, 10, 1}, []}}.

-spec childs(list(tuple())) -> [any()].
childs(Options) ->
    {service_group, ServiceGroup} = lists:keyfind(service_group, 1, Options),
    StartMgmtServer = case config:read(start_mgmt_server) of
                          failed -> false;
                          X -> X
                      end,
    DHTNodeModule = case config:read(start_dht_node) of
                          failed -> false;
                          Y -> Y
                      end,

    AdminServer = sup:worker_desc(admin_server, admin, start_link),
    BenchServer = sup:worker_desc(bench_server, bench_server, start_link),
    MgmtServer = sup:worker_desc(mgmt_server, mgmt_server, start_link,
                                      [ServiceGroup, []]),
    MgmtServerDNCache =
        sup:worker_desc(deadnodecache, dn_cache, start_link,
                             [ServiceGroup]),
    CommLayer =
        sup:supervisor_desc(sup_comm_layer, sup_comm_layer, start_link),
    CommStats =
        sup:worker_desc(comm_stats, comm_stats, start_link, ["comm_layer"]),
    Config = sup:worker_desc(config, config, start_link, [Options]),
    ClientsDelayer =
        sup:worker_desc(clients_msg_delay, msg_delay, start_link,
                             ["clients_group"]),
    BasicServicesDelayer =
        sup:worker_desc(basic_services_msg_delay, msg_delay, start_link,
                             [ServiceGroup]),
    ClientsMonitor =
        sup:worker_desc(clients_monitor, monitor, start_link, ["clients_group"]),
    DHTNodeJoinAt = case util:app_get_env(join_at, random) of
                         random -> [];
                         Id     -> [{{dht_node, id}, Id}, {skip_psv_lb}]
                     end,
    DhtNodeId = randoms:getRandomString(),
    DHTNodeOptions = DHTNodeJoinAt ++ [{first} | Options], % this is the first dht_node in this VM
    DHTNodeGroup = pid_groups:new("dht_node_"),
    DHTNode = sup:supervisor_desc(DhtNodeId, sup_dht_node, start_link,
                                       [{DHTNodeGroup, [{my_sup_dht_node_id, DhtNodeId}
                                         | DHTNodeOptions]}]),
    FailureDetector = sup:worker_desc(fd, fd, start_link, [ServiceGroup]),
    Ganglia = case config:read(ganglia_enable) of
                  true -> sup:worker_desc(ganglia_server, ganglia, start_link, [ServiceGroup]);
                  _ -> []
              end,
    Logger = sup:worker_desc(logger, log, start_link),
    Monitor =
        sup:worker_desc(monitor, monitor, start_link, [ServiceGroup]),
    Service =
        sup:worker_desc(service_per_vm, service_per_vm, start_link,
                             [ServiceGroup]),
    TraceMPath =
        sup:worker_desc(trace_mpath, trace_mpath, start_link,
                             [ServiceGroup]),
    ProtoSched =
        sup:worker_desc(proto_sched, proto_sched, start_link,
                             [ServiceGroup]),
    YAWS =
        sup:supervisor_desc(yaws, sup_yaws, start_link, []),

    Top =
        sup:worker_desc(top, top, start_link,
                             [ServiceGroup]),

    ServicePaxosGroup = sup:supervisor_desc(
                          sup_service_paxos_group, sup_paxos, start_link,
                          [{ServiceGroup, []}]),
    AutoscaleServer =
        case (config:read(autoscale_server) =:= true) andalso
                 StartMgmtServer of
            true -> sup:worker_desc(autoscale_server, autoscale_server,
                                         start_link, [ServiceGroup]);
            _    -> []
        end,
    %% order in the following list is the start order
    BasicServers = [TraceMPath,
                    ProtoSched,
                    Config,
                    Logger,
                    ClientsDelayer,
                    BasicServicesDelayer,
                    ClientsMonitor,
                    Top,
                    Monitor,
                    Service,
                    CommStats,
                    CommLayer,
                    FailureDetector,
                    AdminServer,
                    ServicePaxosGroup,
                    AutoscaleServer],
    Servers = [YAWS, BenchServer],
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
    lists:flatten([BasicServers, MgmtServers, Servers, DHTNodeServer, Ganglia]).

-spec add_additional_nodes() -> ok.
add_additional_nodes() ->
    Size = config:read(nodes_per_vm),
    log:log(info, "Starting ~B nodes", [Size]),
    _ = api_vm:add_nodes(Size - 1),
    ok.

start_first_services(Options) ->
    util:if_verbose("~p start first services...~n", [?MODULE]),
    util:if_verbose("~p start randoms...~n", [?MODULE]),
    randoms:start(),
    util:if_verbose("~p start config...~n", [?MODULE]),
    _ = config:start_link(Options),
    ErrorLoggerFile = filename:join(config:read(log_path),
                                    config:read(log_file_name_errorlogger)),
    util:if_verbose("~p error logger file ~p.~n", [?MODULE, ErrorLoggerFile]),
    case error_logger:logfile({open, ErrorLoggerFile}) of
        ok -> ok;
        {error, Reason} ->
            error_logger:error_msg("cannot open logfile ~.0p: ~.0p",
                                   [ErrorLoggerFile, Reason])
    end,
    util:if_verbose("~p start inets~n", [?MODULE]),
    _ = inets:start(),
    util:if_verbose("~p start first services done.~n", [?MODULE]).

%% @doc Checks whether config parameters of the cyclon process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_string(log_path) and
    config:cfg_is_string(log_file_name_errorlogger) and
    config:cfg_test_and_error(log_path, fun(X) -> X =/= config:read(log_file_name_errorlogger) end,
                          "is not different from log_file_name_errorlogger") and
    config:cfg_is_integer(nodes_per_vm) and
    config:cfg_is_port(yaws_port) and
    config:cfg_is_string(docroot).
