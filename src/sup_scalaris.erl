%  @copyright 2007-2017 Zuse Institute Berlin

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

-include("scalaris.hrl").

-behaviour(supervisor).
-export([init/1]).

-export([supspec/1, childs/1]).

-export([start_link/0, check_config/0]).

%% used in unittest_helper.erl
-export([start_link/1]).
-export([stop_first_services/0]).

-spec start_link() -> {ok, Pid::pid()}
                         | {error, Error::{already_started, Pid::pid()}
                                        | term()}.
start_link() -> start_link([]).

%% called by unittest_helper.erl
-spec start_link([tuple()])
        -> {ok, Pid::pid()} |
           {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link(Options) ->
    ServiceGroup = basic_services,
    Pid = spawn(fun() -> timer:sleep(5*1000), dump_processes() end),
    Link = sup:sup_start({local, main_sup}, ?MODULE,
                         [{service_group, ServiceGroup} | Options]),
    case Link of
        {ok, SupRef} when is_pid(SupRef) ->
            case config:read(start_type) of
                recover -> ok;
                _ ->
                    case pid_groups:find_all(dht_node) of
                        [DhtNodePid] ->
                            comm:send_local(DhtNodePid, {join, start});
                        [] -> ok
                    end
            end,
            add_additional_nodes(),
            exit(Pid, kill),
            case util:is_unittest() of
                true -> ok;
                _    -> io:format("Scalaris started successfully."
                                  " Hit <return> to see the erlang shell prompt.~n")
            end,
            ok;
%%        ignore ->
%%            log:log(
%%              "error in starting sup_scalaris supervisor:"
%%              " supervisor should not return ignore~n",
%%              []);
        {error, Error} ->
            log:log("error in starting sup_scalaris supervisor: ~p~n", [Error])
   end,
   Link.

-spec init([tuple()])
        -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                  PeriodInSeconds::pos_integer()},
                 [ProcessDescr::supervisor:child_spec()]}}.
init(Options) ->
    start_first_services(),
    supspec(Options).

-spec supspec(any()) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                  PeriodInSeconds::pos_integer()}, []}}.
supspec(_) ->
    {ok, {{one_for_one, 10, 1}, []}}.

-spec get_dht_node_descs([tuple()]) -> [ProcessDescr::supervisor:child_spec()].
get_dht_node_descs(Options) ->
  case config:read(start_type) of
    recover ->
      %% creating tuples with DB_names different parts : {DB_type, PID_group, DB_name}
      DB_list = db_util:get_recoverable_dbs(),
      %% creating list of all nodes per vm and removing duplicates
      PID_groups = lists:usort([PidGroup || {_, PidGroup, _} <- DB_list]),

      %% create descriptions for all dht nodes to recover:
      [begin
           Option_new =
               [{list_to_atom(Type), DBName}
               || {Type, X, DBName} <- DB_list, X =:= PID_group],
           DhtNodeId = randoms:getRandomString(),
           TheOptions = [{my_sup_dht_node_id, DhtNodeId} | lists:append(Options, Option_new)],
           sup:supervisor_desc(DhtNodeId, sup_dht_node, start_link,
                               [{PID_group, TheOptions}])
       end || PID_group <- PID_groups];
    _ ->
      DHTNodeJoinAt = case {util:app_get_env(join_at, random),
                            util:app_get_env(join_at_list, no_list)} of
                          {random, no_list} ->
                              [];
                          {_, List} when is_list(List) ->
                              [{{dht_node, id}, hd(List)}, {skip_psv_lb}];
                           {Id, no_list} ->
                              [{{dht_node, id}, Id},       {skip_psv_lb}]
                      end,
      DhtNodeId = randoms:getRandomString(),
      DHTNodeOptions = DHTNodeJoinAt ++ [{first} | Options], % this is the first dht_node in this VM
      DHTNodeGroup = pid_groups:new(dht_node),
      sup:supervisor_desc(DhtNodeId, sup_dht_node, start_link,
        [{DHTNodeGroup, [{my_sup_dht_node_id, DhtNodeId}
          | DHTNodeOptions]}])
  end.

-spec childs(list(tuple())) -> [any()].
childs(Options) ->
    {service_group, ServiceGroup} = lists:keyfind(service_group, 1, Options),
    StartMgmtServer = case config:read(start_mgmt_server) of
                          failed -> false;
                          X -> X
                      end,

    AdminServer = sup:worker_desc(admin_server, admin, start_link),
    BenchServer = sup:worker_desc(bench_server, bench_server, start_link, [ServiceGroup]),
    MgmtServer = sup:worker_desc(mgmt_server, mgmt_server, start_link,
                                      [ServiceGroup, []]),
    MgmtServerDNCache =
        sup:worker_desc(deadnodecache, dn_cache, start_link,
                             [ServiceGroup]),
    CommLayer =
        sup:supervisor_desc(sup_comm_layer, sup_comm_layer, start_link),
    CommStats =
        sup:worker_desc(comm_stats, comm_stats, start_link, [comm_layer]),
    ClientsDelayer =
        sup:worker_desc(clients_msg_delay, msg_delay, start_link,
                             [clients_group]),
    BasicServicesDelayer =
        sup:worker_desc(basic_services_msg_delay, msg_delay, start_link,
                             [ServiceGroup]),
    ClientsMonitor =
        sup:worker_desc(clients_monitor, monitor, start_link, [clients_group]),
    %% Moves several lines to get_dht_node_descs() for recovery mechanims
    DHTNodes = get_dht_node_descs(Options),
    FailureDetector = sup:worker_desc(fd, fd, start_link, [ServiceGroup]),
    Ganglia = case config:read(ganglia_enable) of
                  true -> sup:worker_desc(ganglia_server, ganglia, start_link, [ServiceGroup]);
                  _ -> []
              end,
    Monitor =
        sup:worker_desc(monitor, monitor, start_link, [ServiceGroup]),
    MonitorPerf =
        sup:worker_desc(monitor_perf, monitor_perf, start_link, [ServiceGroup]),
    Service =
        sup:worker_desc(service_per_vm, service_per_vm, start_link,
                             [ServiceGroup]),
    TraceMPath =
        sup:worker_desc(trace_mpath, trace_mpath, start_link,
                             [ServiceGroup]),
    ProtoSched =
        sup:worker_desc(proto_sched, proto_sched, start_link,
                             [ServiceGroup]),

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
                    ClientsDelayer,
                    BasicServicesDelayer,
                    ProtoSched,
                    ClientsMonitor,
                    Top,
                    Monitor,
                    CommStats,
                    CommLayer,
                    FailureDetector,
                    AdminServer,
                    ServicePaxosGroup,
                    AutoscaleServer],
    Servers = [BenchServer],
    MgmtServers =
        case StartMgmtServer orelse util:is_unittest() of
            true -> [MgmtServerDNCache, MgmtServer];
            false -> []
        end,
    DHTNodeServer = case config:read(start_type) of
                        nostart -> []; %% no dht node requested
                        first_nostart -> []; %% no dht node requested
                        failed -> [];
                        _ -> DHTNodes
                    end,
    lists:flatten([BasicServers, MgmtServers, Service, Servers, DHTNodeServer, Ganglia, MonitorPerf]).

-spec add_additional_nodes() -> ok.
add_additional_nodes() ->
    Size = config:read(nodes_per_vm),
    log:log(info, "Starting ~B nodes", [Size]),
    _ = case util:app_get_env(join_at_list, no_list) of
            no_list ->
                api_vm:add_nodes(Size - 1);
            List ->
                api_vm:add_nodes_at_ids(tl(List))
        end,
    ok.

start_first_services() ->
    util:if_verbose("~p start first services...~n", [?MODULE]),
    util:if_verbose("~p start randoms...~n", [?MODULE]),
    randoms:start(),
    util:if_verbose("~p start inets~n", [?MODULE]),
    _ = inets:start(),

    %% for mnesia
    start_mnesia(),

    %% for lb_stats and wpool
    _ = application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),
    _ = application:start(sasl),

    %% for lb_stats
    _ = application:load(os_mon),
    IsLbActive = config:read(lb_active) =:= true,
    ExplicitOsMon = config:read(start_os_mon) =:= true,
    case IsLbActive or ExplicitOsMon of
        true -> %% for lb_stats
            application:set_env(os_mon, start_os_sup, false),
            _ = application:start(os_mon),
            ok;
        _ -> ok
    end,
    case config:read(wpool_js) of
        true -> %% for wpool
            _ = application:start(erlang_js),
            ok;
        _ -> ok
    end,
    util:if_verbose("~p start first services done.~n", [?MODULE]).

%% stop the services we started outside the supervisor tree
%% (those who materialized as processes)
-spec stop_first_services() -> ok.
stop_first_services() ->
    %% config seems not available here, so we stop unconditionally
    _ = application:stop(erlang_js),
    _ = application:stop(os_mon),
    _ = application:stop(sasl),
    _ = inets:stop(),
    ok.

-spec start_mnesia() -> ok.
start_mnesia() ->
  case config:read(db_backend) of
    db_mnesia -> db_mnesia:start();
    _ -> ok
  end.

dump_processes() ->
    Processes = [ {X, process_info(X, [initial_call,current_location, current_stacktrace])}
                  || X <- processes()],
    io:format("~p~n", [lists:last(Processes)]),
    ok.
%% @doc Checks whether config parameters exist and are valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_integer(nodes_per_vm) and
    config:cfg_is_port(yaws_port) and
    config:cfg_is_string(docroot) and
    config:cfg_is_in(start_type, [first, joining, quorum, recover, nostart, first_nostart]).
