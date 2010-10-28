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
%% @version $Id: sup_scalaris.erl 1085 2010-09-02 16:09:37Z kruber@zib.de $
-module(sup_scalaris2).
-author('schuett@zib.de').
-vsn('$Id: sup_scalaris.erl 1085 2010-09-02 16:09:37Z kruber@zib.de $').

-behaviour(supervisor).

-export([start_link/1, start_link/2, init/1]).

-ifdef(with_export_type_support).
-export_type([supervisor_type/0]).
-endif.

-type supervisor_type() :: boot | node.

-spec start_link(supervisor_type()) -> {ok, Pid::pid()}
                                     | ignore
                                     | {error, Error::{already_started,
                                                       Pid::pid()}
                                     | term()}.
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
    scan_environment(),
    Link.

-spec init({supervisor_type(), list(tuple())}) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                                                         PeriodInSeconds::pos_integer()},
                                                        [ProcessDescr::any()]}}.
init({SupervisorType, Options}) ->
    randoms:start(),
    ServiceGroup = pid_groups:new("basic_services_"),
    error_logger:logfile({open, preconfig:cs_log_file()}),
    inets:start(),
    {ok, {{one_for_one, 10, 1}, my_process_list(SupervisorType, ServiceGroup, Options)}}.

-spec my_process_list/3 :: (supervisor_type(), pid_groups:groupname(), list(tuple())) -> [any()].
my_process_list(SupervisorType, ServiceGroup, Options) ->
    EmptyBootServer = preconfig:get_env(empty, false) orelse
                          lists:member({boot_server, empty}, Options),
    
    AdminServer =
        util:sup_worker_desc(admin_server, admin, start_link),
    BenchServer =
        util:sup_worker_desc(bench_server, bench_server, start_link),
    BootServerOptions = case EmptyBootServer of
                            false -> [];
                            _     -> [{empty}]
                        end,
    BootServer =
        util:sup_worker_desc(boot_server, boot_server, start_link, [ServiceGroup, BootServerOptions]),
    BootServerDNCache =
        util:sup_worker_desc(deadnodecache, dn_cache, start_link,
                             [ServiceGroup]),
    CommLayer =
        util:sup_supervisor_desc(sup_comm_layer, sup_comm_layer, start_link),
    Config =
        util:sup_worker_desc(config, config, start_link,
                             [[preconfig:config(), preconfig:local_config()]]),
    ClientsDelayer =
        util:sup_worker_desc(clients_msg_delay, msg_delay, start_link,
                             [clients_group]),
    DHTNodeFirstId = case preconfig:get_env(first_id, random) of
                  random -> [];
                  Id     -> [{{idholder, id}, Id}]
              end,
    DHTNodeOptions = DHTNodeFirstId ++ [{first} | Options], % this is the first dht_node in this VM
    DHTNode =
        util:sup_supervisor_desc(group_node, sup_group_node, start_link, [[{my_sup_dht_node_id, group_node} | DHTNodeOptions]]),
    FailureDetector =
        util:sup_worker_desc(fd, fd, start_link, [ServiceGroup]),
    Ganglia =
        util:sup_worker_desc(ganglia_server, ganglia, start_link),
    Logger =
        util:sup_supervisor_desc(logger, log, start_link),
    MonitorTiming =
        util:sup_worker_desc(monitor_timing, monitor_timing, start_link, [ServiceGroup]),
    Service =
        util:sup_worker_desc(service_per_vm, service_per_vm, start_link, [ServiceGroup]),
    YAWS =
        util:sup_worker_desc(yaws, yaws_wrapper, start_link,
                             [ preconfig:docroot(),
                               [{port, preconfig:yaws_port()},
                                {listen, {0,0,0,0}}],
                               [{max_open_conns, 800},
                                {access_log, false},
                                {logdir, preconfig:log_path()}]
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

-spec scan_environment() -> ok.
scan_environment() ->
    admin:add_nodes(preconfig:cs_instances() - 1),
    ok.
