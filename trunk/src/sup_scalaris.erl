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
%%% File    sup_scalaris.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Supervisor for a boot node (the first node that
%%%         creates a Scalaris network) or "ordinary" node (nodes joining an
%%%         existing Scalaris network) that is responsible for keeping its
%%%         processes running.
%%%
%%%         If one of the supervised processes fails, only the failed process
%%%         will be re-started!
%%% @end
%%% Created : 17 Jan 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(sup_scalaris).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(supervisor).
-include("scalaris.hrl").

-export([start_link/1, init/1]).

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
    Link = supervisor:start_link({local, main_sup}, ?MODULE, SupervisorType),
    case Link of
        {ok, _Pid} ->
            ok;
        ignore ->
            io:format("error in starting scalaris ~p supervisor: supervisor should not return ignore~n",
                      [SupervisorType]);
        {error, Error} ->
            io:format("error in starting scalaris ~p supervisor: ~p~n",
                      [SupervisorType, Error])
    end,
    scan_environment(),
    Link.

-spec init(supervisor_type()) -> {ok, {{one_for_one, MaxRetries::pos_integer(),
                                        PeriodInSeconds::pos_integer()},
                                       [ProcessDescr::any()]}}.
init(SupervisorType) ->
    randoms:start(),
    InstanceId = string:concat("scalaris_", randoms:getRandomId()),
    error_logger:logfile({open, preconfig:cs_log_file()}),
    inets:start(),
    {ok, {{one_for_one, 10, 1}, my_process_list(InstanceId, SupervisorType)}}.

-spec my_process_list/2 :: (instanceid(), supervisor_type()) -> [any()].
my_process_list(InstanceId, SupervisorType) ->
    AdminServer =
        util:sup_worker_desc(admin_server, admin, start_link),
    BenchServer =
        util:sup_worker_desc(bench_server, bench_server, start_link),
    CommPort =
        util:sup_supervisor_desc(comm_port_sup, comm_port_sup, start_link),
    Config =
        util:sup_worker_desc(config, config, start_link,
                             [[preconfig:config(), preconfig:local_config()]]),
    DHTNodeOptions = case SupervisorType of
                         boot -> [first];
                         node -> []
                     end,
    DHTNode =
        util:sup_supervisor_desc(dht_node, sup_dht_node, start_link, [DHTNodeOptions]),
    FailureDetector =
        util:sup_worker_desc(fd, fd, start_link),
    Ganglia =
        util:sup_worker_desc(ganglia_server, ganglia, start_link),
    Logger =
        util:sup_worker_desc(logger, log, start_link),
    MonitorTiming =
        util:sup_worker_desc(monitor_timing, monitor_timing, start_link),
    BootServer =
        util:sup_worker_desc(boot_server, boot_server, start_link),
    Service =
        util:sup_worker_desc(service_per_vm, service_per_vm, start_link),
    YAWS =
        util:sup_worker_desc(yaws, yaws_wrapper, start_link,
                             [ preconfig:docroot(),
                               [{port, preconfig:yaws_port()},
                                {listen, {0,0,0,0}}, {opaque, InstanceId}],
                               [{max_open_conns, 800},
                                {access_log, false},
                                {logdir, preconfig:log_path()}]
                              ]),
    %% order in the following list is the start order
    PreBootServer = [Config,
                     Service,
                     Logger,
                     MonitorTiming,
                     CommPort,
                     FailureDetector,
                     AdminServer],
    PostBootServer = [YAWS,
                      BenchServer,
                      Ganglia,
                      DHTNode],
    % check whether to start the boot server
    case SupervisorType of
        boot ->
            lists:flatten([PreBootServer, BootServer, PostBootServer]);
        node ->
            lists:flatten([PreBootServer, PostBootServer])
    end.

scan_environment() ->
    admin:add_nodes(preconfig:cs_instances() - 1),
    ok.
