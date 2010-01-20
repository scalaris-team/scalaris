%  Copyright 2007-2009 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : cs_sup_standalone.erl
%%% Author  : Thorsten Schuett <schuett@csr-pc11.zib.de>
%%% Description : Supervisor for "standalone" mode
%%%
%%% Created : 17 Aug 2007 by Thorsten Schuett <schuett@csr-pc11.zib.de>
%%%-------------------------------------------------------------------
-module(cs_sup_standalone).
-behaviour(supervisor).
-include("autoconf.hrl").

%% API
-export([start_link/0, scan_environment/0]).
%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    Link = supervisor:start_link({local, main_sup}, ?MODULE, []),
    case Link of
        {ok, _Pid} ->
            ok;
        ignore ->
            io:format("error in starting standalone supervisor: supervisor should not return ignore~n");
        {error, Error} ->
            io:format("error in starting standalone supervisor: ~p~n", [Error])
    end,
    scan_environment(),
    Link.

-ifdef(HAVE_TCERL).
start_tcerl() ->
    tcerl:start().
-else.
start_tcerl() ->
    ok.
-endif.

init([]) ->
    randoms:start(),
    inets:start(),
    %% util:logger(),
    start_tcerl(),
    error_logger:logfile({open, preconfig:cs_log_file()}),
    Config =
        util:sup_worker_desc(config, config, start_link,
                             [[preconfig:config(), preconfig:local_config()]]),
    Service =
        util:sup_worker_desc(service_per_vm, service_per_vm, start_link),
    FailureDetector =
        util:sup_worker_desc(fd, fd, start_link),
    CommunicationPort =
        util:sup_supervisor_desc(comm_port_sup, comm_port_sup, start_link),
    Logger =
        util:sup_worker_desc(logger, log, start_link),
    ChordSharp =
        {chordsharp,
         {cs_sup_or, start_link, []},
         permanent,
         brutal_kill,
         supervisor,
         [cs_sup_or]
        },
    YAWS =
        util:sup_worker_desc(yaws, yaws_wrapper, try_link,
                             [ preconfig:docroot(),
                               [{port, preconfig:yaws_port()},
                                {listen, {0,0,0,0}}],
                               [{max_open_conns, 800},
                                {access_log, false},
                                {logdir, preconfig:log_path()}]
                              ]),
    BenchServer =
        util:sup_worker_desc(bench_server, bench_server, start_link),
    AdminServer =
        util:sup_worker_desc(admin_server, admin, start_link),
    Ganglia =
        util:sup_worker_desc(ganglia_server, ganglia, start_link),
    MonitorTiming =
        util:sup_worker_desc(monitor_timing, monitor_timing, start_link),
    {ok,{{one_for_all,10,1},
         [
          Config,
          Service,
          Logger,
          MonitorTiming,
          FailureDetector,
          CommunicationPort,
          AdminServer,
          YAWS,
          BenchServer,
          Ganglia,
          ChordSharp
         ]}}.

scan_environment() ->
    loadInstances(preconfig:cs_instances()),
    ok.

loadInstances(undefined) ->
    ok;
loadInstances(Instances) ->
    admin:add_nodes(Instances - 1).
