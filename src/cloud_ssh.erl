% @copyright 2013-2015 Zuse Institute Berlin

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

%% @author Maximilian Michels <michels@zib.de>
%% @doc Cloud SSH starts or stops erlang vms on ssh hosts based on alarms
%%      defined for the autoscale process.
%%      The module is used by autoscale if the following option has been set in
%%      scalaris.local.cfg:
%%        {autoscale_cloud_module, cloud_ssh}
%%      The following options can also be set:
%%        {cloud_ssh_hosts, ["host1", "host2", ..., "hostn"]}.
%%        {cloud_ssh_path, "path/to/scalaris/installation/on/host"}.
%%      Additional services besides Scalaris may be specified:
%%        {cloud_ssh_services, [{ServiceStartCmd, ServiceStopCmd}, {Service2StartCmd, Service2StopCmd}, ...]}.
%% @end
%% @version $Id$
-module(cloud_ssh).
-author('michels@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(_X,_Y), ok).

-include("scalaris.hrl").

-behaviour(cloud_beh).

-export([init/0, get_number_of_vms/0, add_vms/1, remove_vms/1, killall_vms/0]).
-export([check_config/0]).

-define(cloud_ssh_key, "2a42cb863313526fca96098a95020db2a904b01157f191a9bb3200829f8596c7").
-define(scalaris_start, "bin/./scalarisctl -e -detached -s -p 14915 -y 8000 -n node1 start").
-define(scalaris_stop, "bin/./scalarisctl -n node1 gstop").

-type status() :: active | inactive.
-type host() :: {string(), status()}.
-type hostlist() :: list(host()).

%%%%%%%%%%%%%%%%%%%%%
%%%% Behavior methods
%%%%%%%%%%%%%%%%%%%%%

-spec init() -> failed | ok.
init() ->
    case get_hosts() of
        failed ->
            Hostnames = config:read(cloud_ssh_hosts),
            HostsWithStatus = lists:map(fun(Host) -> {Host, inactive} end, Hostnames),
            save_hosts(HostsWithStatus);
        _ -> ok
    end.

-spec get_number_of_vms() -> failed | non_neg_integer().
get_number_of_vms() ->
    case get_hosts() of
        failed -> failed;
        {_, List} -> get_number_of_vms(List)
    end.

-spec get_number_of_vms(hostlist()) -> failed | non_neg_integer().
get_number_of_vms(Hosts) ->
    lists:foldl(fun (VM, NumActive) ->
                        case VM of
                            {_IP, active} ->
                                NumActive + 1;
                            _ -> NumActive
                        end
                end, 0, Hosts).

-spec add_vms(pos_integer()) -> failed | ok.
add_vms(N) ->
    add_or_remove_vms(add, N).

-spec remove_vms(pos_integer()) -> failed | ok.
remove_vms(N) ->
    add_or_remove_vms(remove, N).

-spec add_or_remove_vms(add | remove, integer()) ->  failed | ok.
add_or_remove_vms(Flag, Pending) ->
    case get_hosts() of
    	{TLog, Hosts} ->
            UpdatedHosts = add_or_remove_vms(Flag, Pending, Hosts, []),
            save_hosts(TLog, UpdatedHosts);
        failed -> failed
    end.

-spec add_or_remove_vms(add | remove, integer(), hostlist(), hostlist()) -> hostlist().
add_or_remove_vms(_Flag, _Pending = 0, Hosts, UpdatedHosts) ->
    UpdatedHosts ++ Hosts;
add_or_remove_vms(_Flag, _Pending, _Hosts = [], UpdatedHosts) ->
    UpdatedHosts;
add_or_remove_vms(add, Pending, Hosts, UpdatedHosts) ->
    [Host | RemainingHosts] = Hosts,
    case Host of
        {_, active} ->
            add_or_remove_vms(add, Pending, RemainingHosts, UpdatedHosts ++ [Host]);
        {Hostname, _} ->
            scalaris_vm(start, Hostname),
            add_or_remove_vms(add, Pending - 1, RemainingHosts, UpdatedHosts ++ [{Hostname, active}])
    end;
add_or_remove_vms(remove, Pending, Hosts, UpdatedHosts) ->
    [Host | RemainingHosts] = Hosts,
    case Host of
        {Hostname, active} ->
            scalaris_vm(stop, Hostname),
            add_or_remove_vms(remove, Pending - 1, RemainingHosts, UpdatedHosts ++ [{Hostname, inactive}]);
        {_, _} ->
            add_or_remove_vms(remove, Pending, RemainingHosts, UpdatedHosts ++ [Host])
    end.


%%%%%%%%%%%%%%%%%%%
%%%% Helper methods
%%%%%%%%%%%%%%%%%%%

-spec killall_vms() -> failed | ok.
killall_vms() ->
    case get_hosts() of
        {_, Hosts} ->
            lists:foreach(fun({Hostname, Status}) ->
                                  case Status of
                                      active ->
                                          scalaris_vm(stop, Hostname);
                                      inactive ->
                                          ok
                                  end
                          end, Hosts),
            ok;
        failed -> failed
    end.

-spec scalaris_vm(start | stop, string()) -> ok.
scalaris_vm(Action, Hostname) ->
    Scalaris = get_scalaris_service(),
    Services = [Scalaris | get_additional_services()],
    lists:foreach(fun (Service) ->
                          {StartCmd, StopCmd} = Service,
                          ServiceCmd =
                              case Action of
                                  start -> StartCmd;
                                  stop  -> StopCmd
                              end,
                          Cmd = format("ssh -n -f ~s \"(~s)\"", [Hostname, ServiceCmd]),
                          ?TRACE("Executing: ~p~n", [Cmd]),
                          _ = exec(Cmd)
                  end, Services),
    ok.

-spec format(string(), list()) -> string().
format(FormatString, Items) ->
    lists:flatten(io_lib:format(FormatString, Items)).

-spec get_hosts() -> failed | {tx_tlog:tlog_ext(), hostlist()}.
get_hosts() ->
    case api_tx:read(api_tx:new_tlog(), ?cloud_ssh_key) of
        {TLog, {ok, Hosts}} -> {TLog, Hosts};
        _ -> failed
    end.

-spec save_hosts(hostlist()) -> failed | ok.
save_hosts(Hosts) ->
    save_hosts(api_tx:new_tlog(), Hosts).

-spec save_hosts(tx_tlog:tlog_ext(), hostlist()) -> failed | ok.
save_hosts(TLog, Hosts) ->
    case api_tx:req_list(TLog, [{write, ?cloud_ssh_key, Hosts}, {commit}]) of
        {[], [{ok}, {ok}]} -> ok;
        _ -> failed
    end.

-spec get_scalaris_service() -> {string(), string()}.
get_scalaris_service() ->
    Path =
        case config:read(cloud_ssh_path) of
            failed -> "scalaris";
            Arg -> Arg
        end,
    {format("~s/~s", [Path, ?scalaris_start]), format("~s/~s", [Path, ?scalaris_stop])}.

-spec get_additional_services() -> [{string(),string()}].
get_additional_services() ->
    case config:read(cloud_ssh_services) of
        failed -> [];
        [T|H] -> [T|H]
    end.

-spec exec(string()) -> pid().
exec(Cmd) ->
    _ = spawn(os, cmd, [Cmd]).

-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_list(cloud_ssh_hosts) and
    config:cfg_is_string(cloud_ssh_path) and
    config:cfg_is_list(cloud_ssh_services).
