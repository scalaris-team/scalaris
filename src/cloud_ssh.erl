% @copyright 2013 Zuse Institute Berlin

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
%%        {cloud_ssh_args, "arguments for ssh"}.
%%        {cloud_ssh_path, "path/to/scalaris/installation/on/host"}.
%%      Additional services besides Scalaris may be specified:
%%        {cloud_ssh_services, [{ServiceStartCmd, ServiceStopCmd}, {Service2StartCmd, Service2StopCmd}, ...]}.
%% @end
%% @version $Id$
-module(cloud_ssh).
-author('michels@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-behavior(cloud_beh).

-export([init/0, get_number_of_vms/0, add_vms/1, remove_vms/1, killall_vms/0]).

-define(cloud_ssh_key, "2a42cb863313526fca96098a95020db2a904b01157f191a9bb3200829f8596c7").
-define(scalaris_start, "bin/./scalarisctl -e -detached -s -p 14915 -y 8000 -n node start").
-define(scalaris_stop, "bin/./scalarisctl -n node gstop").
 
%%%%%%%%%%%%%%%%%%%%%
%%%% Behavior methods
%%%%%%%%%%%%%%%%%%%%%

-spec init() -> ok.
init() ->
    Hosts =
        case config:read(cloud_ssh_hosts) of
            failed -> [];
            List -> lists:map(fun(Host) -> {Host, inactive} end, List)
        end,
    case api_tx:read(?cloud_ssh_key) of
        {fail, not_found} -> api_tx:write(?cloud_ssh_key, Hosts);
        _ -> ok
    end.

-spec get_number_of_vms() -> non_neg_integer().
get_number_of_vms() ->
    {ok, List} = api_tx:read(?cloud_ssh_key),
    length(List).

-spec add_vms(integer()) -> ok.
add_vms(N) ->
    UpdatedHosts = add_or_remove_vms(add, N),
    {ok} = api_tx:write(?cloud_ssh_key, UpdatedHosts),
    ok.

-spec remove_vms(integer()) -> ok.
remove_vms(N) ->
    UpdatedHosts = add_or_remove_vms(remove, N),
    {ok} = api_tx:write(?cloud_ssh_key, UpdatedHosts),
    ok.

add_or_remove_vms(Flag, Pending) ->
    Hosts = get_hosts(),
    add_or_remove_vms(Flag, Pending, Hosts, []).

add_or_remove_vms(_Flag, _Pending = 0, Hosts, UpdatedHosts) ->
    UpdatedHosts ++ Hosts;
%% case where more vms than possible have been requested
add_or_remove_vms(_Flag, _Pending, _Hosts = [], UpdatedHosts) ->
    UpdatedHosts;
add_or_remove_vms(add, Pending, Hosts, UpdatedHosts) ->
    [Host | RemainingHosts] = Hosts,
    case Host of
        {_, active} ->
            add_or_remove_vms(add, Pending, RemainingHosts, UpdatedHosts ++ [Host]);
        {Hostname, _} ->
            start_scalaris_vm(Hostname),
            add_or_remove_vms(add, Pending - 1, RemainingHosts, UpdatedHosts ++ [{Hostname, active}])
    end;
add_or_remove_vms(remove, Pending, Hosts, UpdatedHosts) ->
    [Host | RemainingHosts] = Hosts,
    case Host of
        {Hostname, active} ->
            stop_scalaris_vm(Hostname),
            add_or_remove_vms(remove, Pending - 1, RemainingHosts, UpdatedHosts ++ [{Hostname, inactive}]);
        {_, _} ->
            add_or_remove_vms(remove, Pending, RemainingHosts, UpdatedHosts ++ [Host])
    end.


%%%%%%%%%%%%%%%%%%%
%%%% Helper methods
%%%%%%%%%%%%%%%%%%%

-spec killall_vms() -> ok.
killall_vms() ->
    Hosts = get_hosts(),
    lists:foreach(fun({Hostname, _}) ->
                          Cmd = lists:flatten(io_lib:format("ssh ~s ~s killall -9 beam.smp", 
                                                            [get_ssh_args(), Hostname])),
                          os:cmd(Cmd)
                  end, Hosts),
    ok.

start_scalaris_vm(Hostname) ->
    Scalaris = get_scalaris_service(),
    Services = [Scalaris | get_additional_services()],
    lists:foreach(fun (Service) ->
                          {StartCmd, _} = Service,
                          Cmd = format("ssh ~s ~s ~s", [get_ssh_args(), Hostname, StartCmd]),
                          io:format("Executing: ~p~n", [Cmd])
                          _ = os:cmd(Cmd)
                  end, Services),
    ok.

stop_scalaris_vm(Hostname) ->
    Scalaris = get_scalaris_service(),
    Services = [Scalaris | get_additional_services()],
    lists:foreach(fun (Service) ->
                          {_, StopCmd} = Service,
                          Cmd = format("ssh ~s ~s ~s", [get_ssh_args(), Hostname, StopCmd]),
                          io:format("Executing: ~p~n", [Cmd])
                          _ = os:cmd(Cmd)
                  end, Services),
    ok.

format(FormatString, Items) ->
    lists:flatten(io_lib:format(FormatString, Items)).

get_hosts() ->
    case api_tx:read(?cloud_ssh_key) of
        {ok, Val} ->
            Val;
        _ -> []
    end.

get_ssh_args() ->
    case config:read(cloud_ssh_args) of
        failed -> "";
        Args -> Args
    end.

get_scalaris_service() ->
    Path =
        case config:read(cloud_ssh_path) of
            failed -> "scalaris";
            Arg -> Arg
        end,
    {format("~s/~s", [Path, ?scalaris_start]), format("~s/~s", [Path, ?scalaris_stop])}.

get_additional_services() ->
    case config:read(cloud_ssh_services) of
        failed -> [];
        [T|H] -> [T|H]
    end.

get_number_of_active_vms(Hosts) ->
    lists:foldl(fun (VM, NumActive) ->
                         case VM of
                             {_IP, active} ->
                                 NumActive + 1;
                             _ -> NumActive
                         end
                end, 0, Hosts).

-spec get_number_of_active_vms() -> integer().
get_number_of_active_vms() ->
    {ok, List} = api_tx:read(?cloud_ssh_key),
    get_number_of_active_vms(List).
