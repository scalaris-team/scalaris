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
%% @doc CLOUD LOCAL starts or stops local erlang vms based on alarms defined for
%%      the autoscale process.
%%      The module is used by autoscale if the following option has been set in
%%      scalaris.local.cfg:
%%        {as_cloud_module, cloud_local}
%%      The following options can also be set:
%%        {cloud_local_min_vms, integer()}.
%%        {cloud_local_max_vms, integer()}.
%% @end
%% @version $Id$
-module(cloud_local).
-author('michels@zib.de').
-vsn('$Id$').

-behavior(cloud_beh).

-include("scalaris.hrl").

-export([init/0, get_number_of_vms/0, add_vms/1, remove_vms/1]).


%%%%%%%%%%%%%%%%%%%%%
%%%% Behavior methods
%%%%%%%%%%%%%%%%%%%%%

-spec init() -> ok.
init() ->
	case config:read(cloud_local_min_vms) of
		failed ->
			config:write(cloud_local_min_vms, 1),
			config:write(cloud_local_max_vms, ?PLUS_INFINITY);
		X -> X
	end,
	ok.


-spec get_number_of_vms() -> non_neg_integer().
get_number_of_vms() ->
	length(erlang:element(2, erl_epmd:names())).

-spec add_vms(integer()) -> ok.
add_vms(N) ->
	BaseScalarisPort = config:read(port),
	BaseYawsPort = config:read(yaws_port),
	{Mega, Secs, _} = now(),
	Time = Mega * 1000000 + Secs,
	SpawnFun = 
		fun (X) -> 
				Port = find_free_port(BaseScalarisPort),
				YawsPort = find_free_port(BaseYawsPort),
				NodeName = lists:flatten(io_lib:format("node~p_~p", [Time, X])),
				Cmd = lists:flatten(io_lib:format("./../bin/scalarisctl -e -detached -s -p ~p -y ~p -n ~s start", 
									[Port, YawsPort, NodeName])),
				io:format("Executing: ~p~n", [Cmd]),
				NumberVMs = get_number_of_vms(),
				_ = os:cmd(Cmd),
				wait_for(fun get_number_of_vms/0, NumberVMs + 1),
				timer:sleep(200)
		end,
	_ = [SpawnFun(X) || X <- lists:seq(1, N), get_number_of_vms() < config:read(cloud_local_max_vms)],
	ok.

-spec remove_vms(integer()) -> ok.
remove_vms(N) ->
	AllVMs = lists:map(fun (El) -> erlang:element(1, El) end, erlang:element(2, erl_epmd:names())),
	VMs = lists:filter(fun (El) -> El =/= "firstnode" end, AllVMs),
	RemoveFun = 
		fun(NodeName) ->						
				Cmd = lists:flatten(io_lib:format("./../bin/scalarisctl -n ~s gstop", 
												  [NodeName])),
				NumberVMs = get_number_of_vms(),
				io:format("Executing: ~p~n", [Cmd]),
				_ = os:cmd(Cmd),
				wait_for(fun get_number_of_vms/0, NumberVMs - 1)
		end,
	_ = [RemoveFun(NodeName) || NodeName <- lists:sublist(VMs, N),
							    get_number_of_vms() > config:read(cloud_local_min_vms)],
	ok.

%%%%%%%%%%%%%%%%%%%
%%%% Helper methods
%%%%%%%%%%%%%%%%%%%
wait_for(Fun, ExpectedValue) ->
	case Fun() of
		ExpectedValue -> ok;
		_ ->
			wait_for(Fun, ExpectedValue)
	end.

-spec find_free_port(integer()) -> integer().
find_free_port(Port) ->
	case gen_tcp:listen(Port, []) of
		{ok, Socket} -> gen_tcp:close(Socket), 
						Port;
		_ -> find_free_port(Port+1)
	end.
