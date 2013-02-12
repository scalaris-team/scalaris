%%% @author Maximilian Michels <max@pvs-pc03.zib.de>
%%% @copyright (C) 2013, Maximilian Michels
%%% @doc
%%%
%%% @end

-module(cloud_local).
-author('michels@zib.de').

-behavior(cloud_beh).

-include("../include/scalaris.hrl").

-export([init/0, get_number_of_vms/0, add_vms/1, remove_vms/1, find_free_port/0, find_free_port/1]).

-spec init() -> ok.
init() ->
	case config:read("as_min_vms") andalso config:read("as_max_vms") of
		failed ->
			config:write("as_min_vms", 1),
			config:write("as_max_vms", ?PLUS_INFINITY);
		X -> X
	end.

-spec get_number_of_vms() -> integer().
get_number_of_vms() ->
	length(erlang:element(2, erl_epmd:names())).

-spec add_vms(integer()) -> ok.
add_vms(N) ->
	BaseScalarisPort = 14195,%config:read(port),
	BaseYawsPort = 8000,%config:read(yaws_port),
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
				os:cmd(Cmd),
				wait_for(fun get_number_of_vms/0, get_number_of_vms() + 1),
				timer:sleep(200)
		end,
	[SpawnFun(X) || X <- lists:seq(1, N), get_number_of_vms() < config:read("as_max_vms")],		   
	ok.

wait_for(Fun, ExpectedValue) ->
	case Fun() of
		ExpectedValue -> ok;
		_ ->
			wait_for(Fun, ExpectedValue)
	end.

-spec remove_vms(integer()) -> ok.
remove_vms(N) ->
	
	ok.

find_free_port() ->
	BasePort = config:read(port),
	find_free_port(BasePort).

find_free_port(Port) ->
	case gen_tcp:listen(Port, []) of
		{ok, Socket} -> gen_tcp:close(Socket), 
						Port;
		_ -> find_free_port(Port+1)
	end.
