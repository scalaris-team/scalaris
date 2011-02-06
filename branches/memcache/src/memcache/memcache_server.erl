%% Author: Christian Hennig (caraboides@googlemail.com)
%% Created: 23.10.2010
%% Description: TODO: Add desciption to memcache_parser
-module(memcache_server).
-import('memcache.hrl').

-export([listen/1,start_link/0]).

-define(TCP_OPTIONS,[binary, {packet, 0}, {active, false}, {reuseaddr, true}]).


-spec start_link() -> {ok, pid()} | ignore.
start_link() ->
    Pid = case config:read(memcacheapi_enable) of
        true ->
			io:format("Starting MemcaheServer~n "),
            {ok, spawn(fun() -> listen(config:read(memcacheapi_port)) end)};
        false ->
            ignore
    end,
	io:format("Memcache is listen on  Port: ~p ~n ",[config:read(memcacheapi_port)]),
	Pid.



%% Listen on the given port, accept the first incoming connection and
%% launch the worker loop, for handel requestes.

listen(Port) ->
    {ok, LSocket} = gen_tcp:listen(Port, ?TCP_OPTIONS),
    do_accept(LSocket).

%% The accept gets its own function so we can loop easily.  Yay tail
%% recursion!

do_accept(LSocket) ->
    {ok, Socket} = gen_tcp:accept(LSocket),
	io:format( "[ memcacheApi ~p ] accept Connection~n", [comm:this()]),
    spawn(fun() -> do_request(Socket) end),
    do_accept(LSocket).

do_request(Socket) ->
   case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
			io:format( "[ memcacheApi ~p ] receive request:~n~p", [comm:this(),Data]),
            Request = memcache_parser:parse_req(Data),
            Response = memcache_api:do_request(Request),
			io:format(" Sending Response: ~p ~n",[Response]),
            gen_tcp:send(Socket, Response),
            do_request(Socket);
        {error, closed} ->
            ok
    end.



