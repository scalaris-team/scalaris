%%%-------------------------------------------------------------------
%%% File    : cyclon.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : Cache implementation, intern it is a gb_set, extern a list
%%%
%%% Created :  1 Dec 2008 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(cyclon.cache).
-author('hennig@zib.de').
-vsn('$Id $ ').

-import(lists).
-import(cs_send).
-import(config).
-import(random).
-import(node).
-import(io).
%% API

-export([add_element/2, get_cache/1, add_list/2, size/1, new/0, get_element/2, get_random_element/1, get_random_subset/2, is_element/2, delete/2 ,update/1]).


%CacheSize = config:read(cyclon_cache_size).
send_req([]) ->
nil;
send_req([{{Cyclon,Node},_}|T]) ->
case Cyclon of
	nil ->
		%io:format("Send to: ~p~n",[Node]),
		cs_send:send(Node,{get_cyclon_pid, cs_send:this(), Node}),
		send_req(T);
	_ ->
		send_req(T)
end.

receive_req(Cache) ->
case cache:size(Cache) ==length(Cache) of % Replace this with a Test of nil !!!!!!!!!!!
	true ->
		Cache;
	false ->
		receive 
			{cyclon_pid,Node,Cyclon} ->
				io:format("+ ~p | ~p ~n",[Node,Cyclon]),
				receive_req(add_element({{Cyclon,Node},0},Cache))				
		after 300 ->
			
			Cache
		end
end.

update(Cache) ->
send_req(Cache),
receive_req(Cache).

delete(_Foo,[]) ->
	[];
delete(Foo,[H|T]) ->
case lt(Foo,H) of
	true ->
		[H|T];
	false ->
		case eq(Foo,H) of
			true ->
				T;
			false ->
				[H]++delete(Foo,T)
		end
end.

is_element(H,T) ->
	lists:keymember(H, 1,T).

get_cache(Foo) ->
	Foo.

add_list([],Foo) ->
	Foo;

add_list([NodePid|T],Foo) ->
	add_list(T,add_element({{nil,NodePid},nil},Foo)).


get_element(_,[]) ->
	nil;
get_element(1,[H|_]) ->
	H;
get_element(N,[_|T]) ->
	get_element(N-1,T).

get_random_element(State) ->
	L=cache:size(State),
	P=random:uniform(L),
	get_element(P,State).

get_random_subset(0,_Cache) -> 
		new();
get_random_subset(N,Cache) -> 
		[get_random_element(Cache)]++get_random_subset(N-cache:size(Cache),Cache).

lt({{A,_},_},{{B,_},_}) ->
case A < B of 
	true 	-> true;
	false	-> false
end.

eq({{_,A},_},{{_,B},_}) ->
case A==B of 
	 true    -> true;
        false   -> false
end.


new() ->
	[].
add_element(Foo,[]) ->
	[Foo];
add_element(Foo,[H|T]) ->
	case lt(Foo,H) of 
		true -> 
			[Foo]++[H|T];
		false -> 
			case eq(Foo,H) of
				true ->
					[Foo|T];
				false ->
					[H]++add_element(Foo,T)
			end
	end.
size([]) ->
0;
size([H|T]) ->
{{Cyclon,_},_}=H,
case Cyclon of
	nil	-> cache:size(T);
	_	-> 1+cache:size(T)
end.












