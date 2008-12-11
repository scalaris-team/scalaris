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
%-import(node).
-import(io).
%% API

-export([add_element/2, get_cache/1, add_list/2, size/1, new/0, get_element/2, get_random_element/1, get_random_subset/2, is_element/2, delete/2 ,update/1, minus/2, merge/3 ]).

-type(cache() :: list({{node:node_type(), node:node_type()}, pos_integer()})).

%% @doc Firstly try to insert QT in Cache on the empty slots, and secondly replacing entries among the ones original sent
-spec(merge/3 :: (cache(), cache(), cache()) -> cache()).
merge(Cache,[],_Send) ->
	Cache;
merge([],QsSub,_SendSub) ->
	QsSub;
merge(Cache,[_QH|_QT],[]) ->
	 Cache;
%     Cache  QsSUB    SendSub 	
merge(Cache,[QH|QT]=Q,[SH|ST] = S) ->
io:format("~p ~p-~p-~p ~n",[self(),length(Cache),length(Q),length(S)]),
case (cache:size(Cache) < config:read(cyclon_cache_size)) of
	true ->
		merge(add_element(QH,Cache),QT,S);
	false ->
		merge(add_element(QH,trim(delete(SH,Cache))),QT,ST)
end.

%% @doc minus(M,N) : { x | x in M and x notin N} 
minus([],_N) ->
	[];
minus([H|T],N) ->
case is_element(H,N) of
 	true -> 
		minus(T,N);
	false ->
		[H]++minus(T,N)
end	.

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
				%io:format("+ ~p | ~p ~n",[Node,Cyclon]),
				receive_req(add_element({{Cyclon,Node},0},Cache))				
		after 300 ->
			
			Cache
		end
end.

%% @doc make the cache valid, by ataching CyclonPids to they NodePids
update(Cache) ->
send_req(Cache),
receive_req(Cache).

%% @doc ensure that one place is empty in the stack, by delete a random entrie if no space left
trim(Cache) ->
	case cache:size(Cache) <  config:read(cyclon_cache_size) of
		true ->
			Cache;
		false ->
			delete(get_random_element(Cache),Cache)
	end.

%% @doc delete an element in the Cache    
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
	case N<1 of
		false ->
			get_element(N-1,T);
		true ->
			nil
	end.

get_random_element(State) ->
	L=cache:size(State),
	P=random:uniform(L),
	get_element(P,State).

worker(_,Target,[]) -> Target;
worker(N,Target,Cache) ->
case N==length(Target) of
	true ->
		Target;
	false ->
		Q = get_random_element(Cache),
		worker(N,add_element(Q,Target),delete(Q,Cache))
	end.
	

get_random_subset(0,_Cache) -> 
		new();
get_random_subset(N,Cache) -> 
		worker(N,[],Cache).

lt({{_,A},_},{{_,B},_}) ->
case A < B of 
	 true    -> true;  %%  Will be expand 
     false   -> false  %%	   later
end.

eq({{_,A},_},{{_,B},_}) ->
case A==B of 
	 true    -> true;  %%  Will be expand 
     false   -> false  %%	   later
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
%% @doc Amount of valid Cache entries 
size([]) ->
0;
size([H|T]) ->
{{Cyclon,_},_}=H,
case Cyclon of
	nil	-> cache:size(T);
	_	-> 1+cache:size(T)
end.












