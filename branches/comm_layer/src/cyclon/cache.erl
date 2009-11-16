%%%-------------------------------------------------------------------
%%% File    : cyclon.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : Cache implementation using a list
%%%
%%% Created :  1 Dec 2008 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id $
-module(cache).
-author('hennig@zib.de').
-vsn('$Id $ ').

-import(lists).
-import(cs_send).
-import(config).
-import(random).
%-import(node).
-import(io).
-import(crypto).
-import(util).
%% API

-export([get_subset_max_age/2, add_element/2, get_cache/1, add_list/2, size/1, new/0, get_random_element/1, get_random_subset/2, is_element/2, delete/2, minus/2, merge/3, trim/1, get_list_of_nodes/1 ,inc_age/1, get_youngest/2, get_oldest/1, ages/1, get_youngest/1]).

% list of {pid of cs_node process, age}
-type(cache() :: list({node:node_type(), pos_integer()})).

%% @doc Firstly try to insert QT in Cache on the empty slots, and secondly replacing entries among the ones original sent
-spec(merge/3 :: (cache(), cache(), cache()) -> cache()).
merge(Cache,[],_Send) ->
    Cache;
merge([],QsSub,_SendSub) ->
    QsSub;
merge(Cache,[_QH|_QT],[]) ->
    Cache;
%     Cache  QsSUB    SendSub 	
merge(Cache,[QH|QT],[SH|ST] = S) ->
    %io:format("~p ~p-~p-~p ~n",[self(),length(Cache),length(Q),length(S)]),
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
    end.


%% @doc 
get_list_of_nodes([]) ->
	[];
get_list_of_nodes([{A,_}|T]) ->
	[A]++get_list_of_nodes(T).
	

%% @doc ensure that one place is empty in the stack, by delete a random entrie if no space left
trim(Cache) ->
	case cache:size(Cache) <  config:read(cyclon_cache_size) of
		true ->
			Cache;
		false ->
			delete(get_random_element(Cache),Cache)
	end.

get_cache(Foo) ->
	Foo.

add_list([],Foo) ->
	Foo;
add_list([NodePid|T],Foo) ->
	add_list(T,add_element({NodePid,0},Foo)).


get_random_element(State) ->
    L=cache:size(State),
    P=randoms:rand_uniform(0, L)+1,
    % picks nth element of state
    lists:nth(P,State).

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

%% @doc find oldest element (randomize if multiple oldest elements)
get_oldest(Cache) ->
    HighestAge = lists:foldl(fun ({_, Age}, MaxAge) ->
				    util:max(Age, MaxAge)
			    end,
			    0,
			    Cache),
    %io:format("Oldest: ~p~n",[HighestAge]),
    OldElements = lists:filter(fun ({_, Age}) ->
				       Age == HighestAge
			       end,
			       Cache),
    get_random_element(OldElements).

get_youngest(X) ->
    get_youngest(1,X).

%% @doc find youngest N element, List of nodes
get_youngest(_,null) ->
    [];
get_youngest(_,[]) ->
    [];
get_youngest(N,Cache) ->
    Order = fun(A, B) ->
		    get_age(A) =< get_age(B)
	    end,
    SortAge = lists:sort(Order,Cache),
		lists:map(fun(X) -> get_node(X) end ,lists:sublist(SortAge,1, N)).

% first_same_age([]) ->
%     0;
% first_same_age([_X]) ->
%     1;
% first_same_age([H|T]) ->
%     case get_age(H) == get_age(hd(T)) of
%         true ->
%             1 + first_same_age(T);
%         false ->
%             1
% 		end.
        
  

get_node({X,_}) ->
    X.

get_age({_,X}) ->
    X.

inc_age(Cache) ->
    lists:map(fun({A,C}) ->
		      {A, C + 1}
              end,
	      Cache).

eq({A,_},{B,_}) ->
	A==B . 


new() ->
	[].
%% @doc Amount of valid Cache entries 
size([]) ->
	0;
size(null) ->
	0;
size([H|T]) ->
	{Node,_}=H,
	case Node of
		nil	-> cache:size(T);
		_	-> 1+cache:size(T)
    end.


get_subset_max_age(MaxAge,null) ->
    [];
get_subset_max_age(MaxAge,Cache) ->
    get_list_of_nodes(lists:filter(fun ({_,Age}) -> Age < MaxAge end ,Cache)).





%% @doc returns true if Element is in Cache
is_element(Element, Cache) ->
    lists:any(fun(SomeElement) -> eq(Element, SomeElement) end, Cache).

%% @doc removes Element from Cache
delete(Element, Cache) ->
    lists:filter(fun(SomeElement) -> not eq(SomeElement, Element) end, Cache).

%% @doc adds Element to Cache or updates Element in Cache
add_element(Element, Cache) ->
    case is_element(Element, Cache) of
	true ->
	    add_element(Element, delete(Element, Cache));
	false ->
	    [Element | Cache]
    end.

ages([]) ->
	[];
ages([{_,Age}|T]) ->
    [Age]++ages(T).
