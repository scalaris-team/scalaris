%% Author: Christian Hennig (caraboides@googlemail.com)
%% Created: 23.10.2010
%% Description: TODO: Add desciption to memcache_api
-module(memcache_api).


%%
%% Include files
%%
-include("memcache.hrl").

%%
%% Exported Functions
%%
-export([do_request/1]).

%%
%% API Functions
%%

do_request({Header,Body}) ->
	PreResponse =
	case Header#req_head.opcode of
		get 	-> do_get(Body);
		getk 	-> do_get(Body);
		set 	-> do_set(Body);
		Other -> 
			not_supportet_op(Other)
	end,
	memcache_parser:build_response(Header,Body,PreResponse).
%%
%% Local Functions
%%

do_get(Body) ->
	cs_api_v2:read(Body#req_body.key).
	

do_set(Body) ->
	cs_api_v2:write(Body#req_body.key, Body#req_body.value).

 not_supportet_op(Other)	->
	io:format("Dont know: ~p~n",[Other]),
	{fail,unknown_command}.


	