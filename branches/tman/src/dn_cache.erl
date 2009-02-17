%  Copyright 2007-2008 Konrad-Zuse-Zentrum f√ºr Informationstechnik Berlin
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
%%% File    : rm_tman.erl
%%% Author  : Christian Hennig <hennig@zib.de>
%%% Description : T-Man ring maintenance
%%%
%%% Created :  12 Jan 2009 by Christian Hennig <hennig@zib.de>
%%%-------------------------------------------------------------------
%% @author Christian Hennig <hennig@zib.de>
%% @copyright 2007-2009 Konrad-Zuse-Zentrum f¸r Informationstechnik Berlin
%% @version $Id$
-module(dn_cache).

-author('hennig@zib.de').
-vsn('$Id$ ').


-export([start_link/1, start/2,
		subscribe/0,
		unsubscribe/0,
	 	add_zombie_candidate/1]).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc spawns a Dead Node Cache
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    Link = spawn_link(?MODULE, start, [InstanceId, self()]),
	receive 
        {init_done} ->
            ok
    end,
    {ok, Link}.



add_zombie_candidate(Node) ->
    get_pid() ! {add_zombie_candidate, Node}.

subscribe() ->
    get_pid() ! {subscribe, self()}.

unsubscribe() ->
    get_pid() ! {unsubscribe, self()}.

      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal Loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
      	erlang:send_after(config:read(zombieDetectorInterval), self(), {zombiehunter}),
        log:log(info,"[ DNC ~p ] starting Dead Node Cache", [self()]),
	    loop(fix_queue:new(config:read(zombieDetectorSize)),gb_sets:new()).

% @doc the Token takes care, that there is only one timermessage for stabilize 
loop(Queue,Subscriber) ->
 	receive
	{zombiehunter} ->
        fix_queue:filter(fun (X) -> cs:send(node:pidX(X),{ping,self(),X}) end,Queue), 
        erlang:send_after(config:read(zombieDetectorInterval), self(), {zombiehunter}),
        loop(Queue,Subscriber);
	{pong,Zombie} ->
        gb_sets:filter(fun (X) -> X ! {zombie,Zombie} end, Subscriber),
        loop(Queue,Subscriber);
	{add_zombie_candidate, Node} ->
		loop(ch_queue:add(Node,Queue),Subscriber);
    {subscribe,Node} ->
		loop(Queue,gb_sets:insert(Node,Subscriber));
	{unsubscribe,Node} ->
		loop(Queue,gb_sets:del_element(Node,Subscriber))
	end.







%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc starts ring maintenance
start(InstanceId, Sup) ->
    process_dictionary:register_process(InstanceId, dn_cache, self()),
   	log:log(info,"[ DNC ~p ] starting DeadNodeCache", [self()]),
    Sup ! {init_done},
    start().

% @private
get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), dn_cache).
