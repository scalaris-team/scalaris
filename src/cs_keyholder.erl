%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : cs_keyholder.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Stores the key for the cs_node process
%%%
%%% Created : 24 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_keyholder.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_keyholder).

-author('schuett@zib.de').
-vsn('$Id: cs_keyholder.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([start_link/1, start/1, set_key/1, get_key/0]).

get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), cs_keyholder).

set_key(Key) ->
    get_pid() ! {set_key_keyholder, Key}.

get_key() ->
    get_pid() ! {get_key_keyholder, self()},
    receive
	{get_key_response_keyholder, Key} ->
	    Key
    end.

loop(Key) ->
    receive
	{set_key_keyholder, NewKey} -> 
	    loop(NewKey);
	{get_key_keyholder, PID} -> 
	    PID ! {get_key_response_keyholder, Key},
	    loop(Key)
	end.

start(InstanceId) ->
    process_dictionary:register_process(InstanceId, cs_keyholder, self()),
    %register(cs_keyholder, self()),
    randoms:init(),
    Key = randoms:getRandomNodeId(),
    %@TODO reimplement
    %error_logger:add_report_handler(cs_error_logger),
    loop(Key).

start_link(InstanceId) ->
    {ok, spawn_link(?MODULE, start, [InstanceId])}.
    
