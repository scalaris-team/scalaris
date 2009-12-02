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
%% @doc this process stores the identifier of the cs_node. If the cs_node is 
%%      restarted his identifier will survive in this process. We could use 
%%      this e.g. when doing load-blancing
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(cs_keyholder).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-include("../include/scalaris.hrl").

-export([start_link/1,init/1, on/2,set_key/1, get_key/0, reinit/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc sets the key of the cs_node
-spec(set_key/1 :: (?RT:key()) -> ok).
set_key(Key) ->
    cs_send:send_local(get_pid() , {set_key_keyholder, Key}),ok.

%% @doc reads the key of the cs_node
-spec(get_key/0 :: () -> ok).
get_key() ->
    cs_send:send_local(get_pid() , {get_key_keyholder, self()}),
    ok.

init(_Arg) ->
    get_initial_key(config:read(key_creator)).
    

start_link(InstanceId) ->
    gen_component:start_link(?MODULE, [InstanceId,[]], [{register, InstanceId, cs_keyholder}]).
    

reinit() ->
    cs_send:send_local(get_pid() , {reinit}).


    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Server process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


on({reinit},_Key) ->
        get_initial_key(config:read(key_creator));
on({set_key_keyholder, NewKey},_Key) -> 
	    NewKey;
on({get_key_keyholder, PID},Key) -> 
	    cs_send:send_local(PID , {get_key_response_keyholder, Key}),
	    Key;
on(_, _State) ->
    unknown_event.

get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), cs_keyholder).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Key creation algorithms
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_initial_key(random) ->
    ?RT:getRandomNodeId();
get_initial_key(random_with_bit_mask) ->
    {Mask1, Mask2} = config:read(key_creator_bitmask),
    (get_initial_key(random) band Mask2) bor Mask1.
