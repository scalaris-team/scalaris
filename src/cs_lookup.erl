%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : cs_lookup.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Public Lookup API; Reliable Lookup
%%%
%%% Created : 10 Apr 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(cs_lookup).



-author('schuett@zib.de').
-vsn('$Id$ ').

-export([unreliable_lookup/2,unreliable_get_key/1]).



-include("../include/scalaris.hrl").

%%%-----------------------Public API----------------------------------


    
	

unreliable_lookup(Key, Msg) ->
    cs_send:send_local(get_pid(cs_node) , {lookup_aux, Key, 0, Msg}).

unreliable_get_key(Key) ->
    unreliable_lookup(Key, {get_key, cs_send:this(), Key}).

%%%-----------------------Implementation------------------------------





get_pid(Id) ->
    InstanceId = erlang:get(instance_id),
%     if
%   	InstanceId == undefined ->
%   	    io:format("~p~n", [util:get_stacktrace()]);
%   	true ->
%   	    ok
%     end,
    process_dictionary:lookup_process(InstanceId, Id).
