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
%%% File    : yaws_wrapper.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Yaws Server wrapper
%%%
%%% Created :  11 Jun 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(yaws_wrapper).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("yaws.hrl").

-export([start_link/3, try_link/3]).

% start yaws
start_link(DocRoot, SL, GL) ->
    ok = application:set_env(yaws, embedded, true),
    ok = application:set_env(yaws, id, "default"),
    Link = yaws_sup:start_link(),
    GC = yaws:create_gconf(GL, "default"),
    SC = yaws:create_sconf(DocRoot, SL),
    %yaws_config:add_yaws_soap_srv(GC),
    SCs = yaws_config:add_yaws_auth([SC]),
    yaws_api:setconf(GC, [SCs]),
    Link.

% try to open yaws
try_link(DocRoot, SL, GL) ->
    ok = application:set_env(yaws, embedded, true),
    ok = application:set_env(yaws, id, "default"),
    Link = yaws_sup:start_link(),
    GC = yaws:create_gconf(GL, "default"),
    SC = yaws:create_sconf(DocRoot, SL),
    %yaws_config:add_yaws_soap_srv(GC),
    SCs = yaws_config:add_yaws_auth([SC]),
    case try_port(SC#sconf.port) of
	true ->
	    yaws_api:setconf(GC, [SCs]),
	    Link;
	false ->
	   log:log(warn,"[ Yaws ] could not start yaws, maybe port ~p is in use~n", [SC#sconf.port]),
	    ignore
    end.

try_port(Port) ->
    case gen_tcp:listen(Port, []) of
	{ok, Sock} ->
	    gen_tcp:close(Sock),
	    true;
	_  ->
	    false
    end.

