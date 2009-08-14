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
%%% File    : boot_app.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : boot application file
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @doc The boot server application
%% @version $Id$
-module(boot_app).

-author('schuett@zib.de').
-vsn('$Id$ ').

-behaviour(application).

-export([start/2, stop/1]).

start(normal, _Args) ->
   
    randoms:start(),
    process_dictionary:start_link(),
   
    Sup = boot_sup:start_link(),
    Sup;
    
start(_, _) ->
    {error, badarg}.

stop(_State) ->
    ok.
