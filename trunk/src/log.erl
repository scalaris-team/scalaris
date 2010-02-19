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
%%% File    : log.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : log service
%%%
%%% Created :  4 Apr 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(log).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("log4erl.hrl").

%-export([log/1, log2file/0]).
-export([start_link/0]).
-export([log/2,log/3,log/4]).


start_link() ->
    application:start(log4erl),
    log4erl:add_console_appender(stdout,{info, config:read(log_format)}), 
    log4erl:change_log_level(config:read(log_level)),
    log(info, "Log4erl started"),
    ignore.

log(Level,Log) ->
    log4erl:log(Level,Log).
log(Level,Log,Data) ->
    log4erl:log(Level,Log,Data).
log(Logger, Level, Log, Data) ->
    log4erl:log(Logger, Level, Log, Data).

