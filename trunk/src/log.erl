% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc log service
%% @version $Id$
-module(log).
-author('schuett@zib.de').
-vsn('$Id$').

-include("log4erl.hrl").

-export([start_link/0]).
-export([log/2, log/3, log/4, set_log_level/1]).
-export([check_config/0]).

-type log_level() :: warn | info | error | fatal | debug.

-spec start_link() -> ignore.
start_link() ->
    application:start(log4erl),
    log4erl:add_console_appender(stdout, {config:read(log_level), config:read(log_format)}),
    log4erl:add_file_appender(file, {preconfig:log_path(),
                                     config:read(log_file_name),
                                     {size, config:read(log_file_size)},
                                     config:read(log_file_rotations),
                                     "txt", config:read(log_level_file)}),
    
%%     log4erl:change_format(stdout, config:read(log_format)),
    log4erl:change_format(file, config:read(log_format_file)),
    ignore.

-spec log(Level::log_level(), LogMsg::any()) -> any().
log(Level, Log) ->
    log4erl:log(Level,Log).

-spec log(Level::log_level(), LogMsg::any(), Data::any()) -> any().
log(Level, Log, Data) ->
    log4erl:log(Level, Log, Data).

-spec log(Logger::atom(), Level::log_level(), LogMsg::any(), Data::any()) -> any().
log(Logger, Level, Log, Data) ->
    log4erl:log(Logger, Level, Log, Data).

-spec set_log_level(Level::log_level() | none) -> any().
set_log_level(Level) ->
    log4erl:change_log_level(Level).

%% @doc Checks whether config parameters of the log4erl process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_in(log_level, [warn, info, error, fatal, debug, none]) and
    config:is_in(log_level_file, [warn, info, error, fatal, debug, none]) and
    
    config:is_string(log_file_name) and
    
    config:is_integer(log_file_size) and
    config:is_greater_than(log_file_size, 0) and
    
    config:is_integer(log_file_rotations) and
    config:is_greater_than(log_file_rotations, 0) and

    config:is_string(log_format) and
    config:is_string(log_format_file).
