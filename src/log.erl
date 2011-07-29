% @copyright 2007-2011 Zuse Institute Berlin

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

-ifdef(with_export_type_support).
-export_type([log_level/0]).
-endif.

-type log_level() :: debug | info | warn | error | fatal.

%% @doc Starts the log4erl process, removes the error_logger and
%%      error_logger_file_h report handlers and registers itself as the (only)
%%      report handler. Note: requires a running config process and can only be
%%      run once!
-spec start_link() -> {ok, Pid::pid()} | ignore |
                      {error, Error::{already_started, Pid::pid()} | shutdown | term()}.
start_link() ->
    Link = log4erl:start(log4erl, []),
    case Link of
        {ok, _} ->
            case util:is_unittest() of
                true ->
                    log4erl:add_appender(?DEFAULT_LOGGER,
                                         {log4erl_ctpal_appender, ctpal},
                                         {config:read(log_level),
                                          config:read(log_format)});
                _ ->
                    log4erl:add_console_appender(stdout,
                                                 {config:read(log_level),
                                                  config:read(log_format)})
            end,
            log4erl:add_file_appender(file, {config:read(log_path),
                                             config:read(log_file_name_log4erl),
                                             {size, config:read(log_file_size)},
                                             config:read(log_file_rotations),
                                             "txt",
                                             config:read(log_level_file)}),

%%             log4erl:change_format(stdout, config:read(log_format)),
            log4erl:change_format(file, config:read(log_format_file)),
            
            % remove the default error_logger's file and tty handlers
            error_logger:delete_report_handler(error_logger_file_h),
            error_logger:delete_report_handler(error_logger_tty_h),
            % there should not be any previous log4erl handler - just in case, delete it:
            error_logger:delete_report_handler(error_logger_log4erl_h),
            % add a log4erl handler instead:
            log4erl:error_logger_handler(),
            ok;
        _ -> ok
    end,
    Link.

-spec log(Level::log_level(), LogMsg::string()) -> any().
log(Level, Log) ->
    log4erl:log(Level,Log).

-spec log(Level::log_level(), LogMsg::string(), Data::list()) -> any().
log(Level, Log, Data) ->
    log4erl:log(Level, Log, Data).

-spec log(Logger::atom(), Level::log_level(), LogMsg::string(), Data::list()) -> any().
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
    
    config:is_string(log_path) and
    config:is_string(log_file_name_log4erl) and
    
    config:is_integer(log_file_size) and
    config:is_greater_than(log_file_size, 0) and
    
    config:is_integer(log_file_rotations) and
    config:is_greater_than(log_file_rotations, 0) and

    config:is_string(log_format) and
    config:is_string(log_format_file).
