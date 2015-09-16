% @copyright 2007-2012 Zuse Institute Berlin

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
-export([log/1, log/2, log/3, log/4, set_log_level/1]).
-export([pal/1, pal/2]).
-export([check_config/0]).

-export_type([log_level/0]).

-type log_level() :: debug | info | warn | error | fatal.

%% @doc Starts the log4erl process, removes the error_logger and
%%      error_logger_file_h report handlers and registers itself as the (only)
%%      report handler. Note: requires a running config process and can only be
%%      run once!
-spec start_link() -> {ok, Pid::pid()}.
start_link() ->
    Link = log4erl:start(log4erl, []),
    case Link of
        {ok, _} ->
            LogLevel = config:read(log_level),
            LogLevelFile = config:read(log_level_file),
            % determine lowest log level, adapt cut-off level accordingly
            case log4erl_utils:to_log(LogLevel, LogLevelFile) of
                true  -> % LogLevel >= LogLevelFile
                    log_filter_codegen:set_cutoff_level(LogLevelFile);
                false -> % LogLevel < LogLevelFile
                    log_filter_codegen:set_cutoff_level(LogLevel)
            end,
            case util:is_unittest() of
                true ->
                    log4erl:add_appender(?DEFAULT_LOGGER,
                                         {log4erl_ctpal_appender, ctpal},
                                         {LogLevel,
                                          config:read(log_format)});
                _ ->
                    log4erl:add_console_appender(stdout,
                                                 {LogLevel,
                                                  config:read(log_format)})
            end,
            ErrorLoggerFile = filename:join(config:read(log_path),
                                            config:read(log_file_name_log4erl)),
            ok = filelib:ensure_dir(ErrorLoggerFile),
            log4erl:add_file_appender(file, {config:read(log_path),
                                             config:read(log_file_name_log4erl),
                                             {size, config:read(log_file_size)},
                                             config:read(log_file_rotations),
                                             "txt",
                                             LogLevelFile}),

%%             log4erl:change_format(stdout, config:read(log_format)),
            log4erl:change_format(file, config:read(log_format_file)),

            % remove the default error_logger's file and tty handlers
            _ = error_logger:logfile(close),
            error_logger:tty(false),
            error_logger:delete_report_handler(error_logger),
            % there should not be any previous log4erl handler - just in case, delete it:
            error_logger:delete_report_handler(error_logger_log4erl_h),
            % erlang >= R15B adds cth_log_redirect in common_test, delete it:
            error_logger:delete_report_handler(cth_log_redirect),
            % now register log4erl error logger:
            log4erl:error_logger_handler(),
            %% check whether erlang error_logger only reports to log4erl
            case gen_event:which_handlers(error_logger) of
                [error_logger_log4erl_h] -> ok;
                Loggers -> log(info, "additional error loggers installed: ~.0p",
                               [[L || L <- Loggers, L =/= error_logger_log4erl_h]])
            end,
            ok
    end,
    Link.

-spec log(LogMsg::string()) -> any().
log(Log) ->
    log(Log, []).

-spec log(Level::log_level(), LogMsg::string()) -> any();
         (LogMsg::string(), Data::list()) -> any().
log(Level, Log) when is_atom(Level) ->
    log4erl:log(Level, Log);
log(Log, Data) ->
    log4erl:log(warn, Log, Data).

-spec log(Level::log_level(), LogMsg::string(), Data::list()) -> any().
log(Level, Log, Data) ->
    log4erl:log(Level, Log, Data).

-spec log(Logger::atom(), Level::log_level(), LogMsg::string(), Data::list()) -> any().
log(Logger, Level, Log, Data) ->
    log4erl:log(Logger, Level, Log, Data).

-spec set_log_level(Level::log_level() | none) -> any().
set_log_level(Level) ->
    log4erl:change_log_level(Level).

% for (temporary) debugging, use log:pal/1 and log:pal/2
-spec pal(LogMsg::string()) -> any().
pal(Log) -> log(Log).

-spec pal(LogMsg::string(), Data::list()) -> any().
pal(Log, Data) -> log(Log, Data).

%% @doc Checks whether config parameters of the log4erl process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:cfg_is_in(log_level, [warn, info, error, fatal, debug, none]) and
    config:cfg_is_in(log_level_file, [warn, info, error, fatal, debug, none]) and
    
    config:cfg_is_string(log_path) and
    config:cfg_is_string(log_file_name_log4erl) and
    
    config:cfg_is_integer(log_file_size) and
    config:cfg_is_greater_than(log_file_size, 0) and
    
    config:cfg_is_integer(log_file_rotations) and
    config:cfg_is_greater_than(log_file_rotations, 0) and

    config:cfg_is_string(log_format) and
    config:cfg_is_string(log_format_file).
