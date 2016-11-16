-module(log4erl).

-author("Ahmed Al-Issaei").
-license("MPL-1.1").

-behaviour(application).

-include("../include/log4erl.hrl").

%% API
-export([change_log_level/1, change_log_level/2]).
-export([change_level/2, change_level/3]).
-export([change_filename/2, change_filename/3]).
-export([add_logger/1, conf/1]).
-export([add_appender/2, add_appender/3]).
-export([add_file_appender/2, add_file_appender/3]).
-export([add_console_appender/2, add_console_appender/3]).
-export([add_smtp_appender/2, add_smtp_appender/3]).
-export([add_syslog_appender/2, add_syslog_appender/3]).
-export([add_xml_appender/2, add_xml_appender/3]).
-export([add_dummy_appender/2, add_dummy_appender/3]).
-export([get_appenders/0, get_appenders/1]).
-export([change_format/2, change_format/3]).
-export([error_logger_handler/0, error_logger_handler/1]).

-export([log/2, log/3, log/4]).

-export([warn/1, warn/2, warn/3]).
-export([info/1, info/2, info/3]).
-export([error/1, error/2, error/3]).
-export([fatal/1, fatal/2, fatal/3]).
-export([debug/1, debug/2, debug/3]).

%% Application callbacks
-export([start/2, stop/1]).

%%======================================
%% application callback functions
%%======================================
start(_Type, []) ->
    ?LOG("Starting log4erl app~n"),
    log4erl_sup:start_link(?DEFAULT_LOGGER).

stop(_State) ->
    log_filter_codegen:reset(),
    ok.

add_logger(Logger) ->
    try_msg({add_logger, Logger}).

%% Appender = {Appender, Name}
add_appender(Appender, Conf) ->
    try_msg({add_appender, ?DEFAULT_LOGGER ,Appender, Conf}).

%% Appender = {Appender, Name}
add_appender(Logger, Appender, Conf) ->
    try_msg({add_appender, Logger, Appender, Conf}).
    
add_console_appender(AName, Conf) ->
    add_appender(?DEFAULT_LOGGER, {console_appender, AName}, Conf).

add_console_appender(Logger, AName, Conf) ->
    add_appender(Logger, {console_appender, AName}, Conf).

add_file_appender(AName, Conf) ->
    add_appender(?DEFAULT_LOGGER, {file_appender, AName}, Conf).

add_file_appender(Logger, AName, Conf) ->
    add_appender(Logger, {file_appender, AName}, Conf).

add_smtp_appender(Name, Conf) ->
    add_appender(?DEFAULT_LOGGER, {smtp_appender, Name}, Conf).

add_smtp_appender(Logger, Name, Conf) ->
    add_appender(Logger, {smtp_appender, Name}, Conf).

add_syslog_appender(Name, Conf) ->
    add_appender(?DEFAULT_LOGGER, {syslog_appender, Name}, Conf).

add_syslog_appender(Logger, Name, Conf) ->
    add_appender(Logger, {syslog_appender, Name}, Conf).

add_xml_appender(Name, Conf) ->
    add_appender(?DEFAULT_LOGGER, {xml_appender, Name}, Conf).

add_xml_appender(Logger, Name, Conf) ->
    add_appender(Logger, {xml_appender, Name}, Conf).

add_dummy_appender(AName, Conf) ->
    add_appender(?DEFAULT_LOGGER, {dummy_appender, AName}, Conf).

add_dummy_appender(Logger, AName, Conf) ->
    add_appender(Logger, {dummy_appender, AName}, Conf).

get_appenders() ->
    try_msg({get_appenders, ?DEFAULT_LOGGER}).

get_appenders(Logger) ->
    try_msg({get_appenders, Logger}).

conf(File) ->
    log4erl_conf:conf(File).

change_format(Appender, Format) ->
    try_msg({change_format, ?DEFAULT_LOGGER, Appender, Format}).
change_format(Logger, Appender, Format) ->
    try_msg({change_format, Logger, Appender, Format}).

change_level(Appender, Level) ->
    try_msg({change_level, ?DEFAULT_LOGGER, Appender, Level}).

change_level(Logger, Appender, Level) ->
    try_msg({change_level, Logger, Appender, Level}).

change_filename(Appender, Filename) ->
    try_msg({change_filename, ?DEFAULT_LOGGER, Appender, Filename}).

change_filename(Logger, Appender, Filename) ->
    try_msg({change_filename, Logger, Appender, Filename}).

error_logger_handler() ->
    error_logger_log4erl_h:add_handler().

error_logger_handler(Args) ->
    error_logger_log4erl_h:add_handler(Args).

%% For default logger
change_log_level(Level) ->
    try_msg({change_log_level, ?DEFAULT_LOGGER, Level}).
change_log_level(Logger, Level) ->
    try_msg({change_log_level, Logger, Level}).

try_msg(Msg) ->
     try
	 handle_call(Msg)
     catch
	 exit:{noproc, _M} ->
	     io:format("log4erl has not been initialized yet. To do so, please run~n"),
	     io:format("> application:start(log4erl).~n"),
	     {error, log4erl_not_started};
	   E:M ->
	     ?LOG2("Error message received by log4erl is ~p:~p~n",[E, M]),
	     {E, M}
     end.

log(Level, Log) ->
    log(Level, Log, []).
log(Level, Log, Data) ->
    %filter_log(Level, {log, ?DEFAULT_LOGGER, Level, Log, Data}).
    log(?DEFAULT_LOGGER, Level, Log, Data).
log(Logger, Level, Log, Data) ->
    filter_log(Level, {log, Logger, Level, Log, Data}).

filter_log(Level, Msg) ->
    %case log4erl_utils:to_log(log_filter:cutoff_level(), Level) of
    case log4erl_utils:to_log(Level, log_filter:cutoff_level()) of
        true ->
            try_msg(Msg);
        false ->
	    ?LOG(">>--- Message filtered by cutoff logger~n"),
            ok
    end.

warn(Log) ->
    log(warn, Log).
%% If 1st parameter is atom, then it is Logger
warn(Logger, Log) when is_atom(Logger) ->
    log(Logger, warn, Log, []);
warn(Log, Data) ->
    log(warn, Log, Data).
warn(Logger, Log, Data) ->
    log(Logger, warn , Log, Data).

info(Log) ->
    log(info, Log).
info(Logger, Log) when is_atom(Logger) ->
    log(Logger, info, Log, []);
info(Log, Data) ->
    log(info, Log, Data).
info(Logger, Log, Data) ->
    log(Logger, info, Log, Data).

error(Log) ->
    log(error, Log).
error(Logger, Log) when is_atom(Logger) ->
    log(Logger, error, Log, []);
error(Log, Data) ->
    log(error, Log, Data).
error(Logger, Log, Data) ->
    log(Logger, error, Log, Data).

fatal(Log) ->
    log(fatal, Log).
fatal(Logger, Log) when is_atom(Logger) ->
    log(Logger, fatal, Log, []);
fatal(Log, Data) ->
    log(fatal, Log, Data).
fatal(Logger, Log, Data) ->
    log(Logger, fatal, Log, Data).

debug(Log) ->
    log(debug, Log).
debug(Logger, Log) when is_atom(Logger) ->
    log(Logger, debug, Log, []);
debug(Log, Data) ->
    log(debug, Log, Data).
debug(Logger, Log, Data) ->
    log(Logger, debug, Log, Data).

handle_call({add_logger, Logger}) ->
    log_manager:add_logger(Logger);
handle_call({add_appender, Logger, Appender, Conf}) ->
    log_manager:add_appender(Logger, Appender, Conf);
handle_call({get_appenders, Logger}) ->
    gen_event:which_handlers(Logger);
handle_call({change_level, Logger, Appender, Level}) ->
    log_manager:change_level(Logger, Appender, Level);
handle_call({change_log_level, Logger, Level}) ->
    log_manager:change_log_level(Logger, Level);
handle_call({change_format, Logger, Appender, Format}) ->
    log_manager:change_format(Logger, Appender, Format);
handle_call({change_filename, Logger, Appender, Filename}) ->
    log_manager:change_filename(Logger, Appender, Filename);
handle_call({log, Logger, Level, Log, Data}) ->
    log_manager:log(Logger, Level, Log, Data).

