-module(log_manager).

-author("Ahmed Al-Issaei").
-license("MPL-1.1").

-include("../include/log4erl.hrl").

%% API
-export([start_link/1]).
-export([change_log_level/2]).
-export([change_level/3]).
-export([change_filename/3]).
-export([add_logger/1]).
-export([add_appender/3]).
-export([change_format/3]).
-export([log/4]).
%% Don't want compile-warnings about the use of erlang:now/0 in
%% this module.
-compile(nowarn_deprecated_function).

start_link(Logger) when is_atom(Logger) ->
    ?LOG2("log_manager adding Logger ~p~n",[Logger]),
    gen_event:start_link({local, Logger});
start_link(Logger) when is_list(Logger) ->
    ?LOG2("log_manager adding Logger ~p~n",[Logger]),    
    gen_event:start_link({local, list_to_atom(Logger)}).

add_logger(Logger) ->
    log4erl_sup:add_logger(Logger).

add_appender(Logger, {Appender, Name} , Conf) ->
    ?LOG2("add_appender ~p with name ~p to ~p with Conf ~p ~n",[Appender, Name, Logger, Conf]),
    log4erl_sup:add_guard(Logger, Appender, Name, Conf).
    
change_log_level(Logger, Level) ->
    notify_logger(Logger, {change_level, Level}).

change_level(Logger, Appender, Level) ->
    call_appender(Logger, Appender, {change_level, Level}).

change_format(Logger, Appender, Format) ->
    call_appender(Logger, Appender, {change_format, Format}).

change_filename(Logger, Appender, Filename) ->
    call_appender(Logger, Appender, {change_filename, Filename}).

%%--------------------------------------------------------------------
%% Logger API functions
%%--------------------------------------------------------------------
log(Logger, Level, Log, Data) ->
    Now = {_, _, Ms} = timestamp(),
    T = calendar:now_to_local_time(Now),
    ?LOG2("Logging:~n ~p ~p ~p ~p~n",[Logger, Level, Log, Data]),
    LL = #log{level=Level, msg=Log, data=Data, time=T, millis = Ms div 1000},
    notify_logger(Logger, {log, LL}).

notify_logger(Logger, Msg) ->
    try
	gen_event:sync_notify(Logger, Msg)
    catch
	exit:noproc ->
	    {error, no_such_logger}
    end.

call_appender(Logger, Appender, Msg) ->
    try
	Apps = gen_event:which_handlers(Logger),
	?LOG2("log_manager:call_appender/3 get apps ~p~n",[Apps]),
	case lists:filter(fun({_,X}) -> X =:= Appender end, Apps) of
	    [] ->
		{error, no_appender_found};
	    [Apps1] ->
		?LOG2("get apps: ~p~n",[Apps1]),
		gen_event:call(Logger, Apps1, Msg)
	end
    catch
	exit:noproc ->
	    {error, no_such_logger}
    end.
    
    
timestamp() ->
    erlang:now().
