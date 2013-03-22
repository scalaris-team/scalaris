%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%% 
%% The Original Code is available on https://code.google.com/p/log4erl/ 
%% file: console_appender.erl.
%% 
%% The Initial Developer of the Original Code is Ahmed Nawras.

%% @author Ahmed Nawras
%% @author Nico Kruber <kruber@zib.de>
%% @doc Log4erl appender using log:pal/1 instead if io:format/1 based on
%%      console_appender.
%% @version $Id$
-module(log4erl_ctpal_appender).
-author('Ahmed Nawras').
-author('kruber@zib.de').
-vsn('$Id$').

-include("log4erl.hrl").

-behaviour(gen_event).
%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2, 
	 handle_info/2, terminate/2, code_change/3]).

-spec init({conf, Conf::[term()]} | {log:log_level()} | {log:log_level(), string()}) -> {ok, #console_appender{}}.
init({conf, Conf}) when is_list(Conf) ->
    CL = lists:foldl(fun(X, List) ->
			     [proplists:get_value(X,Conf)|List]
		     end,
		     [],
		     [level, format]),
    
    %% in case format doesn't exist
    Res = case hd(CL) of
	      undefined ->
		  [_|CL2] = CL,
		  lists:reverse(CL2);
	      _ ->
		  lists:reverse(CL)
	  end,
    init(list_to_tuple(Res));
init({Level}) ->
    init({Level, ?DEFAULT_FORMAT});
init({Level, Format} = _Args) ->
    ?LOG2("Initializing console_appender with args =  ~p~n",[_Args]),
    {ok, Toks} = log_formatter:parse(Format),
    ?LOG2("Tokens received is ~p~n",[Toks]),
    State = #console_appender{level = Level, format = Toks},
    ?LOG2("State is ~p~n",[State]),
    {ok, State}.

-spec handle_event({change_level, Level::log:log_level()} |
                   {log, #log{}}, State::#console_appender{})
        -> {ok, #console_appender{}}.
handle_event({change_level, Level}, State) ->
    State2 = State#console_appender{level = Level},
    ?LOG2("Changed level to ~p~n",[Level]),
    {ok, State2};
handle_event({log,LLog}, State) ->
    ?LOG2("handl_event:log = ~p~n",[LLog]),
    do_log(LLog, State),
    {ok, State}.

-spec handle_call({change_format, Format::string()} | 
                  {change_level, Level::log:log_level()} |
                  term(), State::#console_appender{})
        -> {ok, ok, #console_appender{}}.
handle_call({change_format, Format}, State) ->
    ?LOG2("Old State in console_appender is ~p~n",[State]),
    {ok, Tokens} = log_formatter:parse(Format),
    ?LOG2("Adding format of ~p~n",[Tokens]),
    State1 = State#console_appender{format=Tokens},
    {ok, ok, State1};
handle_call({change_level, Level}, State) ->
    State2 = State#console_appender{level = Level},
    ?LOG2("Changed level to ~p~n",[Level]),
    {ok, ok, State2};
handle_call(_Request, State) ->
    Reply = ok,
    ?LOG2("Received unknown request ~p~n", [_Request]),
    {ok, Reply, State}.

-spec handle_info(_Info::any(), State::#console_appender{}) -> {ok, #console_appender{}}.
handle_info(_Info, State) ->
    {ok, State}.

-spec terminate(Reason::any(), State::#console_appender{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(OldVsn::any(), State::#console_appender{}, Extra::any()) -> {ok, #console_appender{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec do_log(Log::#log{}, State::#console_appender{}) -> ok.
do_log(#log{level = L} = Log,#console_appender{level=Level, format=Format}) ->
    ToLog = log4erl_utils:to_log(L, Level),
    case ToLog of
	true ->
	    M = log_formatter:format(Log, Format),
	    ?LOG2("console_appender result message is ~s~n",[M]),
	    ct:pal(M);
	false ->
	    ok
    end.
