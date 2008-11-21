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
%%% File    : cs_debug.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Debug module
%%%
%%% Created : 21 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(cs_debug).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([new/0, debug/3, dump/4]).

new() ->
    [].

debug(Debug, NewState, Message) ->
    NewDebug = util:trunc([ {calendar:universal_time(), NewState, Message} | Debug], config:debugQueueLength()),
    check(NewDebug, assert(NewState, checks())),
    NewDebug.
	    
check(_, ok) ->
    ok;
check(NewDebug, {fail, Message}) ->
    boot_logger:log_assert(NewDebug, Message).

assert(State, [Check | Rest]) ->
    assert(State, Rest, Check(State));
assert(_, []) ->
    ok.
    
assert(State, Rest, ok) ->
    assert(State, Rest);
assert(_, _, {fail, Message}) ->
    {fail, Message}.


checks() ->
    [
     fun cs_state:assert/1
    ].


dump(FD, Hostname, Debug, Message) ->
    io:format(FD, "================================================~n", []),
    io:format(FD, "assert failed on ~s~n", [Hostname]),
    io:format(FD, "failure message: ~s~n", [Message]),
    io:format(FD, "message trace: ~n", []),
    io:format(FD, "~p ~n", [Debug]),
    %io:format(FD, "~p ~n", [lists:foldl(fun(X, Sum) -> string:concat(dump(X), Sum) end, "", Debug)]),
    ok.

% dump({Time, State, Message}) ->
%     io_lib:format("~p~n~p~n~p", [Time, State, Message]).
