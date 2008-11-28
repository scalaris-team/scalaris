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
%%% File    : log.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : log service
%%%
%%% Created :  4 Apr 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(log).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([log/2, log2file/2]).

log2file(Channel, Message) ->
    case file:open(config:log_log_file(), [append]) of
	{ok, FD} ->
	    io:format(FD, "[ ~w | ~w] ~s~n", [calendar:universal_time(), Channel, Message]),
	    file:close(FD);
	{error, Reason} ->
	    io:format("couldn't open ~w: ~w~n", [config:log_log_file(), Reason]),
	    {error, Reason}
    end.
	    
    
log({ok, FD}, Message) ->
    io:format(FD, "~p~n", [Message]),
    file:close(FD);
log({error, Reason}, _) ->
    io:format("~p~n", [Reason]);
log(Channel, Message) ->
    io:format("~s: ~s~n", [Channel, Message]).
    %log(file:open(io_lib:format("../log/~s.log", [Channel]), append), Message).

