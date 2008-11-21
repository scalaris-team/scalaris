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
%%% File    : boot_logger.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : distributed logger
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
-module(boot_logger).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([start_link/1, start/1, log/1, log_to_file/1, log_assert/2, transaction_log/1]).

%logging on
-define(LOG(S, L), io:format(S, L)).
%logging off
%-define(LOG(S, L), ok).

log_to_file(Message) ->
    Hostname = net_adm:localhost(),
    cs_send:send(config:logPid(), {log_to_file, Hostname, Message}).

log_assert(Debug, Message) ->
    Hostname = net_adm:localhost(),
    cs_send:send(config:logPid(), {log_assert, Hostname, Debug, Message}).
    
log(Message) ->
    Hostname = net_adm:localhost(),
    cs_send:send(config:logPid(), {log, Hostname, Message}).

transaction_log(Message)->
    Hostname = net_adm:localhost(),
    cs_send:send(config:logPid(), {transaction_log, Hostname, Message}).


loop(ErrorLog, DebugLog, TransactionLog) ->
    receive
	{log, Hostname, Message} ->
	    ?LOG("[~30p]~30s: ~s~n", [calendar:universal_time(), Hostname, Message]);
	{log_to_file, Hostname, Message} ->
	    ?LOG("[~30p]~30s: ~s~n", [calendar:universal_time(), Hostname, Message]),
	    io:format(ErrorLog, "[~30p]~30s: ~s~n", [calendar:universal_time(), Hostname, Message]);
	{log_assert, Hostname, Debug, Message} ->
	    cs_debug:dump(DebugLog, Hostname, Debug, Message);
	 {transaction_log, Hostname, Message} ->
	    ?LOG("[~30p]~30s: ~s~n", [calendar:universal_time(), Hostname, Message]),
	    io:format(TransactionLog, "[~30p]~30s: ~s~n", [calendar:universal_time(), Hostname, Message])
    end,
    loop(ErrorLog, DebugLog, TransactionLog).

start(InstanceId) ->
    process_dictionary:register_process(InstanceId, boot_logger, self()),
    register(boot_logger, self()),
    {ok, ErrorLog} = file:open(config:error_log_file(), [append]),
    {ok, DebugLog} = file:open(config:debug_log_file(), [append]),
    {ok, TransactionLog} = file:open(config:transaction_log_file(), [append]),
    loop(ErrorLog, DebugLog, TransactionLog).

start_link(InstanceId) ->
    {ok, spawn_link(?MODULE, start, [InstanceId])}.
