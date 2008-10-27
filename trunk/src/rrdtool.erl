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
%%% File    : rrdtool.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : rrdtool interface
%%%
%%% Created :  15 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: rrdtool.erl 463 2008-05-05 11:14:22Z schuett $
-module(rrdtool).

-author('schuett@zib.de').
-vsn('$Id: rrdtool.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([update/2, graph/4, graph2/3]).

update(File, Value) ->
    Command = io_lib:format('rrdtool update ../data/~s N:~s', [File, Value]),
    logged_exec(Command).

graph(PNGFile, TimeSpan, DataSource, Key) ->
    Command = io_lib:format("rrdtool graph ~w/graphs/~s --units-exponent 0 --start now-~ps DEF:pkt=../data/~s:AVERAGE LINE1:pkt#ff0000:'~s'",
			    [config:docroot(), PNGFile, TimeSpan, DataSource, Key]),
    logged_exec(Command).

%    rrdtool:graph("messages_get_middle_key_response_traffic_1h.png",3600, "messages_get_middle_key_response_traffic.rrd:traffic",  "GetMiddleKeyResponse Bytes/Node/s"),
%    rrdtool:graph("messages_get_middle_key_response_cnt_1h.png",    3600, "messages_get_middle_key_response_cnt.rrd:count",  "GetMiddleKeyResponse Messages/Node/s"),

graph2(Message, MessageText, Intervals) ->
    lists:foreach(fun({Text, Interval}) ->
			  PNGFileTraffic    = io_lib:format("messages_~s_traffic_~s.png", [Message, Text]),
			  PNGFileCount      = io_lib:format("messages_~s_cnt_~s.png", [Message, Text]),
			  DataSourceTraffic = io_lib:format("messages_~s_traffic.rrd:traffic", [Message]),
			  DataSourceCount   = io_lib:format("messages_~s_cnt.rrd:count", [Message]),
			  graph(PNGFileTraffic, Interval, DataSourceTraffic, io_lib:format("~s Bytes/Node/s", [MessageText])),
			  graph(PNGFileCount, Interval, DataSourceCount, io_lib:format("~s Messages/Node/s", [MessageText]))
		  end, Intervals).

logged_exec(Command) ->
    _Output = os:cmd(Command).
%    {ok, Log} = file:open(config:rrd_log_file(), [append]),
%    OutputLength = util:lengthX(Output),
%    if
%	OutputLength > 10 ->
%	    io:format(Log, "~s~n", [Command]),
%	    io:format(Log, "~s~n", [Output]);
%	true ->
%	    io:format(Log, "~s~n", [Command]),
%	    ok
%    end,
%    file:close(Log).
