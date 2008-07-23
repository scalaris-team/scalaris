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
%%% File    : cs_message.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Message Sending and Statistics Logging
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_message.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_message).

-author('schuett@zib.de').
-vsn('$Id: cs_message.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([start/1, start_link/1, dumpMessageStatistics/1, get_details/0]).

%% @type message_log() = {state, gb_trees:gb_tree()}. the message statistics
-record(message_log, {last_reset, last_dump, message_count, message_size}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public Interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_pid() ->
    process_dictionary:lookup_process(erlang:get(instance_id), cs_message).

get_details() ->
    get_pid() ! {get_details, self()},
    receive
	{get_details_response, Details} ->
	    Details
    end.

dumpMessageStatistics(Ring) ->
    TotalMessagesCount =        lists:foldl(fun total_messages_count/2, 0, Ring),
    TotalMessagesSize =         lists:foldl(fun total_messages_size/2, 0, Ring),
    PingMessagesCount =         lists:foldl(fun (X, Acc) -> message_count(ping, X) + Acc end, 0, Ring),
    PingMessagesSize =          lists:foldl(fun (X, Acc) -> message_size(ping, X) + Acc end, 0, Ring),
    PongMessagesCount =         lists:foldl(fun (X, Acc) -> message_count(pong, X) + Acc end, 0, Ring),
    PongMessagesSize =          lists:foldl(fun (X, Acc) -> message_size(pong, X) + Acc end, 0, Ring),
    GetPredMessagesCount =      lists:foldl(fun (X, Acc) -> message_count(get_pred, X) + Acc end, 0, Ring),
    GetPredMessagesSize =       lists:foldl(fun (X, Acc) -> message_size(get_pred, X) + Acc end, 0, Ring),
    GetSuccListMessagesCount =  lists:foldl(fun (X, Acc) -> message_count(get_succ_list, X) + Acc end, 0, Ring),
    GetSuccListMessagesSize =   lists:foldl(fun (X, Acc) -> message_size(get_succ_list, X) + Acc end, 0, Ring),
    GetLoadMessagesCount =      lists:foldl(fun (X, Acc) -> message_count(get_load, X) + Acc end, 0, Ring),
    GetLoadMessagesSize =       lists:foldl(fun (X, Acc) -> message_size(get_load, X) + Acc end, 0, Ring),
    DropDataCount =             lists:foldl(fun (X, Acc) -> message_count(drop_data, X) + Acc end, 0, Ring),
    DropDataSize =              lists:foldl(fun (X, Acc) -> message_count(drop_data, X) + Acc end, 0, Ring),

    NotifyCount =             lists:foldl(fun (X, Acc) -> message_count(notify, X) + Acc end, 0, Ring),
    NotifySize =              lists:foldl(fun (X, Acc) -> message_count(notify, X) + Acc end, 0, Ring),
    GetSuccListResponseCount =  lists:foldl(fun (X, Acc) -> message_count(get_succ_list_response, X) + Acc end, 0, Ring),
    GetSuccListResponseSize =   lists:foldl(fun (X, Acc) -> message_size(get_succ_list_response, X) + Acc end, 0, Ring),
    GetLoadResponseCount =      lists:foldl(fun (X, Acc) -> message_count(get_load_response, X) + Acc end, 0, Ring),
    GetLoadResponseSize =       lists:foldl(fun (X, Acc) -> message_size(get_load_response, X) + Acc end, 0, Ring),
    GetMiddleKeyCount =      lists:foldl(fun (X, Acc) -> message_count(get_middle_key, X) + Acc end, 0, Ring),
    GetMiddleKeySize =       lists:foldl(fun (X, Acc) -> message_size(get_middle_key, X) + Acc end, 0, Ring),
    GetMiddleKeyResponseCount =      lists:foldl(fun (X, Acc) -> message_count(get_middle_key_response, X) + Acc end, 0, Ring),
    GetMiddleKeyResponseSize =       lists:foldl(fun (X, Acc) -> message_size(get_middle_key_response, X) + Acc end, 0, Ring),

    rrdtool:update("total_messages_count.rrd",            mytrunc(TotalMessagesCount / length(Ring))),
    rrdtool:update("total_messages_size.rrd",             mytrunc(TotalMessagesSize / length(Ring))),
    rrdtool:update("messages_ping_cnt.rrd",               mytrunc(PingMessagesCount / length(Ring))),
    rrdtool:update("messages_ping_traffic.rrd",           mytrunc(PingMessagesSize / length(Ring))),
    rrdtool:update("messages_pong_cnt.rrd",               mytrunc(PongMessagesCount / length(Ring))),
    rrdtool:update("messages_pong_traffic.rrd",           mytrunc(PongMessagesSize / length(Ring))),
    rrdtool:update("messages_get_pred_cnt.rrd",           mytrunc(GetPredMessagesCount / length(Ring))),
    rrdtool:update("messages_get_pred_traffic.rrd",       mytrunc(GetPredMessagesSize / length(Ring))),
    rrdtool:update("messages_get_succ_list_cnt.rrd",      mytrunc(GetSuccListMessagesCount / length(Ring))),
    rrdtool:update("messages_get_succ_list_traffic.rrd",  mytrunc(GetSuccListMessagesSize / length(Ring))),
    rrdtool:update("messages_get_load_cnt.rrd",           mytrunc(GetLoadMessagesCount / length(Ring))),
    rrdtool:update("messages_get_load_traffic.rrd",       mytrunc(GetLoadMessagesSize / length(Ring))),
    rrdtool:update("messages_drop_data_cnt.rrd",          mytrunc(DropDataCount / length(Ring))),
    rrdtool:update("messages_drop_data_traffic.rrd",      mytrunc(DropDataSize / length(Ring))),
    rrdtool:update("messages_notify_cnt.rrd",          mytrunc(NotifyCount / length(Ring))),
    rrdtool:update("messages_notify_traffic.rrd",      mytrunc(NotifySize / length(Ring))),
    rrdtool:update("messages_get_succ_list_response_cnt.rrd",          mytrunc(GetSuccListResponseCount / length(Ring))),
    rrdtool:update("messages_get_succ_list_response_traffic.rrd",      mytrunc(GetSuccListResponseSize / length(Ring))),
    rrdtool:update("messages_get_load_response_cnt.rrd",          mytrunc(GetLoadResponseCount / length(Ring))),
    rrdtool:update("messages_get_load_response_traffic.rrd",      mytrunc(GetLoadResponseSize / length(Ring))),
    rrdtool:update("messages_get_middle_key_response_cnt.rrd",          mytrunc(GetMiddleKeyResponseCount / length(Ring))),
    rrdtool:update("messages_get_middle_key_response_traffic.rrd",      mytrunc(GetMiddleKeyResponseSize / length(Ring))),
    rrdtool:update("messages_get_middle_key_cnt.rrd",          mytrunc(GetMiddleKeyCount / length(Ring))),
    rrdtool:update("messages_get_middle_key_traffic.rrd",      mytrunc(GetMiddleKeySize / length(Ring))),

    Intervals = [{"1h", 3600}, {"1d", 24*3600}, {"1w", 7*24*3600}, {"1m", 30*24*3600}],
    rrdtool:graph2("ping", "Ping", Intervals),
    rrdtool:graph2("pong", "Pong", Intervals),
    rrdtool:graph2("get_pred", "GetPred", Intervals),
    rrdtool:graph2("get_succ_list", "GetSuccList", Intervals),
    rrdtool:graph2("get_succ_list_response", "GetSuccListResponse", Intervals),
    rrdtool:graph2("get_load", "GetLoad", Intervals),
    rrdtool:graph2("get_load_response", "GetLoadResponse", Intervals),
    rrdtool:graph2("drop_data", "DropData", Intervals),
    rrdtool:graph2("notify", "Notify", Intervals),
    rrdtool:graph2("get_middle_key", "GetMiddleKey", Intervals),
    rrdtool:graph2("get_middle_key_response", "GetMiddleKeyResponse", Intervals),

    rrdtool:graph("total_messages_count_1h.png",                    3600,     "total_messages_count.rrd:total_messages_cnt",     "Messages/Node/s"),
    rrdtool:graph("total_messages_size_1h.png",                     3600,     "total_messages_size.rrd:total_messages_size",     "Byte/Node/s"),
    rrdtool:graph("total_messages_count_1d.png",                    86400,    "total_messages_count.rrd:total_messages_cnt",     "Messages/Node/s"),
    rrdtool:graph("total_messages_size_1d.png",                     86400,    "total_messages_size.rrd:total_messages_size",     "Byte/Node/s"),
    rrdtool:graph("total_messages_count_1w.png",                    7*86400,  "total_messages_count.rrd:total_messages_cnt",     "Messages/Node/s"),
    rrdtool:graph("total_messages_size_1w.png",                     7*86400,  "total_messages_size.rrd:total_messages_size",     "Byte/Node/s"),
    rrdtool:graph("total_messages_count_1m.png",                    30*86400, "total_messages_count.rrd:total_messages_cnt",     "Messages/Node/s"),
    rrdtool:graph("total_messages_size_1m.png",                     30*86400, "total_messages_size.rrd:total_messages_size",     "Byte/Node/s"),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Implementation Analysis
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

total_messages_count(MessageLog, Acc) ->
    total_messages_count(MessageLog) + Acc.

total_messages_count(#message_log{last_reset = LastReset, last_dump = LastDump, message_count = MessageCount}) ->
    TotalMessages = lists:foldl(fun (Val, Acc) ->
					Val + Acc
				end, 0, gb_trees:values(MessageCount)),
    TotalMessages / timer:now_diff(LastDump, LastReset) * 1000000.

total_messages_size(MessageLog, Acc) ->
    total_messages_size(MessageLog) + Acc.

total_messages_size(#message_log{last_reset = LastReset, last_dump = LastDump, message_size = MessageSize}) ->
    TotalMessages = lists:foldl(fun (Val, Acc) ->
			Val + Acc
		end, 0, gb_trees:values(MessageSize)),
    TotalMessages / timer:now_diff(LastDump, LastReset) * 1000000.

message_count(Tag, #message_log{last_reset = LastReset, last_dump = LastDump, message_count = MessageCount}) ->
    IsDefined = gb_trees:is_defined(Tag, MessageCount),
    if
	IsDefined ->
	    gb_trees:get(Tag, MessageCount) / timer:now_diff(LastDump, LastReset) * 1000000;
	true ->
	    0
    end.

message_size(Tag, #message_log{last_reset = LastReset, last_dump = LastDump, message_size = MessageSize}) ->
    IsDefined = gb_trees:is_defined(Tag, MessageSize),
    if
	IsDefined ->
	    gb_trees:get(Tag, MessageSize) / timer:now_diff(LastDump, LastReset) * 1000000;
	true ->
	    0
    end.

mytrunc(Float) ->
    io_lib:format("~f", [Float]).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Implementation Server
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
new(LastReset, MessageCount, MessageSize) ->
    #message_log{
     last_reset = LastReset,
     last_dump = now(),
     message_count = MessageCount,
     message_size = MessageSize
    }.


inc(Tag, Count, Map) ->
    IsDefined = gb_trees:is_defined(Tag, Map),
    if
	IsDefined ->
	    OldValue = gb_trees:get(Tag, Map),
	    gb_trees:update(Tag, OldValue + Count, Map);
	true ->
	    gb_trees:enter(Tag, Count, Map)
    end.

loop(LastReset, MessageCount, MessageSize) ->
    receive 
	{get_details, PID} ->
	    PID ! {get_details_response, new(LastReset, MessageCount, MessageSize)},
	    loop(now(), gb_trees:empty(), gb_trees:empty());
	{log_message, Tag, Size} ->
	    loop(LastReset, inc(Tag, 1, MessageCount), inc(Tag, Size, MessageSize))
    end.

%% @doc starts the loop
%% @spec start(term()) -> term()
start(InstanceId) ->
    process_dictionary:register_process(InstanceId, cs_message, self()),
    loop(now(), gb_trees:empty(), gb_trees:empty()).

%% @doc starts the message statistics collector
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    {ok, spawn_link(?MODULE, start, [InstanceId])}.
