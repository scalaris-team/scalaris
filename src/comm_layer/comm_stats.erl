% @copyright 2012 Zuse Institute Berlin

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

%% @doc: CommLayer: statistics gathering.
%% @author Nico Kruber <kruber@zib.de>
%% @version $Id$
-module(comm_stats).
-author('kruber@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-include("scalaris.hrl").

-export([start_link/1, init/1, on/2]).

-type state() :: {StartTime::util:time(), RcvCnt::non_neg_integer(),
                  RcvBytes::non_neg_integer(), SendCnt::non_neg_integer(),
                  SendBytes::non_neg_integer()}.

-type message() ::
    {report_stat, RcvCnt::non_neg_integer(), RcvBytes::non_neg_integer(),
     SendCnt::non_neg_integer(), SendBytes::non_neg_integer()}.

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(CommLayerGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [],
                             [ {erlang_register, ?MODULE},
                               {pid_groups_join_as, CommLayerGroup, ?MODULE}
                             ]).

%% @doc initialize: return initial state.
-spec init([]) -> state().
init([]) ->
    {erlang:now(), 0, 0, 0, 0}.

%% @doc message handler
-spec on(message(), State::state()) -> state().
on({report_stat, RcvCnt, RcvBytes, SendCnt, SendBytes},
   {StartTime, PrevRcvCnt, PrevRcvBytes, PrevSendCnt, PrevSendBytes}) ->
    {StartTime,
     PrevRcvCnt + RcvCnt, PrevRcvBytes + RcvBytes,
     PrevSendCnt + SendCnt, PrevSendBytes + SendBytes};

on({web_debug_info, Requestor}, State = {StartTime, RcvCnt, RcvBytes, SendCnt, SendBytes}) ->
    Now = erlang:now(),
    Runtime = timer:now_diff(Now, StartTime) / 1000000,
    {SendCntPerS, SendBytesPerS, RcvCntPerS, RcvBytesPerS} =
        if Runtime =< 0 -> {"n/a", "n/a", "n/a", "n/a"};
           true         -> {SendCnt / Runtime, SendBytes / Runtime,
                            RcvCnt / Runtime, RcvBytes / Runtime}
        end,
    KeyValueList =
        [
         {"sent_tcp_packets",
          lists:flatten(io_lib:format("~p", [SendCnt]))},
         {"~ sent messages/s",
          lists:flatten(io_lib:format("~p", [SendCntPerS]))},
         {"sent total bytes",
          lists:flatten(io_lib:format("~p", [SendBytes]))},
         {"~ sent bytes/s",
          lists:flatten(io_lib:format("~p", [SendBytesPerS]))},
         {"recv_tcp_packets",
          lists:flatten(io_lib:format("~p", [RcvCnt]))},
         {"~ recv messages/s",
          lists:flatten(io_lib:format("~p", [RcvCntPerS]))},
         {"recv total bytes",
          lists:flatten(io_lib:format("~p", [RcvBytes]))},
         {"~ recv bytes/s",
          lists:flatten(io_lib:format("~p", [RcvBytesPerS]))}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.
