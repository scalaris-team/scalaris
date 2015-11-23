% @copyright 2012-2015 Zuse Institute Berlin

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
-export([get_stats/0, get_no_of_ch/0]).

-include("gen_component.hrl").

-type state() :: {StartTime::erlang_timestamp(), RcvCnt::non_neg_integer(),
                  RcvBytes::non_neg_integer(), SendCnt::non_neg_integer(),
                  SendBytes::non_neg_integer()}.

-type message() ::
    {report_stat, RcvCnt::non_neg_integer(), RcvBytes::non_neg_integer(),
     SendCnt::non_neg_integer(), SendBytes::non_neg_integer()}.

-spec get_stats() -> {RcvCnt::non_neg_integer(), RcvBytes::non_neg_integer(),
     SendCnt::non_neg_integer(), SendBytes::non_neg_integer()}.
get_stats() ->
    comm:send_local(?MODULE, {get_stats, self()}),
    receive {get_stats_response, RcvCnt, RcvBytes, SendCnt, SendBytes} ->
                {RcvCnt, RcvBytes, SendCnt, SendBytes}
    end.

%% @doc Get number of channels for the current VM.
%%      The answer is collected synchronously, use only for debugging.
-spec get_no_of_ch() -> {no_of_channels, comm:mypid(), pos_integer()}.
get_no_of_ch() ->
    Pid = pid_groups:find_a(comm_server),
    comm:send_local(Pid, {get_no_of_ch, comm:this()}),
    receive
        ?SCALARIS_RECV({get_no_of_ch_response, CommServer, NoOfCh}, %% ->
                       {no_of_channels, CommServer, NoOfCh})
    end.


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
    { os:timestamp(), 0, 0, 0, 0}.

%% @doc message handler
-spec on(message(), State::state()) -> state().
on({report_stat, RcvCnt, RcvBytes, SendCnt, SendBytes},
   {StartTime, PrevRcvCnt, PrevRcvBytes, PrevSendCnt, PrevSendBytes})
  when RcvCnt >= 0 andalso RcvBytes >= 0 andalso
       SendCnt >= 0 andalso SendBytes >= 0 ->
    {StartTime,
     PrevRcvCnt + RcvCnt, PrevRcvBytes + RcvBytes,
     PrevSendCnt + SendCnt, PrevSendBytes + SendBytes};

on({get_stats, Pid},
   State = {_StartTime, RcvCnt, RcvBytes, SendCnt, SendBytes}) ->
    comm:send_local(Pid, {get_stats_response, RcvCnt, RcvBytes, SendCnt, SendBytes}),
    State;

on({web_debug_info, Requestor}, State = {StartTime, RcvCnt, RcvBytes, SendCnt, SendBytes}) ->
    Now = os:timestamp(),
    Runtime = timer:now_diff(Now, StartTime) / 1000000,
    {SendCntPerS, SendBytesPerS, RcvCntPerS, RcvBytesPerS} =
        if Runtime =< 0 -> {"n/a", "n/a", "n/a", "n/a"};
           true         -> {SendCnt / Runtime, SendBytes / Runtime,
                            RcvCnt / Runtime, RcvBytes / Runtime}
        end,
    KeyValueList =
        [
         {"sent_tcp_packets",
          webhelpers:safe_html_string("~p", [SendCnt])},
         {"~ sent messages/s",
          webhelpers:safe_html_string("~p", [SendCntPerS])},
         {"sent total bytes",
          webhelpers:safe_html_string("~p", [SendBytes])},
         {"~ sent bytes/s",
          webhelpers:safe_html_string("~p", [SendBytesPerS])},
         {"recv_tcp_packets",
          webhelpers:safe_html_string("~p", [RcvCnt])},
         {"~ recv messages/s",
          webhelpers:safe_html_string("~p", [RcvCntPerS])},
         {"recv total bytes",
          webhelpers:safe_html_string("~p", [RcvBytes])},
         {"~ recv bytes/s",
          webhelpers:safe_html_string("~p", [RcvBytesPerS])}
        ],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.
